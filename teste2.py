# local_audit_parser.py
# Parser local de relatórios (JSON) -> Parquet particionado
# Regra v3 da capa:
#  - Remove/ignora linha(s) de confidencialidade
#  - Título = linhas entre a confidencialidade e o primeiro 'Tipo de Relatório'
#  - Tipo = canônico (Final/Preliminar/Acompanhamento/Follow-up). Se não achar, default 'Relatório Final'
#  - Empresa = primeira linha após o tipo (até AUD)
#  - AUD aceita 'AUD - 12345' e normaliza para 'AUD-12345'

import argparse
import json
import re
import sys
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ----------------------------
# Regex / helpers
# ----------------------------
AUD_RE_SPACES = re.compile(r"\bAUD\s*-\s*(\d{5,})\b", re.IGNORECASE)
DATE_RE = re.compile(r"\b(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d{2,4})\b")
WS_RE = re.compile(r"[ \t]+")

CLASS_OPTIONS = {
    "SATISFATÓRIO": "Satisfatório",
    "SATISFATORIO": "Satisfatório",
    "SATISFATÓRIO COM MELHORIAS": "Satisfatório com melhorias",
    "SATISFATORIO COM MELHORIAS": "Satisfatório com melhorias",
    "A MELHORAR": "A melhorar",
    "À MELHORAR": "A melhorar",
    "INSATISFATÓRIO": "Insatisfatório",
    "INSATISFATORIO": "Insatisfatório",
}

RISK_OPTIONS = ["BAIXO", "MÉDIO", "MEDIO", "ALTO", "CRÍTICO", "CRITICO", "OPORTUNIDADE DE MELHORIA"]

REPORT_TYPE_CANON = {
    "relatorio final": "Relatório Final",
    "relatorio preliminar": "Relatório Preliminar",
    "relatorio de acompanhamento": "Relatório de Acompanhamento",
    "relatorio de follow-up": "Relatório de Follow-up",
}

def normalize_text(t: str) -> str:
    if not t:
        return ""
    t = t.replace("\u00ad", "")  # soft hyphen
    t = t.replace("\r", "\n")
    t = WS_RE.sub(" ", t)
    t = re.sub(r"\n{3,}", "\n\n", t).strip()
    return t

def _strip_accents_lower(s: str) -> str:
    if not s:
        return ""
    nfkd = unicodedata.normalize("NFD", s)
    s2 = "".join(ch for ch in nfkd if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", s2).strip().lower()

def try_parse_date_any(s: str) -> Optional[str]:
    m = DATE_RE.search(s or "")
    if not m:
        return None
    raw = m.group(1).replace(".", "/").replace("-", "/")
    parts = raw.split("/")
    if len(parts) != 3:
        return None
    d, mo, y = parts
    if len(y) == 2:
        y = "20" + y
    try:
        dt = datetime(int(y), int(mo), int(d))
        return dt.strftime("%Y-%m-%d")
    except Exception:
        return None

def deep_collect_strings(obj: Any, out: List[str]) -> None:
    if obj is None:
        return
    if isinstance(obj, str):
        out.append(obj); return
    if isinstance(obj, list):
        for it in obj: deep_collect_strings(it, out); return
    if isinstance(obj, dict):
        for v in obj.values(): deep_collect_strings(v, out); return

def extract_pages_or_fallback(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    pages = doc.get("pages")
    if isinstance(pages, list) and pages and isinstance(pages[0], dict) and ("text" in pages[0]):
        return [{"page": p.get("page"), "text": normalize_text(p.get("text", ""))} for p in pages]
    strings: List[str] = []
    deep_collect_strings(doc, strings)
    big = normalize_text("\n".join(strings))
    return [{"page": None, "text": big}]

def find_aud_code(all_text: str) -> Optional[str]:
    m = AUD_RE_SPACES.search(all_text or "")
    return f"AUD-{m.group(1)}" if m else None

def pick_cover_text(pages: List[Dict[str, Any]]) -> str:
    if pages:
        return (pages[0].get("text") or "")[:8000]
    return ""

# ----------------------------
# CAPA - Regra v3
# ----------------------------
def extract_title_and_cover_fields(cover_text: str) -> Dict[str, Optional[str]]:
    raw_lines = [ln.strip() for ln in (cover_text or "").split("\n") if ln.strip()]
    norms = [_strip_accents_lower(x) for x in raw_lines]

    # Remove números de página
    filtered: List[Tuple[str, str]] = []
    for r, n in zip(raw_lines, norms):
        if re.fullmatch(r"\d+", n):
            continue
        filtered.append((r, n))

    # Função para identificar linha(s) de confidencialidade
    def is_conf_line(n: str) -> bool:
        return n.startswith("este relatorio") and ("confidencial" in n or "exclusiv" in n)

    # Remove cabeçalhos frequentes e qualquer linha confidencial do universo analisado
    skip_set = {"auditoria interna", "confidential"}
    filtered2 = [(r, n) for r, n in filtered if n not in skip_set and not is_conf_line(n)]

    # Identificar Tipo de Relatório (primeira ocorrência canônica)
    report_type = None
    rt_idx = None
    for j, (r, n) in enumerate(filtered2):
        canon = REPORT_TYPE_CANON.get(n)
        if canon:
            report_type = canon
            rt_idx = j
            break

    def looks_like_aud(r: str) -> bool:
        return bool(AUD_RE_SPACES.search(r))
    def looks_like_date(n: str) -> bool:
        return bool(DATE_RE.search(n))

    title = None
    company = None

    if rt_idx is not None:
        # Título = todas as linhas ANTES do tipo, ignorando data/AUD
        title_lines: List[str] = []
        for r, n in filtered2[:rt_idx]:
            if looks_like_aud(r) or looks_like_date(n):
                continue
            title_lines.append(r)

        # Unir preservando continuação com '–' ou '-'
        merged: List[str] = []
        for ln in title_lines:
            if merged and merged[-1].rstrip().endswith(("–", "-")):
                prev = merged[-1].rstrip(" –-").rstrip()
                curr = ln.lstrip(" –-").lstrip()
                merged[-1] = f"{prev} – {curr}"
            else:
                merged.append(ln)
        title = " ".join(merged) if merged else None

        # Empresa = primeira linha após o tipo, até AUD
        for r, n in filtered2[rt_idx+1:]:
            if looks_like_aud(r):
                break
            if n:
                company = r
                break
    else:
        # Sem tipo detectado -> default 'Relatório Final'
        report_type = "Relatório Final"
        # Buscar título após a última linha de confidencialidade na lista original
        last_conf = -1
        for i, (r, n) in enumerate(filtered):
            if is_conf_line(n):
                last_conf = i
        after = filtered[last_conf+1:]

        title_lines: List[str] = []
        for r, n in after:
            if REPORT_TYPE_CANON.get(n):
                break
            if looks_like_aud(r) or looks_like_date(n):
                continue
            if "neoenergia" in n or n.startswith("grupo "):
                break
            title_lines.append(r)

        merged: List[str] = []
        for ln in title_lines:
            if merged and merged[-1].rstrip().endswith(("–", "-")):
                prev = merged[-1].rstrip(" –-").rstrip()
                curr = ln.lstrip(" –-").lstrip()
                merged[-1] = f"{prev} – {curr}"
            else:
                merged.append(ln)
        title = " ".join(merged) if merged else None

    emission_date = try_parse_date_any(cover_text)
    return {"title": title, "report_type": report_type, "company": company, "emission_date": emission_date}

# ----------------------------
# Seções do corpo (v3 robusta)
# ----------------------------

def _canon_heading(s: str) -> str:
    """Normaliza linha para comparação de cabeçalho: sem acentos, minúsculo, compactando espaços."""
    s = _strip_accents_lower(s)
    s = s.replace("–", "-")
    s = re.sub(r"[^\w: ]+", "", s)  # remove pontuação (inclui vírgula, ponto etc.)
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _match_heading(norm_line: str, variant: str) -> bool:
    v = _canon_heading(variant)

    # match exato (com/sem :)
    if norm_line == v or norm_line == v + ":":
        return True

    # aceita versão sem espaços (c r o n o g r a m a)
    if norm_line.replace(" ", "") in {v.replace(" ", ""), v.replace(" ", "") + ":"}:
        return True

    # ✅ NOVO: aceita quando o título vem com texto na mesma linha:
    # "Risco / Continuidade...", "Risco: ...", "Risco - ..."
    if norm_line.startswith(v + " ") or norm_line.startswith(v + ":") or norm_line.startswith(v + "-"):
        return True

    # ✅ NOVO: aceita prefixos numéricos tipo "2 risco", "02 risco", "2. risco"
    # (canon_heading remove pontuação, então "2. Risco" vira "2 risco")
    if re.match(rf"^\d+\s+{re.escape(v)}(\s|:|$)", norm_line):
        return True

    return False

# ---- CORREÇÃO: evitar falso positivo (ex.: "riscos," no índice quebrado em linhas)
def _is_heading_candidate(raw_line: str) -> bool:
    s = (raw_line or "").strip()
    if not s:
        return False

    # se termina como frase, provavelmente não é cabeçalho
    if s.endswith((",", ";", ".", ")", "…")):
        return False

    # precisa começar com maiúscula ou estar em caixa alta
    if not (s[0].isupper() or s.isupper()):
        return False

    # ✅ NOVO: cabeçalho "puro" costuma ser curto (evita cortar quando aparece "Escopo ..." no meio)
    # (ex.: "Escopo" / "Alcance" / "Cronograma" / "Risco", etc.)
    if len(s) > 28:
        return False

    # ✅ NOVO: se tiver muitos tokens, é frase/parágrafo, não cabeçalho
    if len(s.split()) > 4:
        return False

    return True

def _find_heading_indices(text: str, start_variants: List[str], end_variants: List[str]) -> Optional[Tuple[int, int]]:
    """
    Retorna (i_start_line, i_end_line_exclusive) usando comparação linha-a-linha
    com tolerância a cabeçalhos espaçados e ':' opcional.

    CORREÇÃO: só considera linha como cabeçalho se passar em _is_heading_candidate().
    """
    lines = (text or "").split("\n")
    norms = [_canon_heading(ln) for ln in lines]

    # localizar start
    start_i = None
    for i, (raw, n) in enumerate(zip(lines, norms)):
        if not _is_heading_candidate(raw):
            continue
        for sv in start_variants:
            if _match_heading(n, sv):
                start_i = i
                break
        if start_i is not None:
            break
    if start_i is None:
        return None

    # localizar próximo end
    end_i = len(lines)
    for j in range(start_i + 1, len(lines)):
        raw = lines[j]
        n = norms[j]
        if not _is_heading_candidate(raw):
            continue
        for ev in end_variants:
            if _match_heading(n, ev):
                end_i = j
                break
        if end_i != len(lines):
            break

    return start_i, end_i

def find_heading_block(text: str, start_variants: List[str], end_variants: List[str]) -> Optional[str]:
    idx = _find_heading_indices(text, start_variants, end_variants)
    if not idx:
        return None

    start_i, end_i = idx
    lines = (text or "").split("\n")

    first_line = lines[start_i]

    # 👉 pega conteúdo que vem na MESMA linha após :
    same_line_content = None
    if ":" in first_line:
        same_line_content = first_line.split(":", 1)[1].strip()

    # 👉 pega linhas seguintes até próximo cabeçalho
    following = lines[start_i + 1 : end_i]

    parts = []
    if same_line_content:
        parts.append(same_line_content)

    if following:
        parts.append("\n".join(following))

    block = "\n".join(parts)

    block = re.sub(r"[ \t]+", " ", block)
    block = re.sub(r"\n{3,}", "\n\n", block)

    block = block.strip(":- \n\t")

    return block or None

def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:
    """
    v3 robusta:
    - Objetivo / Risco / Escopo / Alcance: cabeçalhos ancorados por linha (com ':' opcional e permitindo 'letras espaçadas').
    - Cronograma: lê as 3 primeiras datas após 'Cronograma' até o próximo cabeçalho; aceita ano com 2 dígitos.

    CORREÇÃO: evitar que "riscos," do índice seja interpretado como cabeçalho.
    """
    t = all_text or ""

    objetivo = find_heading_block(
        t,
        start_variants=["Objetivo"],
        end_variants=["Risco", "Riscos", "Escopo", "Alcance", "Cronograma"],
    )

    risco = find_heading_block(
        t,
        start_variants=["Risco", "Riscos"],
        end_variants=["Escopo", "Alcance", "Cronograma"],
    )

    escopo = find_heading_block(
        t,
        start_variants=["Escopo"],
        end_variants=[
            "Alcance",
            "Cronograma",
            "Conclusão", "Conclusao",
            "Avaliação", "Avaliacao",
            "Classificação", "Classificacao",
        ],
    )

    alcance = find_heading_block(
        t,
        start_variants=["Alcance"],
        end_variants=[
            "Cronograma",
            "Conclusão", "Conclusao",
            "Avaliação", "Avaliacao",
            "Classificação", "Classificacao",
        ],
    )

    # ---- Cronograma: pegar as 3 primeiras datas no bloco do cronograma
    cronograma = None
    cr_idx = _find_heading_indices(
        t,
        start_variants=["Cronograma"],
        end_variants=[
            # comuns nos relatórios Neoenergia
            "0 1 – Auditoria Realizada",
            "0 1 - Auditoria Realizada",
            "0 2 – Contexto",
            "0 2 - Contexto",
            "Auditoria Realizada",
            "Contexto",
            # fallback genérico
            "Conclusão", "Conclusao",
            "Classificação", "Classificacao",
            "Constatação", "Constatacao", "CONSTATAÇÕES",
            "Avaliação", "Avaliacao",
        ],
    )
    if cr_idx:
        start_i, end_i = cr_idx
        lines = (t or "").split("\n")
        cro_block = "\n".join(lines[start_i + 1 : end_i]) if end_i > start_i + 1 else "\n".join(lines[start_i + 1 :])
        dates = DATE_RE.findall(cro_block)

        def iso_date(s: str) -> Optional[str]:
            s = s.replace(".", "/").replace("-", "/")
            parts = s.split("/")
            if len(parts) != 3:
                return None
            d, mo, y = parts
            if len(y) == 2:
                y = "20" + y
            try:
                dt = datetime(int(y), int(mo), int(d))
                return dt.strftime("%Y-%m-%d")
            except Exception:
                return None

        if len(dates) >= 3:
            cronograma = {
                "data_inicio_trabalho": iso_date(dates[0]),
                "draft_emitido": iso_date(dates[1]),
                "relatorio_final": iso_date(dates[2]),
            }
        else:
            cronograma = {"raw": re.sub(r"[ \t]+", " ", cro_block).strip()[:600]}

    return {
        "objetivo": objetivo,
        "risco_processo": risco,
        "escopo": escopo,
        "alcance": alcance,
        "cronograma": cronograma,
    }

def extract_classification(all_text: str) -> Optional[str]:
    for key in ["Classificação do Processo", "Classificação do Ambiente de Controle", "Classificação do Ambiente de Controles"]:
        idx = (all_text or "").lower().find(key.lower())
        if idx >= 0:
            window = (all_text or "")[idx: idx + 800].upper()
            for opt in sorted(CLASS_OPTIONS.keys(), key=len, reverse=True):
                if opt in window:
                    return CLASS_OPTIONS[opt]
    return None

def extract_recommendations(all_text: str) -> List[Dict[str, Any]]:
    recs = []
    pattern = re.compile(
        r"Recomendação\s*[\n ]+Responsável\s*[\n ]+Prazo\s*[\n ]+(.*?)(?=\n\s*Recomendação\s*[\n ]+Responsável\s*[\n ]+Prazo|\Z)",
        re.IGNORECASE | re.DOTALL,
    )
    for m in pattern.finditer(all_text or ""):
        block = normalize_text(m.group(1))
        lines = [ln.strip() for ln in block.split("\n") if ln.strip()]
        prazo = None
        resp = None
        for i, ln in enumerate(lines):
            iso = try_parse_date_any(ln)
            if iso:
                prazo = iso
                if i - 1 >= 0:
                    resp = lines[i - 1]
                break
        recs.append(
            {
                "recomendacao": block,
                "responsavel": resp,
                "prazo": prazo,
                "raw_block": block,
            }
        )
    return recs

def extract_findings_minimal(all_text: str) -> List[Dict[str, Any]]:
    t = all_text or ""
    sec = None
    for kw in ["CONSTATAÇÕES E RECOMENDAÇÕES", "CONSTATAÇÕES", "CONSTATAÇÃO", "NAO CONFORMIDADES", "NÃO CONFORMIDADES"]:
        idx = t.upper().find(kw)
        if idx >= 0:
            sec = t[idx:]
            break
    if not sec:
        return []

    parts = re.split(r"\n\s*(\d+\.\d+)\s*\n", normalize_text(sec))
    if len(parts) < 3:
        block = normalize_text(sec)
        risk = None
        up = block.upper()
        for ro in RISK_OPTIONS:
            if ro in up:
                risk = ro.title().replace("Medio", "Médio").replace("Critico", "Crítico")
                break
        return [{
            "finding_id": "F001",
            "tipo": "nao_conformidade",
            "risco_constatacao": risk,
            "constatacao_titulo": None,
            "constatacao_texto": block,
            "raw_block": block,
        }]

    findings = []
    for i in range(1, len(parts) - 1, 2):
        fid = parts[i]
        block = normalize_text(parts[i + 1])
        up = block.upper()
        risk = None
        for ro in RISK_OPTIONS:
            if ro in up:
                risk = ro.title().replace("Medio", "Médio").replace("Critico", "Crítico")
                break
        findings.append({
            "finding_id": fid,
            "tipo": "nao_conformidade",
            "risco_constatacao": risk,
            "constatacao_titulo": None,
            "constatacao_texto": block,
            "raw_block": block,
        })
    return findings

# ----------------------------
# Output rows / dataclass
# ----------------------------
@dataclass
class ParsedReport:
    source_uri: str
    aud_code: Optional[str]
    title: Optional[str]
    report_type: Optional[str]
    company: Optional[str]
    emission_date: Optional[str]
    classification: Optional[str]
    objetivo: Optional[str]
    risco_processo: Optional[str]
    escopo: Optional[str]
    alcance: Optional[str]
    cronograma: Optional[Dict[str, Any]]
    recommendations: List[Dict[str, Any]]
    findings: List[Dict[str, Any]]

def parse_report(source_uri: str, doc: Dict[str, Any]) -> ParsedReport:
    pages = extract_pages_or_fallback(doc)
    all_text = normalize_text("\n\n".join(p.get("text", "") for p in pages if p.get("text")))
    aud_code = find_aud_code(all_text)
    cover = pick_cover_text(pages)
    cover_fields = extract_title_and_cover_fields(cover)
    proc = extract_obj_risk_scope_reach_schedule(all_text)
    classification = extract_classification(all_text)
    recs = extract_recommendations(all_text)
    findings = extract_findings_minimal(all_text)
    return ParsedReport(
        source_uri=source_uri,
        aud_code=aud_code,
        title=cover_fields["title"],
        report_type=cover_fields["report_type"],
        company=cover_fields["company"],
        emission_date=cover_fields["emission_date"],
        classification=classification,
        objetivo=proc["objetivo"],
        risco_processo=proc["risco_processo"],
        escopo=proc["escopo"],
        alcance=proc["alcance"],
        cronograma=proc["cronograma"],
        recommendations=recs,
        findings=findings,
    )

# ----------------------------
# Parquet (local, particionado)
# ----------------------------
def derive_partitions(emission_date_iso: Optional[str]) -> Dict[str, str]:
    if emission_date_iso:
        try:
            dt = datetime.fromisoformat(emission_date_iso)
            return {"ano": f"{dt.year:04d}", "mes": f"{dt.month:02d}"}
        except Exception:
            pass
    return {"ano": "unknown", "mes": "unknown"}

def _remove_partition_paths(root: Path, dataset: str, df: pd.DataFrame, part_cols: List[str]) -> None:
    if not len(df):
        return
    uniq = df[part_cols].drop_duplicates()
    for _, row in uniq.iterrows():
        sub = Path(root) / dataset / "/".join(f"{col}={row[col]}" for col in part_cols)
        if sub.exists():
            for p in sub.rglob("*"):
                if p.is_file():
                    p.unlink(missing_ok=True)
            for p in sorted(sub.rglob("*"), reverse=True):
                if p.is_dir():
                    try: p.rmdir()
                    except OSError: pass
            try: sub.rmdir()
            except OSError: pass

def write_partitioned_dataset(df: pd.DataFrame, base_dir: Path, dataset: str, mode: str, partition_cols: List[str]) -> None:
    if not len(df):
        return
    ds_root = base_dir / dataset
    ds_root.mkdir(parents=True, exist_ok=True)
    if mode == "overwrite_partitions":
        _remove_partition_paths(base_dir, dataset, df, partition_cols)
    table = pa.Table.from_pandas(df, preserve_index=False)
    pq.write_to_dataset(
        table=table,
        root_path=str(ds_root),
        partition_cols=partition_cols,
        basename_template=f"part-{uuid.uuid4().hex}-{{i}}.parquet",
        use_dictionary=True
    )

def write_outputs(parsed: ParsedReport, out_dir: Path, run_mode: str) -> None:
    part = derive_partitions(parsed.emission_date)
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    df_head = pd.DataFrame([{
        "aud_code": parsed.aud_code,
        "title": parsed.title,
        "report_type": parsed.report_type,
        "company": parsed.company,
        "emission_date": parsed.emission_date,
        "objetivo": parsed.objetivo,
        "risco_processo": parsed.risco_processo,
        "escopo": parsed.escopo,
        "alcance": parsed.alcance,
        "cronograma_inicio": (parsed.cronograma or {}).get("data_inicio_trabalho"),
        "cronograma_draft": (parsed.cronograma or {}).get("draft_emitido"),
        "cronograma_final": (parsed.cronograma or {}).get("relatorio_final"),
        "classification": parsed.classification,
        "source_uri": parsed.source_uri,
        "ingestion_ts": ingestion_ts,
        **part,
    }])
    write_partitioned_dataset(df_head, out_dir, "head", run_mode, ["ano", "mes"])

    rec_rows = []
    for idx, r in enumerate(parsed.recommendations or [], start=1):
        rec_rows.append({
            "aud_code": parsed.aud_code,
            "recommendation_id": f"R{idx:03d}",
            "finding_id": None,
            "recomendacao": r.get("recomendacao"),
            "responsavel": r.get("responsavel"),
            "prazo": r.get("prazo"),
            "source_uri": parsed.source_uri,
            "raw_block": r.get("raw_block"),
            "ingestion_ts": ingestion_ts,
            **part,
        })
    if rec_rows:
        write_partitioned_dataset(pd.DataFrame(rec_rows), out_dir, "recommendations", run_mode, ["ano", "mes"])

    fin_rows = []
    for f in (parsed.findings or []):
        fin_rows.append({
            "aud_code": parsed.aud_code,
            "finding_id": f.get("finding_id"),
            "tipo": f.get("tipo"),
            "risco_constatacao": f.get("risco_constatacao"),
            "constatacao_titulo": f.get("constatacao_titulo"),
            "constatacao_texto": f.get("constatacao_texto"),
            "source_uri": parsed.source_uri,
            "raw_block": f.get("raw_block"),
            "ingestion_ts": ingestion_ts,
            **part,
        })
    if fin_rows:
        write_partitioned_dataset(pd.DataFrame(fin_rows), out_dir, "findings", run_mode, ["ano", "mes"])

# ----------------------------
# Debug helpers
# ----------------------------
def dump_debug_files(parsed: ParsedReport, doc: Dict[str, Any], debug_dir: Path) -> None:
    debug_dir.mkdir(parents=True, exist_ok=True)
    base = Path(parsed.source_uri).stem
    try:
        pages = extract_pages_or_fallback(doc)
        all_text = normalize_text("\n\n".join(p.get("text", "") for p in pages if p.get("text")))
        cover = pick_cover_text(pages)
        (debug_dir / f"{base}__all_text.txt").write_text(all_text, encoding="utf-8")
        (debug_dir / f"{base}__cover_text.txt").write_text(cover, encoding="utf-8")
        (debug_dir / f"{base}__parsed.json").write_text(
            json.dumps({
                "aud_code": parsed.aud_code,
                "title": parsed.title,
                "report_type": parsed.report_type,
                "company": parsed.company,
                "emission_date": parsed.emission_date,
                "classification": parsed.classification,
                "objetivo": parsed.objetivo,
                "risco_processo": parsed.risco_processo,
                "escopo": parsed.escopo,
                "alcance": parsed.alcance,
                "cronograma": parsed.cronograma,
                "recommendations_count": len(parsed.recommendations or []),
                "findings_count": len(parsed.findings or []),
            }, ensure_ascii=False, indent=2),
            encoding="utf-8"
        )
    except Exception as e:
        (debug_dir / f"{base}__debug_error.txt").write_text(repr(e), encoding="utf-8")

# ----------------------------
# Main (CLI)
# ----------------------------
def main():
    ap = argparse.ArgumentParser(description="Parser local de relatórios (JSON) -> Parquet particionado")
    ap.add_argument("--input-dir", "--input_dir", dest="input_dir", type=str, help="Pasta com JSONs")
    ap.add_argument("--input-file", "--input_file", dest="input_file", type=str, help="Um único arquivo JSON")
    ap.add_argument("--output-dir", "--output_dir", dest="output_dir", type=str, required=True, help="Pasta de saída (dataset local)")
    ap.add_argument("--run-mode", "--run_mode", dest="run_mode", type=str, default="append", choices=["append", "overwrite_partitions"], help="append | overwrite_partitions")
    ap.add_argument("--max-files", "--max_files", dest="max_files", type=int, default=0, help="0 = sem limite")
    ap.add_argument("--debug", action="store_true", help="Imprime informações de parsing no console")
    ap.add_argument("--save-debug", "--save_debug", dest="save_debug", type=str, help="Pasta para salvar artefatos de debug por arquivo")
    args = ap.parse_args()

    inp_files: List[Path] = []
    if args.input_file:
        p = Path(args.input_file)
        if not p.exists():
            print(f"[ERRO] input-file não existe: {p}", file=sys.stderr)
            sys.exit(2)
        inp_files = [p]
    elif args.input_dir:
        root = Path(args.input_dir)
        if not root.exists():
            print(f"[ERRO] input-dir não existe: {root}", file=sys.stderr)
            sys.exit(2)
        inp_files = sorted(root.rglob("*.json"))
    else:
        print("[ERRO] Informe --input-file ou --input-dir", file=sys.stderr)
        sys.exit(2)

    if args.max_files and len(inp_files) > args.max_files:
        inp_files = inp_files[:args.max_files]

    out_dir = Path(args.output_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    print(f"[INFO] arquivos={len(inp_files)} | output-dir={out_dir} | run-mode={args.run_mode}")

    processed = 0
    for fp in inp_files:
        source_uri = str(fp)
        try:
            with open(fp, "r", encoding="utf-8") as f:
                doc = json.load(f)

            parsed = parse_report(source_uri, doc)
            if args.debug:
                print("---")
                print(f"[DEBUG] {fp.name}")
                print(f" aud_code       = {parsed.aud_code}")
                print(f" title          = {parsed.title}")
                print(f" report_type    = {parsed.report_type}")
                print(f" company        = {parsed.company}")
                print(f" emission_date  = {parsed.emission_date}")
                print(f" classification = {parsed.classification}")
                print(f" objetivo       = {str(parsed.objetivo)[:120] if parsed.objetivo else None}")
                print(f" risco_processo = {str(parsed.risco_processo)[:120] if parsed.risco_processo else None}")
                print(f" escopo         = {str(parsed.escopo)[:120] if parsed.escopo else None}")
                print(f" alcance        = {str(parsed.alcance)[:120] if parsed.alcance else None}")
                print(f" cronograma     = {parsed.cronograma}")
                print(f" recs           = {len(parsed.recommendations)}")
                print(f" findings       = {len(parsed.findings)}")

            write_outputs(parsed, out_dir, args.run_mode)

            if args.save_debug:
                dump_debug_files(parsed, doc, Path(args.save_debug))

            processed += 1
        except Exception as e:
            print(f"[ERRO] falha em {source_uri} | err={repr(e)}", file=sys.stderr)

    print(f"[DONE] processed={processed} | total_input={len(inp_files)}")

if __name__ == "__main__":
    main()
