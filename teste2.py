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

    filtered: List[Tuple[str, str]] = []
    for r, n in zip(raw_lines, norms):
        if re.fullmatch(r"\d+", n):
            continue
        filtered.append((r, n))

    def is_conf_line(n: str) -> bool:
        return n.startswith("este relatorio") and ("confidencial" in n or "exclusiv" in n)

    skip_set = {"auditoria interna", "confidential"}
    filtered2 = [(r, n) for r, n in filtered if n not in skip_set and not is_conf_line(n)]

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
        title_lines: List[str] = []
        for r, n in filtered2[:rt_idx]:
            if looks_like_aud(r) or looks_like_date(n):
                continue
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

        for r, n in filtered2[rt_idx+1:]:
            if looks_like_aud(r):
                break
            if n:
                company = r
                break
    else:
        report_type = "Relatório Final"
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
# Seções do corpo (linha-a-linha, com fim estrito)
# ----------------------------

def _canon_heading(s: str) -> str:
    s = _strip_accents_lower(s)
    s = s.replace("–", "-")
    s = re.sub(r"[^\w: /-]+", "", s)  # mantém : / -
    s = re.sub(r"\s+", " ", s).strip()
    return s

def _strip_leading_numbering(raw: str) -> str:
    # remove "2", "02", "2.1", "2.1 –", etc.
    return re.sub(r"^\s*\d{1,2}(?:\.\d{1,2})*\s*[-–]?\s*", "", raw or "").strip()

def _heading_matches_start(raw_line: str, variant: str) -> bool:
    """
    INÍCIO: permissivo.
    Aceita:
      - "Risco"
      - "Risco:" / "Risco - ..."
      - "Risco Continuidade..." (inline)
      - letras espaçadas (c r o n o g r a m a) via canon sem espaços
    """
    raw = _strip_leading_numbering(raw_line)
    c = _canon_heading(raw)
    v = _canon_heading(variant)

    if c == v or c == v + ":":
        return True

    c_nospace = c.replace(" ", "")
    v_nospace = v.replace(" ", "")
    if c_nospace == v_nospace or c_nospace == v_nospace + ":":
        return True

    # inline (texto na mesma linha): "risco / ..." "risco: ..." "risco - ..." "risco ..."
    if c.startswith(v + " ") or c.startswith(v + ":") or c.startswith(v + "-") or c.startswith(v + "/"):
        return True

    return False

def _heading_matches_end(raw_line: str, variant: str) -> bool:
    """
    FIM: estrito.
    Só encerra se a linha for um TÍTULO PURO (sem texto depois), no máximo com ":".
    Isso impede cortar objetivo/escopo/alcance no meio do texto.
    """
    raw = _strip_leading_numbering(raw_line)
    raw = raw.strip()
    # remove ':' final
    raw2 = raw[:-1].strip() if raw.endswith(":") else raw
    c = _canon_heading(raw2)
    v = _canon_heading(variant)

    if c == v:
        # garante que não tinha mais coisa além do título
        # (se raw2 tinha várias palavras diferentes do título, canon não bateria)
        return True

    # letras espaçadas
    if c.replace(" ", "") == v.replace(" ", ""):
        return True

    return False

def _extract_inline_after_heading(raw_line: str, variant: str) -> Optional[str]:
    """
    Se o título vier com conteúdo na mesma linha ("Risco Continuidade..." / "Risco: ..."),
    captura o texto depois do heading.
    """
    raw = _strip_leading_numbering(raw_line).strip()
    if not raw:
        return None

    # trabalha com versão sem acento/canon pra achar o prefixo, mas devolve no original
    v = _canon_heading(variant)

    # tentativas comuns: "Risco: texto", "Risco - texto", "Risco / texto", "Risco texto"
    m = re.match(r"^\s*(.+?)\s*$", raw)
    if not m:
        return None

    # remove separadores após o heading
    # (fazemos match no começo, ignorando caso/acentos via canon do prefixo)
    # abordagem: pega tokens do raw até cobrir o heading
    raw_canon = _canon_heading(raw)

    if raw_canon == v or raw_canon == v + ":":
        return None

    if raw_canon.startswith(v + ":"):
        # pega após ':'
        idx = raw.find(":")
        if idx >= 0:
            tail = raw[idx+1:].strip()
            return tail or None

    for sep in [" - ", " – ", " / ", "-", "–", "/"]:
        # tenta split uma vez
        parts = raw.split(sep, 1)
        if len(parts) == 2 and _canon_heading(parts[0]).replace(":", "").strip() == v:
            tail = parts[1].strip()
            return tail or None

    # caso "Risco Continuidade..." sem separador
    parts = raw.split(None, 1)
    if len(parts) == 2 and _canon_heading(parts[0]) == v:
        return parts[1].strip() or None

    return None

def _find_heading_indices(text: str, start_variants: List[str], end_variants: List[str]) -> Optional[Tuple[int, int]]:
    lines = (text or "").split("\n")

    start_i = None
    start_variant_used = None
    for i, raw in enumerate(lines):
        for sv in start_variants:
            if _heading_matches_start(raw, sv):
                start_i = i
                start_variant_used = sv
                break
        if start_i is not None:
            break

    if start_i is None:
        return None

    end_i = len(lines)
    for j in range(start_i + 1, len(lines)):
        raw = lines[j]
        for ev in end_variants:
            if _heading_matches_end(raw, ev):
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

    # conteúdo na mesma linha do heading (quando existir)
    inline = None
    for sv in start_variants:
        if _heading_matches_start(lines[start_i], sv):
            inline = _extract_inline_after_heading(lines[start_i], sv)
            break

    block_lines = lines[start_i + 1 : end_i]
    parts = []
    if inline:
        parts.append(inline)
    if block_lines:
        parts.append("\n".join(block_lines))

    block = "\n".join(parts)
    block = re.sub(r"[ \t]+", " ", block)
    block = re.sub(r"\n{3,}", "\n\n", block).strip(":- \n\t")
    return block or None

def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:
    t = all_text or ""

    objetivo = find_heading_block(
        t,
        start_variants=["Objetivo"],
        end_variants=["Risco", "Riscos"],
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

    # Cronograma: pega 3 primeiras datas após heading "Cronograma"
    cronograma = None
    cr_idx = _find_heading_indices(
        t,
        start_variants=["Cronograma"],
        end_variants=[
            "Auditoria Realizada",
            "Contexto",
            "Conclusão", "Conclusao",
            "Classificação", "Classificacao",
            "Constatação", "Constatacao", "CONSTATAÇÕES",
            "Avaliação", "Avaliacao",
            "0 1 - AUDITORIA REALIZADA",
            "0 2 - CONTEXTO",
        ],
    )

    if cr_idx:
        start_i, end_i = cr_idx
        lines = t.split("\n")
        cro_block = "\n".join(lines[start_i + 1 : end_i])

        # conserta ano quebrado tipo "23/07/2\n025"
        cro_block = re.sub(
            r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d)\s*\n\s*(\d{2,3})",
            r"\1\2",
            cro_block,
        )

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
                return datetime(int(y), int(mo), int(d)).strftime("%Y-%m-%d")
            except Exception:
                return None

        if len(dates) >= 3:
            cronograma = {
                "data_inicio_trabalho": iso_date(dates[0]),
                "draft_emitido": iso_date(dates[1]),
                "relatorio_final": iso_date(dates[2]),
            }
        else:
            cronograma = {"raw": normalize_text(cro_block)[:600]}

    return {
        "objetivo": objetivo,
        "risco_processo": risco,
        "escopo": escopo,
        "alcance": alcance,
        "cronograma": cronograma,
    }

# ----------------------------
# Restante (classificação / recomendações / findings / parquet / CLI)
# (igual ao seu)
# ----------------------------

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
            {"recomendacao": block, "responsavel": resp, "prazo": prazo, "raw_block": block}
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
                print(f" objetivo       = {str(parsed.objetivo)[:200] if parsed.objetivo else None}")
                print(f" risco_processo = {str(parsed.risco_processo)[:200] if parsed.risco_processo else None}")
                print(f" escopo         = {str(parsed.escopo)[:200] if parsed.escopo else None}")
                print(f" alcance        = {str(parsed.alcance)[:200] if parsed.alcance else None}")
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