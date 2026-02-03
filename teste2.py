# Glue Python Shell job
# Outputs: Parquet datasets for head, findings, recommendations (PROCESSED)
#
# Required job parameters:
#   --S3_BUCKET           (e.g. meu-bucket)
#   --RAW_PREFIX          (e.g. audit_reports/raw/)
#   --PROCESSED_PREFIX    (e.g. audit_reports/processed/)
#
# Optional parameters:
#   --RUN_MODE            append | overwrite_partitions   (default: append)
#   --MAX_FILES           int (default: 0 = no limit)
#   --LOG_PREFIX          (optional, currently unused)
#
# Libraries (Glue Python Shell):
#   awswrangler, pandas, pyarrow

import sys
import json
import re
import unicodedata
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Tuple

import boto3
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# ----------------------------
# Params
# ----------------------------
ARG_KEYS = ["S3_BUCKET", "RAW_PREFIX", "PROCESSED_PREFIX"]
OPTIONAL_KEYS = ["RUN_MODE", "MAX_FILES", "LOG_PREFIX"]

args = getResolvedOptions(
    sys.argv,
    ARG_KEYS + [k for k in OPTIONAL_KEYS if f"--{k}" in " ".join(sys.argv)]
)

S3_BUCKET = args["S3_BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"].lstrip("/")
PROCESSED_PREFIX = args["PROCESSED_PREFIX"].lstrip("/")

RUN_MODE = args.get("RUN_MODE", "append").lower()  # append | overwrite_partitions
MAX_FILES = int(args.get("MAX_FILES", "0"))

s3 = boto3.client("s3")

# ----------------------------
# Regex / helpers
# ----------------------------
AUD_RE_SPACES = re.compile(r"\bAUD\s*-\s*(\d{5,})\b", re.IGNORECASE)
AUD_RE_CANON = re.compile(r"\bAUD-\d{5,}\b", re.IGNORECASE)
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

def normalize_text(t: str) -> str:
    if not t:
        return ""
    t = t.replace("\u00ad", "")
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
    """Return ISO date YYYY-MM-DD from the FIRST date found in string."""
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
        out.append(obj)
        return
    if isinstance(obj, list):
        for it in obj:
            deep_collect_strings(it, out)
        return
    if isinstance(obj, dict):
        for v in obj.values():
            deep_collect_strings(v, out)
        return

def extract_pages_or_fallback(doc: Dict[str, Any]) -> List[Dict[str, Any]]:
    pages = doc.get("pages")
    if isinstance(pages, list) and pages and isinstance(pages[0], dict) and ("text" in pages[0]):
        out = []
        for p in pages:
            out.append({"page": p.get("page"), "text": normalize_text(p.get("text", ""))})
        return out
    strings: List[str] = []
    deep_collect_strings(doc, strings)
    big = normalize_text("\n".join(strings))
    return [{"page": None, "text": big}]

def find_aud_code(all_text: str) -> Optional[str]:
    # aceita "AUD - 12345" e normaliza pra "AUD-12345"
    m = AUD_RE_SPACES.search(all_text or "")
    if m:
        return f"AUD-{m.group(1)}"
    m2 = AUD_RE_CANON.search(all_text or "")
    return m2.group(0).upper() if m2 else None

def pick_cover_text(pages: List[Dict[str, Any]]) -> str:
    if pages:
        return (pages[0].get("text") or "")[:5000]
    return ""

def extract_title_and_cover_fields(cover_text: str) -> Dict[str, Optional[str]]:
    # (mantive simples como estava; se quiser, depois plugamos a regra v3 mais robusta)
    lines = [ln.strip() for ln in (cover_text or "").split("\n") if ln.strip()]
    aud_idx = None
    for i, ln in enumerate(lines):
        if AUD_RE_SPACES.search(ln) or AUD_RE_CANON.search(ln):
            aud_idx = i
            break

    title = None
    if aud_idx is not None:
        candidates = []
        for ln in lines[max(0, aud_idx - 6):aud_idx]:
            up = ln.upper()
            if "AUDITORIA" in up and "INTERNA" in up:
                continue
            if up in {"AUDITORIA", "INTERNA"}:
                continue
            candidates.append(ln)
        if candidates:
            title = max(candidates, key=len)

    report_type = None
    for ln in lines:
        if "RELATÓRIO" in ln.upper() or "RELATORIO" in ln.upper():
            report_type = ln.strip()
            break

    company = None
    if report_type:
        try:
            idx = next(i for i, ln in enumerate(lines) if ln == report_type)
            for ln in lines[idx+1:idx+8]:
                if AUD_RE_SPACES.search(ln) or AUD_RE_CANON.search(ln):
                    continue
                if "RELATÓRIO" in ln.upper() or "RELATORIO" in ln.upper():
                    continue
                company = ln
                break
        except StopIteration:
            pass

    emission_date = try_parse_date_any(cover_text)
    return {"title": title, "report_type": report_type, "company": company, "emission_date": emission_date}

# ----------------------------
# Loop-based section extraction (robusta)
# ----------------------------
def extract_by_loop(text: str, start_titles: List[str], end_titles: List[str]) -> Optional[str]:
    """
    Lê linha-a-linha.
    Começa quando encontra um título (ex: Objetivo).
    Para quando encontra o próximo título (ex: Risco).
    """
    lines = (text or "").split("\n")

    def canon(line: str) -> str:
        c = _strip_accents_lower(line or "").strip()
        # remove numeração tipo "2", "2.1", "02 –"
        c = re.sub(r"^\d{1,2}(?:\.\d{1,2})*\s*[-–]?\s*", "", c)
        return c

    start_set = { _strip_accents_lower(x).strip() for x in start_titles }
    end_set = { _strip_accents_lower(x).strip() for x in end_titles }

    collecting = False
    buf: List[str] = []

    for line in lines:
        c = canon(line)

        if c in start_set:
            collecting = True
            continue

        if collecting and c in end_set:
            break

        if collecting:
            buf.append(line)

    out = normalize_text("\n".join(buf)).strip(":- \n\t")
    return out or None

def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:
    t = all_text or ""

    objetivo = extract_by_loop(t, ["Objetivo"], ["Risco", "Riscos"])
    risco = extract_by_loop(t, ["Risco", "Riscos"], ["Escopo", "Alcance", "Cronograma"])

    escopo = extract_by_loop(
        t,
        ["Escopo"],
        ["Alcance", "Cronograma", "Conclusão", "Conclusao", "Avaliação", "Avaliacao", "Classificação", "Classificacao"],
    )

    alcance = extract_by_loop(
        t,
        ["Alcance"],
        ["Cronograma", "Conclusão", "Conclusao", "Avaliação", "Avaliacao", "Classificação", "Classificacao"],
    )

    cronograma_txt = extract_by_loop(
        t,
        ["Cronograma"],
        ["Contexto", "Avaliação", "Avaliacao", "Classificação", "Classificacao", "Constatação", "Constatacao", "CONSTATAÇÕES"],
    )

    schedule = None
    if cronograma_txt:
        # conserta datas quebradas tipo "23/07/2\n025"
        cron_fix = re.sub(
            r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d)\s*\n\s*(\d{2,3})",
            r"\1\2",
            cronograma_txt,
        )

        dates = DATE_RE.findall(cron_fix)

        def iso_date_from_raw(raw_date: str) -> Optional[str]:
            raw = (raw_date or "").replace(".", "/").replace("-", "/")
            parts = raw.split("/")
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
            schedule = {
                "data_inicio_trabalho": iso_date_from_raw(dates[0]),
                "draft_emitido": iso_date_from_raw(dates[1]),
                "relatorio_final": iso_date_from_raw(dates[2]),
            }
        else:
            schedule = {"raw": normalize_text(cron_fix)[:600]}

    return {
        "objetivo": objetivo,
        "risco_processo": risco,
        "escopo": escopo,
        "alcance": alcance,
        "cronograma": schedule,
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
        recs.append({"recomendacao": block, "responsavel": resp, "prazo": prazo, "raw_block": block})
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
# Partition helpers
# ----------------------------
def derive_partitions_from_path(key: str) -> Optional[Dict[str, str]]:
    """
    Se o raw vier particionado, ex:
      .../year=2025/month=12/file.json
      .../ano=2025/mes=12/file.json
    usa isso como partição do output.
    """
    k = key.lower()

    m = re.search(r"(?:^|/)year=(\d{4})(?:/|$)", k)
    n = re.search(r"(?:^|/)month=(\d{1,2})(?:/|$)", k)
    if m and n:
        return {"ano": f"{int(m.group(1)):04d}", "mes": f"{int(n.group(1)):02d}"}

    m2 = re.search(r"(?:^|/)ano=(\d{4})(?:/|$)", k)
    n2 = re.search(r"(?:^|/)mes=(\d{1,2})(?:/|$)", k)
    if m2 and n2:
        return {"ano": f"{int(m2.group(1)):04d}", "mes": f"{int(n2.group(1)):02d}"}

    return None

def derive_partitions_from_emission(emission_date_iso: Optional[str]) -> Dict[str, str]:
    if emission_date_iso:
        try:
            dt = datetime.fromisoformat(emission_date_iso)
            return {"ano": f"{dt.year:04d}", "mes": f"{dt.month:02d}"}
        except Exception:
            pass
    return {"ano": "unknown", "mes": "unknown"}

# ----------------------------
# Output rows
# ----------------------------
@dataclass
class ParsedReport:
    source_s3_uri: str
    source_key: str
    aud_code: Optional[str]
    title: Optional[str]
    report_type: Optional[str]
    company: Optional[str]
    emission_date: Optional[str]  # ISO
    classification: Optional[str]
    objetivo: Optional[str]
    risco_processo: Optional[str]
    escopo: Optional[str]
    alcance: Optional[str]
    cronograma: Optional[Dict[str, Any]]
    recommendations: List[Dict[str, Any]]
    findings: List[Dict[str, Any]]

def parse_report(source_s3_uri: str, source_key: str, doc: Dict[str, Any]) -> ParsedReport:
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
        source_s3_uri=source_s3_uri,
        source_key=source_key,
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
# S3 IO
# ----------------------------
def s3_list_json_keys(bucket: str, prefix: str) -> List[str]:
    keys: List[str] = []
    paginator = s3.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            k = obj["Key"]
            if k.lower().endswith(".json"):
                keys.append(k)
                if MAX_FILES and len(keys) >= MAX_FILES:
                    return keys
    return keys

def s3_read_json(bucket: str, key: str) -> Dict[str, Any]:
    body = s3.get_object(Bucket=bucket, Key=key)["Body"].read()
    return json.loads(body)

# ----------------------------
# Parquet writing
# ----------------------------
def write_parquet_outputs(parsed: ParsedReport) -> None:
    # Prioriza partição do PATH do raw; fallback para emission_date
    part = derive_partitions_from_path(parsed.source_key) or derive_partitions_from_emission(parsed.emission_date)
    ingestion_ts = datetime.now(timezone.utc).isoformat()

    base_out = f"s3://{S3_BUCKET}/{PROCESSED_PREFIX.rstrip('/')}"
    head_path = f"{base_out}/head/"
    rec_path = f"{base_out}/recommendations/"
    fin_path = f"{base_out}/findings/"

    # ---- HEAD (1 row)
    head_row = {
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
        "source_s3_uri": parsed.source_s3_uri,
        "ingestion_ts": ingestion_ts,
        **part,
    }
    df_head = pd.DataFrame([head_row])
    wr.s3.to_parquet(
        df=df_head,
        path=head_path,
        dataset=True,
        mode=RUN_MODE,
        partition_cols=["ano", "mes"],
    )

    # ---- RECOMMENDATIONS (N rows)
    rec_rows = []
    for idx, r in enumerate(parsed.recommendations or [], start=1):
        rec_rows.append({
            "aud_code": parsed.aud_code,
            "recommendation_id": f"R{idx:03d}",
            "finding_id": None,
            "recomendacao": r.get("recomendacao"),
            "responsavel": r.get("responsavel"),
            "prazo": r.get("prazo"),
            "source_s3_uri": parsed.source_s3_uri,
            "raw_block": r.get("raw_block"),
            "ingestion_ts": ingestion_ts,
            **part,
        })
    if rec_rows:
        df_rec = pd.DataFrame(rec_rows)
        wr.s3.to_parquet(
            df=df_rec,
            path=rec_path,
            dataset=True,
            mode=RUN_MODE,
            partition_cols=["ano", "mes"],
        )

    # ---- FINDINGS (N rows)
    fin_rows = []
    for f in (parsed.findings or []):
        fin_rows.append({
            "aud_code": parsed.aud_code,
            "finding_id": f.get("finding_id"),
            "tipo": f.get("tipo"),
            "risco_constatacao": f.get("risco_constatacao"),
            "constatacao_titulo": f.get("constatacao_titulo"),
            "constatacao_texto": f.get("constatacao_texto"),
            "source_s3_uri": parsed.source_s3_uri,
            "raw_block": f.get("raw_block"),
            "ingestion_ts": ingestion_ts,
            **part,
        })
    if fin_rows:
        df_fin = pd.DataFrame(fin_rows)
        wr.s3.to_parquet(
            df=df_fin,
            path=fin_path,
            dataset=True,
            mode=RUN_MODE,
            partition_cols=["ano", "mes"],
        )

# ----------------------------
# Main
# ----------------------------
def main():
    raw_prefix = RAW_PREFIX.rstrip("/") + "/"
    keys = s3_list_json_keys(S3_BUCKET, raw_prefix)

    print(f"[INFO] Bucket={S3_BUCKET}")
    print(f"[INFO] RAW_PREFIX={raw_prefix} | files_found={len(keys)} | MAX_FILES={MAX_FILES}")
    print(f"[INFO] PROCESSED_PREFIX={PROCESSED_PREFIX} | RUN_MODE={RUN_MODE}")

    processed = 0
    for key in keys:
        source_uri = f"s3://{S3_BUCKET}/{key}"
        try:
            doc = s3_read_json(S3_BUCKET, key)
            parsed = parse_report(source_uri, key, doc)

            write_parquet_outputs(parsed)

            processed += 1
            if processed % 25 == 0:
                print(f"[INFO] processed={processed}")
        except Exception as e:
            print(f"[ERROR] failed file: {source_uri} | err={repr(e)}")

    print(f"[DONE] processed={processed} | total_found={len(keys)}")

if __name__ == "__main__":
    main()