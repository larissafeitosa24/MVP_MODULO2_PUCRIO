# Glue Python Shell job
# Outputs: Parquet datasets for head, findings, recommendations (PROCESSED)
#
# Required job parameters:
#   --S3_BUCKET           (e.g. meu-bucket)
#   --RAW_PREFIX          (e.g. audit_reports/raw/)
#   --PROCESSED_PREFIX      (e.g. audit_reports/PROCESSED/)
#
# Optional parameters:
#   --RUN_MODE            append | overwrite_partitions   (default: append)
#   --MAX_FILES           int (default: 0 = no limit)
#   --LOG_PREFIX          (e.g. audit_reports/logs/)  (optional, currently unused)
#
# Libraries (Glue Python Shell):
#   awswrangler, pandas, pyarrow

import sys
import json
import re
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import boto3
import pandas as pd
import awswrangler as wr
from awsglue.utils import getResolvedOptions

# ----------------------------
# Params
# ----------------------------
ARG_KEYS = ["S3_BUCKET", "RAW_PREFIX", "PROCESSED_PREFIX"]
OPTIONAL_KEYS = ["RUN_MODE", "MAX_FILES", "LOG_PREFIX"]

args = getResolvedOptions(sys.argv, ARG_KEYS + [k for k in OPTIONAL_KEYS if f"--{k}" in " ".join(sys.argv)])

S3_BUCKET = args["S3_BUCKET"]
RAW_PREFIX = args["RAW_PREFIX"].lstrip("/")  # safe
PROCESSED_PREFIX = args["PROCESSED_PREFIX"].lstrip("/")

RUN_MODE = args.get("RUN_MODE", "append").lower()  # append | overwrite_partitions
MAX_FILES = int(args.get("MAX_FILES", "0"))

s3 = boto3.client("s3")

# ----------------------------
# Regex / helpers
# ----------------------------
AUD_RE = re.compile(r"\bAUD-\d{5,}\b", re.IGNORECASE)
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
    m = AUD_RE.search(all_text or "")
    return m.group(0).upper() if m else None

def pick_cover_text(pages: List[Dict[str, Any]]) -> str:
    if pages:
        return (pages[0].get("text") or "")[:5000]
    return ""

def extract_title_and_cover_fields(cover_text: str) -> Dict[str, Optional[str]]:
    lines = [ln.strip() for ln in (cover_text or "").split("\n") if ln.strip()]
    aud_idx = None
    for i, ln in enumerate(lines):
        if AUD_RE.search(ln):
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
                if AUD_RE.search(ln):
                    continue
                if "RELATÓRIO" in ln.upper() or "RELATORIO" in ln.upper():
                    continue
                company = ln
                break
        except StopIteration:
            pass

    emission_date = try_parse_date_any(cover_text)
    return {"title": title, "report_type": report_type, "company": company, "emission_date": emission_date}

def extract_block_between(all_text: str, start_kw: str, end_kws: List[str]) -> Optional[str]:
    t = all_text or ""
    si = t.lower().find(start_kw.lower())
    if si < 0:
        return None
    tail = t[si + len(start_kw):]
    cut = len(tail)
    for ek in end_kws:
        ei = tail.lower().find(ek.lower())
        if 0 <= ei < cut:
            cut = ei
    out = normalize_text(tail[:cut]).strip(":- \n\t")
    return out or None

def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:
    objetivo = extract_block_between(all_text, "Objetivo", ["Risco", "Escopo", "Alcance", "Cronograma"])
    risco = extract_block_between(all_text, "Risco", ["Escopo", "Alcance", "Cronograma"])
    escopo = extract_block_between(all_text, "Escopo", ["Alcance", "Cronograma", "Conclusão", "Avaliação", "Classificação"])
    alcance = extract_block_between(all_text, "Alcance", ["Cronograma", "Conclusão", "Avaliação", "Classificação"])

    cronograma_txt = extract_block_between(
        all_text,
        "Cronograma",
        ["Contexto", "Avaliação", "Classificação", "Constatação", "CONSTATAÇÕES", "04", "03", "02", "01"],
    )
    schedule = None
    if cronograma_txt:
        dates = DATE_RE.findall(cronograma_txt)
        if len(dates) >= 3:
            schedule = {
                "data_inicio_trabalho": try_parse_date_any(dates[0]),
                "draft_emitido": try_parse_date_any(dates[1]),
                "relatorio_final": try_parse_date_any(dates[2]),
            }
        else:
            schedule = {"raw": cronograma_txt}

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
    """
    Extract repeated blocks with:
      Recomendação / Responsável / Prazo
    Works even if content is long (DOTALL).
    """
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
    """
    Minimal v1 findings extractor:
    - Tries to split by numbered findings like "1.1", "2.3" if present near constatações.
    - Captures a risk label if one of RISK_OPTIONS appears in the block.
    - Does NOT guarantee perfect mapping; designed to evolve.
    """
    t = all_text or ""
    # Try to isolate the findings section roughly
    sec = None
    for kw in ["CONSTATAÇÕES E RECOMENDAÇÕES", "CONSTATAÇÕES", "CONSTATAÇÃO", "NAO CONFORMIDADES", "NÃO CONFORMIDADES"]:
        idx = t.upper().find(kw)
        if idx >= 0:
            sec = t[idx:]
            break
    if not sec:
        return []

    # Split by finding ids like 1.1 / 1.2 / 2.1 ...
    parts = re.split(r"\n\s*(\d+\.\d+)\s*\n", normalize_text(sec))
    if len(parts) < 3:
        # fallback: single block
        block = normalize_text(sec)
        risk = None
        up = block.upper()
        for ro in RISK_OPTIONS:
            if ro in up:
                risk = ro.title().replace("Médio", "Médio").replace("Critico", "Crítico")
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
    # parts = [pre, id1, text1, id2, text2, ...]
    pre = parts[0]
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
# Output rows
# ----------------------------
@dataclass
class ParsedReport:
    source_s3_uri: str
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

def parse_report(source_s3_uri: str, doc: Dict[str, Any]) -> ParsedReport:
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
def derive_partitions(emission_date_iso: Optional[str]) -> Dict[str, str]:
    if emission_date_iso:
        try:
            dt = datetime.fromisoformat(emission_date_iso)
            return {"ano": f"{dt.year:04d}", "mes": f"{dt.month:02d}"}
        except Exception:
            pass
    return {"ano": "unknown", "mes": "unknown"}

def write_parquet_outputs(parsed: ParsedReport) -> None:
    part = derive_partitions(parsed.emission_date)
    ingestion_ts = datetime.now(timezone.utc).isoformat()

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

    # overwrite_partitions works best with partition_cols; append is simplest
    head_path = f"s3://{S3_BUCKET}/{PROCESSED_PREFIX.rstrip('/')}/head/"
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
            "finding_id": None,  # preencheremos quando o parser amarrar
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
        rec_path = f"s3://{S3_BUCKET}/{PROCESSED_PREFIX.rstrip('/')}/recommendations/"
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
        fin_path = f"s3://{S3_BUCKET}/{PROCESSED_PREFIX.rstrip('/')}/findings/"
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
            parsed = parse_report(source_uri, doc)

            # Write Parquet outputs
            write_parquet_outputs(parsed)

            processed += 1
            if processed % 25 == 0:
                print(f"[INFO] processed={processed}")
        except Exception as e:
            # Keep going; log error
            print(f"[ERROR] failed file: {source_uri} | err={repr(e)}")

    print(f"[DONE] processed={processed} | total_found={len(keys)}")

if __name__ == "__main__":
    main()
