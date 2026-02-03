# local_audit_parser.py
# Parser local de relatórios (JSON) -> Parquet particionado

import argparse
import json
import re
import sys
import unicodedata
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq

# ----------------------------
# Helpers
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
    m = DATE_RE.search(s or "")
    if not m:
        return None
    raw = m.group(1).replace(".", "/").replace("-", "/")
    d, mth, y = raw.split("/")
    if len(y) == 2:
        y = "20" + y
    try:
        return datetime(int(y), int(mth), int(d)).strftime("%Y-%m-%d")
    except:
        return None

# ----------------------------
# JSON text extraction
# ----------------------------

def deep_collect_strings(obj: Any, out: List[str]) -> None:
    if obj is None:
        return
    if isinstance(obj, str):
        out.append(obj)
    elif isinstance(obj, list):
        for it in obj:
            deep_collect_strings(it, out)
    elif isinstance(obj, dict):
        for v in obj.values():
            deep_collect_strings(v, out)

def extract_pages_or_fallback(doc: Dict[str, Any]) -> List[str]:
    pages = doc.get("pages")
    if isinstance(pages, list) and pages and isinstance(pages[0], dict) and "text" in pages[0]:
        return [normalize_text(p.get("text", "")) for p in pages]

    strings = []
    deep_collect_strings(doc, strings)
    return [normalize_text("\n".join(strings))]

def find_aud_code(all_text: str) -> Optional[str]:
    m = AUD_RE_SPACES.search(all_text or "")
    return f"AUD-{m.group(1)}" if m else None

# ----------------------------
# LOOP BASED SECTION PARSER
# ----------------------------

def extract_by_loop(text: str, start_titles: List[str], end_titles: List[str]) -> Optional[str]:
    lines = (text or "").split("\n")

    def canon(s: str) -> str:
        return _strip_accents_lower(s or "").strip()

    start_set = {canon(x) for x in start_titles}
    end_set = {canon(x) for x in end_titles}

    collecting = False
    buffer = []

    for line in lines:
        c = canon(line)

        # remove numeração tipo "2 -", "2.1"
        c = re.sub(r"^\d{1,2}(?:\.\d{1,2})*\s*[-–]?\s*", "", c)

        if c in start_set:
            collecting = True
            continue

        if collecting and c in end_set:
            break

        if collecting:
            buffer.append(line)

    block = normalize_text("\n".join(buffer)).strip()
    return block or None

# ----------------------------
# Main business sections
# ----------------------------

def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:

    objetivo = extract_by_loop(
        all_text,
        ["Objetivo"],
        ["Risco", "Riscos"],
    )

    risco = extract_by_loop(
        all_text,
        ["Risco", "Riscos"],
        ["Escopo", "Alcance", "Cronograma"],
    )

    escopo = extract_by_loop(
        all_text,
        ["Escopo"],
        ["Alcance", "Cronograma", "Conclusão", "Conclusao", "Avaliação", "Avaliacao", "Classificação", "Classificacao"],
    )

    alcance = extract_by_loop(
        all_text,
        ["Alcance"],
        ["Cronograma", "Conclusão", "Conclusao", "Avaliação", "Avaliacao", "Classificação", "Classificacao"],
    )

    cronograma_text = extract_by_loop(
        all_text,
        ["Cronograma"],
        ["Auditoria Realizada", "Contexto", "Conclusão", "Conclusao", "Classificação", "Classificacao", "Constatação", "Constatacao", "Avaliação", "Avaliacao"],
    )

    cronograma = None

    if cronograma_text:
        cr_fix = re.sub(
            r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d)\s*\n\s*(\d{2,3})",
            r"\1\2",
            cronograma_text,
        )

        dates = DATE_RE.findall(cr_fix)

        def iso_date(s):
            s = s.replace(".", "/").replace("-", "/")
            d, m, y = s.split("/")
            if len(y) == 2:
                y = "20" + y
            return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")

        if len(dates) >= 3:
            cronograma = {
                "data_inicio_trabalho": iso_date(dates[0]),
                "draft_emitido": iso_date(dates[1]),
                "relatorio_final": iso_date(dates[2]),
            }
        else:
            cronograma = {"raw": normalize_text(cr_fix)[:600]}

    return {
        "objetivo": objetivo,
        "risco_processo": risco,
        "escopo": escopo,
        "alcance": alcance,
        "cronograma": cronograma,
    }

# ----------------------------
# Other extractors
# ----------------------------

def extract_classification(all_text: str) -> Optional[str]:
    for key in [
        "Classificação do Processo",
        "Classificação do Ambiente de Controle",
        "Classificação do Ambiente de Controles",
    ]:
        idx = (all_text or "").lower().find(key.lower())
        if idx >= 0:
            window = (all_text or "")[idx : idx + 800].upper()
            for opt in sorted(CLASS_OPTIONS.keys(), key=len, reverse=True):
                if opt in window:
                    return CLASS_OPTIONS[opt]
    return None

def extract_recommendations(all_text: str) -> List[Dict[str, Any]]:
    recs = []
    pattern = re.compile(
        r"Recomendação\s*[\n ]+Responsável\s*[\n ]+Prazo\s*[\n ]+(.*?)(?=\n\s*Recomendação|\Z)",
        re.IGNORECASE | re.DOTALL,
    )

    for m in pattern.finditer(all_text or ""):
        block = normalize_text(m.group(1))
        lines = [x.strip() for x in block.split("\n") if x.strip()]

        prazo = None
        resp = None

        for i, ln in enumerate(lines):
            iso = try_parse_date_any(ln)
            if iso:
                prazo = iso
                if i - 1 >= 0:
                    resp = lines[i - 1]
                break

        recs.append({
            "recomendacao": block,
            "responsavel": resp,
            "prazo": prazo,
            "raw_block": block,
        })

    return recs

def extract_findings_minimal(all_text: str) -> List[Dict[str, Any]]:
    t = all_text.upper()

    sec = None
    for kw in ["CONSTATAÇÕES", "CONSTATAÇÃO", "NÃO CONFORMIDADES", "NAO CONFORMIDADES"]:
        idx = t.find(kw)
        if idx >= 0:
            sec = all_text[idx:]
            break

    if not sec:
        return []

    parts = re.split(r"\n\s*(\d+\.\d+)\s*\n", normalize_text(sec))

    findings = []

    if len(parts) < 3:
        findings.append({
            "finding_id": "F001",
            "tipo": "nao_conformidade",
            "constatacao_texto": normalize_text(sec),
        })
        return findings

    for i in range(1, len(parts) - 1, 2):
        findings.append({
            "finding_id": parts[i],
            "tipo": "nao_conformidade",
            "constatacao_texto": normalize_text(parts[i + 1]),
        })

    return findings

# ----------------------------
# Data model
# ----------------------------

@dataclass
class ParsedReport:
    source_uri: str
    aud_code: Optional[str]
    objetivo: Optional[str]
    risco_processo: Optional[str]
    escopo: Optional[str]
    alcance: Optional[str]
    cronograma: Optional[Dict[str, Any]]
    classification: Optional[str]
    recommendations: List[Dict[str, Any]]
    findings: List[Dict[str, Any]]

# ----------------------------
# Orchestration
# ----------------------------

def parse_report(source_uri: str, doc: Dict[str, Any]) -> ParsedReport:

    pages = extract_pages_or_fallback(doc)
    all_text = normalize_text("\n\n".join(pages))

    aud_code = find_aud_code(all_text)

    proc = extract_obj_risk_scope_reach_schedule(all_text)
    classification = extract_classification(all_text)
    recs = extract_recommendations(all_text)
    findings = extract_findings_minimal(all_text)

    return ParsedReport(
        source_uri=source_uri,
        aud_code=aud_code,
        objetivo=proc["objetivo"],
        risco_processo=proc["risco_processo"],
        escopo=proc["escopo"],
        alcance=proc["alcance"],
        cronograma=proc["cronograma"],
        classification=classification,
        recommendations=recs,
        findings=findings,
    )

# ----------------------------
# CLI
# ----------------------------

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input-dir", type=str)
    ap.add_argument("--input-file", type=str)
    ap.add_argument("--output-dir", type=str, required=True)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()

    files = []

    if args.input_file:
        files = [Path(args.input_file)]
    elif args.input_dir:
        files = list(Path(args.input_dir).rglob("*.json"))
    else:
        print("Informe --input-file ou --input-dir")
        sys.exit(1)

    Path(args.output_dir).mkdir(parents=True, exist_ok=True)

    for fp in files:
        with open(fp, "r", encoding="utf-8") as f:
            doc = json.load(f)

        parsed = parse_report(str(fp), doc)

        if args.debug:
            print("----", fp.name)
            print("AUD:", parsed.aud_code)
            print("OBJETIVO:", parsed.objetivo[:200] if parsed.objetivo else None)
            print("RISCO:", parsed.risco_processo[:200] if parsed.risco_processo else None)
            print("ESCOPO:", parsed.escopo[:200] if parsed.escopo else None)
            print("ALCANCE:", parsed.alcance[:200] if parsed.alcance else None)
            print("CRONOGRAMA:", parsed.cronograma)

    print("DONE")

if __name__ == "__main__":
    main()