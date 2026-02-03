def _make_heading_regex(words: List[str]) -> re.Pattern:
    """
    Cabeçalho como LINHA PURA (do jeito que está no JSON: \nRisco\n).
    Aceita:
      - "Risco"
      - "Risco:"
      - "2 Risco" / "2. Risco" / "02 – Risco"
      - letras espaçadas (O B J E T I V O), se aparecer como linha isolada
    NÃO aceita "Risco / bla bla" ou "Risco: texto..." na mesma linha,
    porque isso costuma ser fonte de corte indevido no meio do parágrafo.
    """
    alts = []
    for w in words:
        w_up = _strip_accents_lower(w).upper()
        alts.append(re.escape(w_up))
        alts.append(_spaced_letters_pattern(w_up))

    body = "|".join(alts)

    # LINHA PURA:
    # ^ [numeração opcional] [texto do heading] [: opcional] $
    pat = (
        rf"(?im)^\s*"
        rf"(?:\d{{1,2}}(?:\.\d{{1,2}})*)?\s*"
        rf"(?:[-–]?\s*)?"
        rf"(?:{body})\s*"
        rf":?\s*$"
    )
    return re.compile(pat)
    
    def extract_section_span(text: str, start_words: List[str], end_words: List[str]) -> Optional[str]:
    t = text or ""
    start_re = _make_heading_regex(start_words)
    end_re = _make_heading_regex(end_words)

    m = start_re.search(t)
    if not m:
        return None

    # começa DEPOIS da linha do cabeçalho
    start_pos = m.end()

    m2 = end_re.search(t, pos=start_pos)
    end_pos = m2.start() if m2 else len(t)

    block = t[start_pos:end_pos]
    block = normalize_text(block).strip(":- \n\t")
    return block or None
    
    # conserta datas quebradas tipo "23/07/2\n025"
cr_fix = re.sub(
    r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d)\s*\n\s*(\d{2,3})",
    r"\1\2",
    cr_text,
)
dates = DATE_RE.findall(cr_fix)