def extract_section_span(text: str, start_words: List[str], end_words: List[str]) -> Optional[str]:
    t = text or ""

    start_re = _make_heading_regex(start_words)
    end_re = _make_heading_regex(end_words)

    m = start_re.search(t)
    if not m:
        return None

    start_pos = m.end()

    # procurar o PRÓXIMO heading real (com quebra de linha estrutural)
    end_pos = len(t)

    for m2 in end_re.finditer(t, pos=start_pos):

        # pega contexto em volta do possível heading
        before = t[max(0, m2.start()-3):m2.start()]
        after  = t[m2.end():m2.end()+3]

        # heading real costuma ter quebra de linha antes OU depois
        before_ok = "\n" in before
        after_ok  = "\n" in after

        if before_ok or after_ok:
            end_pos = m2.start()
            break

    block = t[start_pos:end_pos]
    block = normalize_text(block).strip(":- \n\t")

    return block or None