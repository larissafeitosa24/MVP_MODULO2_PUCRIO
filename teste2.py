def _match_heading_base(norm_line: str, variant: str) -> bool:
    """Match 'exato' + variações com ':' e letras espaçadas + prefixo numérico."""
    v = _canon_heading(variant)

    # exato (com/sem :)
    if norm_line == v or norm_line == v + ":":
        return True

    # versão sem espaços (c r o n o g r a m a)
    if norm_line.replace(" ", "") in {v.replace(" ", ""), (v.replace(" ", "") + ":")}:
        return True

    # prefixo numérico: "2 risco", "02 risco", "2. risco" (canon_heading remove pontuação)
    if re.match(rf"^\d+\s+{re.escape(v)}(:|$)", norm_line):
        return True

    return False


def _match_heading_start(norm_line: str, variant: str) -> bool:
    """
    Match para INÍCIO da seção:
    aceita 'Risco / ...', 'Risco: ...', 'Risco - ...' (texto na mesma linha).
    """
    if _match_heading_base(norm_line, variant):
        return True
    v = _canon_heading(variant)

    # aceita título com conteúdo na mesma linha
    if norm_line.startswith(v + " ") or norm_line.startswith(v + ":") or norm_line.startswith(v + "-") or norm_line.startswith(v + "/"):
        return True

    # aceita numeração com conteúdo: "2 risco ..." / "02 risco ..."
    if re.match(rf"^\d+\s+{re.escape(v)}(\s|:|-|/)", norm_line):
        return True

    return False


def _match_heading_end(norm_line: str, variant: str) -> bool:
    """
    Match para FIM (delimitador):
    NÃO aceita texto na mesma linha.
    Só encerra quando a linha é 'pura' (Risco / Risco:).
    """
    return _match_heading_base(norm_line, variant)
    
    
    
    def _find_heading_indices(text: str, start_variants: List[str], end_variants: List[str]) -> Optional[Tuple[int, int]]:
    lines = (text or "").split("\n")
    norms = [_canon_heading(ln) for ln in lines]

    # localizar start
    start_i = None
    for i, (raw, n) in enumerate(zip(lines, norms)):
        if not _is_heading_candidate(raw):
            continue
        for sv in start_variants:
            if _match_heading_start(n, sv):   # <-- AQUI (start)
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
            if _match_heading_end(n, ev):     # <-- AQUI (end)
                end_i = j
                break
        if end_i != len(lines):
            break

    return start_i, end_i