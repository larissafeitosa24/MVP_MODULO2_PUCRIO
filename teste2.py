def _find_heading_indices(text: str, start_variants: List[str], end_variants: List[str]) -> Optional[Tuple[int, int]]:
    lines = (text or "").split("\n")

    def is_pure_heading(line: str, variants: List[str]) -> bool:
        canon = _canon_heading(line)
        for v in variants:
            if canon == _canon_heading(v):
                return True
        return False

    # achar início
    start_i = None
    for i, ln in enumerate(lines):
        if is_pure_heading(ln, start_variants):
            start_i = i
            break

    if start_i is None:
        return None

    # achar fim (somente quando aparecer o PRÓXIMO título puro)
    end_i = len(lines)
    for j in range(start_i + 1, len(lines)):
        if is_pure_heading(lines[j], end_variants):
            end_i = j
            break

    return start_i, end_i