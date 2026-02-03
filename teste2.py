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
    
    # ✅ NOVO: conserta datas quebradas tipo "23/07/2\n025"
cro_block = re.sub(
    r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d)\s*\n\s*(\d{2,3})",
    r"\1\2",
    cro_block,
)