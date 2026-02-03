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