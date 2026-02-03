def _is_heading_candidate(raw_line: str) -> bool:
    s = (raw_line or "").strip()
    if not s:
        return False

    # se termina como frase, provavelmente não é cabeçalho
    if s.endswith((",", ";", ".", ")", "…")):
        return False

    # precisa começar com maiúscula ou estar em caixa alta
    if not (s[0].isupper() or s.isupper()):
        return False

    # ✅ NOVO: cabeçalho "puro" costuma ser curto (evita cortar quando aparece "Escopo ..." no meio)
    # (ex.: "Escopo" / "Alcance" / "Cronograma" / "Risco", etc.)
    if len(s) > 28:
        return False

    # ✅ NOVO: se tiver muitos tokens, é frase/parágrafo, não cabeçalho
    if len(s.split()) > 4:
        return False

    return True