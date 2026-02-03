def _find_heading_indices(text: str, start_variants: List[str], end_variants: List[str]) -> Optional[Tuple[int, int]]:
    lines = (text or "").split("\n")

    def canon(x: str) -> str:
        return _canon_heading(x)

    def is_heading(line: str, variants: List[str]) -> bool:
        c = canon(line)
        for v in variants:
            if c == canon(v):
                return True
        return False

    # achar início (linha pura)
    start_i = None
    for i, ln in enumerate(lines):
        if is_heading(ln, start_variants):
            start_i = i
            break

    if start_i is None:
        return None

    # achar fim (próximo heading puro)
    end_i = len(lines)
    for j in range(start_i + 1, len(lines)):
        if is_heading(lines[j], end_variants):
            end_i = j
            break

    return start_i, end_i
    
    
    def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:
    t = all_text or ""

    objetivo = find_heading_block(
        t,
        start_variants=["Objetivo"],
        end_variants=["Risco", "Riscos"],
    )

    risco = find_heading_block(
        t,
        start_variants=["Risco", "Riscos"],
        end_variants=["Escopo", "Alcance", "Cronograma"],
    )

    escopo = find_heading_block(
        t,
        start_variants=["Escopo"],
        end_variants=[
            "Alcance",
            "Cronograma",
            "Conclusão", "Conclusao",
            "Avaliação", "Avaliacao",
            "Classificação", "Classificacao",
        ],
    )

    alcance = find_heading_block(
        t,
        start_variants=["Alcance"],
        end_variants=[
            "Cronograma",
            "Conclusão", "Conclusao",
            "Avaliação", "Avaliacao",
            "Classificação", "Classificacao",
        ],
    )

    # ---- Cronograma
    cronograma = None
    cr_idx = _find_heading_indices(
        t,
        start_variants=["Cronograma"],
        end_variants=[
            "Auditoria Realizada",
            "Contexto",
            "Conclusão", "Conclusao",
            "Classificação", "Classificacao",
            "Constatação", "Constatacao", "CONSTATAÇÕES",
            "Avaliação", "Avaliacao",
        ],
    )

    if cr_idx:
        start_i, end_i = cr_idx
        lines = t.split("\n")
        cro_block = "\n".join(lines[start_i + 1:end_i])

        # conserta datas quebradas
        cro_block = re.sub(
            r"(\d{1,2}[\/\-.]\d{1,2}[\/\-.]\d)\s*\n\s*(\d{2,3})",
            r"\1\2",
            cro_block,
        )

        dates = DATE_RE.findall(cro_block)

        def iso_date(s: str) -> Optional[str]:
            s = s.replace(".", "/").replace("-", "/")
            d, m, y = s.split("/")
            if len(y) == 2:
                y = "20" + y
            try:
                return datetime(int(y), int(m), int(d)).strftime("%Y-%m-%d")
            except:
                return None

        if len(dates) >= 3:
            cronograma = {
                "data_inicio_trabalho": iso_date(dates[0]),
                "draft_emitido": iso_date(dates[1]),
                "relatorio_final": iso_date(dates[2]),
            }
        else:
            cronograma = {"raw": normalize_text(cro_block)[:600]}

    return {
        "objetivo": objetivo,
        "risco_processo": risco,
        "escopo": escopo,
        "alcance": alcance,
        "cronograma": cronograma,
    }