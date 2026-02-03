def extract_obj_risk_scope_reach_schedule(all_text: str) -> Dict[str, Any]:
    t = all_text or ""

    objetivo = extract_by_loop(
        t,
        start_titles=["Objetivo"],
        end_titles=["Risco", "Riscos"],
    )

    risco = extract_by_loop(
        t,
        start_titles=["Risco", "Riscos"],
        end_titles=["Escopo", "Alcance", "Cronograma"],
    )

    escopo = extract_by_loop(
        t,
        start_titles=["Escopo"],
        end_titles=[
            "Alcance",
            "Cronograma",
            "Conclusão", "Conclusao",
            "Avaliação", "Avaliacao",
            "Classificação", "Classificacao",
        ],
    )

    alcance = extract_by_loop(
        t,
        start_titles=["Alcance"],
        end_titles=[
            "Cronograma",
            "Conclusão", "Conclusao",
            "Avaliação", "Avaliacao",
            "Classificação", "Classificacao",
        ],
    )

    # Cronograma (mantém sua lógica de datas depois)
    cronograma_text = extract_by_loop(
        t,
        start_titles=["Cronograma"],
        end_titles=[
            "Auditoria Realizada",
            "Contexto",
            "Conclusão", "Conclusao",
            "Classificação", "Classificacao",
            "Constatação", "Constatacao",
            "Avaliação", "Avaliacao",
        ],
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