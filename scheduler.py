"""
scheduler.py - Agendamento automático de buscas via APScheduler
"""

from __future__ import annotations

import os
from datetime import datetime
from pathlib import Path
from typing import Any

import yaml

from filters import apply_all_filters
from scraper import search_jobs


def load_config(config_path: str = "config.yaml") -> dict[str, Any]:
    """Carrega o arquivo de configuração YAML."""
    path = Path(config_path)
    if not path.exists():
        raise FileNotFoundError(f"Arquivo de configuração não encontrado: {config_path}")

    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f)


def run_all_searches(config: dict[str, Any]) -> None:
    """Executa todas as buscas definidas na config e salva os resultados."""
    import pandas as pd

    searches = config.get("searches", [])
    output_cfg = config.get("output", {})
    out_dir = Path(output_cfg.get("directory", "output"))
    out_dir.mkdir(parents=True, exist_ok=True)
    fmt = output_cfg.get("format", "csv").lower()
    prefix = output_cfg.get("filename_prefix", "vagas")

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

    all_results: list = []

    for i, search in enumerate(searches, 1):
        print(f"\n[{i}/{len(searches)}] Busca: '{search['search_term']}'")

        df = search_jobs(
            search_term=search["search_term"],
            location=search.get("location", "Brazil"),
            sites=search.get("sites"),
            results_wanted=search.get("results_wanted", 25),
            hours_old=search.get("hours_old", 168),
            verbose=True,
        )

        if df.empty:
            print("  Nenhuma vaga encontrada nesta busca.")
            continue

        df = apply_all_filters(
            df,
            skills=search.get("skills"),
            remote_only=search.get("remote_only", False),
        )

        print(f"  Vagas após filtros: {len(df)}")
        all_results.append(df)

    if not all_results:
        print("\nNenhuma vaga encontrada em todas as buscas.")
        return

    combined = pd.concat(all_results, ignore_index=True)
    combined = combined.drop_duplicates(subset=["link"], keep="first")

    filename = f"{prefix}_{timestamp}"

    if fmt == "excel":
        out_path = out_dir / f"{filename}.xlsx"
        combined.to_excel(out_path, index=False)
    else:
        out_path = out_dir / f"{filename}.csv"
        combined.to_csv(out_path, index=False, encoding="utf-8-sig")

    print(f"\nTotal de vagas salvas: {len(combined)}")
    print(f"Arquivo: {out_path.resolve()}")


def start_scheduler(config_path: str = "config.yaml") -> None:
    """
    Inicia o agendador. Executa imediatamente e depois no intervalo configurado.
    """
    try:
        from apscheduler.schedulers.blocking import BlockingScheduler
    except ImportError:
        raise ImportError("apscheduler não está instalado. Execute: pip install apscheduler")

    config = load_config(config_path)
    schedule_cfg = config.get("schedule", {})
    interval_hours = schedule_cfg.get("interval_hours", 24)

    print(f"Agendamento iniciado — intervalo: {interval_hours}h")
    print("Executando busca inicial...\n")

    run_all_searches(config)

    scheduler = BlockingScheduler()
    scheduler.add_job(
        run_all_searches,
        trigger="interval",
        hours=interval_hours,
        args=[config],
        id="vagas_search",
    )

    next_run = scheduler.get_jobs()[0].next_run_time if scheduler.get_jobs() else None
    print(f"\nPróxima execução agendada para: {next_run}")
    print("Pressione Ctrl+C para encerrar.\n")

    try:
        scheduler.start()
    except (KeyboardInterrupt, SystemExit):
        print("\nAgendador encerrado.")
