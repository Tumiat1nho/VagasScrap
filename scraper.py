"""
scraper.py - Lógica de scraping de vagas via python-jobspy
"""

from __future__ import annotations

import time
from typing import Optional

import pandas as pd

VALID_SITES = {"linkedin", "indeed", "glassdoor", "zip_recruiter"}

COLUMNS_RENAME = {
    "title": "titulo",
    "company": "empresa",
    "location": "localizacao",
    "description": "descricao",
    "job_url": "link",
    "date_posted": "data_postagem",
    "is_remote": "remoto",
    "min_amount": "salario_min",
    "max_amount": "salario_max",
    "currency": "moeda",
    "job_type": "tipo_vaga",
    "site": "site",
    "company_url": "url_empresa",
    "emails": "emails",
}

KEEP_COLUMNS = [
    "site",
    "titulo",
    "empresa",
    "localizacao",
    "remoto",
    "tipo_vaga",
    "salario_min",
    "salario_max",
    "moeda",
    "data_postagem",
    "link",
    "emails",       # usado por recruiter.py para extrair email_recrutador
    "descricao",
]


def search_jobs(
    search_term: str,
    location: str = "Brazil",
    sites: Optional[list[str]] = None,
    results_wanted: int = 25,
    hours_old: int = 168,
    country: str = "Brazil",
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Busca vagas de emprego em múltiplos sites via JobSpy.

    Args:
        search_term: Termo de busca (ex: "desenvolvedor python").
        location: Localização (ex: "Rio de Janeiro, Brazil").
        sites: Lista de sites para buscar. Padrão: linkedin, indeed, glassdoor.
        results_wanted: Número de resultados por site.
        hours_old: Filtrar vagas postadas nas últimas N horas.
        country: País para Indeed (ex: "Brazil", "USA").
        verbose: Imprimir progresso no terminal.

    Returns:
        DataFrame com as vagas encontradas.
    """
    try:
        from jobspy import scrape_jobs
    except ImportError:
        raise ImportError(
            "python-jobspy não está instalado. Execute: pip install python-jobspy"
        )

    if sites is None:
        sites = ["linkedin", "indeed", "glassdoor"]

    invalid = [s for s in sites if s not in VALID_SITES]
    if invalid:
        raise ValueError(
            f"Sites inválidos: {invalid}. Válidos: {sorted(VALID_SITES)}"
        )

    if verbose:
        print(f"\nBuscando: '{search_term}' em {location}")
        print(f"Sites: {', '.join(sites)} | Resultados por site: {results_wanted}")
        print(f"Vagas postadas nas últimas {hours_old}h\n")

    all_frames: list[pd.DataFrame] = []

    for site in sites:
        if verbose:
            print(f"  -> Scraping {site}...", end=" ", flush=True)
        try:
            df = scrape_jobs(
                site_name=[site],
                search_term=search_term,
                location=location,
                results_wanted=results_wanted,
                hours_old=hours_old,
                country_indeed=country,
                linkedin_fetch_description=True,
            )
            count = len(df) if df is not None else 0
            if verbose:
                print(f"{count} vagas encontradas")
            if df is not None and not df.empty:
                all_frames.append(df)
        except Exception as exc:
            if verbose:
                print(f"ERRO ({exc})")
        time.sleep(1)

    if not all_frames:
        if verbose:
            print("\nNenhuma vaga encontrada.")
        return pd.DataFrame()

    combined = pd.concat(all_frames, ignore_index=True, sort=False)
    combined = _normalize_dataframe(combined)

    if verbose:
        print(f"\nTotal bruto: {len(combined)} vagas")

    return combined


def _normalize_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    """Renomeia colunas e mantém apenas as relevantes."""
    df = df.rename(columns=COLUMNS_RENAME)

    keep = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[keep].copy()

    if "data_postagem" in df.columns:
        df["data_postagem"] = pd.to_datetime(
            df["data_postagem"], errors="coerce"
        ).dt.strftime("%Y-%m-%d")

    if "remoto" in df.columns:
        df["remoto"] = df["remoto"].fillna(False).astype(bool)

    return df
