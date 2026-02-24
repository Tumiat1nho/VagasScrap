"""
extra_scrapers.py - Scrapers adicionais: Gupy, RemoteOK, Vagas.com,
                    GeekHunter, Trampos.co, Reddit e Workana.

Cada função retorna um DataFrame no mesmo esquema do scraper principal.
Todos os erros são capturados internamente — nunca levantam exceção.
"""

from __future__ import annotations

import re
import time
from datetime import datetime, timezone

import pandas as pd
import requests
from bs4 import BeautifulSoup

_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
}

_EMPTY_ROW: dict = {
    "site": None,
    "titulo": None,
    "empresa": None,
    "localizacao": None,
    "remoto": False,
    "tipo_vaga": None,
    "salario_min": None,
    "salario_max": None,
    "moeda": None,
    "data_postagem": None,
    "link": None,
    "emails": None,
    "descricao": None,
}


def _row(**kwargs) -> dict:
    return {**_EMPTY_ROW, **kwargs}


def _session() -> requests.Session:
    s = requests.Session()
    s.headers.update(_HEADERS)
    return s


def _date_from_iso(raw: str | None) -> str | None:
    if not raw:
        return None
    return str(raw)[:10]


def _date_from_epoch(ts) -> str | None:
    try:
        return datetime.fromtimestamp(float(ts), tz=timezone.utc).strftime("%Y-%m-%d")
    except Exception:
        return None


# ─────────────────────────────────────────────────────────────────────────────
#  Gupy  (API pública)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_gupy(search_term: str, results_wanted: int = 40, verbose: bool = True) -> pd.DataFrame:
    """Vagas via API pública do Gupy (portal usado por centenas de empresas BR)."""
    if verbose:
        print(f"  -> Scraping Gupy...", end=" ", flush=True)
    try:
        url = "https://portal.api.gupy.io/api/v1/jobs"
        rows, offset, limit = [], 0, min(results_wanted, 40)

        while len(rows) < results_wanted:
            r = requests.get(
                url,
                params={"jobName": search_term, "limit": limit, "offset": offset},
                headers=_HEADERS,
                timeout=15,
            )
            r.raise_for_status()
            body = r.json()
            jobs = body.get("data", [])
            if not jobs:
                break

            for job in jobs:
                city  = job.get("city") or ""
                state = job.get("state") or ""
                loc   = ", ".join(p for p in [city, state, "Brazil"] if p)
                wp    = (job.get("workplaceType") or "").lower()
                comp  = job.get("company") or {}

                rows.append(_row(
                    site="gupy",
                    titulo=job.get("name"),
                    empresa=comp.get("name") if isinstance(comp, dict) else str(comp),
                    localizacao=loc,
                    remoto=wp in ("remote", "home_office", "hybrid"),
                    tipo_vaga=job.get("type"),
                    moeda="BRL",
                    data_postagem=_date_from_iso(job.get("publishedDate")),
                    link=job.get("jobUrl"),
                    descricao=job.get("description"),
                ))

            total = body.get("total", 0)
            offset += limit
            if offset >= total or offset >= results_wanted:
                break
            time.sleep(0.5)

        if verbose:
            print(f"{len(rows)} vagas encontradas")
        return pd.DataFrame(rows[:results_wanted]) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  RemoteOK  (API pública JSON)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_remoteok(search_term: str, results_wanted: int = 30, verbose: bool = True) -> pd.DataFrame:
    """Vagas remotas via API pública do RemoteOK."""
    if verbose:
        print(f"  -> Scraping RemoteOK...", end=" ", flush=True)
    try:
        tag = "+".join(search_term.lower().split())
        r = requests.get(
            f"https://remoteok.com/api?tags={tag}",
            headers=_HEADERS,
            timeout=15,
        )
        r.raise_for_status()
        data = r.json()
        jobs = [j for j in data if isinstance(j, dict) and j.get("id") and j.get("position")]

        rows = []
        for job in jobs[:results_wanted]:
            rows.append(_row(
                site="remoteok",
                titulo=job.get("position"),
                empresa=job.get("company"),
                localizacao=job.get("location") or "Remote",
                remoto=True,
                salario_min=job.get("salary_min") or None,
                salario_max=job.get("salary_max") or None,
                moeda="USD" if job.get("salary_min") else None,
                data_postagem=_date_from_epoch(job.get("date")),
                link=job.get("url") or f"https://remoteok.com/remote-jobs/{job.get('id')}",
                emails=job.get("email"),
                descricao=job.get("description"),
            ))

        if verbose:
            print(f"{len(rows)} vagas encontradas")
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Vagas.com  (HTML scraping)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_vagascom(search_term: str, results_wanted: int = 30, verbose: bool = True) -> pd.DataFrame:
    """Vagas do maior job board brasileiro."""
    if verbose:
        print(f"  -> Scraping Vagas.com...", end=" ", flush=True)
    try:
        slug = "-".join(search_term.lower().split())
        r = _session().get(
            f"https://www.vagas.com.br/vagas-de-{slug}",
            timeout=15,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []

        for li in soup.select("li.vaga")[:results_wanted]:
            title_el = li.select_one("h2.cargo a, .cargo a")
            if not title_el:
                continue
            href = title_el.get("href", "")
            company_el = li.select_one(".empresa")
            loc_el     = li.select_one(".localidade, .cidade")
            date_el    = li.select_one("time, .data-publicacao")
            date_raw   = (date_el.get("datetime") or date_el.get_text(strip=True)) if date_el else None

            rows.append(_row(
                site="vagas.com",
                titulo=title_el.get_text(strip=True),
                empresa=company_el.get_text(strip=True) if company_el else None,
                localizacao=loc_el.get_text(strip=True) if loc_el else None,
                moeda="BRL",
                data_postagem=_date_from_iso(date_raw),
                link=f"https://www.vagas.com.br{href}" if href.startswith("/") else href,
            ))

        if verbose:
            print(f"{len(rows)} vagas encontradas")
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  GeekHunter  (API JSON)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_geekHunter(search_term: str, results_wanted: int = 30, verbose: bool = True) -> pd.DataFrame:
    """Vagas de tech do GeekHunter via endpoint JSON."""
    if verbose:
        print(f"  -> Scraping GeekHunter...", end=" ", flush=True)
    try:
        r = _session().get(
            "https://www.geekHunter.com.br/api/v1/opportunities/public_index",
            params={"q": search_term, "per_page": results_wanted, "page": 1},
            timeout=15,
        )
        r.raise_for_status()
        body = r.json()
        jobs = (
            body.get("opportunities")
            or body.get("data")
            or (body if isinstance(body, list) else [])
        )

        rows = []
        for job in jobs[:results_wanted]:
            if not isinstance(job, dict):
                continue
            comp = job.get("company") or {}
            rows.append(_row(
                site="geekHunter",
                titulo=job.get("title") or job.get("name"),
                empresa=comp.get("name") if isinstance(comp, dict) else str(comp or ""),
                localizacao=job.get("city") or job.get("location"),
                remoto=bool(job.get("remote", False)),
                tipo_vaga=job.get("contract_type") or job.get("job_type"),
                salario_min=job.get("salary_from") or job.get("min_salary"),
                salario_max=job.get("salary_to") or job.get("max_salary"),
                moeda="BRL",
                data_postagem=_date_from_iso(job.get("created_at")),
                link=(
                    job.get("url")
                    or f"https://www.geekHunter.com.br/vagas/{job.get('id', '')}"
                ),
                descricao=job.get("description"),
            ))

        if verbose:
            print(f"{len(rows)} vagas encontradas")
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Trampos.co  (HTML scraping)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_trampos(search_term: str, results_wanted: int = 30, verbose: bool = True) -> pd.DataFrame:
    """Vagas de tech/criativo do Trampos.co."""
    if verbose:
        print(f"  -> Scraping Trampos.co...", end=" ", flush=True)
    try:
        r = _session().get(
            "https://trampos.co/oportunidades",
            params={"term": search_term},
            timeout=15,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []

        selectors = [
            "article.opportunity", ".opportunity-item",
            "li.opportunity", ".job-item", ".vaga-item",
        ]
        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if cards:
                break

        for card in cards[:results_wanted]:
            title_el = card.select_one("h2, h3, .title, .cargo, .position")
            if not title_el:
                continue
            link_el = card.select_one("a")
            href = link_el.get("href", "") if link_el else ""
            company_el  = card.select_one(".company, .empresa")
            location_el = card.select_one(".location, .localidade, .cidade")
            title_text  = title_el.get_text(strip=True)

            rows.append(_row(
                site="trampos.co",
                titulo=title_text,
                empresa=company_el.get_text(strip=True) if company_el else None,
                localizacao=location_el.get_text(strip=True) if location_el else None,
                remoto=bool(re.search(r"\bremoto\b|\bremote\b", title_text, re.I)),
                moeda="BRL",
                link=f"https://trampos.co{href}" if href.startswith("/") else href,
            ))

        if verbose:
            print(f"{len(rows)} vagas encontradas")
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Reddit  (JSON API pública — sem autenticação)
# ─────────────────────────────────────────────────────────────────────────────

_REDDIT_SUBS = ["brdev", "remotebrazil", "devBrasil"]
_JOB_KWS     = ["vaga", "contrat", "hiring", "oportunidade", "emprego", "trabalho", "job"]


def scrape_reddit(search_term: str, results_wanted: int = 25, verbose: bool = True) -> pd.DataFrame:
    """Posts de vagas nos subreddits brasileiros de devs."""
    if verbose:
        print(f"  -> Scraping Reddit (r/brdev ...)...", end=" ", flush=True)
    rows = []
    try:
        sess = _session()
        sess.headers["User-Agent"] = "VagasScrap/1.0 (job search aggregator)"

        for sub in _REDDIT_SUBS:
            if len(rows) >= results_wanted:
                break
            r = sess.get(
                f"https://www.reddit.com/r/{sub}/search.json",
                params={
                    "q": f"vaga {search_term}",
                    "restrict_sr": 1,
                    "sort": "new",
                    "limit": 25,
                    "t": "month",
                },
                timeout=15,
            )
            if r.status_code != 200:
                continue

            for post in r.json().get("data", {}).get("children", []):
                d = post.get("data", {})
                title = d.get("title", "")
                body  = d.get("selftext", "")
                combined = (title + " " + body).lower()

                if not any(kw in combined for kw in _JOB_KWS):
                    continue

                link = d.get("url") or f"https://reddit.com{d.get('permalink', '')}"
                rows.append(_row(
                    site=f"reddit_r/{sub}",
                    titulo=title,
                    remoto=bool(re.search(r"\bremoto\b|\bremote\b", combined)),
                    tipo_vaga="Post",
                    data_postagem=_date_from_epoch(d.get("created_utc")),
                    link=link,
                    descricao=body[:2000] or None,
                ))

            time.sleep(1)

        if verbose:
            print(f"{len(rows)} posts encontrados")
        return pd.DataFrame(rows[:results_wanted]) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Workana  (HTML scraping)
# ─────────────────────────────────────────────────────────────────────────────

def scrape_workana(search_term: str, results_wanted: int = 25, verbose: bool = True) -> pd.DataFrame:
    """Projetos/vagas freelance do Workana (BR/LATAM)."""
    if verbose:
        print(f"  -> Scraping Workana...", end=" ", flush=True)
    try:
        r = _session().get(
            "https://www.workana.com/jobs",
            params={"search": search_term, "language": "pt"},
            timeout=15,
        )
        soup = BeautifulSoup(r.text, "html.parser")
        rows = []

        selectors = [
            "article.project", ".project-item",
            "li.project", ".job-item",
        ]
        cards = []
        for sel in selectors:
            cards = soup.select(sel)
            if cards:
                break

        for card in cards[:results_wanted]:
            title_el = card.select_one("h2 a, h3 a, .project-title a, .title a")
            if not title_el:
                continue
            href   = title_el.get("href", "")
            budget = card.select_one(".budget, .price, .valor")

            rows.append(_row(
                site="workana",
                titulo=title_el.get_text(strip=True),
                remoto=True,
                tipo_vaga="Freelance",
                moeda="BRL",
                link=f"https://www.workana.com{href}" if href.startswith("/") else href,
                descricao=budget.get_text(strip=True) if budget else None,
            ))

        if verbose:
            print(f"{len(rows)} projetos encontrados")
        return pd.DataFrame(rows) if rows else pd.DataFrame()

    except Exception as exc:
        if verbose:
            print(f"ERRO ({exc})")
        return pd.DataFrame()


# ─────────────────────────────────────────────────────────────────────────────
#  Dispatcher
# ─────────────────────────────────────────────────────────────────────────────

SCRAPERS: dict[str, tuple] = {
    "gupy":       (scrape_gupy,       "Gupy"),
    "remoteok":   (scrape_remoteok,   "RemoteOK"),
    "vagascom":   (scrape_vagascom,   "Vagas.com"),
    "geekHunter": (scrape_geekHunter, "GeekHunter"),
    "trampos":    (scrape_trampos,    "Trampos.co"),
    "reddit":     (scrape_reddit,     "Reddit r/brdev"),
    "workana":    (scrape_workana,    "Workana"),
}


def search_extra(
    search_term: str,
    sources: list[str] | None = None,
    results_per_source: int = 25,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Executa todos os scrapers extras selecionados e retorna DataFrame unificado.

    Args:
        search_term: Termo de busca.
        sources: Chaves das fontes a usar (None = todas).
        results_per_source: Máximo de resultados por fonte.
        verbose: Imprimir progresso.
    """
    if sources is None:
        sources = list(SCRAPERS.keys())

    frames: list[pd.DataFrame] = []
    for key in sources:
        fn, _ = SCRAPERS.get(key, (None, None))
        if fn is None:
            continue
        try:
            df = fn(search_term, results_wanted=results_per_source, verbose=verbose)
            if df is not None and not df.empty:
                frames.append(df)
        except Exception:
            pass
        time.sleep(0.5)

    if not frames:
        return pd.DataFrame()
    return pd.concat(frames, ignore_index=True, sort=False)
