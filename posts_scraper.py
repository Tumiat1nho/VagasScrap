"""
posts_scraper.py - Busca vagas anunciadas em posts de redes sociais
via DuckDuckGo Search (sem login, sem Selenium).

Plataformas suportadas:
  linkedin_posts  - Posts de recrutadores no LinkedIn (não o job board)
  twitter         - Posts no Twitter/X
  instagram       - Posts no Instagram
  facebook        - Posts no Facebook
"""

from __future__ import annotations

import re
import time
from datetime import datetime

import pandas as pd

# ── Configuração de plataformas ───────────────────────────────────────────────

PLATFORMS: dict[str, dict] = {
    "linkedin_posts": {
        "label": "LinkedIn Posts",
        # Foca em posts e artigos; exclui explicitamente /jobs/ para evitar
        # colisão de URLs com os resultados do JobSpy
        "site_filter": "(site:linkedin.com/posts OR site:linkedin.com/pulse) -site:linkedin.com/jobs",
    },
    "twitter": {
        "label": "Twitter / X",
        "site_filter": "site:twitter.com OR site:x.com",
    },
    "instagram": {
        "label": "Instagram",
        "site_filter": "site:instagram.com",
    },
    "facebook": {
        "label": "Facebook",
        "site_filter": "site:facebook.com",
    },
}

# Frases que indicam que o post é sobre uma vaga
_JOB_PHRASES = [
    "estamos contratando",
    "vaga aberta",
    "vagas abertas",
    "oportunidade de emprego",
    "estou contratando",
    "vagas disponíveis",
    "processo seletivo",
    "we are hiring",
    "job opening",
    "now hiring",
    "join our team",
    "looking for",
    "procurando profissional",
    "buscamos profissional",
]

_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}", re.IGNORECASE
)


def _build_query(search_term: str, location: str, site_filter: str) -> str:
    phrases = " OR ".join(f'"{p}"' for p in _JOB_PHRASES[:6])
    loc = location.split(",")[0].strip()  # "Rio de Janeiro, Brazil" → "Rio de Janeiro"
    return f"({phrases}) {search_term} {loc} {site_filter}"


def _extract_email(text: str) -> str | None:
    found = _EMAIL_RE.findall(text or "")
    filtered = [e for e in found if not e.endswith((".png", ".jpg", ".gif", ".svg"))]
    return filtered[0] if filtered else None


def search_posts(
    search_term: str,
    location: str = "Brazil",
    platforms: list[str] | None = None,
    results_per_platform: int = 15,
    verbose: bool = True,
) -> pd.DataFrame:
    """
    Busca posts de recrutadores sobre vagas em redes sociais via DuckDuckGo.

    Args:
        search_term: Termo de busca (ex: "desenvolvedor python").
        location: Localização para refinar a busca.
        platforms: Lista de plataformas. Padrão: linkedin_posts, twitter.
        results_per_platform: Máximo de resultados por plataforma.
        verbose: Imprimir progresso.

    Returns:
        DataFrame no mesmo formato das vagas do JobSpy, com coluna
        'site' prefixada por 'post_' (ex: 'post_linkedin_posts').
    """
    try:
        from ddgs import DDGS
    except ImportError:
        raise ImportError(
            "duckduckgo-search não está instalado. Execute: pip install duckduckgo-search"
        )

    if platforms is None:
        platforms = ["linkedin_posts", "twitter"]

    invalid = [p for p in platforms if p not in PLATFORMS]
    if invalid:
        raise ValueError(f"Plataformas inválidas: {invalid}. Válidas: {list(PLATFORMS)}")

    all_rows: list[dict] = []

    for platform_key in platforms:
        cfg = PLATFORMS[platform_key]
        label = cfg["label"]

        if verbose:
            print(f"  -> Buscando posts em {label}...", end=" ", flush=True)

        query = _build_query(search_term, location, cfg["site_filter"])

        try:
            with DDGS() as ddgs:
                results = list(ddgs.text(query, max_results=results_per_platform))

            count = len(results)
            if verbose:
                print(f"{count} posts encontrados")

            for r in results:
                snippet = r.get("body", "") or ""
                title = r.get("title", "") or ""
                link = r.get("href", "") or ""
                date_str = r.get("published", "") or ""

                # Tenta normalizar a data
                data_postagem = None
                if date_str:
                    try:
                        data_postagem = datetime.fromisoformat(
                            date_str.replace("Z", "+00:00")
                        ).strftime("%Y-%m-%d")
                    except ValueError:
                        data_postagem = date_str[:10] if len(date_str) >= 10 else None

                all_rows.append({
                    "site": f"post_{platform_key}",
                    "titulo": title,
                    "empresa": None,
                    "localizacao": None,
                    "remoto": False,
                    "tipo_vaga": "Post",
                    "salario_min": None,
                    "salario_max": None,
                    "moeda": None,
                    "data_postagem": data_postagem,
                    "link": link,
                    "emails": _extract_email(snippet),
                    "descricao": snippet,
                })

        except Exception as exc:
            if verbose:
                print(f"ERRO ({exc})")

        time.sleep(1.5)

    if not all_rows:
        return pd.DataFrame()

    return pd.DataFrame(all_rows)
