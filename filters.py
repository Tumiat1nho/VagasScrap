"""
filters.py - Filtros pós-scraping para o DataFrame de vagas
"""

from __future__ import annotations

import re

import pandas as pd


def filter_by_skills(df: pd.DataFrame, skills: list[str]) -> pd.DataFrame:
    """
    Filtra vagas que contenham ao menos uma das skills no título ou descrição.

    Args:
        df: DataFrame de vagas.
        skills: Lista de skills (ex: ["python", "django", "fastapi"]).

    Returns:
        DataFrame filtrado.
    """
    if df.empty or not skills:
        return df

    pattern = "|".join(re.escape(s.strip()) for s in skills if s.strip())
    if not pattern:
        return df

    regex = re.compile(pattern, re.IGNORECASE)

    mask = pd.Series(False, index=df.index)

    for col in ("titulo", "descricao"):
        if col in df.columns:
            mask |= df[col].fillna("").astype(str).str.contains(regex)

    filtered = df[mask].copy()
    return filtered


def filter_remote(df: pd.DataFrame) -> pd.DataFrame:
    """
    Filtra apenas vagas marcadas como remotas.

    Args:
        df: DataFrame de vagas.

    Returns:
        DataFrame com apenas vagas remotas.
    """
    if df.empty:
        return df

    if "remoto" not in df.columns:
        return df

    return df[df["remoto"] == True].copy()


def deduplicate(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove vagas duplicadas com base na URL.

    Args:
        df: DataFrame de vagas.

    Returns:
        DataFrame sem duplicatas.
    """
    if df.empty:
        return df

    if "link" in df.columns:
        return df.drop_duplicates(subset=["link"], keep="first").copy()

    return df


SENIORITY_KEYWORDS: dict[str, list[str]] = {
    "trainee": [
        r"\btrainee\b",
        r"\bestagi[aá]ri[oa]?\b",          # estagiário / estagiária
        r"\best[aá]gio\b",                  # estágio / estagio
        r"\bstage\b",
        r"\bintern(?:ship)?\b",             # intern / internship
        r"\baprendiz\b",
        r"\bjovem\s+aprendiz\b",
        r"\bprograma\s+de\s+est[aá]gio\b",
    ],
    "junior": [
        r"\bj[uú]nior\b",
        r"\bjr\.?\b",
        r"\bentry[\s\-]?level\b",
        r"\brec[eé]m[\s\-]?formad[oa]\b",  # recém-formado/a
        r"\bsem\s+experi[eê]ncia\b",
        r"\bpouca\s+experi[eê]ncia\b",
        r"\biniciante\b",
        r"\bprimeiro\s+emprego\b",
        r"\bn[ií]vel\s+inicial\b",
        r"\bn[ií]vel\s+j[uú]nior\b",
        r"\b0\s*[aà]\s*2\s+anos?\b",        # 0 a 2 anos
        r"\bat[eé]\s+2\s+anos?\b",          # até 2 anos
        r"\bat[eé]\s+1\s+ano\b",            # até 1 ano
        r"\bjovem\s+profissional\b",
        r"\bjovem\s+talento\b",
        r"\bearly[\s\-]?career\b",
        r"\bformand[oa]\b",
        r"\bjunior\s+developer\b",
        r"\bjunior\s+dev\b",
        r"\bdev\s+j[uú]nior\b",
    ],
    "pleno": [
        r"\bpleno\b",
        r"\bnível\s+pleno\b",
        r"\bmid[\s\-]?level\b",
        r"\bmiddle\b",
        r"\bintermedi[aá]ri[oa]\b",
        r"\b[23]\s*[aà]\s*5\s+anos?\b",     # 2/3 a 5 anos
        r"\b2\+\s*anos?\b",                 # 2+ anos
        r"\banalista\b",                    # Analista (nível típico pleno no BR)
    ],
    "senior": [
        r"\bs[eê]nior\b",
        r"\bsr\.?\b",
        r"\bn[ií]vel\s+s[eê]nior\b",
        r"\bespecialista\b",
        r"\btech\s+lead\b",
        r"\bengenheiro\s+s[eê]nior\b",
        r"\b5\s*\+\s*anos?\b",              # 5+ anos
        r"\bmais\s+de\s+5\s+anos?\b",
        r"\b6\s*[aà]\s*\d+\s+anos?\b",     # 6 a N anos
        r"\bprincipal\s+engineer\b",
        r"\bstaff\s+engineer\b",
        r"\barchitect\b",
        r"\barquiteto\b",
        r"\blead\s+developer\b",
        r"\blead\s+dev\b",
    ],
}


def filter_by_seniority(df: pd.DataFrame, levels: list[str]) -> pd.DataFrame:
    """
    Filtra vagas pelo nível de senioridade no título ou descrição.

    Args:
        df: DataFrame de vagas.
        levels: Níveis desejados (ex: ["junior", "pleno"]). Lista vazia = sem filtro.

    Returns:
        DataFrame filtrado. Se levels estiver vazio, retorna df inteiro.
    """
    if df.empty or not levels:
        return df

    patterns: list[str] = []
    for level in levels:
        patterns.extend(SENIORITY_KEYWORDS.get(level, []))

    if not patterns:
        return df

    regex = re.compile("|".join(patterns), re.IGNORECASE)
    mask = pd.Series(False, index=df.index)

    for col in ("titulo", "descricao"):
        if col in df.columns:
            mask |= df[col].fillna("").astype(str).str.contains(regex)

    return df[mask].copy()


def apply_all_filters(
    df: pd.DataFrame,
    skills: list[str] | None = None,
    remote_only: bool = False,
    seniority: list[str] | None = None,
) -> pd.DataFrame:
    """
    Aplica todos os filtros em sequência e remove duplicatas.

    Args:
        df: DataFrame bruto de vagas.
        skills: Lista de skills para filtrar (None = sem filtro).
        remote_only: Se True, mantém apenas vagas remotas.
        seniority: Níveis de senioridade desejados (None ou [] = sem filtro).

    Returns:
        DataFrame final filtrado e deduplicado.
    """
    # Deduplicar job boards e posts separadamente para evitar que URLs do
    # LinkedIn (retornados pelo DDG) sejam removidos por colisão com URLs
    # idênticos já trazidos pelo JobSpy.
    if "site" in df.columns:
        is_post = df["site"].str.startswith("post_", na=False)
        df_jobs = deduplicate(df[~is_post].copy())
        df_posts = deduplicate(df[is_post].copy())
        df = pd.concat([df_jobs, df_posts], ignore_index=True, sort=False)
    else:
        df = deduplicate(df)

    if remote_only:
        df = filter_remote(df)

    if skills:
        df = filter_by_skills(df, skills)

    if seniority:
        df = filter_by_seniority(df, seniority)

    return df.reset_index(drop=True)
