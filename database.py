"""
database.py - Histórico de vagas em SQLite
"""

from __future__ import annotations

import sqlite3
from datetime import datetime
from pathlib import Path

import pandas as pd

DB_PATH = Path("vagas_historico.db")

_CREATE_TABLE = """
CREATE TABLE IF NOT EXISTS vagas (
    id               INTEGER PRIMARY KEY AUTOINCREMENT,
    link             TEXT    UNIQUE,
    site             TEXT,
    titulo           TEXT,
    empresa          TEXT,
    localizacao      TEXT,
    remoto           INTEGER,
    tipo_vaga        TEXT,
    salario_min      REAL,
    salario_max      REAL,
    moeda            TEXT,
    data_postagem    TEXT,
    email_recrutador TEXT,
    nome_recrutador  TEXT,
    termo_busca      TEXT,
    data_coleta      TEXT
)
"""


def _connect() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def init_db() -> None:
    with _connect() as conn:
        conn.execute(_CREATE_TABLE)


def save_jobs(df: pd.DataFrame, search_term: str = "") -> int:
    """
    Insere vagas no banco, ignorando duplicatas (por link).

    Returns:
        Número de vagas novas inseridas.
    """
    init_db()
    if df.empty:
        return 0

    coleta = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    inserted = 0

    with _connect() as conn:
        for _, row in df.iterrows():
            try:
                conn.execute(
                    """
                    INSERT OR IGNORE INTO vagas
                        (link, site, titulo, empresa, localizacao, remoto,
                         tipo_vaga, salario_min, salario_max, moeda,
                         data_postagem, email_recrutador, nome_recrutador,
                         termo_busca, data_coleta)
                    VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)
                    """,
                    (
                        _str(row.get("link")),
                        _str(row.get("site")),
                        _str(row.get("titulo")),
                        _str(row.get("empresa")),
                        _str(row.get("localizacao")),
                        int(bool(row.get("remoto", False))),
                        _str(row.get("tipo_vaga")),
                        _float(row.get("salario_min")),
                        _float(row.get("salario_max")),
                        _str(row.get("moeda")),
                        _str(row.get("data_postagem")),
                        _str(row.get("email_recrutador")),
                        _str(row.get("nome_recrutador")),
                        search_term,
                        coleta,
                    ),
                )
                if conn.execute("SELECT changes()").fetchone()[0]:
                    inserted += 1
            except Exception:
                pass

    return inserted


def load_history(
    limit: int = 1000,
    search_filter: str = "",
    site_filter: str = "",
) -> pd.DataFrame:
    """
    Retorna vagas do histórico, mais recentes primeiro.

    Args:
        limit: Máximo de linhas a retornar.
        search_filter: Filtra por título ou empresa (LIKE).
        site_filter: Filtra por site exato.
    """
    init_db()

    conditions = []
    params: list = []

    if search_filter:
        conditions.append("(titulo LIKE ? OR empresa LIKE ? OR termo_busca LIKE ?)")
        like = f"%{search_filter}%"
        params += [like, like, like]

    if site_filter and site_filter != "Todos":
        conditions.append("site = ?")
        params.append(site_filter)

    where = f"WHERE {' AND '.join(conditions)}" if conditions else ""
    params.append(limit)

    query = f"""
        SELECT id, data_coleta, termo_busca, site, titulo, empresa,
               localizacao, remoto, tipo_vaga, salario_min, salario_max,
               moeda, data_postagem, nome_recrutador, email_recrutador, link
        FROM vagas
        {where}
        ORDER BY data_coleta DESC
        LIMIT ?
    """

    with _connect() as conn:
        return pd.read_sql_query(query, conn, params=params)


def get_stats() -> dict:
    """Retorna estatísticas rápidas do histórico."""
    init_db()
    with _connect() as conn:
        total = conn.execute("SELECT COUNT(*) FROM vagas").fetchone()[0]
        sites = conn.execute(
            "SELECT site, COUNT(*) as n FROM vagas GROUP BY site ORDER BY n DESC"
        ).fetchall()
        last = conn.execute("SELECT MAX(data_coleta) FROM vagas").fetchone()[0]
        with_email = conn.execute(
            "SELECT COUNT(*) FROM vagas WHERE email_recrutador IS NOT NULL AND email_recrutador != ''"
        ).fetchone()[0]
    return {
        "total": total,
        "sites": [(r[0], r[1]) for r in sites],
        "last_search": last,
        "with_email": with_email,
    }


def delete_all() -> None:
    """Remove todos os registros do histórico."""
    init_db()
    with _connect() as conn:
        conn.execute("DELETE FROM vagas")


def get_distinct_sites() -> list[str]:
    """Retorna lista de sites presentes no histórico."""
    init_db()
    with _connect() as conn:
        rows = conn.execute(
            "SELECT DISTINCT site FROM vagas WHERE site IS NOT NULL ORDER BY site"
        ).fetchall()
    return [r[0] for r in rows]


# ── helpers ──────────────────────────────────────────────────────────────────

def _str(val) -> str | None:
    if val is None:
        return None
    s = str(val).strip()
    return s if s not in ("", "nan", "None") else None


def _float(val) -> float | None:
    try:
        return float(val)
    except (TypeError, ValueError):
        return None
