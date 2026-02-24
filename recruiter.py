"""
recruiter.py - Extração de nome e email do recrutador a partir dos dados da vaga
"""

from __future__ import annotations

import re

import pandas as pd

# Regex para e-mails genéricos no texto
_EMAIL_RE = re.compile(
    r"[a-zA-Z0-9._%+\-]+@[a-zA-Z0-9.\-]+\.[a-zA-Z]{2,}",
    re.IGNORECASE,
)

# Padrões para capturar nome do recrutador na descrição
_NAME_PATTERNS = [
    # PT: "Recrutador(a): Nome Sobrenome"
    r"recrut[ao]dor[a]?\s*[:：]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
    # PT: "Contato: Nome Sobrenome"
    r"contato\s*[:：]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
    # EN: "Posted by Nome Sobrenome"
    r"posted\s+by\s+([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
    # EN: "Hiring Manager: Nome"
    r"hiring\s+manager\s*[:：]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
    # EN: "Contact Nome at"
    r"contact\s+([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})\s+at\b",
    # EN: "Recruiter: Nome"
    r"recruiter\s*[:：]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
    # PT/EN: "Responsável: Nome"
    r"respons[aá]vel\s*[:：]\s*([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
    # LinkedIn "Apply to Nome Sobrenome"
    r"apply\s+to\s+([A-ZÀ-Ú][a-zà-ú]+(?:\s+[A-ZÀ-Ú][a-zà-ú]+){1,3})",
]

_NAME_RE = re.compile("|".join(_NAME_PATTERNS), re.IGNORECASE)


def _extract_email_from_text(text: str) -> str | None:
    """Extrai o primeiro e-mail encontrado em um texto livre."""
    if not text:
        return None
    found = _EMAIL_RE.findall(text)
    # Remove e-mails genéricos de imagem/logo que aparecem em HTML convertido
    filtered = [e for e in found if not e.endswith((".png", ".jpg", ".gif", ".svg"))]
    return filtered[0] if filtered else None


def _extract_name_from_text(text: str) -> str | None:
    """Tenta extrair o nome do recrutador de padrões comuns na descrição."""
    if not text:
        return None
    m = _NAME_RE.search(text)
    if not m:
        return None
    # Retorna o primeiro grupo de captura que não seja None
    for group in m.groups():
        if group:
            return group.strip()
    return None


def enrich_recruiter_info(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adiciona as colunas 'email_recrutador' e 'nome_recrutador' ao DataFrame.

    Estratégia:
    - email_recrutador: usa a coluna 'emails' do JobSpy se disponível;
      caso contrário, tenta extrair da descrição via regex.
    - nome_recrutador: extrai da descrição via padrões de texto comuns.
      Retorna None quando não encontrar.
    """
    df = df.copy()

    def _get_email(row: pd.Series) -> str | None:
        # JobSpy já extrai alguns e-mails na coluna 'emails'
        raw = row.get("emails")
        if raw and str(raw) not in ("", "nan", "None"):
            return str(raw).strip()
        # Fallback: regex na descrição
        return _extract_email_from_text(str(row.get("descricao") or ""))

    def _get_name(row: pd.Series) -> str | None:
        return _extract_name_from_text(str(row.get("descricao") or ""))

    df["email_recrutador"] = df.apply(_get_email, axis=1)
    df["nome_recrutador"] = df.apply(_get_name, axis=1)

    # Remove a coluna bruta 'emails' após usar
    if "emails" in df.columns:
        df = df.drop(columns=["emails"])

    return df
