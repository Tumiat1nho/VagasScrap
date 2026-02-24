"""
main.py - CLI principal do VagasScrap

Uso:
  python main.py search -s "desenvolvedor python" -l "Rio de Janeiro"
  python main.py search -s "react" --skills "react,typescript" --remote
  python main.py search -s "data scientist" --sites linkedin indeed --results 50 --format excel
  python main.py schedule
  python main.py config
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime
from pathlib import Path


def cmd_search(args: argparse.Namespace) -> None:
    from filters import apply_all_filters
    from scraper import search_jobs

    skills = [s.strip() for s in args.skills.split(",")] if args.skills else None
    sites = args.sites or ["linkedin", "indeed", "glassdoor"]

    df = search_jobs(
        search_term=args.search,
        location=args.location,
        sites=sites,
        results_wanted=args.results,
        hours_old=args.hours_old,
        verbose=True,
    )

    if df.empty:
        print("Nenhuma vaga encontrada. Tente outros termos ou sites.")
        sys.exit(0)

    df = apply_all_filters(df, skills=skills, remote_only=args.remote)

    if df.empty:
        print("Nenhuma vaga passou pelos filtros aplicados.")
        sys.exit(0)

    print(f"\nTotal após filtros: {len(df)} vagas")
    _print_summary(df)

    out_path = _save_output(df, args)
    print(f"\nSalvo em: {out_path.resolve()}")


def cmd_schedule(args: argparse.Namespace) -> None:
    from scheduler import start_scheduler

    start_scheduler(config_path=args.config)


def cmd_config(args: argparse.Namespace) -> None:
    import yaml
    from pathlib import Path

    config_path = Path(args.config)
    if not config_path.exists():
        print(f"Arquivo de configuração não encontrado: {config_path}")
        sys.exit(1)

    with open(config_path, encoding="utf-8") as f:
        config = yaml.safe_load(f)

    print(f"\nConfiguração atual ({config_path}):\n")
    print(yaml.dump(config, allow_unicode=True, default_flow_style=False))


def _print_summary(df) -> None:
    """Exibe um resumo das vagas encontradas no terminal."""
    import pandas as pd

    print("\n" + "=" * 70)
    print(f"{'SITE':<12} {'TÍTULO':<35} {'EMPRESA':<20}")
    print("=" * 70)

    display = df.head(15)
    for _, row in display.iterrows():
        site = str(row.get("site", ""))[:11]
        title = str(row.get("titulo", ""))[:34]
        company = str(row.get("empresa", ""))[:19]
        print(f"{site:<12} {title:<35} {company:<20}")

    if len(df) > 15:
        print(f"  ... e mais {len(df) - 15} vagas no arquivo de saída.")
    print("=" * 70)


def _save_output(df, args: argparse.Namespace) -> Path:
    """Salva o DataFrame no formato e local configurados."""
    out_dir = Path("output")
    out_dir.mkdir(parents=True, exist_ok=True)

    timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")

    if args.output:
        filename = args.output
    else:
        term_slug = args.search.replace(" ", "_")[:30]
        filename = f"vagas_{term_slug}_{timestamp}"

    fmt = (args.format or "csv").lower()

    if fmt == "excel":
        out_path = out_dir / f"{filename}.xlsx"
        df.to_excel(out_path, index=False)
    else:
        out_path = out_dir / f"{filename}.csv"
        df.to_csv(out_path, index=False, encoding="utf-8-sig")

    return out_path


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="vagasscrap",
        description="VagasScrap - Scraper de vagas de emprego",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Exemplos:
  python main.py search -s "desenvolvedor python" -l "Rio de Janeiro"
  python main.py search -s "react" -l "São Paulo" --skills "react,typescript" --remote
  python main.py search -s "data scientist" --sites linkedin indeed --results 50
  python main.py search -s "engenheiro" --format excel
  python main.py schedule
  python main.py config
        """,
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # --- Subcomando: search ---
    search_parser = subparsers.add_parser("search", help="Buscar vagas agora")

    search_parser.add_argument(
        "-s", "--search",
        required=True,
        metavar="TERMO",
        help='Termo de busca (ex: "desenvolvedor python")',
    )
    search_parser.add_argument(
        "-l", "--location",
        default="Brazil",
        metavar="LOCAL",
        help='Localização (padrão: "Brazil")',
    )
    search_parser.add_argument(
        "--sites",
        nargs="+",
        metavar="SITE",
        choices=["linkedin", "indeed", "glassdoor", "zip_recruiter"],
        help="Sites para buscar (padrão: linkedin indeed glassdoor)",
    )
    search_parser.add_argument(
        "--results",
        type=int,
        default=25,
        metavar="N",
        help="Número de resultados por site (padrão: 25)",
    )
    search_parser.add_argument(
        "--hours-old",
        type=int,
        default=168,
        dest="hours_old",
        metavar="HORAS",
        help="Máximo de horas desde a postagem (padrão: 168 = 7 dias)",
    )
    search_parser.add_argument(
        "--skills",
        metavar="SKILLS",
        help='Skills separadas por vírgula (ex: "python,django,fastapi")',
    )
    search_parser.add_argument(
        "--remote",
        action="store_true",
        help="Filtrar apenas vagas remotas",
    )
    search_parser.add_argument(
        "--format",
        choices=["csv", "excel"],
        default="csv",
        help="Formato de saída: csv ou excel (padrão: csv)",
    )
    search_parser.add_argument(
        "--output",
        metavar="NOME",
        help="Nome do arquivo de saída (sem extensão)",
    )

    # --- Subcomando: schedule ---
    sched_parser = subparsers.add_parser(
        "schedule",
        help="Executar buscas agendadas conforme config.yaml",
    )
    sched_parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="ARQUIVO",
        help="Caminho para o arquivo de configuração (padrão: config.yaml)",
    )

    # --- Subcomando: config ---
    cfg_parser = subparsers.add_parser("config", help="Exibir configuração atual")
    cfg_parser.add_argument(
        "--config",
        default="config.yaml",
        metavar="ARQUIVO",
        help="Caminho para o arquivo de configuração (padrão: config.yaml)",
    )

    return parser


def main() -> None:
    parser = build_parser()
    args = parser.parse_args()

    if args.command == "search":
        cmd_search(args)
    elif args.command == "schedule":
        cmd_schedule(args)
    elif args.command == "config":
        cmd_config(args)


if __name__ == "__main__":
    main()
