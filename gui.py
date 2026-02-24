"""
gui.py - Interface desktop do VagasScrap
Inicie com: python gui.py  ou  abrir_gui.bat
"""

from __future__ import annotations

import concurrent.futures
import os
import queue
import sys
import threading
import webbrowser
from datetime import datetime
from pathlib import Path
from tkinter import ttk
import tkinter as tk

import pandas as pd
import customtkinter as ctk

ctk.set_appearance_mode("dark")
ctk.set_default_color_theme("blue")


class QueueWriter:
    """Redireciona stdout para a fila de log."""

    def __init__(self, q: queue.Queue):
        self.q = q

    def write(self, text: str):
        if text:
            self.q.put(text)

    def flush(self):
        pass


class VagasScrapGUI(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("VagasScrap")
        self.geometry("1050x720")
        self.minsize(850, 600)

        self.log_queue: queue.Queue = queue.Queue()
        self.is_running = False
        self.results_double_var = ctk.DoubleVar(value=25)
        self.extra_terms_list: list[str] = []

        self._build_ui()
        self._poll_log_queue()
        self._log("VagasScrap iniciado. Configure a busca e clique em 'Buscar Vagas'.\n")

    # ------------------------------------------------------------------ #
    #  Layout principal                                                    #
    # ------------------------------------------------------------------ #

    def _build_ui(self):
        self.grid_columnconfigure(1, weight=1)
        self.grid_rowconfigure(0, weight=1)

        self.left = ctk.CTkScrollableFrame(self, width=295, corner_radius=0)
        self.left.grid(row=0, column=0, sticky="nsew")
        self.left.grid_columnconfigure(0, weight=1)

        self.right = ctk.CTkFrame(self, corner_radius=0)
        self.right.grid(row=0, column=1, sticky="nsew")
        self.right.grid_rowconfigure(2, weight=1)
        self.right.grid_columnconfigure(0, weight=1)

        self._build_form(self.left)
        self._build_log(self.right)

    def _build_form(self, p: ctk.CTkScrollableFrame):
        row = 0

        ctk.CTkLabel(p, text="VagasScrap",
                     font=ctk.CTkFont(size=22, weight="bold")).grid(
            row=row, column=0, pady=(20, 2), padx=18, sticky="w")
        row += 1

        ctk.CTkLabel(p, text="Buscador de vagas de emprego",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=row, column=0, pady=(0, 18), padx=18, sticky="w")
        row += 1

        # ── Termo de busca ──────────────────────────────────────────────
        self._label(p, row, "Termo de busca  *")
        row += 1
        self.search_entry = ctk.CTkEntry(
            p, placeholder_text='ex: "desenvolvedor python"', height=36)
        self.search_entry.grid(row=row, column=0, padx=18, pady=(0, 12), sticky="ew")
        row += 1

        # ── Termos adicionais ────────────────────────────────────────────
        self._label(p, row, "Termos adicionais  (opcional)")
        row += 1
        ctk.CTkLabel(p, text="Busca para cada termo e combina os resultados",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=row, column=0, padx=18, pady=(0, 4), sticky="w")
        row += 1
        add_frame = ctk.CTkFrame(p, fg_color="transparent")
        add_frame.grid(row=row, column=0, padx=18, pady=(0, 4), sticky="ew")
        add_frame.grid_columnconfigure(0, weight=1)
        self.extra_term_entry = ctk.CTkEntry(
            add_frame, placeholder_text='ex: "analista de dados"', height=32)
        self.extra_term_entry.grid(row=0, column=0, sticky="ew", padx=(0, 6))
        self.extra_term_entry.bind("<Return>", lambda e: self._add_extra_term())
        ctk.CTkButton(add_frame, text="+", width=36, height=32,
                      command=self._add_extra_term).grid(row=0, column=1)
        row += 1
        self.extra_terms_frame = ctk.CTkFrame(p, fg_color="transparent")
        self.extra_terms_frame.grid(row=row, column=0, padx=18, pady=(0, 12), sticky="ew")
        self.extra_terms_frame.grid_columnconfigure(0, weight=1)
        row += 1

        # ── Localização ─────────────────────────────────────────────────
        self._label(p, row, "Localização")
        row += 1
        self.location_entry = ctk.CTkEntry(
            p, placeholder_text="ex: Rio de Janeiro, Brazil", height=36)
        self.location_entry.insert(0, "Brazil")
        self.location_entry.grid(row=row, column=0, padx=18, pady=(0, 12), sticky="ew")
        row += 1

        # ── Sites (job boards) ───────────────────────────────────────────
        self._label(p, row, "Job boards")
        row += 1
        self.site_vars: dict[str, ctk.BooleanVar] = {}
        for site in ["linkedin", "indeed", "glassdoor"]:
            var = ctk.BooleanVar(value=True)
            self.site_vars[site] = var
            ctk.CTkCheckBox(p, text=site.capitalize(), variable=var).grid(
                row=row, column=0, padx=28, pady=2, sticky="w")
            row += 1
        row += 1

        # ── Posts de recrutadores ────────────────────────────────────────
        self._label(p, row, "Posts de recrutadores")
        row += 1
        ctk.CTkLabel(p, text="Busca em posts de redes sociais via DuckDuckGo",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=row, column=0, padx=18, pady=(0, 4), sticky="w")
        row += 1
        self.post_vars: dict[str, ctk.BooleanVar] = {}
        post_platforms = [
            ("linkedin_posts", "LinkedIn Posts"),
            ("twitter",        "Twitter / X"),
            ("instagram",      "Instagram"),
            ("facebook",       "Facebook"),
        ]
        for key, label in post_platforms:
            var = ctk.BooleanVar(value=False)
            self.post_vars[key] = var
            ctk.CTkCheckBox(p, text=label, variable=var).grid(
                row=row, column=0, padx=28, pady=2, sticky="w")
            row += 1
        row += 1

        # ── Sites brasileiros & freelance ────────────────────────────────
        self._label(p, row, "Sites brasileiros & freelance")
        row += 1
        ctk.CTkLabel(p, text="Gupy, RemoteOK, Vagas.com, Reddit e outros",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=row, column=0, padx=18, pady=(0, 4), sticky="w")
        row += 1
        self.extra_vars: dict[str, ctk.BooleanVar] = {}
        extra_sites = [
            ("gupy",       "Gupy",              True),
            ("remoteok",   "RemoteOK",           True),
            ("vagascom",   "Vagas.com",          True),
            ("geekHunter", "GeekHunter",         False),
            ("trampos",    "Trampos.co",         False),
            ("reddit",     "Reddit (r/brdev)",   False),
            ("workana",    "Workana (Freelance)", False),
        ]
        for key, label, default in extra_sites:
            var = ctk.BooleanVar(value=default)
            self.extra_vars[key] = var
            ctk.CTkCheckBox(p, text=label, variable=var).grid(
                row=row, column=0, padx=28, pady=2, sticky="w")
            row += 1
        row += 1

        # ── Resultados por site ──────────────────────────────────────────
        self._label(p, row, "Resultados por site")
        row += 1
        self.results_label = ctk.CTkLabel(p, text="25 vagas",
                                          font=ctk.CTkFont(size=11), text_color="gray")
        self.results_label.grid(row=row, column=0, padx=18, pady=(0, 2), sticky="w")
        row += 1
        ctk.CTkSlider(p, from_=5, to=100, number_of_steps=19,
                      variable=self.results_double_var,
                      command=self._on_results_slider).grid(
            row=row, column=0, padx=18, pady=(0, 12), sticky="ew")
        row += 1

        # ── Período ──────────────────────────────────────────────────────
        self._label(p, row, "Postadas nos últimos")
        row += 1
        self.hours_var = ctk.StringVar(value="168")
        hours_frame = ctk.CTkFrame(p, fg_color="transparent")
        hours_frame.grid(row=row, column=0, padx=18, pady=(0, 12), sticky="ew")
        hours_frame.grid_columnconfigure(0, weight=1)
        ctk.CTkOptionMenu(hours_frame,
                          values=["24", "48", "72", "168", "336", "720"],
                          variable=self.hours_var).grid(row=0, column=0, sticky="ew")
        ctk.CTkLabel(hours_frame, text="horas  (168 = 7 dias)",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=1, column=0, sticky="w", pady=(2, 0))
        row += 1

        # ── Skills ───────────────────────────────────────────────────────
        self._label(p, row, "Filtrar por skills  (opcional)")
        row += 1
        self.skills_entry = ctk.CTkEntry(
            p, placeholder_text="ex: python,django,fastapi", height=36)
        self.skills_entry.grid(row=row, column=0, padx=18, pady=(0, 4), sticky="ew")
        row += 1
        ctk.CTkLabel(p, text="Separadas por vírgula",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=row, column=0, padx=18, pady=(0, 12), sticky="w")
        row += 1

        # ── Senioridade ──────────────────────────────────────────────────
        self._label(p, row, "Senioridade  (opcional)")
        row += 1
        ctk.CTkLabel(p, text="Nenhum marcado = todas as senioridades",
                     font=ctk.CTkFont(size=11), text_color="gray").grid(
            row=row, column=0, padx=18, pady=(0, 4), sticky="w")
        row += 1
        self.seniority_vars: dict[str, ctk.BooleanVar] = {}
        for key, label in [("trainee", "Trainee / Estágio"),
                            ("junior",  "Júnior"),
                            ("pleno",   "Pleno"),
                            ("senior",  "Sênior")]:
            var = ctk.BooleanVar(value=False)
            self.seniority_vars[key] = var
            ctk.CTkCheckBox(p, text=label, variable=var).grid(
                row=row, column=0, padx=28, pady=2, sticky="w")
            row += 1
        row += 1

        # ── Remoto ───────────────────────────────────────────────────────
        self.remote_var = ctk.BooleanVar(value=False)
        ctk.CTkCheckBox(p, text="Apenas vagas remotas",
                        variable=self.remote_var).grid(
            row=row, column=0, padx=18, pady=(0, 12), sticky="w")
        row += 1

        # ── Formato ──────────────────────────────────────────────────────
        self._label(p, row, "Formato de saída")
        row += 1
        self.format_var = ctk.StringVar(value="csv")
        fmt_frame = ctk.CTkFrame(p, fg_color="transparent")
        fmt_frame.grid(row=row, column=0, padx=18, pady=(0, 18), sticky="w")
        ctk.CTkRadioButton(fmt_frame, text="CSV",
                           variable=self.format_var, value="csv").pack(
            side="left", padx=(0, 18))
        ctk.CTkRadioButton(fmt_frame, text="Excel",
                           variable=self.format_var, value="excel").pack(side="left")
        row += 1

        # ── Separador ────────────────────────────────────────────────────
        ctk.CTkFrame(p, height=1, fg_color="gray30").grid(
            row=row, column=0, sticky="ew", padx=18, pady=4)
        row += 1

        # ── Botões ───────────────────────────────────────────────────────
        self.search_btn = ctk.CTkButton(
            p, text="Buscar Vagas", height=42,
            font=ctk.CTkFont(size=14, weight="bold"),
            command=self._start_search)
        self.search_btn.grid(row=row, column=0, padx=18, pady=(10, 6), sticky="ew")
        row += 1

        ctk.CTkButton(p, text="Ver Histórico", height=36,
                      fg_color="transparent", border_width=1,
                      command=self._open_history).grid(
            row=row, column=0, padx=18, pady=(0, 8), sticky="ew")
        row += 1

        ctk.CTkButton(p, text="Abrir pasta output", height=36,
                      fg_color="transparent", border_width=1,
                      command=self._open_output).grid(
            row=row, column=0, padx=18, pady=(0, 8), sticky="ew")
        row += 1

        ctk.CTkButton(p, text="Limpar log", height=32,
                      fg_color="transparent", border_width=1,
                      text_color="gray", border_color="gray40",
                      command=self._clear_log).grid(
            row=row, column=0, padx=18, pady=(0, 24), sticky="ew")

    def _build_log(self, p: ctk.CTkFrame):
        header = ctk.CTkFrame(p, height=46, corner_radius=0,
                              fg_color=("gray82", "gray18"))
        header.grid(row=0, column=0, sticky="ew")
        header.grid_propagate(False)
        header.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(header, text="Log de execução",
                     font=ctk.CTkFont(size=13, weight="bold")).grid(
            row=0, column=0, padx=15, pady=10, sticky="w")

        # ── Barra de progresso (oculta até iniciar busca) ────────────────
        self.progress_frame = ctk.CTkFrame(p, fg_color=("gray88", "gray16"),
                                           corner_radius=0)
        self.progress_frame.grid(row=1, column=0, sticky="ew")
        self.progress_frame.grid_columnconfigure(0, weight=1)
        self.progress_bar = ctk.CTkProgressBar(self.progress_frame, height=8)
        self.progress_bar.grid(row=0, column=0, sticky="ew", padx=15, pady=(8, 2))
        self.progress_bar.set(0)
        self.progress_label = ctk.CTkLabel(
            self.progress_frame, text="",
            font=ctk.CTkFont(size=11), text_color="gray")
        self.progress_label.grid(row=1, column=0, sticky="w", padx=15, pady=(0, 6))
        self.progress_frame.grid_remove()  # oculto por padrão

        self.log_box = ctk.CTkTextbox(
            p, font=ctk.CTkFont(family="Consolas", size=12),
            wrap="word", state="disabled", corner_radius=0)
        self.log_box.grid(row=2, column=0, sticky="nsew")

        self.status_var = ctk.StringVar(value="Pronto.")
        status = ctk.CTkFrame(p, height=30, corner_radius=0,
                              fg_color=("gray78", "gray15"))
        status.grid(row=3, column=0, sticky="ew")
        status.grid_propagate(False)
        ctk.CTkLabel(status, textvariable=self.status_var,
                     font=ctk.CTkFont(size=11)).pack(side="left", padx=12, pady=5)

    # ------------------------------------------------------------------ #
    #  Helpers                                                             #
    # ------------------------------------------------------------------ #

    def _show_progress(self, total: int):
        self.progress_bar.set(0)
        self.progress_label.configure(text=f"0 / {total} fontes concluídas")
        self.progress_frame.grid()

    def _update_progress(self, completed: int, total: int):
        self.progress_bar.set(completed / total if total else 1)
        self.progress_label.configure(text=f"{completed} / {total} fontes concluídas")

    def _hide_progress(self):
        self.progress_frame.grid_remove()

    def _label(self, parent, row, text):
        ctk.CTkLabel(parent, text=text,
                     font=ctk.CTkFont(weight="bold")).grid(
            row=row, column=0, padx=18, pady=(6, 2), sticky="w")

    def _add_extra_term(self):
        text = self.extra_term_entry.get().strip()
        if not text or text in self.extra_terms_list:
            return
        self.extra_terms_list.append(text)
        self.extra_term_entry.delete(0, "end")
        self._refresh_extra_terms()

    def _remove_extra_term(self, term: str):
        if term in self.extra_terms_list:
            self.extra_terms_list.remove(term)
        self._refresh_extra_terms()

    def _refresh_extra_terms(self):
        for w in self.extra_terms_frame.winfo_children():
            w.destroy()
        for i, term in enumerate(self.extra_terms_list):
            row_frame = ctk.CTkFrame(
                self.extra_terms_frame, fg_color=("gray80", "gray25"), corner_radius=6)
            row_frame.grid(row=i, column=0, sticky="ew", pady=2)
            row_frame.grid_columnconfigure(0, weight=1)
            ctk.CTkLabel(row_frame, text=term, anchor="w",
                         font=ctk.CTkFont(size=12)).grid(
                row=0, column=0, padx=8, pady=4, sticky="ew")
            ctk.CTkButton(
                row_frame, text="×", width=26, height=24,
                fg_color="transparent", text_color="gray60",
                hover_color=("gray70", "gray35"),
                command=lambda t=term: self._remove_extra_term(t),
            ).grid(row=0, column=1, padx=(0, 4))

    def _on_results_slider(self, value):
        self.results_label.configure(text=f"{int(value)} vagas")

    def _log(self, text: str):
        self.log_box.configure(state="normal")
        self.log_box.insert("end", text)
        self.log_box.see("end")
        self.log_box.configure(state="disabled")

    def _clear_log(self):
        self.log_box.configure(state="normal")
        self.log_box.delete("1.0", "end")
        self.log_box.configure(state="disabled")

    def _poll_log_queue(self):
        try:
            while True:
                self._log(self.log_queue.get_nowait())
        except queue.Empty:
            pass
        self.after(100, self._poll_log_queue)

    def _open_output(self):
        out = Path("output")
        out.mkdir(exist_ok=True)
        os.startfile(str(out.resolve()))

    # ------------------------------------------------------------------ #
    #  Busca                                                               #
    # ------------------------------------------------------------------ #

    def _start_search(self):
        if self.is_running:
            return

        term = self.search_entry.get().strip()
        if not term:
            self._log("\nERRO: Informe um termo de busca.\n")
            return

        sites         = [s for s, v in self.site_vars.items()  if v.get()]
        extra_sources = [k for k, v in self.extra_vars.items() if v.get()] or None
        post_platforms = [k for k, v in self.post_vars.items()  if v.get()] or None

        if not sites and not extra_sources and not post_platforms:
            self._log("\nERRO: Selecione ao menos uma fonte de busca.\n")
            return

        skills_raw = self.skills_entry.get().strip()
        skills    = [s.strip() for s in skills_raw.split(",") if s.strip()] or None
        seniority = [k for k, v in self.seniority_vars.items() if v.get()] or None

        params = {
            "search_terms":  [term] + self.extra_terms_list,
            "location":      self.location_entry.get().strip() or "Brazil",
            "sites":         sites,
            "extra_sources": extra_sources,
            "results_wanted": int(self.results_double_var.get()),
            "hours_old":     int(self.hours_var.get()),
            "skills":        skills,
            "seniority":     seniority,
            "remote_only":   self.remote_var.get(),
            "fmt":           self.format_var.get(),
            "post_platforms": post_platforms,
        }

        self.is_running = True
        self.search_btn.configure(state="disabled", text="Buscando...")
        self.status_var.set("Buscando vagas...")

        threading.Thread(target=self._run_search, args=(params,), daemon=True).start()

    def _run_search(self, params: dict):
        old_stdout = sys.stdout
        sys.stdout = QueueWriter(self.log_queue)

        try:
            from database import save_jobs
            from filters import apply_all_filters
            from recruiter import enrich_recruiter_info

            self.log_queue.put("\n" + "─" * 52 + "\n")
            self.log_queue.put(
                f"Iniciando busca: {datetime.now().strftime('%d/%m/%Y %H:%M:%S')}\n")

            search_terms: list[str] = params["search_terms"]

            # ── Montar lista de tarefas individuais ─────────────────────
            # Cada tarefa é (label, kind, term, source)
            tasks: list[tuple] = []
            for term in search_terms:
                for site in (params["sites"] or []):
                    tasks.append((f"{site} [{term}]", "jobspy", term, site))
                for src in (params["extra_sources"] or []):
                    tasks.append((f"{src} [{term}]", "extra", term, src))
                for plat in (params["post_platforms"] or []):
                    tasks.append((f"{plat} [{term}]", "posts", term, plat))

            total = len(tasks)
            self.log_queue.put(f"Executando {total} buscas em paralelo...\n")
            self.after(0, lambda: self._show_progress(total))

            completed_count = 0
            lock = threading.Lock()

            def run_task(task: tuple) -> pd.DataFrame:
                nonlocal completed_count
                label, kind, term, source = task
                result = pd.DataFrame()
                try:
                    if kind == "jobspy":
                        from scraper import search_jobs
                        result = search_jobs(
                            search_term=term,
                            location=params["location"],
                            sites=[source],
                            results_wanted=params["results_wanted"],
                            hours_old=params["hours_old"],
                            verbose=True,
                        )
                    elif kind == "extra":
                        from extra_scrapers import search_extra
                        result = search_extra(
                            search_term=term,
                            sources=[source],
                            results_per_source=params["results_wanted"],
                            verbose=True,
                        )
                    elif kind == "posts":
                        from posts_scraper import search_posts
                        result = search_posts(
                            search_term=term,
                            location=params["location"],
                            platforms=[source],
                            results_per_platform=15,
                            verbose=True,
                        )
                except Exception as exc:
                    self.log_queue.put(f"  ERRO [{label}]: {exc}\n")

                with lock:
                    completed_count += 1
                    c = completed_count
                self.after(0, lambda c=c: self._update_progress(c, total))
                return result

            max_workers = min(8, total) if total else 1
            with concurrent.futures.ThreadPoolExecutor(max_workers=max_workers) as executor:
                results = list(executor.map(run_task, tasks))

            all_frames = [df for df in results if df is not None and not df.empty]
            df = pd.concat(all_frames, ignore_index=True, sort=False) if all_frames else pd.DataFrame()

            if df.empty:
                self.log_queue.put("\nNenhuma vaga encontrada.\n")
                self.after(0, lambda: self.status_var.set("Nenhuma vaga encontrada."))
                return

            # Enriquecer com dados do recrutador
            self.log_queue.put("\nExtraindo informações de recrutadores...\n")
            df = enrich_recruiter_info(df)
            with_email = df["email_recrutador"].notna().sum()
            with_name = df["nome_recrutador"].notna().sum()
            self.log_queue.put(
                f"  Emails encontrados: {with_email} | Nomes encontrados: {with_name}\n")

            df = apply_all_filters(
                df,
                skills=params["skills"],
                remote_only=params["remote_only"],
                seniority=params["seniority"],
            )

            if df.empty:
                self.log_queue.put("\nNenhuma vaga passou pelos filtros aplicados.\n")
                self.after(0, lambda: self.status_var.set("Nenhuma vaga após filtros."))
                return

            # Salvar no histórico (com todos os termos concatenados)
            combined_term = " + ".join(search_terms)
            new_in_db = save_jobs(df, search_term=combined_term)
            self.log_queue.put(f"Histórico: {new_in_db} nova(s) vaga(s) adicionada(s)\n")

            # Salvar arquivo
            out_dir = Path("output")
            out_dir.mkdir(exist_ok=True)
            timestamp = datetime.now().strftime("%Y-%m-%d_%H-%M")
            slug = search_terms[0].replace(" ", "_")[:25]
            name = f"vagas_{slug}_{timestamp}"

            if params["fmt"] == "excel":
                out_path = out_dir / f"{name}.xlsx"
                df.to_excel(out_path, index=False)
            else:
                out_path = out_dir / f"{name}.csv"
                df.to_csv(out_path, index=False, encoding="utf-8-sig")

            self.log_queue.put(f"\nTotal de vagas salvas: {len(df)}\n")
            self.log_queue.put(f"Arquivo: {out_path.resolve()}\n")
            self.log_queue.put("─" * 52 + "\n")

            n = len(df)
            self.after(0, lambda: self.status_var.set(
                f"Concluído — {n} vagas salvas em output/"))

        except Exception as exc:
            self.log_queue.put(f"\nERRO: {exc}\n")
            self.after(0, lambda: self.status_var.set("Erro durante a busca."))

        finally:
            sys.stdout = old_stdout
            self.is_running = False
            self.after(0, self._hide_progress)
            self.after(0, lambda: self.search_btn.configure(
                state="normal", text="Buscar Vagas"))

    # ------------------------------------------------------------------ #
    #  Janela de histórico                                                 #
    # ------------------------------------------------------------------ #

    def _open_history(self):
        """Abre a janela de histórico de vagas."""
        win = ctk.CTkToplevel(self)
        win.title("Histórico de Vagas")
        win.geometry("1200x650")
        win.minsize(900, 500)
        win.grab_set()

        win.grid_rowconfigure(1, weight=1)
        win.grid_columnconfigure(0, weight=1)

        # ── Barra superior ────────────────────────────────────────────────
        top = ctk.CTkFrame(win, corner_radius=0, fg_color=("gray82", "gray18"), height=52)
        top.grid(row=0, column=0, sticky="ew")
        top.grid_propagate(False)
        top.grid_columnconfigure(2, weight=1)

        ctk.CTkLabel(top, text="Histórico de Vagas",
                     font=ctk.CTkFont(size=14, weight="bold")).grid(
            row=0, column=0, padx=15, pady=12, sticky="w")

        # Filtro de site
        from database import get_distinct_sites, get_stats
        sites = ["Todos"] + get_distinct_sites()
        site_filter_var = ctk.StringVar(value="Todos")
        ctk.CTkOptionMenu(top, values=sites, variable=site_filter_var, width=130).grid(
            row=0, column=1, padx=(0, 8), pady=10)

        # Campo de busca
        search_var = ctk.StringVar()
        search_entry = ctk.CTkEntry(top, textvariable=search_var,
                                    placeholder_text="Filtrar por título, empresa ou busca...",
                                    width=300, height=32)
        search_entry.grid(row=0, column=2, padx=(0, 8), pady=10, sticky="w")

        # Stats
        stats = get_stats()
        stats_text = (f"Total: {stats['total']} vagas  |  "
                      f"Com email: {stats['with_email']}  |  "
                      f"Última busca: {stats['last_search'] or '—'}")
        stats_label = ctk.CTkLabel(top, text=stats_text,
                                   font=ctk.CTkFont(size=11), text_color="gray")
        stats_label.grid(row=0, column=3, padx=15, sticky="e")

        # ── Treeview ─────────────────────────────────────────────────────
        tree_frame = ctk.CTkFrame(win, corner_radius=0)
        tree_frame.grid(row=1, column=0, sticky="nsew")
        tree_frame.grid_rowconfigure(0, weight=1)
        tree_frame.grid_columnconfigure(0, weight=1)

        cols = ("data_coleta", "site", "titulo", "empresa", "localizacao",
                "remoto", "nome_recrutador", "email_recrutador")
        col_labels = {
            "data_coleta":       "Coletado em",
            "site":              "Site",
            "titulo":            "Título",
            "empresa":           "Empresa",
            "localizacao":       "Localização",
            "remoto":            "Remoto",
            "nome_recrutador":   "Recrutador",
            "email_recrutador":  "Email recrutador",
        }
        col_widths = {
            "data_coleta":      130,
            "site":              80,
            "titulo":           230,
            "empresa":          160,
            "localizacao":      140,
            "remoto":            60,
            "nome_recrutador":  150,
            "email_recrutador": 200,
        }

        # Estilo ttk adaptado ao tema escuro
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("History.Treeview",
                        background="#2b2b2b", foreground="white",
                        rowheight=26, fieldbackground="#2b2b2b",
                        borderwidth=0, font=("Segoe UI", 10))
        style.configure("History.Treeview.Heading",
                        background="#1f1f1f", foreground="#aaaaaa",
                        relief="flat", font=("Segoe UI", 10, "bold"))
        style.map("History.Treeview",
                  background=[("selected", "#1f538d")],
                  foreground=[("selected", "white")])

        tree = ttk.Treeview(tree_frame, columns=cols, show="headings",
                            style="History.Treeview", selectmode="browse")

        for col in cols:
            tree.heading(col, text=col_labels[col],
                         command=lambda c=col: _sort_tree(tree, c, False))
            tree.column(col, width=col_widths[col], minwidth=50, anchor="w")

        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=tree.yview)
        hsb = ttk.Scrollbar(tree_frame, orient="horizontal", command=tree.xview)
        tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)

        tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")

        # Clique duplo abre o link no navegador
        def _on_double_click(event):
            item = tree.focus()
            if not item:
                return
            row_id = tree.item(item, "tags")
            if row_id:
                webbrowser.open(row_id[0])

        tree.bind("<Double-1>", _on_double_click)

        # ── Barra inferior ────────────────────────────────────────────────
        bottom = ctk.CTkFrame(win, corner_radius=0, fg_color=("gray78", "gray15"), height=44)
        bottom.grid(row=2, column=0, sticky="ew")
        bottom.grid_propagate(False)

        count_var = ctk.StringVar(value="")
        ctk.CTkLabel(bottom, textvariable=count_var,
                     font=ctk.CTkFont(size=11)).pack(side="left", padx=12, pady=10)

        def _export():
            from database import load_history
            df = load_history(search_filter=search_var.get(),
                              site_filter=site_filter_var.get())
            if df.empty:
                return
            out = Path("output")
            out.mkdir(exist_ok=True)
            ts = datetime.now().strftime("%Y-%m-%d_%H-%M")
            path = out / f"historico_{ts}.csv"
            df.to_csv(path, index=False, encoding="utf-8-sig")
            os.startfile(str(out.resolve()))

        def _clear_all():
            import tkinter.messagebox as mb
            if mb.askyesno("Limpar histórico",
                           "Tem certeza que deseja apagar todo o histórico?",
                           parent=win):
                from database import delete_all
                delete_all()
                _load_data()

        ctk.CTkButton(bottom, text="Exportar CSV", width=120, height=30,
                      command=_export).pack(side="right", padx=(0, 8), pady=7)
        ctk.CTkButton(bottom, text="Limpar histórico", width=130, height=30,
                      fg_color="#8b1a1a", hover_color="#6b1010",
                      command=_clear_all).pack(side="right", padx=(0, 6), pady=7)

        # ── Carregar dados ────────────────────────────────────────────────
        def _load_data(*_):
            from database import load_history
            df = load_history(search_filter=search_var.get(),
                              site_filter=site_filter_var.get())
            tree.delete(*tree.get_children())
            for _, row in df.iterrows():
                remoto = "Sim" if row.get("remoto") else "Não"
                values = (
                    str(row.get("data_coleta") or "")[:16],
                    str(row.get("site") or ""),
                    str(row.get("titulo") or ""),
                    str(row.get("empresa") or ""),
                    str(row.get("localizacao") or ""),
                    remoto,
                    str(row.get("nome_recrutador") or ""),
                    str(row.get("email_recrutador") or ""),
                )
                link = str(row.get("link") or "")
                tree.insert("", "end", values=values, tags=(link,))
            count_var.set(f"{len(df)} vagas exibidas")

        def _sort_tree(tv, col, reverse):
            data = [(tv.set(k, col), k) for k in tv.get_children("")]
            data.sort(reverse=reverse)
            for i, (_, k) in enumerate(data):
                tv.move(k, "", i)
            tv.heading(col, command=lambda: _sort_tree(tv, col, not reverse))

        # Filtros disparam recarga
        search_var.trace_add("write", _load_data)
        site_filter_var.trace_add("write", _load_data)

        _load_data()


if __name__ == "__main__":
    app = VagasScrapGUI()
    app.mainloop()
