"""
Extrator de CATMATs Pro  —  v2.4
Motor gráfico: CustomTkinter  (tema claro/escuro nativo, cantos arredondados)
Identidade visual: inspirada no BPS / DESID (Gov.br)

Arquivo único e autossuficiente: a interface, o motor de extração e o motor de
consolidação DW + DA (aba 3) vivem todos aqui.

    python ExtratorCatmat.py                      -> abre a interface
    python ExtratorCatmat.py -e ENTRADA -s SAIDA  -> consolidação em linha de comando
"""

import re
import csv
import codecs
import requests
import pandas as pd
from io import StringIO
from typing import Tuple, List, Optional
import os
import sys
import time
import glob
import argparse
import unicodedata
from datetime import date, datetime
from pathlib import Path
from openpyxl import Workbook, load_workbook
from openpyxl.styles import Alignment, Font, PatternFill
import shutil
import json
import queue
import xlsxwriter
import multiprocessing
from array import array
import threading
import traceback
import math
from concurrent.futures import ThreadPoolExecutor, as_completed
import tkinter as tk
from tkinter import filedialog, messagebox, scrolledtext
import customtkinter as ctk
import tkinter.ttk as ttk

# =============================================================================
# PALETA  —  neutros Gov.br + acento azul Gov + verde BPS + amarelo BPS
# =============================================================================
C_BG         = "#F4F5F7"   # cinza-papel (fundo geral)
C_SURFACE    = "#FFFFFF"   # branco (cards / frames)
C_BORDER     = "#DDE1E9"   # borda sutil
C_TEXT       = "#1A1D23"   # quase-preto
C_TEXT_MED   = "#555B6E"   # texto secundário
C_TEXT_LIGHT = "#8A92A6"   # placeholder / hint
C_ACCENT     = "#1351B4"   # azul Gov.br (primário)
C_ACCENT_H   = "#0C3784"   # hover do azul
C_GREEN      = "#168821"   # verde BPS (sucesso)
C_GREEN_H    = "#0E5C17"   # hover verde
C_YELLOW     = "#FFCD07"   # amarelo BPS (destaque / faixa)
C_ORANGE     = "#E37222"   # aviso
C_RED        = "#C0392B"   # erro / cancelar
C_LOG_BG     = "#13141A"   # terminal escuro
C_LOG_FG     = "#E8EAF0"   # texto terminal

ctk.set_appearance_mode("light")
ctk.set_default_color_theme("blue")

# =============================================================================
# LÓGICA DE NEGÓCIO
# =============================================================================

pausar_extracao     = threading.Event()
pausar_busca_catmat = threading.Event()
# Estado "set" = liberado. Inicializar aqui evita que um wait() bloqueie para
# sempre em qualquer fluxo que esqueça de chamar .set() antes de começar.
pausar_extracao.set()
pausar_busca_catmat.set()
cancelar_busca_catmat = False
_lock_pausa_conexao = threading.Lock()   # várias threads podem detectar a queda juntas

requests.packages.urllib3.disable_warnings(
    requests.packages.urllib3.exceptions.InsecureRequestWarning
)

# Session compartilhada — reutiliza conexões TCP/TLS entre todas as requisições
# Evita o overhead de handshake (~200-400ms) a cada chamada
_http = requests.Session()
_http.verify = False
_http.headers.update({"Accept-Encoding": "gzip, deflate", "Connection": "keep-alive"})
_adapter = requests.adapters.HTTPAdapter(
    pool_connections=4, pool_maxsize=12, max_retries=0
)
_http.mount("https://", _adapter)
_http.mount("http://",  _adapter)

URL_BASE = "https://dadosabertos.compras.gov.br"
TIMEOUT  = 120
_MAX_PAGINAS = 20000   # trava contra laço infinito se a API paginar sem fim

# =============================================================================
# COTA DO COMPRAS.GOV
# -----------------------------------------------------------------------------
# O Compras.gov fica atrás de um Azure API Management que limita cada cliente a
# ~100 requisições por MINUTO (medido em 29/09/2026: 97-100 passam, depois vem
# 429 "Rate limit is exceeded. Try again in N seconds" com Retry-After de até
# 45 s). A cota é UMA só para todos os módulos: pesquisa de preço e catálogo
# (PDMs, itens) gastam do mesmo saldo. O BPS é outro servidor e fica de fora.
#
# Por isso a velocidade máxima é a cota, não o número de threads: todas as
# chamadas ao Compras.gov passam por _get_compras, que espaça as saídas para
# ficar logo abaixo do limite e, se ainda assim vier um 429, segura TODAS as
# threads pelo tempo que o servidor pediu (Retry-After) e repete.
# =============================================================================
COTA_COMPRAS_POR_MINUTO = 90   # margem abaixo das ~100 medidas: um 429 custa até 45 s
WORKERS_COMPRAS = 3            # 3 em paralelo cobrem a latência (~0,8 s) e enchem a cota


class _CotaCompras:
    """Espaçamento global das requisições + pausa global em caso de 429."""

    def __init__(self, por_minuto):
        self._intervalo = 60.0 / por_minuto
        self._lock      = threading.Lock()
        self._proximo   = 0.0          # instante mínimo da próxima saída
        self._pausa_ate = 0.0          # 429: ninguém sai antes disto

    def aguardar(self):
        with self._lock:
            saida = max(time.monotonic(), self._proximo, self._pausa_ate)
            self._proximo = saida + self._intervalo
        espera = saida - time.monotonic()
        if espera > 0:
            time.sleep(espera)

    def bloquear(self, segundos):
        with self._lock:
            self._pausa_ate = max(self._pausa_ate, time.monotonic() + segundos)
            self._proximo   = max(self._proximo, self._pausa_ate)


_cota_compras = _CotaCompras(COTA_COMPRAS_POR_MINUTO)


def _retry_after(resp) -> float:
    """Segundos pedidos pelo servidor num 429 (+ meio segundo de folga)."""
    try:
        return float(resp.headers.get("Retry-After")) + 0.5
    except (TypeError, ValueError):
        m = re.search(r"(\d+)\s*second", resp.text or "")
        return (int(m.group(1)) if m else 15) + 0.5


def _get_compras(url, params, timeout=TIMEOUT, max_429=10):
    """GET no Compras.gov respeitando a cota. Um 429 pausa todas as threads
    pelo Retry-After e a chamada é repetida (até max_429 vezes). Erros de rede
    sobem como exceção, como num _http.get comum."""
    resp = None
    for _ in range(max_429):
        _cota_compras.aguardar()
        resp = _http.get(url, params=params, timeout=timeout)
        if resp.status_code != 429:
            return resp
        espera = _retry_after(resp)
        _erros_api.contar_429(espera)
        _cota_compras.bloquear(espera)
    return resp

# =============================================================================
# REGISTRO DE ERROS DE API  —  apuração: QUAL erro, em QUAL fonte
# -----------------------------------------------------------------------------
# Todo erro de rede/API passa por _erros_api.registrar(fonte, diagnosticar(...)):
#   "DA"        → Compras.gov, Pesquisa de Preço (registros de preço)
#   "CATÁLOGO"  → Compras.gov, catálogo de materiais (PDMs e CATMATs)
#   "BPS"       → API de Dados Abertos do Ministério da Saúde
#   "PROGRAMA"  → erro interno do extrator, NÃO da API (antes aparecia como
#                 "erro na API", sem pista do que era)
# Cada ocorrência vai para o log da tela e para Log_Erros_API_<data>.csv (com a
# URL completa, que pode ser colada no navegador para reproduzir). Ao fim da
# extração, apurar() agrupa as ocorrências por fonte e tipo de erro.
# =============================================================================

_HTTP_SIGNIFICADO = {
    400: "requisição recusada: parâmetro inválido (código, data ou tipo)",
    401: "não autorizado",
    403: "acesso negado: bloqueio do servidor, da rede ou do proxy",
    404: "não encontrado: o endpoint mudou ou o código não existe",
    408: "o servidor encerrou a requisição por demora",
    429: "limite de requisições excedido (rate limit)",
    500: "erro interno no servidor da API",
    502: "bad gateway: o servidor intermediário não obteve resposta da API",
    503: "serviço indisponível: API em manutenção ou sobrecarregada",
    504: "gateway timeout: a API demorou demais para responder",
}


def _trecho(texto, n=300) -> str:
    return re.sub(r"\s+", " ", str(texto or "")).strip()[:n]


def diagnosticar(resp=None, exc=None) -> dict:
    """Traduz uma resposta HTTP de erro ou uma exceção em
    {"tipo", "http", "detalhe", "resposta", "url", "conexao"}.
    conexao=True quando não houve comunicação com o servidor (queda de rede)."""
    if resp is not None:
        st = resp.status_code
        try:
            corpo = resp.text
        except Exception:
            corpo = ""
        return {"tipo": f"HTTP {st}", "http": st,
                "detalhe": _HTTP_SIGNIFICADO.get(st) or
                           ("erro no servidor da API" if st >= 500 else "resposta de erro"),
                "resposta": _trecho(corpo), "url": getattr(resp, "url", ""),
                "conexao": False}

    E = requests.exceptions
    resp_exc = getattr(exc, "response", None)
    if isinstance(exc, E.HTTPError) and resp_exc is not None:
        return diagnosticar(resp=resp_exc)
    req = getattr(exc, "request", None)
    url = getattr(req, "url", "") or ""
    msg = str(exc)
    baixo = msg.lower()
    conexao = isinstance(exc, E.ConnectionError)
    if isinstance(exc, E.ConnectTimeout):
        tipo, det = "Timeout de conexão", ("o servidor não aceitou a conexão a tempo "
                                           "(fora do ar, rede lenta ou bloqueio)")
    elif isinstance(exc, E.ReadTimeout):
        tipo, det = "Timeout de leitura", f"conectou, mas a API não respondeu em {TIMEOUT} s"
    elif isinstance(exc, E.SSLError):
        tipo, det = "Erro SSL/TLS", "falha no certificado ou na conexão segura (proxy corporativo?)"
    elif isinstance(exc, E.ProxyError):
        tipo, det = "Erro de proxy", "o proxy da rede recusou ou não alcançou o servidor"
    elif isinstance(exc, E.ConnectionError):
        if any(s in baixo for s in ("nameresolution", "getaddrinfo", "name or service",
                                    "nodename", "failed to resolve", "no address")):
            tipo, det = "Falha de DNS", "o nome do servidor não foi resolvido (sem internet ou DNS bloqueado)"
        elif "refused" in baixo or "recusad" in baixo or "10061" in baixo:
            tipo, det = "Conexão recusada", "o servidor (ou firewall) recusou a conexão"
        elif any(s in baixo for s in ("reset", "aborted", "remotedisconnected",
                                      "connection broken", "10054")):
            tipo, det = "Conexão interrompida", "a conexão caiu no meio da resposta"
        else:
            tipo, det = "Erro de conexão", "sem comunicação com o servidor"
    elif isinstance(exc, ValueError):        # inclui o JSONDecodeError do requests
        tipo, det = "Resposta inválida", "a API respondeu com conteúdo que não é JSON válido"
    elif isinstance(exc, E.RequestException):
        tipo, det = "Erro de requisição", type(exc).__name__
    else:
        # Não é a API: é um erro no próprio extrator. Aponta função e linha.
        onde = ""
        try:
            quadros = traceback.extract_tb(exc.__traceback__)
            nossos = [q for q in quadros
                      if os.path.basename(q.filename) == os.path.basename(__file__)]
            quadro = (nossos or quadros)[-1]
            onde = f" em {quadro.name}(), linha {quadro.lineno}"
        except Exception:
            pass
        tipo, det = "Erro interno do programa", f"{type(exc).__name__}{onde}"
    return {"tipo": tipo, "http": None, "detalhe": det, "resposta": _trecho(msg),
            "url": url, "conexao": conexao}


class _RegistroErros:
    """Guarda cada erro de API (thread-safe), grava o CSV de apuração e resume."""

    CAMPOS = ["data_hora", "fonte", "endpoint", "codigo", "pagina", "tipo_erro",
              "http", "detalhe", "observacao", "resposta_da_api", "mensagem_erro", "url"]

    def __init__(self):
        self._lock = threading.Lock()
        self.ao_registrar = None        # callback(msg): a interface liga no log
        self.iniciar("")

    def iniciar(self, pasta):
        """Zera o registro no início de cada extração. O CSV só é criado se
        houver erro, na pasta de destino (ou na pasta do programa)."""
        with self._lock:
            self.itens = []
            self._ultimo = {}
            self.n_429 = 0
            self.espera_429 = 0.0
            nome = f"Log_Erros_API_{datetime.now():%Y%m%d_%H%M%S}.csv"
            self.arquivo = os.path.join(pasta, nome) if pasta else nome
            self._criado = False

    def contar_429(self, segundos):
        """429 tratado pela cota não é erro (a chamada é repetida), mas explica
        lentidão: entra só na apuração final."""
        with self._lock:
            self.n_429 += 1
            self.espera_429 += segundos

    @staticmethod
    def formatar(it) -> str:
        onde = " ".join(p for p in (
            it["endpoint"],
            f"cód {it['codigo']}" if it["codigo"] != "" else "",
            f"pág {it['pagina']}" if it["pagina"] != "" else "") if p)
        txt = f"[{it['fonte']}] {onde}: {it['tipo_erro']} — {it['detalhe']}"
        if it["observacao"]:
            txt += f" ({it['observacao']})"
        if it["resposta_da_api"]:
            txt += f" | resposta da API: {it['resposta_da_api'][:160]}"
        elif it["mensagem_erro"]:
            txt += f" | erro: {it['mensagem_erro'][:160]}"
        return txt

    def registrar(self, fonte, diag, codigo="", pagina="", obs="") -> str:
        url = diag.get("url") or ""
        endpoint = ""
        if url:
            endpoint = url.split("?", 1)[0].rstrip("/").rsplit("/", 1)[-1]
        it = {"data_hora": datetime.now().strftime("%d/%m/%Y %H:%M:%S"),
              "fonte": fonte, "endpoint": endpoint, "codigo": str(codigo),
              "pagina": "" if pagina in (None, "") else str(pagina),
              "tipo_erro": diag["tipo"], "http": diag.get("http") or "",
              "detalhe": diag["detalhe"], "observacao": obs,
              # Com HTTP, o texto veio da API; sem HTTP, é a mensagem da exceção
              "resposta_da_api": (diag.get("resposta") or "") if diag.get("http") else "",
              "mensagem_erro": "" if diag.get("http") else (diag.get("resposta") or ""),
              "url": url}
        msg = self.formatar(it)
        with self._lock:
            self.itens.append(it)
            self._ultimo[(fonte, str(codigo))] = it
            try:
                with open(self.arquivo, "a", encoding="utf-8-sig", newline="") as f:
                    w = csv.DictWriter(f, fieldnames=self.CAMPOS, delimiter=";")
                    if not self._criado:
                        w.writeheader()
                        self._criado = True
                    w.writerow(it)
            except Exception:
                pass                    # sem o CSV, o log da tela ainda mostra o erro
        cb = self.ao_registrar
        if cb:
            try:
                cb(msg)
            except Exception:
                pass
        return msg

    def resumo(self, codigo, fontes=("DA", "PROGRAMA")) -> str:
        """Último erro de um código, em poucas palavras (para as mensagens da tela)."""
        with self._lock:
            for fonte in fontes:
                it = self._ultimo.get((fonte, str(codigo)))
                if it:
                    return f"{it['fonte']}: {it['tipo_erro']} ({it['detalhe']})"
        return "detalhes no Log_Erros_API"

    def apurar(self) -> List[str]:
        """Linhas da apuração final: ocorrências por fonte e tipo de erro."""
        with self._lock:
            itens = list(self.itens)
            n_429, espera = self.n_429, self.espera_429
        linhas = []
        if itens:
            grupos = {}
            for it in itens:
                g = grupos.setdefault((it["fonte"], it["tipo_erro"], it["detalhe"]),
                                      {"n": 0, "codigos": set(), "exemplo": it})
                g["n"] += 1
                if it["codigo"]:
                    g["codigos"].add(it["codigo"])
            linhas.append(f"🔎 Apuração dos erros de API ({len(itens)} ocorrência(s)):")
            for (fonte, tipo, det), g in sorted(grupos.items(), key=lambda kv: -kv[1]["n"]):
                cods = sorted(g["codigos"])
                ex = g["exemplo"]
                linhas.append(f"   • [{fonte}] {tipo} — {det}: {g['n']}x em "
                              f"{len(cods)} código(s)"
                              + (f" (ex.: {', '.join(cods[:5])}{' …' if len(cods) > 5 else ''})"
                                 if cods else ""))
                if ex["resposta_da_api"]:
                    linhas.append(f"       resposta da API: {ex['resposta_da_api'][:200]}")
                elif ex["mensagem_erro"]:
                    linhas.append(f"       erro: {ex['mensagem_erro'][:200]}")
            linhas.append(f"   Detalhes (URL de cada erro): {os.path.abspath(self.arquivo)}")
        if n_429:
            linhas.append(f"⏳ Compras.gov pediu pausa por limite de requisições (HTTP 429) "
                          f"{n_429}x — {espera:.0f} s de espera no total.")
        return linhas


_erros_api = _RegistroErros()

# =============================================================================
# TIPOS DE BUSCA  —  espelha o seletor "tipo" do endpoint de Pesquisa de Preço
#   /modulo-pesquisa-preco/1_consultarMaterial?tipo={tipo}&codigo={codigo}
# =============================================================================
TIPO_CATMAT = "codigoItemCatalogo"
TIPO_PDM    = "codigoPdm"
ROTULO_TIPO = {TIPO_CATMAT: "CATMAT", TIPO_PDM: "PDM"}
TIPO_POR_ROTULO = {v: k for k, v in ROTULO_TIPO.items()}

# Detectado na 1ª requisição e reaproveitado nas demais:
#   None  → ainda não sabemos
#   True  → API aceita a assinatura nova (tipo + codigo)
#   False → API ainda na assinatura antiga (codigoItemCatalogo)
_API_ACEITA_TIPO = None

ordem_final_colunas = [
    "idCompra","idItemCompra","forma","modalidade","criterioJulgamento",
    "numeroItemCompra","descricaoItem","codigoItemCatalogo","codigoPdm","nomeUnidadeFornecimento",
    "siglaUnidadeFornecimento","nomeUnidadeMedida","capacidadeUnidadeFornecimento","siglaUnidadeMedida",
    "Unidade de Fornecimento","capacidade","quantidade","precoUnitario","Preco Total","percentualMaiorDesconto",
    "niFornecedor","nomeFornecedor","marca","codigoUasg","nomeUasg",
    "codigoMunicipio","municipio","estado","codigoOrgao","nomeOrgao",
    "poder","esfera","dataCompra","dataHoraAtualizacaoCompra","dataHoraAtualizacaoItem",
    "dataResultado","dataHoraAtualizacaoUasg","codigoClasse","nomeClasse",
]


def _nome_da_parte(base_filename, ext_padrao, parte, simples=False):
    """Nome do arquivo de cada parte.
    simples=False -> base_part1.xlsx, base_part2.xlsx      (padrão do extrator)
    simples=True  -> base.xlsx, base - Parte 2.xlsx        (arquivo por fonte)
    """
    base, ext = os.path.splitext(base_filename)
    if not ext or ext.lower() != ext_padrao: ext = ext_padrao
    if simples:
        return f"{base}{ext}" if parte == 1 else f"{base} - Parte {parte}{ext}"
    return f"{base}_part{parte}{ext}"


class ExcelChunkWriter:
    """
    Escreve .xlsx em modo STREAMING (openpyxl write_only).

    O modo padrão do openpyxl mantém a planilha inteira em memória e só serializa
    tudo no save(), o que concentra o custo no encerramento — exatamente onde o
    usuário fica esperando. Medido com 30 mil linhas x 39 colunas:

        modo padrão   → 5,5 s durante + 9,5 s NO FINAL + 333 MB de RAM
        write_only    → 9,3 s durante + 0,9 s NO FINAL +   0 MB de RAM

    O tempo "durante" é absorvido pelas esperas de rede (0,5 s por página); o
    tempo "no final" é espera pura. Em uma extração de 230 mil registros isso
    troca ~70 s de espera no encerramento por ~7 s, e ~2,5 GB de RAM por nada.

    Contrapartida: em write_only o workbook só pode ser salvo UMA vez, então não
    há como reescrever o arquivo periodicamente. A proteção contra queda no meio
    da execução passa a ser um espelho .parcial.csv, gravado em append (16 ms por
    página de 500 linhas) e apagado quando o .xlsx é fechado com sucesso.

    Abas extras (ex.: "BPS") entram por write_dataframe(df, aba="BPS"): cada uma
    tem cabeçalho, contagem e espelho próprios. Quando qualquer aba chega ao
    limite, a pasta de trabalho inteira passa para a parte seguinte.
    """

    def __init__(self, base_filename, sheet_name="Dados CATMAT",
                 max_rows_per_file=1_000_000, espelho=True, nome_simples=False):
        self.base_filename = base_filename
        self.sheet_name    = sheet_name
        self.max_rows      = max_rows_per_file
        self.nome_simples  = nome_simples   # ver _nome_da_parte
        self.part          = 1
        self.files_saved: List[str] = []
        self._finalizado   = False
        self._usar_espelho = espelho
        # nome da aba -> {"header", "ws", "cab", "linhas", "esp_f", "esp_w"}
        self._abas: dict   = {}
        self._new_workbook()

    def _filepath(self):
        return _nome_da_parte(self.base_filename, ".xlsx", self.part, self.nome_simples)

    def _espelho_path(self, nome=None):
        base, _ = os.path.splitext(self.base_filename)
        if nome is None or nome == self.sheet_name:
            return f"{base}.parcial.csv"
        return f"{base}.{nome}.parcial.csv"

    def _aba(self, nome):
        a = self._abas.get(nome)
        if a is None:
            a = self._abas[nome] = {"header": [], "ws": None, "cab": False,
                                    "linhas": 0, "esp_f": None, "esp_w": None}
        return a

    def _new_workbook(self):
        # write_only exige create_sheet(); wb.active não existe nesse modo
        self.wb = Workbook(write_only=True)
        for a in self._abas.values():
            a["ws"] = None; a["cab"] = False; a["linhas"] = 0
        self._ws(self.sheet_name)            # a aba principal é sempre a primeira

    def _ws(self, nome):
        """Worksheet da aba na parte atual, criada (com cabeçalho) sob demanda."""
        a = self._aba(nome)
        if a["ws"] is None:
            a["ws"] = self.wb.create_sheet(nome)
        if not a["cab"] and a["header"]:
            a["ws"].append(a["header"]); a["cab"] = True
            self._abrir_espelho(nome)
        return a["ws"]

    def _abrir_espelho(self, nome):
        """Espelho .parcial.csv — rede de segurança enquanto o .xlsx não fecha."""
        a = self._aba(nome)
        if not self._usar_espelho or a["esp_f"] is not None:
            return
        try:
            a["esp_f"] = open(self._espelho_path(nome), "w",
                              encoding="utf-8-sig", newline="")
            a["esp_w"] = csv.writer(a["esp_f"], delimiter=";")
            a["esp_w"].writerow(a["header"])
        except Exception:
            self._usar_espelho = False      # sem espelho é melhor que falhar
            a["esp_f"] = a["esp_w"] = None

    def _fechar_espelho(self, apagar):
        for nome, a in self._abas.items():
            if a["esp_f"] is None: continue
            try:
                a["esp_f"].close()
            except Exception:
                pass
            if apagar:
                try:
                    os.remove(self._espelho_path(nome))
                except Exception:
                    pass
            a["esp_f"] = a["esp_w"] = None

    def _rollover(self):
        path = self._filepath(); self.wb.save(path); self.files_saved.append(path)
        self.part += 1; self._new_workbook()

    def write_dataframe(self, df: pd.DataFrame, aba: Optional[str] = None):
        if df is None or df.empty: return
        nome = aba or self.sheet_name
        a = self._aba(nome)
        if not a["header"]: a["header"] = list(df.columns)
        faltantes = [c for c in a["header"] if c not in df.columns]
        if faltantes:
            df = df.copy()          # não mutar o DataFrame do chamador
            for col in faltantes: df[col] = pd.NA
        df = df[a["header"]]
        for linha in df.itertuples(index=False, name=None):
            if a["linhas"] + 1 > self.max_rows:
                self._rollover()
            ws = self._ws(nome)
            # openpyxl levanta IllegalCharacterError em caracteres de controle,
            # frequentes no texto livre vindo da API — sanitiza na gravação
            limpa = [None if pd.isna(v) else
                     (_CTRL_ILEGAIS.sub(" ", v) if isinstance(v, str) else v)
                     for v in linha]
            ws.append(limpa)
            if a["esp_w"] is not None:
                a["esp_w"].writerow(["" if v is None else v for v in limpa])
            a["linhas"] += 1

    def arquivos_extras(self) -> List[str]:
        """Arquivos além dos principais — no Excel as abas extras ficam no mesmo arquivo."""
        return []

    def flush(self, intervalo_min=30, fator=20):
        """
        Em write_only o .xlsx não pode ser reescrito no meio do caminho: o que se
        garante aqui é que os espelhos .parcial.csv estejam em disco.
        """
        for a in self._abas.values():
            if a["esp_f"] is None: continue
            try:
                a["esp_f"].flush()
            except Exception:
                return None
        principal = self._abas.get(self.sheet_name)
        return (self._espelho_path() if principal and principal["esp_f"] is not None
                else None)

    def _descartar_workbook(self):
        """
        Um workbook write_only coletado sem save() deixa os geradores internos do
        openpyxl abertos, e o lxml despeja 'Exception ignored ... LxmlSyntaxError'
        no stderr durante o garbage collector. close() encerra os streams.
        """
        for a in self._abas.values():
            try:
                if a["ws"] is not None:
                    a["ws"].close()  # encerra os geradores de escrita da planilha
            except Exception:
                pass
        try:
            self.wb.close()
        except Exception:
            pass

    def finalize(self) -> List[str]:
        if self._finalizado:
            return self.files_saved
        self._finalizado = True
        ok = True
        if any(a["linhas"] > 0 for a in self._abas.values()):
            path = self._filepath()
            try:
                self.wb.save(path)
                if path not in self.files_saved: self.files_saved.append(path)
            except Exception:
                ok = False          # mantém o espelho: é tudo o que restou
        else:
            self._descartar_workbook()   # nada a salvar: fecha sem ruído
        # Espelho só é descartado quando o .xlsx foi fechado com sucesso
        self._fechar_espelho(apagar=ok)
        return self.files_saved


class CSVChunkWriter:
    def __init__(self, base_filename, sep=";", encoding="utf-8-sig", max_rows_per_file=1_000_000,
                 nome_simples=False):
        self.base_filename = base_filename; self.sep = sep
        self.encoding = encoding; self.max_rows = max_rows_per_file
        self.nome_simples = nome_simples    # ver _nome_da_parte
        self.part = 1; self.current_row_count = 0
        self.files_saved: List[str] = []; self.header_written = False
        self.header: List[str] = []
        # CSV não tem abas: cada aba extra (ex.: "BPS") vira um arquivo à parte,
        # base_BPS_part1.csv, com o seu próprio escritor
        self._abas: dict = {}

    def _filepath(self):
        return _nome_da_parte(self.base_filename, ".csv", self.part, self.nome_simples)

    def write_dataframe(self, df: pd.DataFrame, aba: Optional[str] = None):
        if df is None or df.empty: return
        if aba:
            sub = self._abas.get(aba)
            if sub is None:
                base, ext = os.path.splitext(self.base_filename)
                sub = self._abas[aba] = CSVChunkWriter(
                    f"{base}_{aba}{ext or '.csv'}", sep=self.sep,
                    encoding=self.encoding, max_rows_per_file=self.max_rows)
            sub.write_dataframe(df)
            return
        # O conjunto de colunas varia entre páginas (processar_dataframe_final
        # descarta colunas 100% vazias). Sem reindexar pelo cabeçalho da 1ª
        # página, o append gravaria valores sob colunas erradas.
        if not self.header:
            self.header = list(df.columns)
        df = df.reindex(columns=self.header)
        if self.current_row_count + len(df) > self.max_rows:
            self.part += 1; self.current_row_count = 0; self.header_written = False
        path = self._filepath()
        df.to_csv(path, sep=self.sep, index=False,
                  mode="a" if self.header_written else "w",
                  header=not self.header_written, encoding=self.encoding)
        self.header_written = True; self.current_row_count += len(df)
        if path not in self.files_saved: self.files_saved.append(path)

    def flush(self, intervalo_min=30, fator=20):
        """CSV já é gravado em append a cada página — nada a fazer."""
        return None

    def arquivos_extras(self) -> List[str]:
        """Arquivos das abas extras (ex.: base_BPS_part1.csv)."""
        return [p for sub in self._abas.values() for p in sub.files_saved]

    def finalize(self) -> List[str]:
        return self.files_saved + self.arquivos_extras()


class EscritorPorFonte:
    """Um arquivo por fonte — "Classe 6505 - DA" e "Classe 6505 - BPS" — em
    vez de uma pasta de trabalho com as abas Dados CATMAT e BPS.

    Mesma interface dos outros writers: write_dataframe(df, aba=ABA_BPS) vai
    para o arquivo do BPS, o resto para o do DA. Sem uma das fontes, o
    arquivo dela simplesmente não existe (caminho None).
    """

    def __init__(self, caminho_da, caminho_bps, fmt):
        def _novo(caminho, aba):
            if caminho is None:
                return None
            if fmt == "csv":
                return CSVChunkWriter(caminho, nome_simples=True)
            return ExcelChunkWriter(caminho, sheet_name=aba, nome_simples=True)
        self._da  = _novo(caminho_da, "Dados CATMAT")
        self._bps = _novo(caminho_bps, ABA_BPS)

    def write_dataframe(self, df: pd.DataFrame, aba: Optional[str] = None):
        alvo = self._bps if (aba == ABA_BPS or self._da is None) else self._da
        if alvo is not None:
            alvo.write_dataframe(df)

    def flush(self, intervalo_min=30, fator=20):
        for w in (self._da, self._bps):
            if w is not None:
                w.flush(intervalo_min, fator)
        return None

    def arquivos_extras(self) -> List[str]:
        """Os arquivos do BPS acompanham os do DA (ex.: no "salvar como")."""
        if self._da is None or self._bps is None:
            return []
        return list(self._bps.files_saved)

    def finalize(self) -> List[str]:
        return [p for w in (self._da, self._bps) if w is not None for p in w.finalize()]


def converter_data_para_api(data_dd_mm_yyyy: str) -> Optional[str]:
    s = data_dd_mm_yyyy.strip()
    if not s: return None
    try:
        p = s.split("-")
        if len(p) != 3: return None
        dd, mm, yyyy = p
        if len(dd) == 2 and len(mm) == 2 and len(yyyy) == 4:
            int(dd); int(mm); int(yyyy)
            return f"{yyyy}-{mm}-{dd}"
    except (ValueError, AttributeError):
        pass
    return None


def validar_e_obter_datas(ini: str, fim: str):
    i_api = f_api = None
    if ini.strip():
        i_api = converter_data_para_api(ini)
        if i_api is None:
            return None, None, f"Data de Inicio invalida: '{ini}'\nUse DD-MM-AAAA (ex: 01-01-2024)"
    if fim.strip():
        f_api = converter_data_para_api(fim)
        if f_api is None:
            return None, None, f"Data Final invalida: '{fim}'\nUse DD-MM-AAAA (ex: 31-12-2024)"
    return i_api, f_api, None


# =============================================================================
# PARSER DE PÁGINA CSV
# -----------------------------------------------------------------------------
# A API devolve CSV com campos de texto livre (descricaoItem) que podem conter
# quebras de linha cruas, ';' e aspas desbalanceadas. Três armadilhas conhecidas:
#
#   1. str.splitlines() quebra em \x0b \x0c \x1c-\x1e \x85 \u2028 \u2029, que o
#      parser CSV (e o servidor) tratam como texto comum → fragmentos fantasma.
#   2. Contar ';' com str.split ignora aspas → campos citados com ';' interno
#      viram "excesso de colunas" e a linha é remontada torta.
#   3. Descartar linhas que contêm só '"' apaga o fechamento de um campo
#      multilinha → o parser engole as linhas seguintes e some com registros.
#
# A abordagem aqui é: csv.reader sobre o texto bruto (que já resolve quebras
# dentro de campos citados), remontagem no nível de CAMPO e conferência do
# número de registros contra o esperado da página.
# =============================================================================

# Quebras que str.splitlines() reconhece mas o CSV não
_QUEBRAS_FALSAS = re.compile(r"[\x0b\x0c\x1c\x1d\x1e\x85\u2028\u2029]")
# Caracteres de controle que o openpyxl recusa ao gravar .xlsx
_CTRL_ILEGAIS   = re.compile(r"[\x00-\x08\x0b\x0c\x0e-\x1f]")
# Linhas de metadados (totalRegistros / total paginas / paginas restantes)
_RE_METADADO    = re.compile(
    r'^\s*"?\s*total\s*(de\s*)?(registros|p[áa]ginas?|p[áa]ginas?\s+restantes)\s*:',
    re.IGNORECASE)


def _limpar_campo(v):
    """Normaliza um campo: tira controles ilegais e colapsa quebras internas."""
    if not v:
        return ""
    v = v.replace("\r\n", " ").replace("\r", " ").replace("\n", " ")
    v = _QUEBRAS_FALSAS.sub(" ", v)
    v = _CTRL_ILEGAIS.sub(" ", v)
    return re.sub(r"[ \t]{2,}", " ", v).strip()


def _ler_registros(texto, quoting):
    """csv.reader sobre o texto bruto — resolve quebras dentro de campos citados."""
    try:
        return list(csv.reader(StringIO(texto, newline=""), delimiter=";",
                               quotechar='"', quoting=quoting, strict=False))
    except csv.Error:
        return []


def _remontar(registros, ncols, idx_livre):
    """
    Remonta registros quebrados.
      len < ncols → fragmento: a quebra caiu DENTRO de um campo, então o último
                    campo do fragmento e o primeiro do seguinte são as duas
                    metades do mesmo campo (por isso a junção é por campo, e
                    não pela linha inteira).
      len > ncols → ';' extra em campo sem aspas: recolhe o excedente de volta
                    para o campo de texto livre em vez de descartar a linha.
    Retorna (linhas, reparos, descartes).
    """
    linhas = []; buf = None; reparos = 0; descartes = 0
    for reg in registros:
        remontado = False
        if buf is not None:
            cabeca = (buf[-1] + " " + (reg[0] if reg else "")).strip()
            reg = buf[:-1] + [cabeca] + reg[1:]
            buf = None; remontado = True

        n = len(reg)
        if n < ncols:
            buf = reg                       # ainda incompleto — segue acumulando
            continue
        if n == ncols:
            if remontado: reparos += 1
            linhas.append(reg)
        elif idx_livre is not None and idx_livre < ncols:
            excedente = n - ncols
            reg = (reg[:idx_livre]
                   + [";".join(reg[idx_livre:idx_livre + excedente + 1])]
                   + reg[idx_livre + excedente + 1:])
            reparos += 1
            linhas.append(reg)
        else:
            descartes += 1
    if buf is not None:
        descartes += 1                      # fragmento órfão no fim da página
    return linhas, reparos, descartes


def _montar_pagina(texto, quoting):
    """Extrai (header, linhas, reparos, descartes) de uma página com um dado quoting."""
    registros = [r for r in _ler_registros(texto, quoting) if any(c.strip() for c in r)]
    registros = [r for r in registros if not _RE_METADADO.match(r[0] if r else "")]
    if not registros:
        return None, [], 0, 0
    header = [c.strip() for c in registros[0]]
    ncols  = len(header)
    if ncols < 2:
        return None, [], 0, 0
    idx_livre = header.index("descricaoItem") if "descricaoItem" in header else None
    linhas, reparos, descartes = _remontar(registros[1:], ncols, idx_livre)
    return header, linhas, reparos, descartes


def parse_pagina_csv(csv_text, esperado_na_pagina=None):
    """
    Converte o CSV de uma página em DataFrame com diagnóstico confiável.

    Retorna (df, diag), diag = {
        "linhas", "esperado", "reparos", "descartes", "modo", "ok", "motivo"
    }
    'ok' é False sempre que a página não entregou exatamente os registros
    esperados — é essa conferência (e não uma heurística de ';') que garante
    que nenhuma página problemática passe batido.
    """
    diag = {"linhas": 0, "esperado": esperado_na_pagina, "reparos": 0,
            "descartes": 0, "modo": "aspas", "ok": True, "motivo": ""}
    if not csv_text:
        diag.update(ok=False, motivo="pagina vazia")
        return pd.DataFrame(), diag

    melhor = None
    for modo, quoting in (("aspas", csv.QUOTE_MINIMAL), ("literal", csv.QUOTE_NONE)):
        header, linhas, reparos, descartes = _montar_pagina(csv_text, quoting)
        if header is None:
            continue
        cand = {"header": header, "linhas": linhas, "reparos": reparos,
                "descartes": descartes, "modo": modo}
        # Bateu o esperado no modo com aspas: não precisa da segunda leitura
        if esperado_na_pagina is not None and len(linhas) == esperado_na_pagina:
            melhor = cand
            break
        # Senão, fica com o que recupera mais registros e descarta menos
        if melhor is None or (len(linhas), -descartes) > (len(melhor["linhas"]),
                                                          -melhor["descartes"]):
            melhor = cand

    if melhor is None:
        diag.update(ok=False, motivo="cabecalho nao identificado")
        return pd.DataFrame(), diag

    dados = [[_limpar_campo(c) for c in ln] for ln in melhor["linhas"]]
    df = pd.DataFrame(dados, columns=melhor["header"], dtype=str)

    diag.update(linhas=len(df), reparos=melhor["reparos"],
                descartes=melhor["descartes"], modo=melhor["modo"])
    if esperado_na_pagina is not None and len(df) != esperado_na_pagina:
        diag["ok"] = False
        diag["motivo"] = f"{len(df)} de {esperado_na_pagina} registros"
    elif melhor["descartes"]:
        diag["ok"] = False
        diag["motivo"] = f"{melhor['descartes']} linha(s) descartada(s)"
    return df, diag


def parse_csv_text(csv_text: str) -> pd.DataFrame:
    """Compatibilidade: mantém a assinatura antiga sobre o novo motor."""
    df, _ = parse_pagina_csv(csv_text)
    return df


def ler_pagina_catmat(codigo, pagina, URL_BASE, TAMANHO_PAGINA, TIMEOUT,
                      data_compra_inicio=None, data_compra_fim=None,
                      tipo=TIPO_CATMAT):
    """
    Lê uma página de Registros de Preço.

    tipo — equivale ao seletor "tipo" do endpoint de Pesquisa de Preço:
        TIPO_CATMAT ("codigoItemCatalogo") → codigo é um CATMAT
        TIPO_PDM    ("codigoPdm")          → codigo é um PDM (traz todos os
                                             CATMATs do PDM de uma só vez)

    Envia a assinatura nova (tipo + codigo). Se o servidor recusar (400/404) e a
    busca for por CATMAT, refaz com a assinatura antiga (codigoItemCatalogo),
    mantendo compatibilidade com instâncias ainda não atualizadas da API.
    """
    global _API_ACEITA_TIPO
    URL = f"{URL_BASE}/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"

    base = {"tamanhoPagina": TAMANHO_PAGINA, "pagina": int(pagina)}
    if data_compra_inicio: base["dataCompraInicio"] = data_compra_inicio
    if data_compra_fim:    base["dataCompraFim"]    = data_compra_fim

    def _requisitar(params):
        """Retorna (csv_text, diag, status_http). csv_text=None quando falhou;
        diag = diagnosticar(...) do erro."""
        try:
            resp = _get_compras(URL, params)       # 429 já tratado pela cota
            if resp.status_code >= 400:
                diag = diagnosticar(resp=resp)
                if resp.status_code == 429:
                    diag["detalhe"] += " — persistiu mesmo após as pausas pedidas pela API"
                return None, diag, resp.status_code
            return resp.content.decode("utf-8-sig", errors="replace"), None, 200
        except requests.exceptions.RequestException as e:
            return None, diagnosticar(exc=e), None

    def _falha(diag, obs=""):
        """Registra o erro (fonte DA) e devolve a mensagem ERRO_* do contrato."""
        _erros_api.registrar("DA", diag, codigo=codigo, pagina=pagina, obs=obs)
        prefixo = "ERRO_CONEXAO" if diag.get("conexao") else "ERRO_REQUISICAO"
        return f"{prefixo}: {diag['tipo']} — {diag['detalhe']}"

    # ── 1ª opção: assinatura nova (tipo + codigo) ────────────────────────────
    if _API_ACEITA_TIPO is not False:
        csv_text, diag, status = _requisitar(
            dict(base, tipo=tipo, codigo=str(int(codigo))))
        if csv_text is not None:
            _API_ACEITA_TIPO = True
            return None, csv_text
        # Só cai para o modo legado quando o servidor recusa a assinatura
        if status not in (400, 404):
            return None, _falha(diag)
        # Assinatura nova recusada: por PDM não há alternativa
        if tipo != TIPO_CATMAT:
            return None, _falha(diag, f"esta instância da API não aceita busca por {tipo}")
        _API_ACEITA_TIPO = False

    # ── 2ª opção: assinatura antiga — existe apenas para CATMAT ──────────────
    if tipo != TIPO_CATMAT:
        return None, ("ERRO_REQUISICAO: esta instância da API não aceita busca "
                      f"por {tipo}. Selecione CATMAT.")

    csv_text, diag, _ = _requisitar(dict(base, codigoItemCatalogo=int(codigo)))
    if csv_text is not None:
        return None, csv_text
    return None, _falha(diag, "assinatura antiga (codigoItemCatalogo)")


def _normalizar_campo(item: dict, *candidatos, default=""):
    """Retorna o primeiro campo encontrado no dict entre os candidatos."""
    for c in candidatos:
        if c in item and item[c] is not None:
            return item[c]
    return default


def buscar_pdms_por_classe(codigo_classe: int, URL_BASE: str, TIMEOUT: int,
                           max_tentativas: int = 3):
    """Busca todos os PDMs de uma classe com retry automático e backoff."""
    URL = f"{URL_BASE}/modulo-material/3_consultarPdmMaterial"
    TAMANHO_PAGINA = 500
    all_pdms = []; pagina_atual = 1; total_paginas = 1; total_registros_api = 0

    while pagina_atual <= total_paginas:
        tentativa = 0
        sucesso   = False
        data      = None
        while tentativa < max_tentativas and not sucesso:
            try:
                # 429 é tratado pela cota (_get_compras): espera o Retry-After
                resp = _get_compras(URL, params={
                    "codigoClasse": codigo_classe, "pagina": pagina_atual,
                    "tamanhoPagina": TAMANHO_PAGINA, "bps": "false"
                }, timeout=TIMEOUT)
                resp.raise_for_status()
                data    = resp.json()
                sucesso = True
            except Exception as e:
                diag   = diagnosticar(exc=e)
                espera = (3 if diag["conexao"] else 2) * (tentativa + 1)
                _erros_api.registrar(
                    "CATÁLOGO", diag, codigo=f"classe {codigo_classe}", pagina=pagina_atual,
                    obs=f"tentativa {tentativa+1}/{max_tentativas}"
                        + (f", nova tentativa em {espera} s"
                           if tentativa + 1 < max_tentativas else ""))
                time.sleep(espera)
                tentativa += 1

        if not sucesso or data is None:
            return None

        if "resultado" in data:
            all_pdms.extend(data["resultado"])
        if pagina_atual == 1:
            total_registros_api = int(data.get("totalRegistros", 0))
            total_paginas = (math.ceil(total_registros_api / TAMANHO_PAGINA)
                             if total_registros_api > 0 else 1)
            print(f"Classe {codigo_classe}: {total_registros_api} PDMs / "
                  f"{total_paginas} página(s)")
        pagina_atual += 1       # o ritmo entre páginas é dado pela cota

    if not all_pdms:
        _erros_api.registrar("CATÁLOGO", {
            "tipo": "Classe sem PDMs", "http": 200,
            "detalhe": "a API respondeu, mas sem nenhum PDM (classe inexistente ou vazia?)",
            "resposta": "", "url": ""}, codigo=f"classe {codigo_classe}")
        return None

    # Normalizar campos — a API pode retornar nomes variados
    rows_norm = []
    for item in all_pdms:
        cod  = _normalizar_campo(item, "codigoPdm", "codigo", "id", "codigoItem")
        desc = _normalizar_campo(item, "nomePdm", "nome", "descricao", "descricaoPdm", "descricaoItem")
        # status pode ser bool True/False, string "ATIVO"/"INATIVO", ou inteiro
        raw_status = _normalizar_campo(item, "statusPdm", "status", "ativo", "situacao")
        if isinstance(raw_status, bool):
            status = "Ativo" if raw_status else "Inativo"
        elif isinstance(raw_status, str):
            status = "Ativo" if raw_status.upper() in ("ATIVO", "TRUE", "S", "SIM", "1") else "Inativo"
        elif isinstance(raw_status, (int, float)):
            status = "Ativo" if raw_status == 1 else "Inativo"
        else:
            status = "Ativo"
        rows_norm.append({"codigoPdm": cod, "nomePdm": desc, "statusPdm": status,
                          "_classe": str(codigo_classe)})

    df = pd.DataFrame(rows_norm).drop_duplicates(subset=["codigoPdm"])
    return df, total_registros_api


def buscar_catmats_por_pdm(codigos_pdm, URL_BASE, TIMEOUT, app,
                           log_fn=None, max_workers=5):
    """
    Busca CATMATs de múltiplos PDMs em paralelo usando ThreadPoolExecutor.
    As threads cobrem a latência; o ritmo real é o da cota (_get_compras).
    """
    global cancelar_busca_catmat
    URL   = f"{URL_BASE}/modulo-material/4_consultarItemMaterial"
    total = len(codigos_pdm)
    all_catmats  = []
    pdms_com_erro = []
    completed     = 0
    lock          = threading.Lock()   # protege all_catmats e pdms_com_erro

    def _fetch_pdm(idx_pdm):
        """Worker: busca todas as páginas de CATMATs de um PDM."""
        i, pdm_code = idx_pdm
        if cancelar_busca_catmat:
            return
        resultados = []; pagina_atual = 1; total_paginas = 1
        try:
            while pagina_atual <= total_paginas:
                if cancelar_busca_catmat: break
                # 429 é tratado pela cota; se persistir, raise_for_status
                # marca o PDM como erro e ele entra na próxima tentativa
                resp = _get_compras(URL, params={
                    "codigoPdm": pdm_code, "pagina": pagina_atual,
                    "tamanhoPagina": 500, "bps": "false"
                }, timeout=TIMEOUT)
                resp.raise_for_status()
                data = resp.json()
                if "resultado" in data:
                    resultados.extend(data["resultado"])
                if pagina_atual == 1:
                    total_paginas = data.get("totalPaginas", 1)
                pagina_atual += 1
        except Exception as e:
            _erros_api.registrar("CATÁLOGO", diagnosticar(exc=e), codigo=f"PDM {pdm_code}",
                                 pagina=pagina_atual, obs="lista de CATMATs do PDM")
            with lock:
                pdms_com_erro.append(pdm_code)
            return
        if resultados:
            with lock:
                all_catmats.extend(resultados)

    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = {
            executor.submit(_fetch_pdm, (i, pdm)): (i, pdm)
            for i, pdm in enumerate(codigos_pdm)
        }
        for future in as_completed(futures):
            pausar_busca_catmat.wait()        # respeita pausa
            if cancelar_busca_catmat:
                app.after(0, lambda: app.set_status_explorador("Busca cancelada."))
                executor.shutdown(wait=False, cancel_futures=True)
                break
            completed += 1
            i, pdm_code = futures[future]
            msg = f"PDM {pdm_code} ({completed}/{total})..."
            app.after(0, lambda m=msg: app.set_status_explorador(m))
            if log_fn and (completed % 5 == 0 or completed == total):
                app.after(0, lambda m=msg: log_fn(m))

    return pd.DataFrame(all_catmats) if all_catmats else None, pdms_com_erro



def _int_do_rodape(csv_text, rotulo):
    """
    Lê um inteiro do rodapé da resposta (ex.: 'totalPaginas: 1.234').
    Tolera separador de milhar — '\\d+' sozinho capturaria apenas o '1'.
    Retorna None quando o rótulo não aparece na resposta.
    """
    m = re.search(rotulo + r"\s*:\s*([\d.,]+)", csv_text, re.IGNORECASE)
    if not m:
        return None
    digitos = re.sub(r"\D", "", m.group(1))
    return int(digitos) if digitos else None


def _fetch_catmat_registros(codigo, d_ini, d_fim, salvar_corr, pasta_corr,
                            _pausar_conexao_fn=None, tipo=TIPO_CATMAT,
                            _cancelado_fn=None, TAMANHO_PAGINA=500):
    if _pausar_conexao_fn is None:
        _pausar_conexao_fn = lambda: None  # no-op se não fornecida
    if _cancelado_fn is None:
        _cancelado_fn = lambda: False
    """
    Worker puro: busca e processa todas as páginas de um CATMAT ou de um PDM,
    conforme `tipo` (TIPO_CATMAT | TIPO_PDM).
    Pode rodar em qualquer thread — não acessa estado compartilhado.
    Retorna: (codigo, dfs_e_meta, tipo, reg_esp, pag_corr)
      tipo: "ok" | "vazio" | "erro" | "conexao"
      dfs_e_meta: lista de (df_processado, marca, num_pagina)
                  marca: "" | "reparada" | "perda"
    """
    dfs_e_meta  = []
    pag_corr    = {}
    reg_esp     = 0
    pagina_atual = 1; total_paginas = None

    try:
        while True:
            _, csv_text = ler_pagina_catmat(codigo, 1, URL_BASE, TAMANHO_PAGINA, TIMEOUT,
                                            d_ini, d_fim, tipo=tipo)
            if csv_text and csv_text.startswith("ERRO_CONEXAO"):
                # Pausa automática + contagem regressiva de 60s antes de retentar
                _pausar_conexao_fn()
                for seg_restante in range(60, 0, -1):
                    # Se o usuário clicar Retomar manualmente, interrompe a contagem
                    if pausar_extracao.is_set():
                        break
                    time.sleep(1)
                # Retoma automaticamente ao fim da contagem (ou imediatamente se
                # o usuário já clicou Retomar)
                pausar_extracao.set()
                continue
            break
        if csv_text is None or csv_text.startswith("ERRO_REQUISICAO"):
            return codigo, [], "erro", 0, {}   # já registrado em ler_pagina_catmat

        reg_esp = _int_do_rodape(csv_text, "totalRegistros") or 0
        if reg_esp == 0:
            # HTTP 200 que não é CSV (página HTML de erro, JSON do gateway...)
            # não pode passar como "0 registros": vira erro e é retentado
            primeira = csv_text.lstrip().split("\n", 1)[0]
            if "totalRegistros" not in csv_text and ";" not in primeira:
                _erros_api.registrar("DA", {
                    "tipo": "Resposta inesperada", "http": 200,
                    "detalhe": "HTTP 200, mas o conteúdo não é o CSV da Pesquisa de Preço",
                    "resposta": _trecho(csv_text), "url": ""}, codigo=codigo, pagina=1)
                return codigo, [], "erro", 0, {}
            return codigo, [], "vazio", 0, {}

        # Total de páginas: o rodapé da resposta é a fonte primária. O cálculo
        # por totalRegistros entra como conferência e, quando o rodapé falta,
        # como substituto — o código antigo caía em 1 nesse caso e truncava a
        # extração nos 500 primeiros registros em silêncio.
        pag_rodape    = _int_do_rodape(csv_text, r"total\s*(?:de\s*)?p[áa]ginas?")
        pag_calculado = max(1, math.ceil(reg_esp / TAMANHO_PAGINA))
        total_paginas = max(pag_rodape or 0, pag_calculado)
        # Sem rodapé não há como conferir a paginação: só nesse caso vale
        # insistir enquanto as páginas voltarem cheias.
        confiar_no_rodape = pag_rodape is not None

        while True:
            # Respeita pausa/cancelamento também ENTRE PÁGINAS de um mesmo código
            pausar_extracao.wait()
            if _cancelado_fn():
                break

            esperado_pag = max(0, min(TAMANHO_PAGINA,
                                      reg_esp - TAMANHO_PAGINA * (pagina_atual - 1)))
            df_pag, diag = parse_pagina_csv(csv_text, esperado_pag or None)
            # "perda"    → não foi possível reconstituir a página fielmente
            # "reparada" → houve conserto, mas todos os registros foram recuperados
            if not diag["ok"]:
                marca = "perda"
            elif diag["reparos"]:
                marca = "reparada"
            else:
                marca = ""

            if marca == "perda":
                pag_corr.setdefault(codigo, []).append(str(pagina_atual))
                if salvar_corr and pasta_corr:
                    dest = os.path.join(pasta_corr,
                        f"cod_{codigo}_pag_{pagina_atual}_corr.csv")
                    try:
                        with open(dest, "w", encoding="utf-8-sig") as f:
                            f.write(csv_text)
                    except Exception:
                        pass

            if df_pag is not None and not df_pag.empty:
                if tipo == TIPO_PDM:
                    # Busca por PDM devolve vários CATMATs: preserva o
                    # codigoItemCatalogo original e apenas anota o PDM de origem
                    df_pag.loc[:, "codigoPdm"] = str(codigo)
                else:
                    df_pag.loc[:, "codigoItemCatalogo"] = str(codigo)
                df_proc = processar_dataframe_final(df_pag, ordem_final_colunas)
                dfs_e_meta.append((df_proc, marca, pagina_atual))

            if total_paginas is None:
                total_paginas = 1

            # Com rodapé, ele manda: nenhuma requisição além do que ele informa.
            # Sem rodapé, página cheia é o único indício de que ainda há dados.
            insistir = (not confiar_no_rodape
                        and df_pag is not None and len(df_pag) >= TAMANHO_PAGINA)
            pagina_atual += 1
            if pagina_atual > total_paginas and not insistir:
                break
            if pagina_atual > _MAX_PAGINAS:      # trava de segurança
                break
            # Sem pausa fixa entre páginas: o ritmo é o da cota (_get_compras)
            _, csv_text = ler_pagina_catmat(codigo, pagina_atual, URL_BASE,
                                            TAMANHO_PAGINA, TIMEOUT,
                                            d_ini, d_fim, tipo=tipo)
            if csv_text is None or csv_text.startswith("ERRO_"):
                # O erro já foi registrado; o relatório de integridade acusa a
                # divergência deste código
                break

        return codigo, dfs_e_meta, ("ok" if dfs_e_meta else "vazio"), reg_esp, pag_corr

    except Exception as e:
        # Erro no próprio extrator (ex.: mudança de schema), não na API
        _erros_api.registrar("PROGRAMA", diagnosticar(exc=e), codigo=codigo,
                             pagina=pagina_atual)
        return codigo, [], "erro", 0, {}


# =============================================================================
# BPS — BANCO DE PREÇOS EM SAÚDE (API de Dados Abertos do Ministério da Saúde)
# -----------------------------------------------------------------------------
# GET /economia-da-saude/bps?codigoCatmat=...&dataCompraInicio=...&pagina=...
# Traz os mesmos registros da extração SQL do dbbps (extracao_bps.sql). A API
# não informa o total de registros: a paginação segue até vir uma página
# incompleta. Também não filtra por PDM, então um PDM é expandido nos seus
# CATMATs pelo catálogo do Compras.gov antes da consulta.
# =============================================================================

URL_BPS = "https://apidadosabertos.saude.gov.br/economia-da-saude/bps"
TAMANHO_PAGINA_BPS = 500           # máximo aceito pela API
ABA_BPS = "BPS"
# Cabeçalho da aba BPS: os campos da API, com os nomes da API, na ORDEM das
# colunas do extracao_bps.sql (à direita, a coluna equivalente do SQL).
COLUNAS_BPS = [
    "codigoCatmat",             # codigoBR
    "descricaoItem",            # descricaoCATMAT
    "unidadeFornecimento",      # unidadeFornecimento
    "capacidade",               # capacidade (a API entrega x100: 25000.0 = 250,00)
    "siglaUnidadeMedida",       # unidadeMedida
    "unidadeMedidaCapacidade",  # unidadeFornecimentoCapacidade
    "codigoClasse",             # codigoClasse
    "nomeClasse",               # descricaoClasse
    "codigoPdm",                # pdm
    "nomePdm",                  # descricaoPDM
    "registroAnvisa",           # anvisa
    "generico",                 # generico
    "anoCompra",                # anoCompra
    "dataCompra",               # compra
    "dataInsercao",             # insercao
    "modalidade",               # modalidadeCompra
    "tipoCompra",               # tipoCompra
    "nomeInstituicao",          # nomeInstituicao
    "cnpjInstituicao",          # cnpjInstituicao
    "municipio",                # municipioInstituicao
    "estado",                   # uf
    "esfera",                   # esfera
    "cnpjFornecedor",           # cnpjFornecedor
    "nomeFornecedor",           # fornecedor
    "cnpjFabricante",           # cnpjFabricante
    "nomeFabricante",           # fabricante
    "quantidade",               # qtdItensComprados
    "precoUnitario",            # precoUnitario
    "precoTotal",               # precoTotal
    "numeroProcessoCompra",     # numeroProcesso
    "numeroAta",                # numeroAtaPrecos
    "validadeCompra",           # validadeCompra
    "observacoes",              # observacoes
    # No SQL as colunas de grupo ficam comentadas no fim (fora da exportação);
    # aqui vêm depois das 33, sem mexer na posição das demais.
    "codigoGrupo",              # codigoGrupo
    "nomeGrupo",                # descricaoGrupo
]

# A consulta ao BPS roda em paralelo com a do Compras.gov (servidores
# diferentes), então não soma tempo à extração. "orq" coordena um código por
# vez; o outro pool faz as chamadas de cada CATMAT (um PDM tem dezenas).
_pool_bps_orq = ThreadPoolExecutor(max_workers=2, thread_name_prefix="bps-orq")
_pool_bps     = ThreadPoolExecutor(max_workers=4, thread_name_prefix="bps")
# Descrição do BPS por CATMAT ("" = CATMAT sem nenhuma compra no BPS).
# Zerado no início de cada extração.
_cache_desc_bps: dict = {}
_cache_desc_lock = threading.Lock()

# ── Limpeza da descrição: ESPELHO do bloco ds_catmat do extracao_bps.sql ────
# Mesmas regras, na mesma ordem, para que a descrição que o extrator grava
# seja idêntica à da extração SQL. Mudou uma regra lá? Mude aqui também.
_TAGS_BPS = re.compile(r"</?(div|span|p|br|b|i|strong|em)[^>]*>", re.IGNORECASE)
_ENTIDADES_BPS = (
    ("&#193;", "Á"), ("&#194;", "Â"), ("&#195;", "Ã"), ("&#199;", "Ç"),
    ("&#201;", "É"), ("&#202;", "Ê"), ("&#205;", "Í"), ("&#211;", "Ó"),
    ("&#212;", "Ô"), ("&#213;", "Õ"), ("&#218;", "Ú"), ("&#220;", "Ü"),
    ("&#225;", "á"), ("&#226;", "â"), ("&#227;", "ã"), ("&#231;", "ç"),
    ("&#233;", "é"), ("&#234;", "ê"), ("&#237;", "í"), ("&#243;", "ó"),
    ("&#244;", "ô"), ("&#245;", "õ"), ("&#250;", "ú"), ("&#252;", "ü"),
    ("&#186;", "º"), ("&#170;", "ª"),
)
_ESP = r"[ \t\n\r\f\v]"            # o [[:space:]] do PostgreSQL
_RE_DOIS_PONTOS_BPS = re.compile(_ESP + r"*:" + _ESP + r"*")
_RE_ESP_VIRGULA_BPS = re.compile(_ESP + r"+,")
_RE_ESPACOS_BPS     = re.compile(_ESP + r"+")


def limpar_descricao_bps(texto) -> str:
    """Descrição do BPS limpa exatamente como o extracao_bps.sql limpa."""
    if not texto:
        return ""
    s = _TAGS_BPS.sub("", str(texto))
    for entidade, caractere in _ENTIDADES_BPS:
        s = s.replace(entidade, caractere)
    s = s.replace("¿", "°").replace("*", "")
    s = _RE_DOIS_PONTOS_BPS.sub(": ", s)
    s = _RE_ESP_VIRGULA_BPS.sub(",", s)
    s = _RE_ESPACOS_BPS.sub(" ", s)
    return s.strip(" ")


def _cod_bps(valor) -> str:
    """CATMAT como chave: só dígitos, sem zeros à esquerda."""
    s = str(valor if valor is not None else "").strip()
    return (s.lstrip("0") or "0") if s.isdigit() else ""


def _get_bps(params, cancelado=None, tentativas=4, contexto=""):
    """Uma página da API do BPS. Devolve (lista, erro); lista=None se falhou."""
    ultimo = diag = None
    for t in range(tentativas):
        pausar_extracao.wait()
        if cancelado and cancelado():
            return None, "cancelado"
        try:
            r = _http.get(URL_BPS, params=params, timeout=TIMEOUT)
            if r.status_code == 429 or r.status_code >= 500:
                diag = diagnosticar(resp=r)
                time.sleep(15 * (t + 1) if r.status_code == 429 else 3 * (t + 1))
                continue
            r.raise_for_status()
            dados = r.json()
            if not isinstance(dados, dict):
                raise ValueError(f"JSON sem o campo 'bps': {_trecho(r.text, 120)}")
            return dados.get("bps") or [], None
        except (requests.exceptions.RequestException, ValueError) as e:
            diag = diagnosticar(exc=e)
            time.sleep(3 * (t + 1))
    if diag is not None:
        ultimo = f"{diag['tipo']} — {diag['detalhe']}"
        _erros_api.registrar("BPS", diag, codigo=params.get("codigoCatmat", ""),
                             pagina=params.get("pagina", ""),
                             obs=", ".join(p for p in (contexto, f"{tentativas} tentativas") if p))
    return None, ultimo


def buscar_bps_catmat(codigo, d_ini=None, d_fim=None, cancelado=None):
    """Todos os registros do BPS de um CATMAT no período. Devolve (lista, erro)."""
    base = {"codigoCatmat": _cod_bps(codigo), "tamanhoPagina": TAMANHO_PAGINA_BPS}
    if d_ini: base["dataCompraInicio"] = d_ini
    if d_fim: base["dataCompraFim"]    = d_fim
    registros = []
    for pagina in range(1, _MAX_PAGINAS + 1):
        lote, erro = _get_bps(dict(base, pagina=pagina), cancelado,
                              contexto="registros do período (aba BPS)")
        if lote is None:
            return None, erro
        registros.extend(lote)
        if len(lote) < TAMANHO_PAGINA_BPS:
            break
    return registros, None


def _descricao_bps_sem_periodo(codigo, cancelado=None):
    """Descrição de um CATMAT que não teve compra no BPS dentro do período:
    basta um registro de qualquer data. None = falha; "" = nunca comprado."""
    lote, _ = _get_bps({"codigoCatmat": _cod_bps(codigo), "pagina": 1,
                        "tamanhoPagina": 1}, cancelado,
                       contexto="descrição do CATMAT")
    if lote is None:
        return None
    return limpar_descricao_bps(lote[0].get("descricaoItem")) if lote else ""


def catmats_do_pdm(pdm, tentativas=3):
    """CATMATs de um PDM no catálogo do Compras.gov. None se a API falhar."""
    URL = f"{URL_BASE}/modulo-material/4_consultarItemMaterial"
    codigos, pagina, total_paginas = [], 1, 1
    while pagina <= total_paginas:
        diag = None
        for t in range(tentativas):
            try:
                # gasta da mesma cota da pesquisa de preço (429 tratado lá)
                resp = _get_compras(URL, params={"codigoPdm": int(pdm), "pagina": pagina,
                                                 "tamanhoPagina": 500, "bps": "false"})
                resp.raise_for_status()
                data = resp.json()
                break
            except (requests.exceptions.RequestException, ValueError) as e:
                diag = diagnosticar(exc=e)
                time.sleep(3 * (t + 1))
        else:
            _erros_api.registrar("CATÁLOGO", diag, codigo=f"PDM {pdm}", pagina=pagina,
                                 obs=f"lista de CATMATs do PDM para o BPS, {tentativas} tentativas")
            return None
        codigos += [_cod_bps(i.get("codigoItem")) for i in data.get("resultado", [])]
        if pagina == 1:
            total_paginas = int(data.get("totalPaginas") or 1)
        pagina += 1
    return [c for c in dict.fromkeys(codigos) if c]


def _bps_do_codigo(codigo, tipo, d_ini, d_fim, cancelado=None) -> dict:
    """Registros do BPS de um código da extração: o próprio CATMAT, ou todos
    os CATMATs de um PDM. A descrição já sai limpa (limpar_descricao_bps).

    Devolve {"df": DataFrame|None, "desc": {catmat: descrição},
             "falhas": [catmats que não responderam], "trocadas": 0}
    """
    res = {"df": None, "desc": {}, "falhas": [], "trocadas": 0}
    if tipo == TIPO_PDM:
        catmats = catmats_do_pdm(codigo)
        if catmats is None:
            res["falhas"].append(f"PDM {codigo} (lista de CATMATs)")
            return res
    else:
        catmats = [_cod_bps(codigo)]
    futuros = [(c, _pool_bps.submit(buscar_bps_catmat, c, d_ini, d_fim, cancelado))
               for c in catmats if c]
    registros = []
    for c, fut in futuros:                  # na ordem dos CATMATs, não de chegada
        lote, _erro = fut.result()
        if lote is None:
            res["falhas"].append(c)
            continue
        for r in lote:
            r["descricaoItem"] = limpar_descricao_bps(r.get("descricaoItem"))
            if r["descricaoItem"]:
                res["desc"].setdefault(_cod_bps(r.get("codigoCatmat")), r["descricaoItem"])
        registros.extend(lote)
    if registros:
        extras = [k for k in registros[0] if k not in COLUNAS_BPS]
        res["df"] = pd.DataFrame(registros).reindex(columns=COLUNAS_BPS + extras)
    with _cache_desc_lock:
        _cache_desc_bps.update(res["desc"])
    return res


def _aplicar_descricao_bps(dfs_e_meta, bps: dict, cancelado=None):
    """Troca o descricaoItem do Compras.gov pela descrição do BPS do mesmo
    CATMAT. CATMAT sem compra no BPS no período ganha uma consulta sem data;
    sem nenhuma compra no BPS, fica o texto do Compras.gov."""
    usados = set()
    for df, _m, _p in dfs_e_meta:
        if "codigoItemCatalogo" in df.columns:
            usados.update(_cod_bps(c) for c in df["codigoItemCatalogo"])
    usados.discard("")
    with _cache_desc_lock:
        faltam = [c for c in usados
                  if c not in bps["desc"] and c not in _cache_desc_bps]
    futuros = [(c, _pool_bps.submit(_descricao_bps_sem_periodo, c, cancelado))
               for c in faltam]
    for c, fut in futuros:
        desc = fut.result()
        if desc is not None:                # falha não entra no cache: tenta de novo depois
            with _cache_desc_lock:
                _cache_desc_bps[c] = desc
    with _cache_desc_lock:
        mapa = {c: _cache_desc_bps.get(c, "") for c in usados}
    mapa.update(bps["desc"])
    mapa = {c: d for c, d in mapa.items() if d}
    if not mapa:
        return
    for df, _m, _p in dfs_e_meta:
        if "codigoItemCatalogo" not in df.columns or "descricaoItem" not in df.columns:
            continue
        novo = df["codigoItemCatalogo"].map(_cod_bps).map(mapa)
        trocar = novo.notna() & (novo != df["descricaoItem"])
        if trocar.any():
            df.loc[trocar, "descricaoItem"] = novo[trocar]
            bps["trocadas"] += int(trocar.sum())


def _fetch_codigo(codigo, d_ini, d_fim, salvar_corr, pasta_corr,
                  _pausar_conexao_fn=None, tipo=TIPO_CATMAT,
                  _cancelado_fn=None, bps_cfg=None):
    """Compras.gov e/ou BPS do mesmo código, conforme as fontes escolhidas.

    Devolve a 5-tupla de _fetch_catmat_registros + o resultado do BPS (dict
    de _bps_do_codigo, ou None quando o BPS não foi consultado). Sem o
    Compras.gov, a 5-tupla vem com o status "pulado".
    bps_cfg: None (só Compras.gov) | {"compras", "aba", "descricao"} — ver
    App._config_bps. Com descricao=True e sem a fonte BPS, só a descrição de
    cada CATMAT é consultada (uma chamada leve por CATMAT).
    """
    cfg = bps_cfg or {"compras": True, "aba": False, "descricao": False}
    fut = (_pool_bps_orq.submit(_bps_do_codigo, codigo, tipo, d_ini, d_fim,
                                _cancelado_fn) if cfg["aba"] else None)
    if cfg["compras"]:
        res = _fetch_catmat_registros(codigo, d_ini, d_fim, salvar_corr, pasta_corr,
                                      _pausar_conexao_fn, tipo, _cancelado_fn)
    else:
        res = (codigo, [], "pulado", 0, {})
    bps = None
    if fut is not None:
        try:
            bps = fut.result()
        except Exception as e:              # BPS nunca derruba a extração do Compras.gov
            _erros_api.registrar("PROGRAMA", diagnosticar(exc=e), codigo=codigo,
                                 obs="ao montar os dados do BPS")
            bps = {"df": None, "desc": {}, "falhas": [f"{codigo} ({type(e).__name__})"],
                   "trocadas": 0}
    elif cfg["descricao"]:
        bps = {"df": None, "desc": {}, "falhas": [], "trocadas": 0}
    if bps is None:
        return res + (None,)
    if cfg["descricao"] and res[2] == "ok":
        try:
            _aplicar_descricao_bps(res[1], bps, _cancelado_fn)
        except Exception as e:
            _erros_api.registrar("PROGRAMA", diagnosticar(exc=e), codigo=codigo,
                                 obs="ao trocar a descrição pelo texto do BPS")
    return res + (bps,)


def processar_dataframe_final(df: pd.DataFrame, ordem_colunas: List[str]) -> pd.DataFrame:
    if df.empty: return df
    fc = df.columns[0]
    df = df[~df[fc].astype(str).str.contains("totalRegistros|totalPaginas",
                                              case=False, na=False)].copy()
    if df.empty: return df

    # Mapa sigla → nome completo para preencher nomeUnidadeFornecimento ausente
    _SIGLA_NOME = {
        "FR-AM": "Frasco-Ampola", "FR": "Frasco", "CAPS": "Cápsula",
        "COMPR": "Comprimido", "AM": "Ampola", "UN": "Unidade",
        "SER": "Seringa", "BIS": "Bisnaga", "BLIS": "Blister",
        "BOL": "Bolsa", "BOM": "Bombona", "CA": "Cartucho",
        "CI": "Curie", "CJ": "Conjunto", "DOSE(S)": "Dose(s)",
        "DOSES": "Dose(s)", "DRAG": "Drágea", "EMB": "Embalagem",
        "EMP": "Emplastro", "ENV": "Envelope", "FLAC": "Flaconete",
        "G": "Grama", "GL": "Galão", "GLOB": "Glóbulo",
        "KG": "Quilograma", "L": "Litro", "MCG": "Micrograma",
        "MCU": "Milicurie", "MG": "Miligrama",
        "MIL CTE": "Milheiro de Cartelas", "ML": "Mililitro",
        "PAST": "Pastilha", "POTE": "Pote", "RO": "Rolo",
        "SAC": "Sachê", "SUP": "Supositório", "TAB": "Tablete",
        "TBO": "Tubo", "TBTE": "Tubete", "UI": "Unid. Internacional",
    }

    def _val(row, col):
        v = row.get(col)
        s = str(v).strip() if pd.notna(v) else ""
        return "" if s in ("", "nan", "None", "null") else s

    def uf(row):
        nome  = _val(row, "nomeUnidadeFornecimento")
        sigla = _val(row, "siglaUnidadeFornecimento")
        cap   = _val(row, "capacidadeUnidadeFornecimento")
        medi  = _val(row, "siglaUnidadeMedida")

        # Se nomeUnidade vazio, tentar preencher pela sigla
        if not nome and sigla:
            nome = _SIGLA_NOME.get(sigla.upper(), sigla)

        # capacidade: ignorar se for 0,00 ou 0.00 ou 0
        try:
            cap_num = float(cap.replace(".", "").replace(",", ".")) if cap else 0
        except Exception:
            cap_num = 0
        cap_valida = bool(cap) and cap_num != 0

        # Montar: Nome + capacidade + siglaUnidadeMedida
        # Se não houver capacidade válida, ignorar também siglaUnidadeMedida
        partes = [nome] if nome else []
        if cap_valida:
            partes.append(cap)
            if medi:
                partes.append(medi)
        return " ".join(partes)

    df["Unidade de Fornecimento"] = df.apply(uf, axis=1)

    def tof(v):
        if pd.isna(v): return 0.0
        try: return float(str(v).replace(".", "").replace(",", "."))
        except: return 0.0

    # Sem os .get() a ausência de uma coluna vira KeyError, que o worker
    # captura no except genérico e reporta como "erro de API" — uma mudança de
    # schema na origem apareceria como indisponibilidade, sem pista no log.
    if "precoUnitario" in df.columns and "quantidade" in df.columns:
        df["Preco Total"] = df["precoUnitario"].apply(tof) * df["quantidade"].apply(tof)
    else:
        df["Preco Total"] = 0.0
    for col in ["nomeUnidadeMedida","percentualMaiorDesconto"]:
        if col in df.columns and (df[col].isnull().all() or
                                   df[col].astype(str).str.strip().eq("").all()):
            df = df.drop(columns=[col])
    exist = [c for c in ordem_colunas if c in df.columns]
    extra = [c for c in df.columns if c not in exist]
    return df[exist + extra]


# =============================================================================
# COMPONENTES DE UI  (helpers)
# =============================================================================

WELCOME = """\
Olá! Bem-vindo ao Extrator de CATMATs Pro.

Sua ferramenta para extrair e descobrir dados no Portal de Compras Governamentais!

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
O que este programa faz?
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

Este programa possui tres funcoes principais em abas separadas:

  1. Extracao por CATMAT (esta aba)
     Se voce ja tem uma lista de codigos de materiais (CATMATs), esta aba
     busca todas as informacoes de compras, corrige problemas nos dados e
     consolida tudo em um arquivo Excel ou CSV.

     Em "Fontes" (vale tambem para a aba 2) escolha de onde extrair:
       Compras.gov + BPS -> planilha com as abas Dados CATMAT e BPS (no
                            CSV, um arquivo _BPS ao lado do principal)
       so Compras.gov    -> como antes, so a aba Dados CATMAT
       so BPS            -> arquivo ..._BPS com os registros do Banco de
                            Precos em Saude (API de Dados Abertos da Saude)
     Com "Usar a descricao do BPS", o descricaoItem do Compras.gov passa a
     ser o texto do BPS do mesmo CATMAT. As colunas do BPS seguem a ordem
     do extracao_bps.sql. Com "Salvar um arquivo por fonte", cada classe
     gera "Classe 6505 - DA" e "Classe 6505 - BPS" em vez de uma pasta de
     trabalho com as duas abas.

  2. Extracao por Classes (aba ao lado)
     Se voce quer descobrir novos itens, pode comecar com o codigo de uma
     ou mais Classes, encontrar todos os Padroes Descritivos de Materiais
     (PDMs) dentro delas e, em seguida, listar todos os CATMATs relacionados
     para extracao.

  3. Consolidacao DW + DA (ultima aba)
     Junta o historico do DW (SIASG) com o do DA (Dados Abertos) em uma
     planilha por Classe, removendo do DA todo registro que ja exista no
     DW. A chave e o identificador de 22 digitos do item da compra.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Primeiros Passos
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  - Para uma extracao direta com uma lista pronta, use esta aba.
    Escolha em "Buscar por" se os codigos da planilha sao CATMATs ou PDMs
    (equivale ao parametro "tipo" da API de Pesquisa de Preco):
      CATMAT -> coluna codigoItemCatalogo
      PDM    -> coluna codigoPdm  (traz todos os itens do PDM de uma vez)
    A coluna generica "codigo" tambem e aceita nos dois modos.

  - Para descobrir itens, use a aba "Extracao por Classes" e, ao final,
    envie os CATMATs encontrados para a extracao nesta aba.
    Nessa aba, marcando "Extrair precos direto por PDM" o programa pula a
    expansao PDM -> CATMAT e consulta a Pesquisa de Preco com tipo=codigoPdm,
    o que reduz drasticamente o numero de requisicoes.

  - Utilize os filtros de data (DD-MM-AAAA) para restringir os resultados
    a um periodo especifico de compras (Data de Inicio e Data Final).

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Acompanhe todo o processo em tempo real neste log. Bom trabalho!
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


BEMVINDO_CONSOLIDACAO = """\
Consolidacao DW + DA  —  remocao de duplicatas

Junta os dados do DW (SIASG) e do DA (Dados Abertos / Compras.gov) em uma
planilha por Classe, com duas abas (dw-XXXX e da-XXXX), descartando do DA
todo registro que ja exista no DW.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Regra de duplicidade
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A chave e a identificacao do item da compra, com 22 digitos:

  DW  -> coluna "Identif Item Compra" (ja vem com 22 digitos)
  DA  -> idCompra (completado com zeros a esquerda ate 17)
         + numeroItemCompra (completado ate 5)

Se a chave do DA existir no DW, a linha do DA sai: o DW e a fonte
preferencial, por trazer mais informacao.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Descricao do item
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

A coluna "Descricao" do DW recebe o descritivo que o DA usa para o mesmo
CATMAT, para as duas abas falarem a mesma lingua. CATMAT do DW que nao
aparece em nenhum arquivo do DA mantem a descricao do proprio DW.

━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
Como usar
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━

  1. Adicione as pastas (ou arquivos) com os CSVs/XLSX do DW e do DA.
     Com "Detectar", a origem de cada arquivo e descoberta pelo cabecalho;
     use DW ou DA para forcar a classificacao.
  2. Escolha a pasta de saida.
  3. Clique em "Consolidar e Remover Duplicatas".

Cada classe vira uma pasta de trabalho com as abas DW e DA. Se uma delas
passar do limite de 1.048.576 linhas do Excel, o excedente vai para
"... - Parte 2", cortando em fronteira de ano.

"Processos" divide a gravacao entre nucleos do computador - uma classe por
processo. Comece com 1 e va aumentando: o ganho depende da maquina.

Alem das planilhas por classe, sao gerados o Relatorio_Consolidacao.xlsx
(contagens por classe) e, quando houver, o linhas_em_quarentena.csv com as
linhas corrompidas na origem — preservadas na integra e fora das planilhas.
━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━
"""


def _lbl(parent, text, size=12, weight="normal", color=C_TEXT, **kw):
    return ctk.CTkLabel(parent, text=text, font=("Segoe UI", size, weight),
                        text_color=color, **kw)


def _btn(parent, text, command, variant="secondary", width=0, **kw):
    pal = {
        "primary":   (C_SURFACE,  C_ACCENT,  C_SURFACE,  C_ACCENT_H),
        "success":   (C_SURFACE,  C_GREEN,   C_SURFACE,  C_GREEN_H),
        "danger":    (C_SURFACE,  C_RED,     C_SURFACE,  "#992B1E"),
        "secondary": (C_TEXT,     "#E4E7EF", C_TEXT,     C_BORDER),
        "ghost":     (C_ACCENT,   "transparent", C_ACCENT_H, "#E8EDF8"),
    }
    tc, bg, htc, hbg = pal.get(variant, pal["secondary"])
    return ctk.CTkButton(parent, text=text, command=command,
                         font=("Segoe UI", 12), fg_color=bg, text_color=tc,
                         hover_color=hbg, corner_radius=6,
                         width=width, height=32, **kw)


def _entry(parent, textvariable=None, placeholder="", width=200, **kw):
    return ctk.CTkEntry(parent, textvariable=textvariable,
                        placeholder_text=placeholder,
                        font=("Segoe UI", 12),
                        fg_color=C_SURFACE, text_color=C_TEXT,
                        border_color=C_BORDER, border_width=1,
                        corner_radius=6, width=width,
                        placeholder_text_color=C_TEXT_LIGHT, **kw)


def _sep(parent, pady=(6,6)):
    ctk.CTkFrame(parent, height=1, fg_color=C_BORDER,
                 corner_radius=0).pack(fill="x", padx=14, pady=pady)


def _card(parent, title="", **kw):
    outer = ctk.CTkFrame(parent, fg_color=C_SURFACE, corner_radius=8,
                         border_width=1, border_color=C_BORDER, **kw)
    if title:
        _lbl(outer, title, size=11, weight="bold", color=C_TEXT_MED)\
            .pack(anchor="w", padx=14, pady=(10,4))
        _sep(outer, pady=(0,6))
    return outer


# =============================================================================
# APLICATIVO PRINCIPAL
# =============================================================================

class App(ctk.CTk):
    def __init__(self):
        super().__init__()
        self.title("Extrator de CATMATs Pro  |  BPS / DESID")
        self.withdraw()                          # esconde até centralizar
        self.geometry("1100x860")
        self.minsize(960, 720)
        self.configure(fg_color=C_BG)

        # estado
        self.processing           = False
        self.codes_iterator       = None
        self.writer               = None
        self.codigos_lista: List  = []
        self.paginas_corrompidas  = {}
        self.registros_esperados  = {}
        self.registros_baixados   = {}
        self.total_baixados       = 0
        self.count_corrigidas     = 0
        self.count_reparadas      = 0
        self._modo_por_classe     = False
        self.count_vazios         = 0
        self._data_inicio         = None
        self._data_fim            = None
        self._tipo_busca          = TIPO_CATMAT
        self.lista_pdms_df        = pd.DataFrame()
        self.lista_catmats: List  = []

        # Construir interface ANTES de centralizar
        self._build_header()
        self._build_tabs()

        # Cada erro de API aparece no log com fonte, tipo e resposta da API.
        # O registro é chamado pelas threads de extração: a tela só é tocada
        # na thread principal.
        _erros_api.ao_registrar = lambda m: self._ui(lambda: self._log("🔴 " + m, "err"))

        # Centralizar após tudo construído — delay generoso para o Tkinter
        # calcular dimensões reais antes de exibir
        self.after(200, self._centralizar)

    def _centralizar(self):
        self.update_idletasks()
        sw = self.winfo_screenwidth()
        sh = self.winfo_screenheight()
        # winfo_width retorna 1 até a janela aparecer; usar reqwidth como fallback
        ww = self.winfo_reqwidth()  or 1100
        wh = self.winfo_reqheight() or 800
        # Respeitar o geometry definido (1100x800)
        ww = max(ww, 1100)
        wh = max(wh, 860)
        x = max(0, (sw - ww) // 2)
        y = max(0, (sh - wh) // 2)
        self.geometry(f"{ww}x{wh}+{x}+{y}")
        self.deiconify()

    # ── HEADER ────────────────────────────────────────────────────────────────
    def _build_header(self):
        hdr = ctk.CTkFrame(self, fg_color=C_ACCENT, corner_radius=0, height=50)
        hdr.pack(fill="x"); hdr.pack_propagate(False)
        _lbl(hdr, "  Extrator de CATMATs Pro", size=15, weight="bold",
             color=C_SURFACE).pack(side="left", padx=6)
        _lbl(hdr, "BPS · DESID · Ministério da Saúde  ",
             size=10, color="#A8BFDF").pack(side="right")
        # faixa amarela
        ctk.CTkFrame(self, height=3, fg_color=C_YELLOW,
                     corner_radius=0).pack(fill="x")

    # ── TABS ──────────────────────────────────────────────────────────────────
    def _build_tabs(self):
        self.tabs = ctk.CTkTabview(
            self, fg_color=C_BG,
            segmented_button_fg_color=C_BORDER,
            segmented_button_selected_color=C_ACCENT,
            segmented_button_selected_hover_color=C_ACCENT_H,
            segmented_button_unselected_color=C_BORDER,
            segmented_button_unselected_hover_color="#C5CAD5",
            text_color=C_TEXT, text_color_disabled=C_TEXT_LIGHT,
            corner_radius=0)
        self.tabs.pack(fill="both", expand=True)
        self.tabs.add("  Extração por CATMAT  ")
        self.tabs.add("  Extração por Classes  ")
        self.tabs.add("  Consolidação DW + DA  ")
        self._build_tab_extracao(self.tabs.tab("  Extração por CATMAT  "))
        self._build_tab_explorador(self.tabs.tab("  Extração por Classes  "))
        self._build_tab_consolidacao(self.tabs.tab("  Consolidação DW + DA  "))

    # ── ABA 1 ─────────────────────────────────────────────────────────────────
    def _build_tab_extracao(self, parent):
        parent.configure(fg_color=C_BG)
        # Frame normal sem scroll — tudo deve caber na tela
        wrap = ctk.CTkFrame(parent, fg_color=C_BG, corner_radius=0)
        wrap.pack(fill="both", expand=True, padx=12, pady=8)

        # — Card 1: entrada —
        c1 = _card(wrap, "1.  Dados para a Extração")
        c1.pack(fill="x", pady=(0,8))
        inn = ctk.CTkFrame(c1, fg_color="transparent")
        inn.pack(fill="x", padx=14, pady=(0,12))

        # arquivo
        r = ctk.CTkFrame(inn, fg_color="transparent"); r.pack(fill="x", pady=3)
        _lbl(r, "Arquivo de Códigos:", color=C_TEXT_MED).pack(side="left", padx=(0,8))
        self.var_arquivo = tk.StringVar()
        _entry(r, textvariable=self.var_arquivo,
               placeholder="Selecione .xlsx ou .csv", width=420)\
            .pack(side="left", expand=True, fill="x")
        _btn(r, "Procurar…", self._escolher_arquivo, variant="ghost", width=90)\
            .pack(side="left", padx=(8,0))

        _sep(inn)

        # tipo de busca — espelha o parâmetro "tipo" da API
        rt = ctk.CTkFrame(inn, fg_color="transparent"); rt.pack(fill="x", pady=3)
        _lbl(rt, "Buscar por:", color=C_TEXT_MED).pack(side="left", padx=(0,10))
        self.var_tipo1 = tk.StringVar(value=ROTULO_TIPO[TIPO_CATMAT])
        ctk.CTkSegmentedButton(
            rt, values=[ROTULO_TIPO[TIPO_CATMAT], ROTULO_TIPO[TIPO_PDM]],
            variable=self.var_tipo1, command=self._on_tipo_extracao,
            font=("Segoe UI", 12), width=180, corner_radius=6,
            fg_color=C_BG, selected_color=C_ACCENT, selected_hover_color=C_ACCENT_H,
            unselected_color=C_BG, unselected_hover_color=C_BORDER,
            text_color=C_TEXT).pack(side="left")
        self.lbl_hint_tipo = _lbl(rt, "coluna esperada no arquivo: codigoItemCatalogo",
                                  size=10, color=C_TEXT_LIGHT)
        self.lbl_hint_tipo.pack(side="left", padx=(12,0))

        # um arquivo por classe — a classe vem do próprio registro (codigoClasse),
        # ou de uma coluna "classe" no arquivo de entrada, quando houver
        rpc = ctk.CTkFrame(inn, fg_color="transparent"); rpc.pack(fill="x", pady=3)
        self._row_por_classe1 = rpc
        self.var_por_classe1 = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(rpc, text="Salvar um arquivo por classe",
                        variable=self.var_por_classe1,
                        command=self._toggle_pasta_classe1,
                        font=("Segoe UI", 12), text_color=C_TEXT,
                        fg_color=C_ACCENT, border_color=C_BORDER).pack(side="left")
        _lbl(rpc, "  (separa a saída em classe_XXXX; a classe vem dos próprios "
                  "registros ou da coluna \"classe\" do arquivo)",
             size=10, color=C_TEXT_LIGHT).pack(side="left")

        # Pasta de destino — os arquivos de cada classe são gravados direto nela,
        # sem diálogo ao final
        self.frame_pasta_classe1 = ctk.CTkFrame(inn, fg_color="transparent")
        row_pc = ctk.CTkFrame(self.frame_pasta_classe1, fg_color="transparent")
        row_pc.pack(fill="x")
        _lbl(row_pc, "Pasta de destino:", color=C_TEXT_MED, size=11)\
            .pack(side="left", padx=(0,8))
        self.var_pasta_classe1 = tk.StringVar()
        _entry(row_pc, textvariable=self.var_pasta_classe1,
               placeholder="Selecione a pasta onde os arquivos serão salvos",
               width=380).pack(side="left", expand=True, fill="x")
        _btn(row_pc, "📂  Procurar", self._escolher_pasta_classe1,
             variant="ghost", width=100).pack(side="left", padx=(8,0))

        _sep(inn)

        # datas
        r2 = ctk.CTkFrame(inn, fg_color="transparent"); r2.pack(fill="x", pady=3)
        _lbl(r2, "Data de Início:", color=C_TEXT_MED).pack(side="left", padx=(0,6))
        self.var_ini1 = tk.StringVar()
        _entry(r2, textvariable=self.var_ini1, placeholder="DD-MM-AAAA", width=130)\
            .pack(side="left")
        _lbl(r2, "Data Final:", color=C_TEXT_MED).pack(side="left", padx=(20,6))
        self.var_fim1 = tk.StringVar()
        _entry(r2, textvariable=self.var_fim1, placeholder="DD-MM-AAAA", width=130)\
            .pack(side="left")

        _sep(inn)

        # formato
        r3 = ctk.CTkFrame(inn, fg_color="transparent"); r3.pack(fill="x", pady=3)
        _lbl(r3, "Formato de Saída:", color=C_TEXT_MED).pack(side="left", padx=(0,12))
        self.var_fmt = tk.StringVar(value="xlsx")
        for txt, val in [("Excel (.xlsx)","xlsx"), ("CSV (.csv)","csv")]:
            ctk.CTkRadioButton(r3, text=txt, variable=self.var_fmt, value=val,
                               font=("Segoe UI",12), text_color=C_TEXT,
                               fg_color=C_ACCENT, border_color=C_BORDER)\
                .pack(side="left", padx=(0,16))

        # Fontes — valem para as extrações das abas 1 e 2 (como o formato)
        _lbl(r3, "Fontes:", color=C_TEXT_MED).pack(side="left", padx=(12,10))
        self.var_fonte_compras = tk.BooleanVar(value=True)
        self.var_fonte_bps     = tk.BooleanVar(value=True)
        for txt, var in (("Compras.gov", self.var_fonte_compras),
                         ("BPS", self.var_fonte_bps)):
            ctk.CTkCheckBox(r3, text=txt, variable=var, command=self._toggle_bps,
                            font=("Segoe UI",12), text_color=C_TEXT,
                            fg_color=C_GREEN, border_color=C_BORDER)\
                .pack(side="left", padx=(0,12))
        self.var_desc_bps = tk.BooleanVar(value=True)
        self.chk_desc_bps = ctk.CTkCheckBox(
            r3, text="Usar a descrição do BPS", variable=self.var_desc_bps,
            font=("Segoe UI",12), text_color=C_TEXT,
            fg_color=C_GREEN, border_color=C_BORDER)
        self.chk_desc_bps.pack(side="left", padx=(8,0))

        _sep(inn)

        # corrompidos
        r4 = ctk.CTkFrame(inn, fg_color="transparent"); r4.pack(fill="x", pady=3)
        self.var_salvar_corr = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(r4, text="Salvar cópias dos CSV corrompidos",
                        variable=self.var_salvar_corr, command=self._toggle_pasta,
                        font=("Segoe UI",12), text_color=C_TEXT,
                        fg_color=C_ACCENT, border_color=C_BORDER)\
            .pack(side="left")
        # Vale para as abas 1 e 2: "Classe 6505 - DA" + "Classe 6505 - BPS"
        # em vez de uma pasta de trabalho com as duas abas
        self.var_por_fonte = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(r4, text="Salvar um arquivo por fonte",
                        variable=self.var_por_fonte,
                        font=("Segoe UI",12), text_color=C_TEXT,
                        fg_color=C_ACCENT, border_color=C_BORDER)\
            .pack(side="left", padx=(24,0))
        _lbl(r4, "  (ex.: \"Classe 6505 - DA\" e \"Classe 6505 - BPS\")",
             size=10, color=C_TEXT_LIGHT).pack(side="left")
        self.frame_pasta = ctk.CTkFrame(inn, fg_color="transparent")
        rp = ctk.CTkFrame(self.frame_pasta, fg_color="transparent")
        rp.pack(fill="x", pady=3)
        _lbl(rp, "Pasta:", color=C_TEXT_MED).pack(side="left", padx=(0,8))
        self.var_pasta = tk.StringVar()
        _entry(rp, textvariable=self.var_pasta,
               placeholder="Pasta de destino", width=380)\
            .pack(side="left", expand=True, fill="x")
        _btn(rp, "Procurar…", self._escolher_pasta, variant="ghost", width=90)\
            .pack(side="left", padx=(8,0))

        # — Card 2: estatísticas —
        c2 = _card(wrap, "2.  Resumo da Execução")
        c2.pack(fill="x", pady=(0,8))
        grid = ctk.CTkFrame(c2, fg_color="transparent")
        grid.pack(fill="x", padx=14, pady=(0,8))
        stats = [
            ("Códigos Processados",   "k_proc",  C_ACCENT),
            ("Registros Consolidados","k_reg",   C_GREEN),
            ("Reparadas · Com Perda", "k_corr",  C_ORANGE),
            ("Códigos sem Dados",     "k_vaz",   C_RED),
        ]
        self._stats = {}
        for col, (nome, key, cor) in enumerate(stats):
            cell = ctk.CTkFrame(grid, fg_color=C_BG, corner_radius=6,
                                border_width=1, border_color=C_BORDER)
            cell.grid(row=0, column=col, padx=5, pady=4, sticky="ew")
            grid.grid_columnconfigure(col, weight=1)
            _lbl(cell, nome, size=10, color=C_TEXT_MED).pack(pady=(6,1))
            lv = ctk.CTkLabel(cell, text="0", font=("Segoe UI",17,"bold"),
                              text_color=cor)
            lv.pack(pady=(0,6))
            self._stats[key] = lv

        # — Card 3: log —
        c3 = _card(wrap, "3.  Log e Progresso")
        c3.pack(fill="x", pady=(0,4))

        brow = ctk.CTkFrame(c3, fg_color="transparent")
        brow.pack(fill="x", padx=14, pady=(0,4))
        self.lbl_status = _lbl(brow, "Status: Ocioso", size=11,
                                color=C_TEXT_MED, anchor="w")
        self.lbl_status.pack(side="left", expand=True, fill="x")
        self.lbl_pct = _lbl(brow, "0%", size=11, weight="bold", color=C_GREEN)
        self.lbl_pct.pack(side="right", padx=(8,0))

        self.progress = ctk.CTkProgressBar(c3, fg_color=C_BORDER,
                                           progress_color=C_GREEN,
                                           corner_radius=3, height=6)
        self.progress.set(0)
        self.progress.pack(fill="x", padx=14, pady=(0,8))

        log_wrap = ctk.CTkFrame(c3, fg_color=C_LOG_BG, corner_radius=6)
        log_wrap.pack(fill="x", padx=14, pady=(0,10))
        self.log = scrolledtext.ScrolledText(
            log_wrap, bg=C_LOG_BG, fg=C_LOG_FG,
            font=("Consolas",10), wrap="word",
            relief="flat", bd=0, state="normal",
            height=9,
            insertbackground=C_LOG_FG)
        self.log.pack(fill="x", padx=6, pady=6)
        for tag, cor in [("ok","#4EC94E"),("warn","#F4A11D"),
                         ("err","#E05C5C"),("info","#7EB8F7"),
                         ("date","#FFCD07")]:
            self.log.tag_config(tag, foreground=cor)
        self._log(WELCOME, "info")

        # botões ficam no wrap (fora do card), sempre visíveis
        br = ctk.CTkFrame(wrap, fg_color="transparent")
        br.pack(fill="x", pady=(4,4))
        self.btn_start = _btn(br, "▶  Iniciar Extração", self._start,
                              variant="primary", width=160)
        self.btn_start.pack(side="left", padx=(0,8))
        self.btn_cancel = _btn(br, "✖  Cancelar", self._cancelar,
                               variant="secondary", width=100)
        self.btn_cancel.configure(state="disabled")
        self.btn_cancel.pack(side="left", padx=(0,8))
        self.btn_pause = _btn(br, "⏸  Pausar", self._pausar,
                              variant="secondary", width=100)
        self.btn_pause.configure(state="disabled")
        self.btn_pause.pack(side="left", padx=(0,8))
        self.btn_log = _btn(br, "💾  Salvar Log", self._salvar_log,
                            variant="secondary", width=120)
        self.btn_log.configure(state="disabled")
        self.btn_log.pack(side="left")

    # ── ABA 2 ─────────────────────────────────────────────────────────────────
    def _build_tab_explorador(self, parent):
        parent.configure(fg_color=C_BG)

        # ── Card 1: Classes (topo) ────────────────────────────────────────────
        c1 = _card(parent, "1.  Buscar PDMs por Classes")
        c1.pack(fill="x", padx=12, pady=(8,6))
        inn1 = ctk.CTkFrame(c1, fg_color="transparent")
        inn1.pack(fill="x", padx=14, pady=(0,10))
        _lbl(inn1, "Informe as Classes para extração separadas por  ;",
             color=C_TEXT_MED, size=11).pack(anchor="w", pady=(0,4))
        r = ctk.CTkFrame(inn1, fg_color="transparent")
        r.pack(fill="x")
        self.var_classe = tk.StringVar()
        ent = _entry(r, textvariable=self.var_classe,
                     placeholder="ex.: 20115 ; 20116 ; 20117", width=400)
        ent.pack(side="left", expand=True, fill="x")
        ent.bind("<Return>", lambda e: self._buscar_pdms())
        _btn(r, "Buscar PDMs", self._buscar_pdms,
             variant="primary", width=140).pack(side="left", padx=(10,0))
        _btn(r, "⚡  Buscar e Extrair", self._buscar_e_extrair_classes,
             variant="success", width=160).pack(side="left", padx=(8,0))
        self.lbl_pdm_count = _lbl(r, "", size=11, color=C_GREEN)
        self.lbl_pdm_count.pack(side="right", padx=8)
        row_chk = ctk.CTkFrame(inn1, fg_color="transparent")
        row_chk.pack(fill="x", pady=(8,0))
        self.var_arquivo_por_classe = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(row_chk,
                        text="Deseja salvar um arquivo por classe?",
                        variable=self.var_arquivo_por_classe,
                        command=self._toggle_pasta_por_classe,
                        font=("Segoe UI", 12), text_color=C_TEXT,
                        fg_color=C_ACCENT, border_color=C_BORDER)            .pack(side="left")
        _lbl(row_chk, "  (gera um arquivo separado para cada classe informada)",
             size=10, color=C_TEXT_LIGHT).pack(side="left")

        # Extração direta por PDM — usa tipo=codigoPdm na Pesquisa de Preço
        row_pdm = ctk.CTkFrame(inn1, fg_color="transparent")
        row_pdm.pack(fill="x", pady=(4,0))
        self.var_extrair_por_pdm = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(row_pdm,
                        text="Extrair preços direto por PDM (tipo=codigoPdm)",
                        variable=self.var_extrair_por_pdm,
                        font=("Segoe UI", 12), text_color=C_TEXT,
                        fg_color=C_ACCENT, border_color=C_BORDER).pack(side="left")
        _lbl(row_pdm, "  (dispensa a expansão PDM → CATMAT: muito mais rápido)",
             size=10, color=C_TEXT_LIGHT).pack(side="left")
        # Linha de pasta de destino — visível só quando checkbox marcado
        self.frame_pasta_classes = ctk.CTkFrame(inn1, fg_color="transparent")
        row_pasta = ctk.CTkFrame(self.frame_pasta_classes, fg_color="transparent")
        row_pasta.pack(fill="x")
        _lbl(row_pasta, "Pasta de destino:", color=C_TEXT_MED, size=11)            .pack(side="left", padx=(0,8))
        self.var_pasta_classes = tk.StringVar()
        _entry(row_pasta, textvariable=self.var_pasta_classes,
               placeholder="Selecione a pasta onde os arquivos serão salvos",
               width=380).pack(side="left", expand=True, fill="x")
        _btn(row_pasta, "📂  Procurar", self._escolher_pasta_classes,
             variant="ghost", width=100).pack(side="left", padx=(8,0))

        # ── Área central: tabela (esquerda) + busca avulsa (direita) ─────────
        mid = ctk.CTkFrame(parent, fg_color=C_BG)
        mid.pack(fill="both", expand=True, padx=12, pady=(0,4))

        # Painel lateral DIREITO: Busca Avulsa por PDMs
        # — deve ser empacotado ANTES do c2 para reservar espaço antes do expand
        cav = _card(mid, "Busca Avulsa por PDMs")
        cav.pack(side="right", fill="y", padx=(6,0))
        _lbl(cav, "Códigos PDM (um por linha):",
             size=11, color=C_TEXT_MED).pack(anchor="w", padx=14, pady=(0,4))
        self.txt_avulso = ctk.CTkTextbox(cav, font=("Consolas",11),
                                         fg_color=C_SURFACE, text_color=C_TEXT,
                                         border_width=1, border_color=C_BORDER,
                                         width=190, corner_radius=6)
        self.txt_avulso.pack(fill="both", expand=True, padx=14)
        _btn(cav, "🔍  Buscar CATMATs\n(PDMs da lista)",
             self._buscar_avulso, variant="primary")            .pack(fill="x", padx=14, pady=(8,10))

        # Card 2: PDMs Encontrados — expande para ocupar o espaço restante
        c2 = _card(mid, "2.  PDMs Encontrados")
        c2.pack(side="left", fill="both", expand=True)

        frow = ctk.CTkFrame(c2, fg_color="transparent")
        frow.pack(fill="x", padx=14, pady=(0,4))
        _lbl(frow, "Filtro:", color=C_TEXT_MED).pack(side="left", padx=(0,6))
        self.var_filtro = tk.StringVar(value="todos")
        for txt, val in [("Todos","todos"),("Ativos","ativo"),("Inativos","inativo")]:
            ctk.CTkRadioButton(frow, text=txt, variable=self.var_filtro, value=val,
                               command=self._filtrar,
                               font=("Segoe UI",11), text_color=C_TEXT,
                               fg_color=C_ACCENT, border_color=C_BORDER)                .pack(side="left", padx=(0,10))
        _btn(frow, "🔍  Buscar CATMATs (PDMs da tabela)", self._buscar_catmats,
             variant="secondary", width=240).pack(side="left", padx=(16,0))
        _btn(frow, "Exportar PDMs", self._exp_pdms,
             variant="ghost", width=120).pack(side="right")

        # Treeview
        style = ttk.Style()
        style.theme_use("clam")
        style.configure("BPS.Treeview", background=C_SURFACE, foreground=C_TEXT,
                        fieldbackground=C_SURFACE, rowheight=26,
                        font=("Segoe UI",10), borderwidth=0)
        style.configure("BPS.Treeview.Heading", background=C_BG,
                        foreground=C_TEXT_MED, font=("Segoe UI",10,"bold"),
                        relief="flat")
        style.map("BPS.Treeview",
                  background=[("selected", C_ACCENT)],
                  foreground=[("selected", C_SURFACE)])

        tf = ctk.CTkFrame(c2, fg_color=C_SURFACE, corner_radius=0)
        tf.pack(fill="both", expand=True, padx=8, pady=(0,8))
        vsb = ttk.Scrollbar(tf, orient="vertical", command=None)
        vsb.pack(side="right", fill="y")
        self.tree = ttk.Treeview(tf, columns=("cod","desc","status"),
                                 show="headings", style="BPS.Treeview",
                                 selectmode="extended",
                                 yscrollcommand=vsb.set)
        vsb.configure(command=self.tree.yview)
        self.tree.heading("cod",    text="Cód. PDM")
        self.tree.heading("desc",   text="Descrição")
        self.tree.heading("status", text="Status")
        self.tree.column("cod",    width=90,   anchor="center", stretch=False)
        self.tree.column("desc",   width=9999, anchor="w",      stretch=True)
        self.tree.column("status", width=80,   anchor="center", stretch=False)
        self.tree.pack(fill="both", expand=True)

        # ── Card 3: Ações (rodapé) ────────────────────────────────────────────
        c3 = _card(parent, "3.  Ações")
        c3.pack(fill="x", padx=12, pady=(0,8))

        ar = ctk.CTkFrame(c3, fg_color="transparent")
        ar.pack(fill="x", padx=14, pady=(0,4))
        _btn(ar, "⚡  Buscar e Extrair", self._buscar_e_extrair,
             variant="primary").pack(side="left", padx=(0,8))
        self.btn_exp_cat = _btn(ar, "📥  Exportar CATMATs Encontrados",
                                self._exp_catmats, variant="ghost")
        self.btn_exp_cat.configure(state="disabled")
        self.btn_exp_cat.pack(side="left", padx=(0,16))
        self.lbl_exp_status = _lbl(ar, "", size=11, color=C_TEXT_MED)
        self.lbl_exp_status.pack(side="left", expand=True, fill="x")

        dr = ctk.CTkFrame(c3, fg_color="transparent")
        dr.pack(fill="x", padx=14, pady=(0,4))
        _lbl(dr, "Data de Início:", color=C_TEXT_MED).pack(side="left", padx=(0,6))
        self.var_ini2 = tk.StringVar()
        _entry(dr, textvariable=self.var_ini2, placeholder="DD-MM-AAAA", width=130)            .pack(side="left")
        _lbl(dr, "Data Final:", color=C_TEXT_MED).pack(side="left", padx=(16,6))
        self.var_fim2 = tk.StringVar()
        _entry(dr, textvariable=self.var_fim2, placeholder="DD-MM-AAAA", width=130)            .pack(side="left")

        cr = ctk.CTkFrame(c3, fg_color="transparent")
        cr.pack(fill="x", padx=14, pady=(0,10))
        self.btn_pb = _btn(cr, "⏸  Pausar Busca", self._pausar_busca,
                           variant="secondary", width=130)
        self.btn_pb.configure(state="disabled")
        self.btn_pb.pack(side="left", padx=(0,8))
        self.btn_cb = _btn(cr, "✖  Cancelar Busca", self._cancelar_busca,
                           variant="danger", width=130)
        self.btn_cb.configure(state="disabled")
        self.btn_cb.pack(side="left", padx=(0,16))
        self.btn_ini_exp = _btn(cr,
            "▶  Iniciar Extração com CATMATs Encontrados",
            self._iniciar_exp, variant="success")
        self.btn_ini_exp.configure(state="disabled")
        self.btn_ini_exp.pack(side="left")

    # ── ABA 3 ─────────────────────────────────────────────────────────────────
    def _build_tab_consolidacao(self, parent):
        """Consolida DW (SIASG) + DA (Compras.gov) removendo do DA o que já está
        no DW. Motor: consolidar_dw_da.consolidar()."""
        parent.configure(fg_color=C_BG)

        # estado próprio da aba — nada é compartilhado com as abas 1 e 2
        self._cons_entradas   = []      # [{"caminho": str, "origem": "auto|dw|da"}]
        self._cons_rodando    = False
        self._cons_cancelar   = False
        self._cons_ultima_saida = ""

        # ── Card 1: entradas ─────────────────────────────────────────────────
        c1 = _card(parent, "1.  Arquivos de Entrada  (CSV/XLSX do DW e do DA)")
        c1.pack(fill="x", padx=12, pady=(8,6))
        inn1 = ctk.CTkFrame(c1, fg_color="transparent")
        inn1.pack(fill="x", padx=14, pady=(0,10))

        rb = ctk.CTkFrame(inn1, fg_color="transparent"); rb.pack(fill="x", pady=(0,6))
        _lbl(rb, "Origem:", color=C_TEXT_MED).pack(side="left", padx=(0,8))
        self.var_cons_origem = tk.StringVar(value="Detectar")
        ctk.CTkSegmentedButton(
            rb, values=["Detectar", "DW", "DA"], variable=self.var_cons_origem,
            font=("Segoe UI", 12), width=200, corner_radius=6,
            fg_color=C_BG, selected_color=C_ACCENT, selected_hover_color=C_ACCENT_H,
            unselected_color=C_BG, unselected_hover_color=C_BORDER,
            text_color=C_TEXT).pack(side="left")
        _btn(rb, "📁  Pasta…", self._cons_add_pasta, variant="primary", width=100)\
            .pack(side="left", padx=(12,6))
        _btn(rb, "📄  Arquivos…", self._cons_add_arquivos, variant="secondary",
             width=110).pack(side="left", padx=(0,6))
        _btn(rb, "Remover", self._cons_remover, variant="ghost", width=90)\
            .pack(side="left", padx=(0,6))
        _btn(rb, "Limpar", self._cons_limpar, variant="ghost", width=80)\
            .pack(side="left")
        _lbl(rb, "  \"Detectar\" descobre DW/DA pelo cabeçalho do arquivo",
             size=10, color=C_TEXT_LIGHT).pack(side="left", padx=(10,0))

        tf = ctk.CTkFrame(inn1, fg_color=C_SURFACE, corner_radius=0)
        tf.pack(fill="x")
        vsb_c = ttk.Scrollbar(tf, orient="vertical")
        vsb_c.pack(side="right", fill="y")
        self.tree_cons = ttk.Treeview(tf, columns=("origem","caminho"),
                                      show="headings", style="BPS.Treeview",
                                      selectmode="extended", height=5,
                                      yscrollcommand=vsb_c.set)
        vsb_c.configure(command=self.tree_cons.yview)
        self.tree_cons.heading("origem",  text="Origem")
        self.tree_cons.heading("caminho", text="Pasta / Arquivo")
        self.tree_cons.column("origem",  width=90, anchor="center", stretch=False)
        self.tree_cons.column("caminho", width=9999, anchor="w", stretch=True)
        self.tree_cons.pack(fill="x")

        # ── Card 2: saída e opções ───────────────────────────────────────────
        c2 = _card(parent, "2.  Saída e Opções")
        c2.pack(fill="x", padx=12, pady=(0,6))
        inn2 = ctk.CTkFrame(c2, fg_color="transparent")
        inn2.pack(fill="x", padx=14, pady=(0,10))

        rs = ctk.CTkFrame(inn2, fg_color="transparent"); rs.pack(fill="x", pady=3)
        _lbl(rs, "Pasta de saída:", color=C_TEXT_MED).pack(side="left", padx=(0,8))
        self.var_cons_saida = tk.StringVar()
        _entry(rs, textvariable=self.var_cons_saida,
               placeholder="Onde as planilhas por classe serão gravadas",
               width=420).pack(side="left", expand=True, fill="x")
        _btn(rs, "📂  Procurar", self._cons_escolher_saida, variant="ghost",
             width=100).pack(side="left", padx=(8,0))

        ro = ctk.CTkFrame(inn2, fg_color="transparent"); ro.pack(fill="x", pady=3)
        _lbl(ro, "Prefixo:", color=C_TEXT_MED).pack(side="left", padx=(0,6))
        self.var_cons_prefixo = tk.StringVar(value="DA-DW Classe ")
        _entry(ro, textvariable=self.var_cons_prefixo, width=180).pack(side="left")
        _lbl(ro, "Sufixo:", color=C_TEXT_MED).pack(side="left", padx=(14,6))
        self.var_cons_sufixo = tk.StringVar()
        _entry(ro, textvariable=self.var_cons_sufixo,
               placeholder="ex.: _2021_a_2026", width=140).pack(side="left")
        _lbl(ro, "Ano mín.:", color=C_TEXT_MED).pack(side="left", padx=(14,6))
        self.var_cons_ano_min = tk.StringVar()
        _entry(ro, textvariable=self.var_cons_ano_min, placeholder="AAAA",
               width=70).pack(side="left")
        _lbl(ro, "Ano máx.:", color=C_TEXT_MED).pack(side="left", padx=(10,6))
        self.var_cons_ano_max = tk.StringVar()
        _entry(ro, textvariable=self.var_cons_ano_max, placeholder="AAAA",
               width=70).pack(side="left")

        rc = ctk.CTkFrame(inn2, fg_color="transparent"); rc.pack(fill="x", pady=3)
        _lbl(rc, "Processos:", color=C_TEXT_MED).pack(side="left", padx=(0,6))
        # Um processo por classe divide a gravação, que é onde está ~92% do
        # tempo. Padrão 1 (sequencial): o paralelo é opt-in até você validar.
        try:
            nucleos = os.cpu_count() or 1
        except Exception:
            nucleos = 1
        opcoes = [str(n) for n in (1, 2, 3, 4, 6, 8) if n <= max(1, nucleos)] or ["1"]
        self.var_cons_processos = tk.StringVar(value="1")
        ctk.CTkOptionMenu(rc, values=opcoes, variable=self.var_cons_processos,
                          font=("Segoe UI",12), width=62, height=28,
                          corner_radius=6, fg_color=C_SURFACE,
                          button_color=C_BORDER, button_hover_color=C_ACCENT,
                          text_color=C_TEXT,
                          dropdown_font=("Segoe UI",12)).pack(side="left", padx=(0,18))
        self.var_cons_dup = tk.BooleanVar(value=True)
        ctk.CTkCheckBox(rc, text="Gerar CSV de auditoria das duplicatas",
                        variable=self.var_cons_dup, font=("Segoe UI",12),
                        text_color=C_TEXT, fg_color=C_ACCENT,
                        border_color=C_BORDER).pack(side="left", padx=(0,18))
        self.var_cons_dedup_interno = tk.BooleanVar(value=False)
        ctk.CTkCheckBox(rc, text="Remover repetições internas do DA",
                        variable=self.var_cons_dedup_interno, font=("Segoe UI",12),
                        text_color=C_TEXT, fg_color=C_ACCENT,
                        border_color=C_BORDER).pack(side="left")
        _lbl(rc, "  (costumam ser registros distintos)", size=10,
             color=C_TEXT_LIGHT).pack(side="left")

        # ── Resumo ───────────────────────────────────────────────────────────
        grid = ctk.CTkFrame(parent, fg_color=C_BG)
        grid.pack(fill="x", padx=12, pady=(0,6))
        stats = [
            ("Linhas do DW",        "c_dw",   C_ACCENT),
            ("Linhas do DA lidas",  "c_lidas", C_TEXT_MED),
            ("Duplicatas Removidas","c_dup",  C_ORANGE),
            ("Linhas do DA Mantidas","c_mant", C_GREEN),
        ]
        self._stats_cons = {}
        for col, (nome, key, cor) in enumerate(stats):
            cell = ctk.CTkFrame(grid, fg_color=C_SURFACE, corner_radius=6,
                                border_width=1, border_color=C_BORDER)
            cell.grid(row=0, column=col, padx=5, pady=0, sticky="ew")
            grid.grid_columnconfigure(col, weight=1)
            _lbl(cell, nome, size=10, color=C_TEXT_MED).pack(pady=(6,1))
            lv = ctk.CTkLabel(cell, text="0", font=("Segoe UI",17,"bold"),
                              text_color=cor)
            lv.pack(pady=(0,6))
            self._stats_cons[key] = lv

        # ── Card 3: log e progresso ──────────────────────────────────────────
        c3 = _card(parent, "3.  Log e Progresso")
        c3.pack(fill="both", expand=True, padx=12, pady=(0,4))

        brow = ctk.CTkFrame(c3, fg_color="transparent")
        brow.pack(fill="x", padx=14, pady=(0,4))
        self.lbl_cons_status = _lbl(brow, "Status: Ocioso", size=11,
                                    color=C_TEXT_MED, anchor="w")
        self.lbl_cons_status.pack(side="left", expand=True, fill="x")
        self.lbl_cons_pct = _lbl(brow, "0%", size=11, weight="bold", color=C_GREEN)
        self.lbl_cons_pct.pack(side="right", padx=(8,0))

        self.progress_cons = ctk.CTkProgressBar(c3, fg_color=C_BORDER,
                                                progress_color=C_GREEN,
                                                corner_radius=3, height=6)
        self.progress_cons.set(0)
        self.progress_cons.pack(fill="x", padx=14, pady=(0,8))

        log_wrap = ctk.CTkFrame(c3, fg_color=C_LOG_BG, corner_radius=6)
        log_wrap.pack(fill="both", expand=True, padx=14, pady=(0,10))
        self.log_cons = scrolledtext.ScrolledText(
            log_wrap, bg=C_LOG_BG, fg=C_LOG_FG, font=("Consolas",10),
            wrap="word", relief="flat", bd=0, state="normal", height=7,
            insertbackground=C_LOG_FG)
        self.log_cons.pack(fill="both", expand=True, padx=6, pady=6)
        for tag, cor in [("ok","#4EC94E"),("warn","#F4A11D"),
                         ("err","#E05C5C"),("info","#7EB8F7")]:
            self.log_cons.tag_config(tag, foreground=cor)

        # ── Botões ───────────────────────────────────────────────────────────
        br = ctk.CTkFrame(parent, fg_color="transparent")
        br.pack(fill="x", padx=12, pady=(0,8))
        self.btn_cons_start = _btn(br, "▶  Consolidar e Remover Duplicatas",
                                   self._cons_iniciar, variant="primary", width=250)
        self.btn_cons_start.pack(side="left", padx=(0,8))
        self.btn_cons_cancel = _btn(br, "✖  Cancelar", self._cons_cancelar_click,
                                    variant="secondary", width=100)
        self.btn_cons_cancel.configure(state="disabled")
        self.btn_cons_cancel.pack(side="left", padx=(0,8))
        self.btn_cons_abrir = _btn(br, "📂  Abrir Pasta de Saída",
                                   self._cons_abrir_pasta, variant="ghost", width=170)
        self.btn_cons_abrir.configure(state="disabled")
        self.btn_cons_abrir.pack(side="left", padx=(0,8))
        self.btn_cons_log = _btn(br, "💾  Salvar Log", self._cons_salvar_log,
                                 variant="secondary", width=120)
        self.btn_cons_log.configure(state="disabled")
        self.btn_cons_log.pack(side="left")

        self._log_cons(BEMVINDO_CONSOLIDACAO, "info")

        # ── LOG HELPERS ───────────────────────────────────────────────────────────
    def _log(self, msg: str, tag: str = ""):
        self.log.configure(state="normal")
        self.log.insert("end", msg + "\n", tag)
        self.log.see("end")
        self.log.configure(state="disabled")

    def set_status(self, txt): self.lbl_status.configure(text=txt)
    def set_status_explorador(self, txt): self.lbl_exp_status.configure(text=txt)
    def _stat(self, key, val): self._stats[key].configure(text=str(val))

    def _atualizar_stat_paginas(self):
        """Card de páginas: reparadas (recuperadas) · com perda (registros sumiram)."""
        self._stat("k_corr", f"{self.count_reparadas} · {self.count_corrigidas}")



    # ── CALLBACKS ABA 1 ───────────────────────────────────────────────────────
    def _escolher_arquivo(self):
        p = filedialog.askopenfilename(
            filetypes=[("Excel/CSV","*.xlsx *.csv"),("Todos","*.*")])
        if p: self.var_arquivo.set(p)

    def _escolher_pasta(self):
        p = filedialog.askdirectory()
        if p: self.var_pasta.set(p)

    def _toggle_pasta(self):
        if self.var_salvar_corr.get():
            self.frame_pasta.pack(fill="x", padx=14, pady=3)
        else:
            self.frame_pasta.pack_forget()

    def _toggle_bps(self):
        # A descrição do BPS entra no descricaoItem do Compras.gov: sem o
        # Compras.gov não há o que trocar. Sem a fonte BPS ela continua
        # possível — vem de uma consulta leve, só da descrição.
        self.chk_desc_bps.configure(
            state="normal" if self.var_fonte_compras.get() else "disabled")

    def _fontes_ok(self) -> bool:
        if self.var_fonte_compras.get() or self.var_fonte_bps.get():
            return True
        messagebox.showerror("Nenhuma fonte",
            "Marque ao menos uma fonte: Compras.gov, BPS ou as duas.")
        return False

    def _config_bps(self):
        """Fontes lidas na thread principal (Tk não é thread-safe).
            compras   -> extrai do Compras.gov
            aba       -> extrai do BPS (aba BPS; no modo só BPS, a aba principal)
            descricao -> troca o descricaoItem do Compras.gov pelo texto do BPS
        Também zera o cache de descrições, que vale só para uma extração."""
        with _cache_desc_lock:
            _cache_desc_bps.clear()
        self._por_fonte = bool(self.var_por_fonte.get())   # lido por _novo_writer
        compras = bool(self.var_fonte_compras.get())
        return {"compras": compras, "aba": bool(self.var_fonte_bps.get()),
                "descricao": compras and bool(self.var_desc_bps.get())}

    def _log_config_bps(self, cfg):
        fontes = " + ".join(n for n, on in (("Compras.gov", cfg["compras"]),
                                            ("BPS", cfg["aba"])) if on)
        txt = f"🧭 Fontes: {fontes}"
        if cfg["compras"]:
            txt += (" | descrição do BPS no descricaoItem: "
                    + ("sim" if cfg["descricao"] else "não"))
        if getattr(self, "_por_fonte", False):
            txt += " | um arquivo por fonte"
        self._log(txt, "info")

    def _novo_writer(self, pasta, fmt, classe=None):
        """Writer da extração de uma classe (ou do arquivo único, classe=None).

        Um arquivo por fonte  -> "Classe 6505 - DA" e "Classe 6505 - BPS"
        Compras.gov (+ BPS)   -> classe_6505_part1, com a aba BPS dentro
        Só BPS                -> classe_6505_BPS_part1, com a aba BPS só
                                 (sem uma aba Dados CATMAT vazia)
        """
        cfg = getattr(self, "_bps_cfg", None) or {"compras": True, "aba": False}
        ext = "csv" if fmt == "csv" else "xlsx"
        no_destino = lambda nome: os.path.join(pasta, nome) if pasta else nome
        if getattr(self, "_por_fonte", False):
            rotulo = f"Classe {classe}" if classe else "dados_completos_extraidos"
            return EscritorPorFonte(
                no_destino(f"{rotulo} - DA.{ext}") if cfg["compras"] else None,
                no_destino(f"{rotulo} - {ABA_BPS}.{ext}") if cfg["aba"] else None,
                fmt)
        base = f"classe_{classe}" if classe else "dados_completos_extraidos"
        if not cfg["compras"]:
            caminho = no_destino(f"{base}_{ABA_BPS}.{ext}")
            return (CSVChunkWriter(caminho) if fmt == "csv"
                    else ExcelChunkWriter(caminho, sheet_name=ABA_BPS))
        caminho = no_destino(f"{base}.{ext}")
        return CSVChunkWriter(caminho) if fmt == "csv" else ExcelChunkWriter(caminho)

    @staticmethod
    def _aba_bps(cfg):
        """Onde gravar as linhas do BPS: aba própria, ou a principal no modo só BPS."""
        return ABA_BPS if cfg["compras"] else None

    def _toggle_pasta_classe1(self):
        """Mostra o campo de pasta apenas quando o modo por classe está ativo."""
        if self.var_por_classe1.get():
            # after= ancora o campo logo abaixo do checkbox; sem isso o pack
            # jogaria o frame para o fim da seção, longe do que ele controla
            self.frame_pasta_classe1.pack(fill="x", padx=0, pady=(2,0),
                                          after=self._row_por_classe1)
        else:
            self.frame_pasta_classe1.pack_forget()
            self.var_pasta_classe1.set("")

    def _escolher_pasta_classe1(self):
        p = filedialog.askdirectory(
            title="Escolha a pasta de destino dos arquivos por classe")
        if p:
            self.var_pasta_classe1.set(p)

    def _on_tipo_extracao(self, valor=None):
        """Atualiza a dica de coluna quando o usuário troca CATMAT ↔ PDM."""
        tipo = TIPO_POR_ROTULO.get(self.var_tipo1.get(), TIPO_CATMAT)
        self.lbl_hint_tipo.configure(
            text=f"coluna esperada no arquivo: {tipo}")

    def _tipo_extracao(self):
        """Tipo de busca selecionado na aba de extração."""
        return TIPO_POR_ROTULO.get(self.var_tipo1.get(), TIPO_CATMAT)

    def _start(self):
        if not self._fontes_ok(): return
        arq = self.var_arquivo.get().strip()
        if not arq:
            messagebox.showerror("Arquivo obrigatório",
                                 "Selecione um arquivo de códigos."); return
        tipo = self._tipo_extracao()
        por_classe = self.var_por_classe1.get()
        mapa = {}
        try:
            df_c = pd.read_excel(arq) if arq.lower().endswith(".xlsx") \
                   else pd.read_csv(arq, sep=";")
            # Aceita a coluna do tipo escolhido ou a coluna genérica 'codigo'
            col = next((c for c in (tipo, "codigo") if c in df_c.columns), None)
            if col is None:
                messagebox.showerror("Coluna ausente",
                    f"O arquivo deve ter a coluna '{tipo}' (ou 'codigo')."); return
            codigos = pd.Series(df_c[col]).dropna()\
                        .astype(int).drop_duplicates().tolist()

            # Agrupamento explícito: se o arquivo trouxer a classe de cada código,
            # ela manda. Sem essa coluna, a classe é lida do próprio registro.
            if por_classe:
                col_cl = next((c for c in ("classe", "Classe", "codigoClasse")
                               if c in df_c.columns), None)
                if col_cl:
                    val = df_c[[col, col_cl]].dropna()
                    for cl, grupo in val.groupby(val[col_cl].astype(str)
                                                    .str.strip().str.replace(r"\.0$", "", regex=True)):
                        mapa[cl] = grupo[col].astype(int).drop_duplicates().tolist()
        except Exception as e:
            messagebox.showerror("Erro ao ler arquivo", str(e)); return
        d_i, d_f, err = validar_e_obter_datas(self.var_ini1.get(), self.var_fim1.get())
        if err: messagebox.showerror("Data inválida", err); return

        # Modo por classe grava vários arquivos: a pasta precisa ser conhecida
        # ANTES de começar, para que cada classe seja salva em ato contínuo.
        pasta = ""
        if por_classe:
            pasta = self.var_pasta_classe1.get().strip()
            if not pasta:
                messagebox.showinfo("Pasta de destino",
                    "A opção 'um arquivo por classe' gera vários arquivos.\n\n"
                    "Escolha a pasta onde eles serão salvos.")
                pasta = filedialog.askdirectory(
                    title="Escolha a pasta de destino dos arquivos por classe")
                if not pasta:
                    messagebox.showwarning("Extração não iniciada",
                        "Nenhuma pasta escolhida. Selecione a pasta de destino "
                        "ou desmarque 'Salvar um arquivo por classe'."); return
                self.var_pasta_classe1.set(pasta)
            if not os.path.isdir(pasta):
                messagebox.showerror("Pasta inválida",
                    f"A pasta não existe:\n{pasta}"); return
            origem = (f"coluna '{col_cl}' do arquivo" if mapa
                      else "campo codigoClasse dos registros")
            self._log(f"🗂️  Um arquivo por classe — origem da classe: {origem}.", "info")
            self._log(f"📂 Destino: {pasta}", "info")

        self._iniciar_processo(codigos, self.var_fmt.get(), d_i, d_f,
                               catmats_por_classe=mapa, tipo=tipo,
                               por_classe=por_classe, pasta_destino=pasta)

    def _cancelar(self):
        if not self.processing: return
        self.processing = False
        pausar_extracao.set()   # desbloqueia wait() na thread para ela poder sair

    def _pausar(self):
        if pausar_extracao.is_set():
            # Estava rodando → pausar
            pausar_extracao.clear()
            self.btn_pause.configure(text="▶  Retomar")
            self.set_status("Status: Pausado")
        else:
            # Estava pausado → retomar
            pausar_extracao.set()
            self.btn_pause.configure(text="⏸  Pausar")
            self.set_status("Status: Retomando…")

    def _pausar_por_conexao(self):
        """Pausa automática ao detectar queda de rede — NÃO cancela a extração.
        Retoma automaticamente em 60s ou imediatamente se o usuário clicar Retomar.
        Chamada pelas threads de extração (várias ao mesmo tempo): o teste-e-
        pausa é atômico e a interface só é tocada na thread principal."""
        if not self.processing: return
        with _lock_pausa_conexao:
            if not pausar_extracao.is_set(): return  # outra thread já pausou
            pausar_extracao.clear()

        def _mostrar():
            self.btn_pause.configure(state="normal", text="▶  Retomar agora")
            self.set_status("Status: Sem conexão — retentando em 60s")
            self._log("\n⚠️  Rede indisponível — retentando automaticamente em 60s.\n"
                      "   Clique em ▶  Retomar agora para tentar imediatamente.", "warn")
            # Atualizar contagem regressiva no status a cada segundo
            def _countdown(seg):
                if not self.processing or pausar_extracao.is_set(): return
                self.set_status(f"Status: Sem conexão — retentando em {seg}s")
                if seg > 0:
                    self.after(1000, lambda: _countdown(seg - 1))
            self.after(1000, lambda: _countdown(59))
        self._ui(_mostrar)

    def _salvar_log(self):
        p = filedialog.asksaveasfilename(defaultextension=".txt",
                                         filetypes=[("Texto","*.txt")])
        if p:
            try:
                with open(p,"w",encoding="utf-8") as f: f.write(self.log.get("1.0","end"))
                messagebox.showinfo("Salvo", f"Log salvo em:\n{p}")
            except Exception as e:
                messagebox.showerror("Erro", str(e))

    # ── CALLBACKS ABA 2 ───────────────────────────────────────────────────────
    # ── SPINNER (overlay translúcido) ─────────────────────────────────────────
    def _show_spinner(self, msg="Buscando…"):
        """Exibe overlay com spinner animado sobre a aba."""
        self._spinner_active = True
        self._spinner_frame = ctk.CTkFrame(self, fg_color="#FFFFFF",
                                           corner_radius=12,
                                           border_width=1, border_color=C_BORDER)
        self._spinner_frame.place(relx=0.5, rely=0.5, anchor="center")
        self._spinner_chars = ["⠋","⠙","⠹","⠸","⠼","⠴","⠦","⠧","⠇","⠏"]
        self._spinner_idx   = 0
        self._lbl_spin_icon = ctk.CTkLabel(self._spinner_frame,
            text=self._spinner_chars[0],
            font=("Segoe UI", 28), text_color=C_ACCENT)
        self._lbl_spin_icon.pack(padx=40, pady=(22,4))
        self._lbl_spin_msg = ctk.CTkLabel(self._spinner_frame,
            text=msg, font=("Segoe UI", 13), text_color=C_TEXT_MED)
        self._lbl_spin_msg.pack(padx=40, pady=(0,22))
        self._animate_spinner()

    def _animate_spinner(self):
        if not self._spinner_active: return
        self._spinner_idx = (self._spinner_idx + 1) % len(self._spinner_chars)
        self._lbl_spin_icon.configure(text=self._spinner_chars[self._spinner_idx])
        self.after(80, self._animate_spinner)

    def _hide_spinner(self):
        self._spinner_active = False
        if hasattr(self, "_spinner_frame") and self._spinner_frame.winfo_exists():
            self._spinner_frame.destroy()

    def _buscar_pdms(self, acao_pos_busca=None):
        entrada = self.var_classe.get().strip()
        if not entrada:
            messagebox.showerror("Campo vazio", "Informe ao menos um código de Classe."); return

        partes = [p.strip() for p in entrada.split(";") if p.strip()]
        invalidas = [p for p in partes if not p.isdigit()]
        if invalidas:
            messagebox.showerror("Código inválido",
                f"Valores não numéricos: {', '.join(invalidas)}\n"
                "Use apenas números separados por ;")
            return

        self._show_spinner(f"Buscando PDMs de {len(partes)} classe(s)…")
        self.lbl_pdm_count.configure(text="")

        def _thread():
            todos_dfs = []; erros = []
            for cod in partes:
                self.after(0, lambda c=cod: self._lbl_spin_msg.configure(
                    text=f"Buscando classe {c}…"))
                res = buscar_pdms_por_classe(int(cod), URL_BASE, TIMEOUT)
                if res is None:
                    erros.append(cod)
                else:
                    df_c, _ = res
                    todos_dfs.append(df_c)
            self.after(0, lambda: self._on_pdms_carregados(todos_dfs, erros, acao_pos_busca))

        threading.Thread(target=_thread, daemon=True).start()

    def _on_pdms_carregados(self, todos_dfs, erros, acao_pos_busca=None):
        self._hide_spinner()
        if not todos_dfs:
            self.lbl_pdm_count.configure(text="Nenhum PDM encontrado.")
            self.lista_pdms_df = pd.DataFrame(); self._fill_tree([]); return

        df = pd.concat(todos_dfs, ignore_index=True).drop_duplicates(subset=["codigoPdm"])
        df["_col_codigo"] = df["codigoPdm"]
        df["_col_desc"]   = df["nomePdm"]
        df["_col_status"] = df["statusPdm"]
        self.lista_pdms_df        = df
        self._todos_dfs_por_classe = todos_dfs  # preservar para mapa classe→catmats
        self._fill_tree([[r["_col_codigo"], r["_col_desc"], r["_col_status"]]
                         for _, r in df.iterrows()])

        msg = f"{len(df)} PDMs de {len(todos_dfs)} classe(s)"
        if erros: msg += f"  ·  ⚠ Falha: {', '.join(erros)}"
        self.lbl_pdm_count.configure(text=msg)
        if erros:
            messagebox.showwarning("Classes com falha",
                f"Não foi possível buscar: {', '.join(erros)}")
        self.var_filtro.set("todos")
        self.btn_exp_cat.configure(state="disabled")
        self.btn_ini_exp.configure(state="disabled")
        self.lista_catmats = []

        if acao_pos_busca == "extrair":
            self._continuar_busca_e_extrai(df)

    def _filtrar(self):
        if self.lista_pdms_df.empty: return
        f = self.var_filtro.get(); df = self.lista_pdms_df
        if f == "ativo":    df = df[df["_col_status"] == "Ativo"]
        elif f == "inativo": df = df[df["_col_status"] == "Inativo"]
        self._fill_tree([[r["_col_codigo"], r["_col_desc"], r["_col_status"]]
                         for _, r in df.iterrows()])
        # Manter checkbox em sincronia após filtrar
        if hasattr(self, "var_sel_todos"): self.var_sel_todos.set(False)

    def _toggle_selecionar_todos(self):
        """Seleciona ou deseleciona todos os itens visíveis na tabela."""
        items = self.tree.get_children()
        if self.var_sel_todos.get():
            self.tree.selection_set(items)
        else:
            self.tree.selection_remove(items)

    def _fill_tree(self, rows):
        for i in self.tree.get_children(): self.tree.delete(i)
        for row in rows:
            tag = "ativo" if str(row[2]).lower() == "ativo" else "inativo"
            self.tree.insert("", "end", values=row, tags=(tag,))
        self.tree.tag_configure("ativo",   foreground=C_GREEN)
        self.tree.tag_configure("inativo", foreground=C_TEXT_LIGHT)
        # Resetar checkbox ao recarregar
        if hasattr(self, "var_sel_todos"): self.var_sel_todos.set(False)

    def _exp_pdms(self):
        rows = [self.tree.item(i,"values") for i in self.tree.get_children()]
        if not rows: messagebox.showerror("Vazio","Nenhum PDM."); return
        p = filedialog.asksaveasfilename(defaultextension=".csv",
                                         initialfile="PDMs_exportados.csv",
                                         filetypes=[("CSV","*.csv")])
        if p:
            pd.DataFrame(rows, columns=["Código PDM","Descrição","Status"])\
              .to_csv(p, index=False, sep=";", encoding="utf-8-sig")
            messagebox.showinfo("Exportado", f"Salvo em:\n{p}")

    def _pdms_selecionados_codigos(self):
        """Retorna lista de codigoPdm dos itens selecionados na árvore."""
        sel = self.tree.selection()
        return [int(self.tree.item(i,"values")[0]) for i in sel] if sel else []

    def _pdms_sel(self):
        sel = self.tree.selection()
        return [int(self.tree.item(i,"values")[0]) for i in sel] if sel else []

    def _buscar_avulso(self):
        txt = self.txt_avulso.get("1.0", "end").strip()
        if not txt: messagebox.showerror("Vazio","Informe ao menos um código PDM."); return
        pdms = []; inv = []
        # Aceita separador ; ou nova linha
        for parte in txt.replace("\n", ";").split(";"):
            parte = parte.strip()
            if not parte: continue
            try: pdms.append(int(parte))
            except ValueError: inv.append(parte)
        if inv: messagebox.showwarning("Inválidos", f"Ignorados: {', '.join(inv)}")
        if pdms: self._start_busca(pdms, "apenas_buscar")

    def _buscar_catmats(self):
        pdms = self._pdms_sel()
        if not pdms: messagebox.showerror("Nenhum selecionado","Selecione PDMs."); return
        self._start_busca(pdms, "apenas_buscar")

    def _buscar_e_extrair(self):
        if not self._fontes_ok(): return
        pdms = self._pdms_sel()
        if not pdms: messagebox.showerror("Nenhum selecionado","Selecione PDMs."); return
        # Modo direto: pula a descoberta de CATMATs e consulta a Pesquisa de
        # Preço com tipo=codigoPdm
        if self.var_extrair_por_pdm.get():
            d_i, d_f, err = validar_e_obter_datas(self.var_ini2.get(), self.var_fim2.get())
            if err: messagebox.showerror("Data inválida", err); return
            self._log(f"⏩ {len(pdms)} PDMs — extração direta (tipo=codigoPdm).", "info")
            self._iniciar_processo(pdms, self.var_fmt.get(), d_i, d_f, tipo=TIPO_PDM)
            self.after(100, lambda: self.tabs.set("  Extração por CATMAT  "))
            return
        self._start_busca(pdms, "extrair")

    def _start_busca(self, pdms, acao):
        global cancelar_busca_catmat
        cancelar_busca_catmat = False
        pausar_busca_catmat.set()
        self.btn_pb.configure(state="normal")
        self.btn_cb.configure(state="normal")
        self.btn_pb.configure(text="⏸  Pausar Busca")
        self.set_status_explorador("Iniciando busca…")
        threading.Thread(target=self._thread_busca,
                         args=(pdms, acao), daemon=True).start()

    def _thread_busca(self, pdms, acao):
        df1, err1 = buscar_catmats_por_pdm(pdms, URL_BASE, TIMEOUT, self)
        df2 = None; err2 = []
        if err1 and not cancelar_busca_catmat:
            self.after(0, lambda: self.set_status_explorador(
                f"2ª tentativa para {len(err1)} PDMs…"))
            time.sleep(2)
            df2, err2 = buscar_catmats_por_pdm(err1, URL_BASE, TIMEOUT, self)
        dfs = [d for d in [df1, df2] if d is not None and not d.empty]
        df_final = pd.concat(dfs, ignore_index=True) if dfs else None
        self.after(0, lambda: self._on_busca(df_final, err2, acao))

    def _on_busca(self, df, falhas, acao):
        self.btn_pb.configure(state="disabled")
        self.btn_cb.configure(state="disabled")
        if df is not None and "codigoItem" in df.columns:
            self.lista_catmats = df["codigoItem"].dropna().astype(int).tolist()
            n = len(self.lista_catmats)
            msg = f"✅ {n} CATMATs encontrados"
            if falhas: msg += f" · ⚠ {len(falhas)} PDMs com falha"
            self.set_status_explorador(msg)
            self.btn_exp_cat.configure(state="normal")
            self.btn_ini_exp.configure(state="normal", text=f"▶  Iniciar Extração com {n} CATMATs Encontrados")
            if falhas:
                messagebox.showwarning("PDMs com falha",
                    f"Falha persistente em:\n{', '.join(map(str,falhas))}")
            if acao == "extrair": self._iniciar_exp()
        else:
            self.lista_catmats = []
            if not cancelar_busca_catmat:
                self.set_status_explorador("Nenhum CATMAT encontrado.")
            self.btn_exp_cat.configure(state="disabled")
            self.btn_ini_exp.configure(state="disabled")

    def _exp_catmats(self):
        if not self.lista_catmats:
            messagebox.showerror("Vazio","Nenhum CATMAT."); return
        p = filedialog.asksaveasfilename(defaultextension=".csv",
                                         initialfile="CATMATs_descobertos.csv",
                                         filetypes=[("CSV","*.csv")])
        if p:
            pd.DataFrame(self.lista_catmats, columns=["codigoItemCatalogo"])\
              .to_csv(p, index=False, sep=";", encoding="utf-8-sig")
            messagebox.showinfo("Exportado", f"{len(self.lista_catmats)} CATMATs:\n{p}")

    def _pausar_busca(self):
        if pausar_busca_catmat.is_set():
            pausar_busca_catmat.clear()
            self.btn_pb.configure(text="▶  Retomar Busca")
            self.set_status_explorador("Busca pausada.")
        else:
            pausar_busca_catmat.set()
            self.btn_pb.configure(text="⏸  Pausar Busca")
            self.set_status_explorador("Retomando…")

    def _toggle_pasta_por_classe(self):
        """Mostra/oculta o campo de pasta quando o checkbox é marcado."""
        if self.var_arquivo_por_classe.get():
            self.frame_pasta_classes.pack(fill="x", padx=0, pady=(6,0))
        else:
            self.frame_pasta_classes.pack_forget()
            self.var_pasta_classes.set("")

    def _escolher_pasta_classes(self):
        p = filedialog.askdirectory(title="Escolha a pasta de destino dos arquivos por classe")
        if p:
            self.var_pasta_classes.set(p)

    def _cancelar_busca(self):
        global cancelar_busca_catmat
        cancelar_busca_catmat = True

    def _iniciar_exp(self):
        if not self._fontes_ok(): return
        if not self.lista_catmats:
            messagebox.showerror("Vazio","Nenhum CATMAT disponível."); return
        d_i, d_f, err = validar_e_obter_datas(self.var_ini2.get(), self.var_fim2.get())
        if err: messagebox.showerror("Data inválida", err); return
        self._log(f"🔎 {len(self.lista_catmats)} CATMATs via explorador.", "info")
        por_classe = self.var_arquivo_por_classe.get()
        # Usa o mapa já construído em _on_busca_e_extrai (se existir)
        mapa = getattr(self, "_catmats_por_classe", {}) if por_classe else {}
        if por_classe and not mapa:
            messagebox.showwarning("Aviso",
                "Use Buscar e Extrair para gerar arquivos por classe.")
            return
        self._iniciar_processo(self.lista_catmats, self.var_fmt.get(), d_i, d_f,
                               catmats_por_classe=mapa, tipo=TIPO_CATMAT)
        self.after(100, lambda: self.tabs.set("  Extração por CATMAT  "))

    def _buscar_e_extrair_classes(self):
        """Fluxo automatizado: para cada classe, faz PDMs → CATMATs → Extração → Salva."""
        if not self._fontes_ok(): return
        entrada = self.var_classe.get().strip()
        if not entrada:
            messagebox.showerror("Campo vazio", "Informe ao menos um código de Classe."); return

        partes = [p.strip() for p in entrada.split(";") if p.strip()]
        invalidas = [p for p in partes if not p.isdigit()]
        if invalidas:
            messagebox.showerror("Código inválido",
                "Valores nao numericos: " + ", ".join(invalidas)); return

        por_classe = self.var_arquivo_por_classe.get()
        pasta_dest = self.var_pasta_classes.get().strip() if por_classe else ""

        if por_classe and len(partes) > 1 and not pasta_dest:
            messagebox.showerror("Pasta obrigatória",
                "Selecione uma pasta de destino para salvar os arquivos por classe."); return

        d_i, d_f, err = validar_e_obter_datas(self.var_ini2.get(), self.var_fim2.get())
        if err: messagebox.showerror("Data inválida", err); return

        fmt = self.var_fmt.get()
        self._salvar_corr = self.var_salvar_corr.get()   # capturado na thread principal
        self._pasta_corr  = self.var_pasta.get()
        self._bps_cfg     = self._config_bps()
        self._pasta_classes_destino = pasta_dest
        _erros_api.iniciar(pasta_dest)
        self.processing   = True
        self.total_baixados = 0
        self.count_corrigidas = 0
        self.count_reparadas  = 0
        self.count_vazios = 0
        pausar_extracao.set()
        pausar_busca_catmat.set()  # necessário para o fluxo automatizado
        global cancelar_busca_catmat
        cancelar_busca_catmat = False   # zera resíduo de cancelamento anterior

        self.log.configure(state="normal"); self.log.delete("1.0","end")
        self.log.configure(state="disabled")
        for k, v in [("k_proc","0"),("k_reg","0"),("k_corr","0 · 0"),("k_vaz","0")]:
            self._stat(k, v)
        self.progress.set(0); self.lbl_pct.configure(text="0%")
        self.set_status("Status: Iniciando…")
        self.btn_start.configure(state="disabled")
        self.btn_cancel.configure(state="normal", fg_color=C_RED,
                                   hover_color="#992B1E", text_color=C_SURFACE)
        self.btn_pause.configure(state="normal", text="⏸  Pausar")
        self.btn_log.configure(state="disabled")

        self._log("💾 Formato: " + ("CSV" if fmt == "csv" else "Excel"), "info")
        self._log_config_bps(self._bps_cfg)
        if d_i or d_f:
            def fd(s):
                p = s.split("-"); return p[2]+"-"+p[1]+"-"+p[0]
            txt = "📅 Filtro de datas:"
            if d_i: txt += "  Início: " + fd(d_i)
            if d_f: txt += "  Fim: "    + fd(d_f)
            self._log(txt, "date")
        self._log("📋 " + str(len(partes)) + " classe(s): " + " | ".join(partes) + "\n", "info")
        tipo_busca = TIPO_PDM if self.var_extrair_por_pdm.get() else TIPO_CATMAT
        self._tipo_busca = tipo_busca
        self._log("🎯 Tipo de busca: " + ROTULO_TIPO[tipo_busca] +
                  "  (tipo=" + tipo_busca + ")", "info")
        if pasta_dest:
            self._log("📂 Destino: " + pasta_dest, "info")

        self.after(100, lambda: self.tabs.set("  Extração por CATMAT  "))
        threading.Thread(
            target=self._fluxo_classes_thread,
            args=(partes, pasta_dest, fmt, d_i, d_f, tipo_busca),
            daemon=True
        ).start()

    def _fluxo_classes_thread(self, classes_lista, pasta_dest, fmt, d_ini, d_fim,
                              tipo_busca=TIPO_CATMAT):
        """
        Thread principal do fluxo por classe.
        tipo_busca = TIPO_CATMAT → Classe → PDMs → CATMATs → Registros de Preços
        tipo_busca = TIPO_PDM    → Classe → PDMs → Registros de Preços (direto)
        Retry em todos os níveis: classes, PDMs e CATMATs com erro.
        """
        total_classes_orig = len(classes_lista)
        salvar_corr  = getattr(self, "_salvar_corr", False)
        pasta_corr   = getattr(self, "_pasta_corr", "")
        bps_cfg      = getattr(self, "_bps_cfg", None)
        total_catmats_acum = 0
        classes_com_falha  = []   # classes que não retornaram PDMs

        # ── Helper: extrai os Registros de Preços de uma lista de códigos ─────
        # (CATMATs quando tipo_busca=TIPO_CATMAT, PDMs quando TIPO_PDM)
        def _extrair_codigos(classe, idx_c, total_c, codigos_lista):

            # ── 3. Extração dos Registros de Preços ───────────────────────────
            writer   = self._novo_writer(pasta_dest, fmt, classe)

            reg_baixados    = {}
            reg_esperados   = {}
            pag_corrompidas = {}
            total_baixados_classe = 0
            vazios_classe         = 0
            catmats_com_erro      = []
            total_cat  = len(codigos_lista)
            writer_lock = threading.Lock()
            state_lock  = threading.Lock()
            comp_count  = [0]
            reg_bps     = {}          # código -> registros do BPS gravados
            bps_falhas  = []          # CATMATs/PDMs que o BPS não respondeu
            bps_erro    = set()       # códigos com alguma falha no BPS
            bps_soma    = {"linhas": 0, "trocadas": 0}

            def _gravar_bps(codigo, bps):
                if bps is None:
                    return
                n = 0 if bps["df"] is None else len(bps["df"])
                if n:
                    with writer_lock:
                        writer.write_dataframe(bps["df"], aba=self._aba_bps(bps_cfg))
                with state_lock:
                    reg_bps[codigo] = reg_bps.get(codigo, 0) + n
                    bps_soma["linhas"]   += n
                    bps_soma["trocadas"] += bps["trocadas"]
                    bps_falhas.extend(bps["falhas"])
                    if bps["falhas"]:
                        bps_erro.add(codigo)
                    if not bps_cfg["compras"]:          # só BPS: é ele quem conta
                        self.total_baixados += n
                    reg = self.total_baixados
                if not bps_cfg["compras"]:
                    self._ui(lambda r=reg: self._stat("k_reg", f"{r:,}".replace(",",".")))
                if bps["falhas"]:
                    ftxt = ("⚠️  BPS sem resposta para " + ", ".join(map(str, bps["falhas"]))
                            + " (código " + str(codigo) + ").")
                    self._ui(lambda t=ftxt: self._log(t, "warn"))

            def _processar_resultado(codigo, dfs_e_meta, tipo, reg_esp, pag_corr, bps=None):
                nonlocal total_baixados_classe, vazios_classe, total_catmats_acum
                # Código com erro volta para o retry, que busca o BPS de novo:
                # gravar agora duplicaria as linhas da aba BPS
                if tipo != "erro":
                    _gravar_bps(codigo, bps)
                if tipo == "conexao":
                    pass  # já tratado dentro de _fetch_catmat_registros via pausa automática
                elif tipo == "pulado":                  # só BPS: Compras.gov não consultado
                    if not reg_bps.get(codigo):
                        vtxt = "ℹ️  " + str(codigo) + ": 0 registros no BPS."
                        self._ui(lambda t=vtxt: self._log(t, "info"))
                        with state_lock:
                            vazios_classe += 1
                            self.count_vazios += 1
                            v = self.count_vazios
                        self._ui(lambda vv=v: self._stat("k_vaz", vv))
                elif tipo == "erro":
                    with state_lock:
                        catmats_com_erro.append(codigo)
                    etxt = ("⚠️  " + ROTULO_TIPO.get(tipo_busca, "Código") + " " + str(codigo)
                            + ": " + _erros_api.resumo(codigo) + " — será retentado.")
                    self._ui(lambda t=etxt: self._log(t, "warn"))
                elif tipo == "vazio":
                    vtxt = "ℹ️  " + str(codigo) + ": 0 registros."
                    self._ui(lambda t=vtxt: self._log(t, "info"))
                    with state_lock:
                        vazios_classe += 1
                        self.count_vazios += 1
                        v = self.count_vazios
                    self._ui(lambda vv=v: self._stat("k_vaz", vv))
                else:
                    baixados = sum(len(df) for df, _, _ in dfs_e_meta)
                    with writer_lock:
                        for df_proc, _, _ in dfs_e_meta:
                            writer.write_dataframe(df_proc)
                    with state_lock:
                        total_baixados_classe += baixados
                        reg_baixados[codigo]   = baixados
                        reg_esperados[codigo]  = reg_esp
                        self.total_baixados   += baixados
                        for k, v in pag_corr.items():
                            pag_corrompidas.setdefault(k, []).extend(v)
                        n_corr = sum(len(v) for v in pag_corr.values())
                        self.count_corrigidas += n_corr
                        reg = self.total_baixados
                    self._ui(lambda r=reg: self._stat("k_reg", f"{r:,}".replace(",",".")))
                    n_rep = sum(1 for _, mk, _ in dfs_e_meta if mk == "reparada")
                    if n_rep:
                        with state_lock:
                            self.count_reparadas += n_rep
                    for _, marca, pag in dfs_e_meta:
                        if marca == "perda":
                            self._ui(lambda cod=codigo, p=pag:
                                self._log(f"❌  Cód {cod} Pág {p}: registros perdidos.", "err"))
                        elif marca == "reparada":
                            self._ui(lambda cod=codigo, p=pag:
                                self._log(f"⚠️  Cód {cod} Pág {p}: reparada (íntegra).", "warn"))
                    if pag_corr or n_rep:
                        self._ui(self._atualizar_stat_paginas)
                with state_lock:
                    comp_count[0] += 1
                    total_catmats_acum += 1
                    comp = comp_count[0]
                    tca  = total_catmats_acum
                self._ui(lambda v=tca: self._stat("k_proc", v))
                pct = (idx_c - 1 + comp / total_cat) / total_c
                self._ui(lambda c=classe, i=comp, t=total_cat, p=pct:
                    (self.set_status("Status: Classe " + c + " — " + str(i) + "/" + str(t)),
                     self.progress.set(p),
                     self.lbl_pct.configure(text=str(int(p*100)) + "%")))
                return tipo

            # Paralelo até WORKERS_COMPRAS: quem limita o ritmo é a cota (_get_compras)
            with ThreadPoolExecutor(max_workers=WORKERS_COMPRAS) as executor:
                futures = {
                    executor.submit(_fetch_codigo, cod,
                                    d_ini, d_fim, salvar_corr, pasta_corr,
                                    self._pausar_por_conexao, tipo_busca,
                                    lambda: not self.processing, bps_cfg): cod
                    for cod in codigos_lista
                }
                for future in as_completed(futures):
                    pausar_extracao.wait()
                    if not self.processing:
                        executor.shutdown(wait=False, cancel_futures=True); break
                    cod, dfs_m, tipo, reg_e, pag_c, bps = future.result()
                    res = _processar_resultado(cod, dfs_m, tipo, reg_e, pag_c, bps)
                    if res == "conexao": break

            # Retry sequencial dos códigos com erro
            rotulo = ROTULO_TIPO.get(tipo_busca, "código")
            for espera in [15, 30]:
                if not catmats_com_erro or not self.processing: break
                n_err = len(catmats_com_erro)
                self._ui(lambda n=n_err, e=espera, rt=rotulo:
                    self._log("♻️  Retry " + str(n) + " " + rt + "(s) com erro (aguardando " +
                              str(e) + "s)…", "warn"))
                time.sleep(espera)
                retry_list = list(catmats_com_erro); catmats_com_erro.clear()
                with ThreadPoolExecutor(max_workers=WORKERS_COMPRAS) as executor:
                    futures = {
                        executor.submit(_fetch_codigo, cod,
                                        d_ini, d_fim, salvar_corr, pasta_corr,
                                        self._pausar_por_conexao, tipo_busca,
                                        lambda: not self.processing, bps_cfg): cod
                        for cod in retry_list
                    }
                    for future in as_completed(futures):
                        if not self.processing: break
                        cod, dfs_m, tipo, reg_e, pag_c, bps = future.result()
                        _processar_resultado(cod, dfs_m, tipo, reg_e, pag_c, bps)

            if catmats_com_erro:
                n_def = len(catmats_com_erro)
                self._ui(lambda n=n_def, rt=rotulo:
                    self._log("❌  " + str(n) + " " + rt + "(s) sem resposta após 3 tentativas: "
                              + ", ".join(map(str, catmats_com_erro[:20]))
                              + (" …" if n > 20 else "")
                              + " (motivo de cada um na apuração ao final)", "err"))
                # O Compras.gov falhou de vez, mas o BPS é outro servidor: os
                # dados dele ainda entram na aba BPS
                if bps_cfg["aba"] and self.processing:
                    for cod in catmats_com_erro:
                        _gravar_bps(cod, _bps_do_codigo(cod, tipo_busca, d_ini, d_fim,
                                                        lambda: not self.processing))

            if not self.processing: return True

            # ── 4. Finalizar arquivo desta classe ─────────────────────────────
            parts = writer.finalize()
            arqs  = ", ".join(os.path.basename(p) for p in parts) if parts else "(sem dados)"
            n_arq = total_baixados_classe if bps_cfg["compras"] else bps_soma["linhas"]
            self._ui(lambda c=classe, a=arqs, n=n_arq:
                self._log("📁  Classe " + c + " — " + str(n) + " registros → " + a, "info"))
            if bps_cfg["compras"] and (bps_cfg["aba"] or bps_cfg["descricao"]):
                partes_txt = []
                if bps_cfg["aba"]:
                    partes_txt.append(f"{bps_soma['linhas']:,}".replace(",", ".")
                                      + " registros do BPS (aba " + ABA_BPS + ")")
                if bps_cfg["descricao"]:
                    partes_txt.append("descricaoItem trocado pelo texto do BPS em "
                                      + f"{bps_soma['trocadas']:,}".replace(",", ".") + " linha(s)")
                btxt = "🏥  Classe " + classe + " — " + " | ".join(partes_txt)
                self._ui(lambda t=btxt: self._log(t, "info"))
            if bps_falhas:
                ftxt = ("❌  BPS sem resposta (" + str(len(bps_falhas)) + "): "
                        + ", ".join(map(str, bps_falhas[:20]))
                        + (" …" if len(bps_falhas) > 20 else ""))
                self._ui(lambda t=ftxt: self._log(t, "err"))

            # ── 5. Relatório de integridade desta classe ──────────────────────
            rel_nome    = "Relatorio_Integridade_" + classe + ".xlsx"
            rel_caminho = os.path.join(pasta_dest, rel_nome) if pasta_dest else rel_nome
            try:
                wb = Workbook(); ws = wb.active; ws.title = "Integridade_" + classe
                if not bps_cfg["compras"]:              # só BPS
                    ws.append([tipo_busca, "registros BPS", "status"])
                    for c in codigos_lista:
                        n = reg_bps.get(c, 0)
                        ws.append([c, n, "ERRO: BPS sem resposta — "
                                         + _erros_api.resumo(c, ("BPS", "CATÁLOGO", "PROGRAMA"))
                                   if c in bps_erro
                                   else "OK" if n else "sem registros no BPS"])
                else:
                    ws.append([tipo_busca,"esperados","baixados","paginas","status"]
                              + (["registros BPS"] if bps_cfg["aba"] else []))
                for c in (codigos_lista if bps_cfg["compras"] else []):
                    bx = int(reg_baixados.get(c, 0))
                    ex = int(reg_esperados.get(c, 0))
                    pg = pag_corrompidas.get(c, [])
                    d  = abs(ex - bx)
                    st = ("ERRO_API_PERSISTENTE — " + _erros_api.resumo(c)
                          if c in catmats_com_erro else
                          "OK" if d == 0 else
                          "OK (divergencia: " + str(bx) + "/" + str(ex) + ")" if d <= 2 else
                          "Inconsistencia Grave (" + str(bx) + "/" + str(ex) + ")")
                    ws.append([c, ex, bx, ", ".join(map(str, pg)), st]
                              + ([reg_bps.get(c, 0)] if bps_cfg["aba"] else []))
                if catmats_com_erro:
                    ws.append([])
                    ws.append(["--- " + rotulo + "s sem resposta apos 3 tentativas ---"])
                    for c in catmats_com_erro:
                        ws.append([c, 0, 0, "", "ERRO_API_PERSISTENTE — "
                                   + _erros_api.resumo(c)])
                wb.save(rel_caminho)
                self._ui(lambda r=rel_nome:
                    self._log("📊  Relatório: " + r, "info"))
            except Exception as e:
                etxt = "⚠️ Relatório classe " + classe + " não salvo: " + str(e)
                self._ui(lambda t=etxt: self._log(t, "warn"))
            return True

        # ── Helper: processa uma classe completa ──────────────────────────────
        def _processar_classe(classe, idx_c, total_c):
            ic, tc = idx_c, total_c
            sep = "─" * 50
            hdr = sep + "\n📦  CLASSE " + classe + "  (" + str(ic) + "/" + str(tc) + ")\n" + sep
            self._ui(lambda h=hdr: self._log("\n" + h, "date"))
            self._ui(lambda c=classe, i=ic, t=tc:
                self.set_status("Status: Classe " + c + " (" + str(i) + "/" + str(t) + ") — PDMs…"))

            # ── 1. PDMs ───────────────────────────────────────────────────────
            resultado = buscar_pdms_por_classe(int(classe), URL_BASE, TIMEOUT)
            if resultado is None:
                return False   # falhou — será retentada
            df_pdms, _ = resultado
            pdms_lista  = df_pdms["codigoPdm"].astype(int).tolist()
            n_pdms = len(pdms_lista)
            self._ui(lambda c=classe, n=n_pdms:
                self._log("✅  Classe " + c + ": " + str(n) + " PDMs.", "ok"))

            # ── 2. CATMATs ────────────────────────────────────────────────────
            # No modo tipo=codigoPdm a expansão PDM → CATMAT é desnecessária:
            # a própria Pesquisa de Preço devolve todos os itens do PDM.
            if tipo_busca == TIPO_PDM:
                self._ui(lambda c=classe, n=n_pdms:
                    self._log("⏩  Classe " + c + ": extração direta de " + str(n) +
                              " PDMs (sem expandir CATMATs).", "info"))
                return _extrair_codigos(classe, idx_c, total_c, pdms_lista)

            self._ui(lambda c=classe, n=n_pdms:
                self.set_status("Status: Classe " + c + " — CATMATs (" + str(n) + " PDMs)…"))

            _log_pdm = lambda m: self._log("  🔍 " + m, "info")
            df_cat1, erros1 = buscar_catmats_por_pdm(pdms_lista, URL_BASE, TIMEOUT, self, log_fn=_log_pdm)
            df_cat2 = None; erros2 = []
            if erros1 and not cancelar_busca_catmat:
                n_e1 = len(erros1)
                self._ui(lambda n=n_e1:
                    self._log("♻️  2ª tentativa (10s) para " + str(n) + " PDMs com erro…", "warn"))
                time.sleep(3)
                df_cat2, erros2 = buscar_catmats_por_pdm(erros1, URL_BASE, TIMEOUT, self, log_fn=_log_pdm)
            # 3ª tentativa para PDMs que ainda falharam
            df_cat3 = None; erros3 = []
            if erros2 and not cancelar_busca_catmat:
                n_e2 = len(erros2)
                self._ui(lambda n=n_e2:
                    self._log("♻️  3ª tentativa (20s) para " + str(n) + " PDMs com erro…", "warn"))
                time.sleep(8)
                df_cat3, erros3 = buscar_catmats_por_pdm(erros2, URL_BASE, TIMEOUT, self, log_fn=_log_pdm)
                if erros3:
                    falha_pdm = ", ".join(map(str, erros3))
                    self._ui(lambda t=falha_pdm:
                        self._log("❌  PDMs sem resposta após 3 tentativas: " + t, "err"))

            dfs_c = [d for d in [df_cat1, df_cat2, df_cat3] if d is not None and not d.empty]
            df_catmats = pd.concat(dfs_c, ignore_index=True) if dfs_c else None
            if df_catmats is None or "codigoItem" not in df_catmats.columns:
                self._ui(lambda c=classe:
                    self._log("⚠️  Classe " + c + ": nenhum CATMAT. Pulando.", "warn"))
                return True   # PDMs foram encontrados mas sem CATMATs — não é falha de PDMs

            catmats_lista = df_catmats["codigoItem"].dropna().astype(int).tolist()
            n_cat = len(catmats_lista)
            self._ui(lambda c=classe, n=n_cat:
                self._log("✅  Classe " + c + ": " + str(n) + " CATMATs.", "ok"))

            return _extrair_codigos(classe, idx_c, total_c, catmats_lista)

        # ── Loop principal por classe ─────────────────────────────────────────
        for idx_classe, classe in enumerate(classes_lista, 1):
            if not self.processing: break
            ok = _processar_classe(classe, idx_classe, total_classes_orig)
            if not ok:
                self._ui(lambda c=classe:
                    self._log("⚠️  Classe " + c + ": sem PDMs na 1ª tentativa. Fila de retry.", "warn"))
                classes_com_falha.append(classe)

        # ── Retry classes sem PDMs (10s → 20s) ───────────────────────────────
        for num_tent, espera in enumerate([3, 8], 2):
            if not classes_com_falha or not self.processing: break
            n_f = len(classes_com_falha)
            self._ui(lambda n=n_f, e=espera, t=num_tent:
                self._log("\n♻️  " + str(n) + " classe(s) sem PDMs — tentativa " +
                          str(t) + "/3 (aguardando " + str(e) + "s)…", "warn"))
            time.sleep(espera)
            ainda_falha = []
            for idx, classe in enumerate(classes_com_falha, 1):
                if not self.processing: break
                ok = _processar_classe(classe, idx, len(classes_com_falha))
                if not ok:
                    ainda_falha.append(classe)
            classes_com_falha = ainda_falha

        if classes_com_falha:
            falhas = ", ".join(classes_com_falha)
            self._ui(lambda t=falhas:
                self._log("❌  Classes sem PDMs após 3 tentativas: " + t, "err"))

        # ── Todas as classes processadas ──────────────────────────────────────
        self._ui(self._finalizar_fluxo_classes)

    def _log_apuracao_erros(self):
        """Resumo final: quantos erros, de que tipo, em qual fonte (DA/BPS...)."""
        linhas = _erros_api.apurar()
        if not linhas:
            self._log("✅ Nenhum erro de API nesta extração.", "ok")
            return
        for ln in linhas:
            self._log(ln, "warn")

    def _finalizar_fluxo_classes(self):
        """Chamado ao término de todas as classes."""
        foi_cancelado = not self.processing
        self.processing = False
        self.progress.set(1.0); self.lbl_pct.configure(text="100%")
        self.set_status("Status: Concluído!" if not foi_cancelado else "Status: Cancelado")
        self._log(
            "\n🎉 Todas as classes processadas com sucesso!" if not foi_cancelado
            else "\n🛑 Extração cancelada.", "info")
        self._log_apuracao_erros()

        self.btn_start.configure(state="normal")
        self.btn_cancel.configure(state="disabled",
                                   fg_color="#E4E7EF", hover_color=C_BORDER,
                                   text_color=C_TEXT)
        self.btn_pause.configure(state="disabled", text="⏸  Pausar")
        self.btn_log.configure(state="normal")

        pasta = getattr(self, "_pasta_classes_destino", "").strip()
        if pasta:
            messagebox.showinfo("Concluído",
                "Extração finalizada!\nTodos os arquivos foram salvos em:\n" + pasta)
        else:
            messagebox.showinfo("Concluído", "Extração finalizada!")

        # ── MOTOR DE EXTRAÇÃO ─────────────────────────────────────────────────────
    def _iniciar_processo(self, codigos, fmt, d_ini, d_fim, catmats_por_classe=None,
                          tipo=TIPO_CATMAT, por_classe=None, pasta_destino=None):
        if not codigos: return
        if not self._fontes_ok(): return
        self.processing            = True
        self.codigos_lista         = codigos
        self._data_inicio          = d_ini
        self._data_fim             = d_fim
        self._fmt                  = fmt
        self._tipo_busca           = tipo
        # Tk não é thread-safe: capturar aqui, na thread principal
        self._salvar_corr          = self.var_salvar_corr.get()
        self._pasta_corr           = self.var_pasta.get()
        self._bps_cfg              = self._config_bps()
        self.registros_bps         = {}      # código -> registros gravados na aba BPS
        self.bps_falhas            = []
        self.bps_erro              = set()   # códigos com alguma falha no BPS
        self.total_bps             = 0
        self.bps_trocadas          = 0
        self._catmats_por_classe_ativo = catmats_por_classe or {}
        # Um arquivo por classe. Quando não há mapa prévio de códigos→classe,
        # a classe é lida do campo codigoClasse de cada registro.
        self._modo_por_classe = (bool(self._catmats_por_classe_ativo)
                                 if por_classe is None else bool(por_classe))
        # Pasta escolhida pelo usuário para salvar os arquivos por classe.
        # pasta_destino explícito evita que a pasta da aba 2 vaze para a aba 1.
        if pasta_destino is not None:
            self._pasta_classes_destino = pasta_destino
        else:
            self._pasta_classes_destino = (self.var_pasta_classes.get().strip()
                                           if hasattr(self, "var_pasta_classes") else "")
        self.paginas_corrompidas   = {}
        self.registros_esperados   = {}
        self.registros_baixados    = {}
        self.total_baixados        = 0
        self.count_corrigidas      = 0
        self.count_reparadas       = 0
        self.count_vazios          = 0
        self.codigos_com_erro      = set()
        _erros_api.iniciar(self._pasta_classes_destino.strip())
        pausar_extracao.set()

        # Se há arquivo por classe, usamos um writer por classe (criados sob demanda)
        # Senão, writer único
        if self._modo_por_classe:
            self.writer = None  # será None; usamos self._writers_por_classe
            self._writers_por_classe = {}  # classe → writer
        else:
            self.writer = self._novo_writer("", fmt)
            self._writers_por_classe = {}

        self.log.configure(state="normal"); self.log.delete("1.0","end")
        self.log.configure(state="disabled")
        self._log(f"💾 Formato: {'CSV' if fmt == 'csv' else 'Excel'}", "info")
        self._log_config_bps(self._bps_cfg)
        if d_ini or d_fim:
            def fd(s): p = s.split("-"); return f"{p[2]}-{p[1]}-{p[0]}"
            txt = "📅 Filtro de datas:"
            if d_ini: txt += f"  Início: {fd(d_ini)}"
            if d_fim:  txt += f"  Fim: {fd(d_fim)}"
            self._log(txt, "date")
        self._log(f"🎯 Tipo de busca: {ROTULO_TIPO.get(tipo, tipo)}  (tipo={tipo})", "info")
        self._log(f"🔎 {len(codigos)} códigos carregados.\n", "info")

        for k, v in [("k_proc",f"0 / {len(codigos)}"),
                     ("k_reg","0"),("k_corr","0 · 0"),("k_vaz","0")]:
            self._stat(k, v)
        self.progress.set(0); self.lbl_pct.configure(text="0%")
        self.set_status("Status: Processando…")
        self.btn_start.configure(state="disabled")
        self.btn_cancel.configure(state="normal", fg_color=C_RED,
                                   hover_color="#992B1E", text_color=C_SURFACE)
        self.btn_pause.configure(state="normal", text="⏸  Pausar")
        self.btn_log.configure(state="disabled")
        # Lança extração em thread separada — UI continua responsiva
        self._extracao_thread_obj = threading.Thread(
            target=self._extracao_thread, daemon=True)
        self._extracao_thread_obj.start()

    # ─────────────────────────────────────────────────────────────────────────
    # MOTOR DE EXTRAÇÃO — roda 100% em thread separada
    # Comunicação com UI exclusivamente via self.after(0, callback)
    # ─────────────────────────────────────────────────────────────────────────

    def _get_writer_para(self, codigo: int, classe: str = None):
        if not self._modo_por_classe:
            return self.writer
        # Mapa explícito (explorador ou coluna "classe" do arquivo) tem prioridade;
        # sem ele, vale a classe lida do próprio registro.
        classe_do_cod = None
        for cl, cats in self._catmats_por_classe_ativo.items():
            if codigo in cats:
                classe_do_cod = cl; break
        if classe_do_cod is None:
            classe_do_cod = classe or "sem_classe"
        classe_do_cod = re.sub(r'[\\/:*?"<>|]', "_", str(classe_do_cod)).strip() or "sem_classe"
        if classe_do_cod not in self._writers_por_classe:
            # Salva direto na pasta escolhida pelo usuário (se informada)
            pasta = getattr(self, "_pasta_classes_destino", "").strip()
            self._writers_por_classe[classe_do_cod] = self._novo_writer(
                pasta, self._fmt, classe_do_cod)
        return self._writers_por_classe[classe_do_cod]

    def _ui(self, fn):
        """Agenda fn() na thread principal de forma segura."""
        self.after(0, fn)

    def _extracao_thread(self):
        """Thread de extração paralela — 4 CATMATs simultâneos."""
        codigos     = self.codigos_lista
        total       = len(codigos)
        salvar_corr = getattr(self, "_salvar_corr", False)
        pasta_corr  = getattr(self, "_pasta_corr", "")
        d_ini       = self._data_inicio
        d_fim       = self._data_fim
        tipo_busca  = getattr(self, "_tipo_busca", TIPO_CATMAT)
        bps_cfg     = getattr(self, "_bps_cfg", None)
        writer_locks: dict = {}   # id(writer) → Lock
        state_lock  = threading.Lock()
        comp_count  = [0]

        def _wlock(codigo, classe=None):
            w = self._get_writer_para(codigo, classe)
            k = id(w)
            if k not in writer_locks:
                writer_locks[k] = threading.Lock()
            return w, writer_locks[k]

        def _escrever(codigo, df_proc, aba=None):
            """Roteia o DataFrame para o writer certo, quebrando por classe."""
            if (not self._modo_por_classe or self._catmats_por_classe_ativo
                    or "codigoClasse" not in df_proc.columns):
                w, lk = _wlock(codigo)
                with lk: w.write_dataframe(df_proc, aba=aba)
                return
            # Classe lida do próprio registro: uma partição por classe encontrada
            chaves = (df_proc["codigoClasse"].astype(str).str.strip()
                      .replace({"": "sem_classe", "nan": "sem_classe",
                                "None": "sem_classe"}))
            for classe, parte in df_proc.groupby(chaves, sort=False):
                w, lk = _wlock(codigo, str(classe))
                with lk: w.write_dataframe(parte, aba=aba)

        def _gravar_bps(codigo, bps):
            if bps is None:
                return
            n = 0 if bps["df"] is None else len(bps["df"])
            if n:
                _escrever(codigo, bps["df"], aba=self._aba_bps(bps_cfg))
                _flush_periodico()
            with state_lock:
                self.registros_bps[codigo] = self.registros_bps.get(codigo, 0) + n
                self.total_bps    += n
                self.bps_trocadas += bps["trocadas"]
                self.bps_falhas.extend(bps["falhas"])
                if bps["falhas"]:
                    self.bps_erro.add(codigo)
                if not bps_cfg["compras"]:              # só BPS: é ele quem conta
                    self.total_baixados += n
                reg = self.total_baixados
            if not bps_cfg["compras"]:
                self._ui(lambda r=reg: self._stat("k_reg", f"{r:,}".replace(",",".")))
            if bps["falhas"]:
                ftxt = (f"⚠️  BPS sem resposta para {', '.join(map(str, bps['falhas']))}"
                        f" (código {codigo}).")
                self._ui(lambda t=ftxt: self._log(t, "warn"))

        def _flush_periodico():
            """Descarrega em disco o que já foi montado (throttled no writer)."""
            alvos = (list(self._writers_por_classe.values())
                     if self._modo_por_classe else
                     ([self.writer] if self.writer else []))
            for w in alvos:
                lk = writer_locks.get(id(w))
                if lk is None:
                    continue
                with lk:
                    w.flush()

        with ThreadPoolExecutor(max_workers=WORKERS_COMPRAS) as executor:
            futures = {
                executor.submit(_fetch_codigo, cod,
                                d_ini, d_fim, salvar_corr, pasta_corr,
                                self._pausar_por_conexao, tipo_busca,
                                lambda: not self.processing, bps_cfg): cod
                for cod in codigos
            }
            for future in as_completed(futures):
                pausar_extracao.wait()
                if not self.processing:
                    executor.shutdown(wait=False, cancel_futures=True); break

                codigo, dfs_e_meta, tipo, reg_esp, pag_corr, bps = future.result()
                # Nesta aba não há retry do Compras.gov: o BPS entra mesmo
                # quando o código veio vazio ou com erro de lá
                _gravar_bps(codigo, bps)

                with state_lock:
                    comp_count[0] += 1
                    comp = comp_count[0]

                if tipo == "conexao":
                    pass  # já tratado dentro de _fetch_catmat_registros via pausa automática

                elif tipo == "pulado":             # só BPS: Compras.gov não consultado
                    n_bps = self.registros_bps.get(codigo, 0)
                    if n_bps:
                        self._ui(lambda cod=codigo, n=n_bps:
                            self._log(f"✅  Cód {cod}: {n} registros no BPS.", "ok"))
                    else:
                        with state_lock:
                            self.count_vazios += 1
                            v = self.count_vazios
                        self._ui(lambda cod=codigo: self._log(f"ℹ️  {cod}: 0 registros no BPS.", "info"))
                        self._ui(lambda vv=v: self._stat("k_vaz", vv))

                elif tipo in ("erro", "vazio"):
                    txt = (f"⚠️  {codigo}: sem registro — {_erros_api.resumo(codigo)}."
                           if tipo == "erro" else f"ℹ️  {codigo}: 0 registros.")
                    with state_lock:
                        if tipo == "erro":
                            self.codigos_com_erro.add(codigo)
                        self.count_vazios += 1
                        v = self.count_vazios
                        self.registros_baixados[codigo]  = 0
                        self.registros_esperados[codigo] = reg_esp
                    self._ui(lambda t=txt: self._log(t, "info"))
                    self._ui(lambda vv=v: self._stat("k_vaz", vv))

                else:  # "ok"
                    baixados = sum(len(df) for df, _, _ in dfs_e_meta)
                    for df_proc, _, _ in dfs_e_meta:
                        _escrever(codigo, df_proc)
                    _flush_periodico()
                    with state_lock:
                        self.total_baixados             += baixados
                        self.registros_baixados[codigo]  = baixados
                        self.registros_esperados[codigo] = reg_esp
                        reg = self.total_baixados
                        for k, v in pag_corr.items():
                            self.paginas_corrompidas.setdefault(k, []).extend(v)
                        n_c = sum(len(v) for v in pag_corr.values())
                        self.count_corrigidas += n_c
                    self._ui(lambda r=reg: self._stat("k_reg", f"{r:,}".replace(",",".")))
                    n_rep = sum(1 for _, mk, _ in dfs_e_meta if mk == "reparada")
                    if n_rep:
                        with state_lock:
                            self.count_reparadas += n_rep
                    for _, marca, pag in dfs_e_meta:
                        if marca == "perda":
                            self._ui(lambda cod=codigo, p=pag:
                                self._log(f"❌  Cód {cod} Pág {p}: registros perdidos.", "err"))
                        elif marca == "reparada":
                            self._ui(lambda cod=codigo, p=pag:
                                self._log(f"⚠️  Cód {cod} Pág {p}: reparada (íntegra).", "warn"))
                        else:
                            self._ui(lambda cod=codigo, p=pag:
                                self._log(f"✅  Cód {cod} Pág {p}: OK.", "ok"))
                    if pag_corr or n_rep:
                        self._ui(self._atualizar_stat_paginas)

                pct = comp / total
                self._ui(lambda p=pct, c=comp, t=total: (
                    self.progress.set(p),
                    self.lbl_pct.configure(text=f"{int(p*100)}%"),
                    self.set_status(f"Status: Processando... ({c}/{t})"),
                    self._stat("k_proc", f"{c} / {t}")
                ))

        self._ui(self._finalizar)

    def _finalizar(self):
        """Chamado na thread principal via after() ao término da extração."""
        foi_cancelado = not self.processing
        self.processing = False
        self.progress.set(1.0); self.lbl_pct.configure(text="100%")
        self.set_status("Status: Concluído!" if not foi_cancelado else "Status: Cancelado")
        self._log("\n🎉 Extração concluída!" if not foi_cancelado
                  else "\n🛑 Extração cancelada.", "info")
        self._log_apuracao_erros()

        # Finalizar writers
        parts = []
        if self._modo_por_classe and self._writers_por_classe:
            for classe, w in self._writers_por_classe.items():
                p = w.finalize(); parts.extend(p)
                if p: self._log(f"📂 Classe {classe}: {', '.join(p)}", "info")
        elif self.writer:
            parts = self.writer.finalize()

        if parts:
            self._log(f"💾 Arquivos gerados: {', '.join(parts)}", "info")

        bps_cfg = getattr(self, "_bps_cfg", None) or {"compras": True, "aba": False,
                                                       "descricao": False}
        if bps_cfg["aba"] or bps_cfg["descricao"]:
            partes_txt = []
            if bps_cfg["aba"]:
                partes_txt.append(f"{self.total_bps:,} registros"
                                  + (f" na aba {ABA_BPS}" if bps_cfg["compras"] else ""))
            if bps_cfg["descricao"]:
                partes_txt.append(f"descricaoItem trocado pelo texto do BPS em "
                                  f"{self.bps_trocadas:,} linha(s)")
            self._log(("🏥 BPS: " + " | ".join(partes_txt)).replace(",", "."), "info")
            if self.bps_falhas:
                self._log(f"❌ BPS sem resposta ({len(self.bps_falhas)}): "
                          + ", ".join(map(str, self.bps_falhas[:20]))
                          + (" …" if len(self.bps_falhas) > 20 else ""), "err")

        # Relatório de integridade
        try:
            wb = Workbook(); ws = wb.active; ws.title = "Relatorio Integridade"
            tipo_rel = getattr(self, "_tipo_busca", TIPO_CATMAT)
            if not bps_cfg["compras"]:                  # só BPS
                ws.append([tipo_rel, "registros BPS", "status"])
                for c in self.codigos_lista:
                    n = self.registros_bps.get(c, 0)
                    ws.append([c, n, "ERRO: BPS sem resposta — "
                                     + _erros_api.resumo(c, ("BPS", "CATÁLOGO", "PROGRAMA"))
                               if c in self.bps_erro
                               else "OK" if n else "sem registros no BPS"])
            else:
                ws.append([tipo_rel, "esperados","baixados","paginas","status"]
                          + (["registros BPS"] if bps_cfg["aba"] else []))
            for c in (self.codigos_lista if bps_cfg["compras"] else []):
                bx = int(self.registros_baixados.get(c,0))
                ex = int(self.registros_esperados.get(c,0))
                pg = self.paginas_corrompidas.get(c,[])
                d  = abs(ex-bx)
                st = (f"ERRO_API — {_erros_api.resumo(c)}"
                      if c in getattr(self, "codigos_com_erro", ()) else
                      "OK" if d==0 else
                      f"OK (divergencia: {bx}/{ex})" if d<=2 else
                      f"Inconsistencia Grave ({bx}/{ex})")
                ws.append([c,ex,bx,", ".join(map(str,pg)),st]
                          + ([self.registros_bps.get(c, 0)] if bps_cfg["aba"] else []))
            pasta_rel = getattr(self, "_pasta_classes_destino", "").strip()
            cam_rel = (os.path.join(pasta_rel, "Relatorio_Integridade.xlsx")
                       if pasta_rel else "Relatorio_Integridade.xlsx")
            wb.save(cam_rel)
            self._log(f"📊 {cam_rel} gerado.", "info")
        except Exception as e:
            self._log(f"⚠️ Relatório não salvo: {e}", "warn")

        self.btn_start.configure(state="normal")
        self.btn_cancel.configure(state="disabled",
                                   fg_color="#E4E7EF", hover_color=C_BORDER,
                                   text_color=C_TEXT)
        self.btn_pause.configure(state="disabled", text="⏸  Pausar")
        self.btn_log.configure(state="normal")

        if not parts:
            messagebox.showinfo("Sem dados","Nenhum dado válido baixado.")
            return

        n_classes = len(self._writers_por_classe) if self._modo_por_classe else 0
        ext = os.path.splitext(parts[0])[1]

        resumo_linhas = [
            "Processo Concluido!",
            chr(8212)*40,
            f"Codigos Processados:     {len(self.codigos_lista)}",
            f"Registros Consolidados:  {self.total_baixados:,}",
        ]
        if bps_cfg["compras"]:
            resumo_linhas += [
                f"Paginas Reparadas:       {self.count_reparadas}",
                f"Paginas com Perda:       {self.count_corrigidas}",
            ]
        resumo_linhas.append(f"Codigos sem Registros:   {self.count_vazios}")
        if bps_cfg["compras"] and bps_cfg["aba"]:
            resumo_linhas.append(f"Registros do BPS:        {self.total_bps:,}")
        if n_classes >= 1:
            resumo_linhas.append(f"Arquivos por classe:     {n_classes}")
        messagebox.showinfo("Resumo", "\n".join(resumo_linhas))

        # Modo por classe: mesmo com uma única classe os arquivos já têm nome
        # próprio (classe_XXXX) e destino definido — não faz sentido pedir
        # "salvar como" para eles.
        if n_classes >= 1:
            pasta_dest = getattr(self, "_pasta_classes_destino", "").strip()
            nomes = "\n".join(os.path.basename(p) for p in parts)
            if pasta_dest:
                # Os writers já gravaram direto na pasta ao longo da execução;
                # o relatório de integridade também. Nada a copiar nem a perguntar.
                self._log(f"📁 {len(parts)} arquivo(s) em: {pasta_dest}", "info")
                messagebox.showinfo("Concluído",
                    f"{len(parts)} arquivo(s) salvos em:\n{pasta_dest}\n\n{nomes}")
            else:
                # Sem pasta definida: pede agora e copia
                pasta_dest = filedialog.askdirectory(
                    title=f"Escolha a pasta para salvar os {len(parts)} arquivo(s)")
                if pasta_dest:
                    for arq in parts:
                        shutil.copy(arq, os.path.join(pasta_dest, os.path.basename(arq)))
                    try:
                        shutil.copy("Relatorio_Integridade.xlsx",
                                    os.path.join(pasta_dest, "Relatorio_Integridade.xlsx"))
                    except Exception:
                        pass
                    self._log(f"📁 {len(parts)} arquivo(s) salvos em: {pasta_dest}", "info")
                    messagebox.showinfo("Concluído",
                        f"{len(parts)} arquivo(s) salvos em:\n{pasta_dest}\n\n{nomes}")
                else:
                    messagebox.showwarning("Atenção",
                        f"Nenhuma pasta escolhida. Arquivos na pasta do programa:\n{nomes}")
        else:
            # No CSV a aba BPS é um arquivo à parte: ele acompanha o principal
            extras = self.writer.arquivos_extras() if self.writer else []
            principais = [p for p in parts if p not in extras]
            ultimo = principais[-1] if principais else parts[-1]
            tipos  = [("Excel","*.xlsx")] if ext == ".xlsx" else [("CSV","*.csv")]
            dest   = filedialog.asksaveasfilename(
                        defaultextension=ext,
                        initialfile=os.path.basename(ultimo),
                        filetypes=tipos)
            if dest:
                if not dest.lower().endswith(ext): dest += ext
                shutil.copy(ultimo, dest)
                copiados = []
                for arq in extras:
                    if arq == ultimo: continue
                    alvo = os.path.join(os.path.dirname(dest), os.path.basename(arq))
                    shutil.copy(arq, alvo); copiados.append(os.path.basename(arq))
                messagebox.showinfo("Salvo",
                    f"Dados salvos em:\n{dest}"
                    + (f"\n\nDados do BPS: {', '.join(copiados)}" if copiados else "")
                    + "\n\nRelatorio de integridade na pasta do programa.")
            else:
                messagebox.showwarning("Atencao", f"Arquivo permanece em:\n{ultimo}")

    # ─────────────────────────────────────────────────────────────────────────
    # ABA 3 — CONSOLIDAÇÃO DW + DA (remoção de duplicatas)
    # A regra de negócio vive em consolidar_dw_da.py; aqui só há interface.
    # ─────────────────────────────────────────────────────────────────────────

    def _log_cons(self, msg: str, tag: str = ""):
        self.log_cons.configure(state="normal")
        self.log_cons.insert("end", msg + "\n", tag)
        self.log_cons.see("end")
        self.log_cons.configure(state="disabled")

    def _stat_cons(self, key, val):
        self._stats_cons[key].configure(text=str(val))

    @staticmethod
    def _fmt_br(n) -> str:
        """1234567 -> 1.234.567"""
        return f"{n:,}".replace(",", ".")

    # ── entradas ─────────────────────────────────────────────────────────────
    _ROTULO_ORIGEM = {"Detectar": "auto", "DW": "dw", "DA": "da"}

    def _cons_add(self, caminhos):
        origem = self._ROTULO_ORIGEM.get(self.var_cons_origem.get(), "auto")
        ja = {e["caminho"] for e in self._cons_entradas}
        novos = 0
        for c in caminhos:
            if c and c not in ja:
                self._cons_entradas.append({"caminho": c, "origem": origem})
                ja.add(c); novos += 1
        if novos:
            self._cons_atualizar_tree()

    def _cons_add_pasta(self):
        p = filedialog.askdirectory(
            title="Pasta com os CSVs/XLSX do DW e/ou do DA")
        if p:
            self._cons_add([p])

    def _cons_add_arquivos(self):
        arqs = filedialog.askopenfilenames(
            title="Arquivos do DW e/ou do DA",
            filetypes=[("CSV/Excel","*.csv *.xlsx *.xlsm"),("Todos","*.*")])
        if arqs:
            self._cons_add(list(arqs))

    def _cons_atualizar_tree(self):
        self.tree_cons.delete(*self.tree_cons.get_children())
        rotulo = {v: k for k, v in self._ROTULO_ORIGEM.items()}
        for i, e in enumerate(self._cons_entradas):
            self.tree_cons.insert("", "end", iid=str(i),
                                  values=(rotulo.get(e["origem"], "Detectar"),
                                          e["caminho"]))

    def _cons_remover(self):
        sel = self.tree_cons.selection()
        if not sel:
            messagebox.showinfo("Nada selecionado",
                                "Selecione na lista o que deseja remover.")
            return
        for i in sorted((int(s) for s in sel), reverse=True):
            del self._cons_entradas[i]
        self._cons_atualizar_tree()

    def _cons_limpar(self):
        self._cons_entradas = []
        self._cons_atualizar_tree()

    def _cons_escolher_saida(self):
        p = filedialog.askdirectory(title="Pasta de saída da consolidação")
        if p:
            self.var_cons_saida.set(p)

    def _cons_abrir_pasta(self):
        pasta = self._cons_ultima_saida or self.var_cons_saida.get().strip()
        if not pasta:
            return
        try:
            os.startfile(pasta)                     # Windows
        except Exception:
            messagebox.showinfo("Pasta de saída", pasta)

    def _cons_salvar_log(self):
        p = filedialog.asksaveasfilename(defaultextension=".txt",
                                         filetypes=[("Texto","*.txt")])
        if p:
            try:
                with open(p, "w", encoding="utf-8") as f:
                    f.write(self.log_cons.get("1.0", "end"))
                messagebox.showinfo("Salvo", f"Log salvo em:\n{p}")
            except Exception as e:
                messagebox.showerror("Erro", str(e))

    # ── execução ─────────────────────────────────────────────────────────────
    def _cons_iniciar(self):
        if self._cons_rodando:
            return
        if not self._cons_entradas:
            messagebox.showerror("Sem entradas",
                "Adicione ao menos uma pasta ou arquivo do DW/DA."); return

        saida = self.var_cons_saida.get().strip()
        if not saida:
            messagebox.showerror("Pasta de saída",
                "Escolha a pasta onde as planilhas serão gravadas."); return

        def _ano(var, nome):
            v = var.get().strip()
            if not v:
                return None, True
            if not (v.isdigit() and len(v) == 4):
                messagebox.showerror("Ano inválido",
                    f"{nome} deve ter 4 dígitos (ex.: 2021)."); return None, False
            return int(v), True

        ano_min, ok = _ano(self.var_cons_ano_min, "Ano mínimo")
        if not ok: return
        ano_max, ok = _ano(self.var_cons_ano_max, "Ano máximo")
        if not ok: return
        if ano_min and ano_max and ano_min > ano_max:
            messagebox.showerror("Filtro de ano",
                "O ano mínimo não pode ser maior que o ano máximo."); return

        # A saída dentro da entrada faz as planilhas geradas voltarem como
        # entrada numa segunda execução — serão ignoradas, mas custam leitura.
        saida_abs = os.path.abspath(saida)
        for e in self._cons_entradas:
            ent_abs = os.path.abspath(e["caminho"])
            if os.path.isdir(ent_abs) and \
               (saida_abs == ent_abs or saida_abs.startswith(ent_abs + os.sep)):
                if not messagebox.askyesno("Saída dentro da entrada",
                    "A pasta de saída está dentro de uma pasta de entrada.\n\n"
                    "Os arquivos gerados serão relidos (e ignorados) em uma "
                    "próxima execução.\n\nDeseja continuar mesmo assim?"):
                    return
                break

        # Tk não é thread-safe: tudo é lido aqui, na thread principal.
        params = dict(
            entradas=[e["caminho"] for e in self._cons_entradas if e["origem"] == "auto"],
            dw=[e["caminho"] for e in self._cons_entradas if e["origem"] == "dw"],
            da=[e["caminho"] for e in self._cons_entradas if e["origem"] == "da"],
            saida=saida,
            prefixo=self.var_cons_prefixo.get() or "DA-DW Classe ",
            sufixo=self.var_cons_sufixo.get().strip(),
            ano_min=ano_min, ano_max=ano_max,
            salvar_duplicatas=bool(self.var_cons_dup.get()),
            dedup_interno_da=bool(self.var_cons_dedup_interno.get()),
            processos=int(self.var_cons_processos.get() or 1),
        )

        self._cons_rodando  = True
        self._cons_cancelar = False
        self._cons_ultima_saida = saida
        self.log_cons.configure(state="normal")
        self.log_cons.delete("1.0", "end")
        self.log_cons.configure(state="disabled")
        for k in ("c_dw", "c_lidas", "c_dup", "c_mant"):
            self._stat_cons(k, "0")
        self.progress_cons.set(0)
        self.lbl_cons_pct.configure(text="0%")
        self.lbl_cons_status.configure(text="Status: Consolidando…")
        self.btn_cons_start.configure(state="disabled")
        self.btn_cons_cancel.configure(state="normal", fg_color=C_RED,
                                       hover_color="#992B1E", text_color=C_SURFACE)
        self.btn_cons_abrir.configure(state="disabled")
        self.btn_cons_log.configure(state="disabled")

        self._log_cons(f"📁 Saída: {saida}", "info")
        if ano_min or ano_max:
            self._log_cons(f"📅 Filtro de ano: "
                           f"{ano_min or '—'} a {ano_max or '—'}", "info")
        if params["processos"] > 1:
            self._log_cons(f"⚙ Gravação em {params['processos']} processos "
                           f"paralelos (uma classe por processo).", "info")
        if params["dedup_interno_da"]:
            self._log_cons("⚠ Dedup interno do DA ativo: repetições da mesma "
                           "chave dentro do próprio DA também serão removidas.",
                           "warn")
        self._log_cons("", "")

        threading.Thread(target=self._cons_thread, args=(params,),
                         daemon=True).start()

    def _cons_cancelar_click(self):
        if not self._cons_rodando:
            return
        self._cons_cancelar = True
        self.lbl_cons_status.configure(text="Status: Cancelando…")
        self._log_cons("\n🛑 Cancelamento solicitado — encerrando…", "warn")

    def _cons_thread(self, params):
        """Roda o motor fora da thread da UI; toda a volta é via self.after(0, …)."""
        try:
            res = consolidar(
                log=lambda m: self._ui(lambda: self._log_cons(m)),
                progresso=lambda f, r="": self._ui(
                    lambda: self._cons_progresso(f, r)),
                cancelado=lambda: self._cons_cancelar,
                **params)
            self._ui(lambda: self._cons_finalizar(res, None))
        except Exception as e:
            self._ui(lambda e=e: self._cons_finalizar(None, e))

    def _cons_progresso(self, fracao, rotulo=""):
        fracao = max(0.0, min(1.0, float(fracao)))
        self.progress_cons.set(fracao)
        self.lbl_cons_pct.configure(text=f"{int(fracao * 100)}%")
        if rotulo:
            self.lbl_cons_status.configure(text=f"Status: {rotulo}")

    def _cons_finalizar(self, res, erro):
        self._cons_rodando = False
        self.btn_cons_start.configure(state="normal")
        self.btn_cons_cancel.configure(state="disabled", fg_color="#E4E7EF",
                                       hover_color=C_BORDER, text_color=C_TEXT)
        self.btn_cons_log.configure(state="normal")

        if erro is not None:
            self.lbl_cons_status.configure(text="Status: Erro")
            self._log_cons(f"\n❌ Falha na consolidação: "
                           f"{type(erro).__name__}: {erro}", "err")
            messagebox.showerror("Erro na consolidação", str(erro))
            return

        if res.get("cancelado"):
            self.lbl_cons_status.configure(text="Status: Cancelado")
            self.progress_cons.set(0); self.lbl_cons_pct.configure(text="0%")
            return

        t = res.get("totais", {})
        self._stat_cons("c_dw",    self._fmt_br(t.get("dw", 0)))
        self._stat_cons("c_lidas", self._fmt_br(t.get("da_lidas", 0)))
        self._stat_cons("c_dup",   self._fmt_br(t.get("da_dup", 0)))
        self._stat_cons("c_mant",  self._fmt_br(t.get("da_mantidas", 0)))

        gerados = res.get("gerados", {})
        if not gerados:
            self.lbl_cons_status.configure(text="Status: Nada a consolidar")
            n_dw, n_da = res.get("arquivos_dw", 0), res.get("arquivos_da", 0)
            if not (n_dw or n_da):
                detalhe = ("Nenhum arquivo das entradas foi reconhecido como DW "
                           "ou DA.\nConfira os cabeçalhos ou classifique "
                           "manualmente em \"Origem\".")
            else:
                detalhe = (f"{n_dw} arquivo(s) do DW e {n_da} do DA foram lidos, "
                           "mas nenhuma linha válida sobrou.\nVeja o "
                           "linhas_em_quarentena.csv e o filtro de ano.")
            self._log_cons("\n⚠ " + detalhe.replace("\n", "\n   "), "warn")
            messagebox.showwarning("Nada a consolidar", detalhe)
            return

        self.progress_cons.set(1.0); self.lbl_cons_pct.configure(text="100%")
        self.lbl_cons_status.configure(text="Status: Concluído!")
        self.btn_cons_abrir.configure(state="normal")

        pct = (t.get("da_dup", 0) / t["da_lidas"] * 100) if t.get("da_lidas") else 0
        self._log_cons("\n" + "═" * 58, "info")
        self._log_cons(f"✅ {res.get('arquivos', len(gerados))} planilha(s) gerada(s) em: "
                       f"{res.get('pasta_saida','')}", "ok")
        self._log_cons(f"   Relatório: {res.get('relatorio','')}", "ok")
        self._log_cons(f"   DA removido (já estava no DW): "
                       f"{self._fmt_br(t.get('da_dup', 0))}  ({pct:.2f}% do DA)", "ok")
        if res.get("arquivo_duplicatas"):
            self._log_cons(f"   Auditoria das duplicatas: "
                           f"{res['arquivo_duplicatas']}", "ok")
        if res.get("ignorados"):
            self._log_cons(f"⚠ {len(res['ignorados'])} arquivo(s) ignorado(s) "
                           f"(cabeçalho não reconhecido).", "warn")
        if res.get("quarentena"):
            self._log_cons(f"⚠ {self._fmt_br(res['quarentena'])} linha(s) "
                           f"corrompida(s) fora das planilhas — conteúdo "
                           f"preservado em {res.get('arquivo_quarentena','')}.",
                           "warn")
        if res.get("catmat_prefixos"):
            self._log_cons("• CATMAT do DW com dígitos a mais à esquerda "
                           "(mantidos os 6 da direita): "
                           + " | ".join(f"{k} → {self._fmt_br(v)} linhas"
                                        for k, v in res["catmat_prefixos"].items()),
                           "info")
        if res.get("descricao_dw"):
            d = res["descricao_dw"]
            self._log_cons(f"• Descrição do DW: {self._fmt_br(d['do_da'])} linha(s) "
                           f"com o descritivo do DA | {self._fmt_br(d['mantidas'])} "
                           f"mantida(s) do DW ({self._fmt_br(d['catmats_sem_da'])} "
                           f"CATMAT(s) sem compra no DA)", "info")
        if res.get("celulas_higienizadas"):
            self._log_cons(f"⚠ {self._fmt_br(res['celulas_higienizadas'])} célula(s) "
                           f"tinham caracteres de controle e foram higienizadas "
                           f"(o texto foi mantido, os caracteres viraram espaço).",
                           "warn")
        if res.get("modalidades_desconhecidas"):
            self._log_cons("⚠ Códigos de modalidade não mapeados: "
                           + ", ".join(res["modalidades_desconhecidas"])
                           + "\n   Complete MAPA_MODALIDADE_DA em "
                             "consolidar_dw_da.py.", "warn")
        self._log_cons("═" * 58, "info")

        messagebox.showinfo("Concluído",
            f"Consolidação finalizada!\n\n"
            f"Planilhas geradas: {res.get('arquivos', len(gerados))}\n"
            f"Linhas do DW: {self._fmt_br(t.get('dw', 0))}\n"
            f"Duplicatas removidas do DA: {self._fmt_br(t.get('da_dup', 0))}\n"
            f"Linhas do DA mantidas: {self._fmt_br(t.get('da_mantidas', 0))}\n\n"
            f"Pasta: {res.get('pasta_saida','')}")


# =============================================================================
# =============================================================================
#  MOTOR DA ABA 3 — CONSOLIDAÇÃO DW + DA (remoção de duplicatas)
#
#  Antes vivia em consolidar_dw_da.py; foi trazido para cá para o programa
#  ser um arquivo único. Nada abaixo depende da interface: consolidar() é
#  chamada tanto pela aba 3 quanto pela linha de comando.
# =============================================================================
# =============================================================================


# =============================================================================
# CONFIGURAÇÃO  (ajuste aqui se precisar)
# =============================================================================

# Limite físico de linhas de uma aba do Excel (inclui o cabeçalho).
# Ao estourar, o script cria automaticamente "dw-6505 (2)", "dw-6505 (3)"...
LIMITE_LINHAS_PLANILHA = 1_048_576

# Formatos aplicados na planilha final (iguais aos do modelo enviado).
FORMATO_DATA = "DD/MM/YYYY"
FORMATO_QTDE = "#,##0"
FORMATO_MOEDA = 'R$ #,##0.00'

# True  -> CATMAT gravado como texto, preservando zeros à esquerda ("000183").
# False -> CATMAT gravado como número (183). Atenção: perde os zeros.
CATMAT_COMO_TEXTO = True

# Data do DA usada para preencher as colunas "Ano" e "dataCompra".
# Alterne para "dataResultado" se preferir alinhar com o "Ano Resultado Compra"
# do DW.
CAMPO_DATA_DA = "dataCompra"

# Formatar a data em dd/mm/aaaa custa ~35% de tempo a mais na gravação.
# Com False, a data sai como aaaa-mm-dd (mais rápido, ainda é data de verdade).
# (FORMATAR_DATA_BR saiu: no xlsxwriter o formato de data vem do
#  default_date_format da pasta de trabalho, não de cada célula.)

# Modalidades do DA (códigos do SIASG). Complete se aparecerem códigos novos —
# o script avisa no fim quais códigos não estavam mapeados.
MAPA_MODALIDADE_DA = {
    "1": "Convite",
    "2": "Tomada de Preços",
    "3": "Concorrência",
    "4": "Concorrência Internacional",
    "5": "Pregão",
    "6": "Dispensa de Licitação",
    "7": "Inexigibilidade de Licitação",
    "20": "Concurso",
    "22": "Regime Diferenciado de Contratações",
    "99": "Não informado",
}

# Sigla -> nome da unidade de fornecimento (mesmo mapa do Extrator de CATMATs).
_SIGLA_NOME = {
    "FR-AM": "Frasco-Ampola", "FR": "Frasco", "CAPS": "Cápsula",
    "COMPR": "Comprimido", "AM": "Ampola", "UN": "Unidade",
    "SER": "Seringa", "BIS": "Bisnaga", "BLIS": "Blister",
    "BOL": "Bolsa", "BOM": "Bombona", "CA": "Cartucho",
    "CI": "Curie", "CJ": "Conjunto", "DOSE(S)": "Dose(s)",
    "DOSES": "Dose(s)", "DRAG": "Drágea", "EMB": "Embalagem",
    "EMP": "Emplastro", "ENV": "Envelope", "FLAC": "Flaconete",
    "G": "Grama", "GL": "Galão", "GLOB": "Glóbulo",
    "KG": "Quilograma", "L": "Litro", "MCG": "Micrograma",
    "MCU": "Milicurie", "MG": "Miligrama",
    "MIL CTE": "Milheiro de Cartelas", "ML": "Mililitro",
    "PAST": "Pastilha", "POTE": "Pote", "RO": "Rolo",
    "SAC": "Sachê", "SUP": "Supositório", "TAB": "Tablete",
    "TBO": "Tubo", "TBTE": "Tubete", "UI": "Unid. Internacional",
}

_MESES_PT = {
    "jan": 1, "fev": 2, "mar": 3, "abr": 4, "mai": 5, "jun": 6,
    "jul": 7, "ago": 8, "set": 9, "out": 10, "nov": 11, "dez": 12,
}

# Em algumas exportações do DW as colunas de código vêm com o cabeçalho EM BRANCO,
# logo depois da coluna descritiva correspondente (ex.: "Descrição Material
# Servico" seguida de uma coluna sem nome que contém o CATMAT). Este mapa
# reconstrói esses nomes.
_DW_CODIGO_SEGUINTE = {
    "descricao material servico": "catmat",
    "classe material": "classe",
    "padrao desc material": "inc",
    "orgao sup unid partic": "uasgsup",
    "orgao unid partic": "uasguni",
    "orgao vinc uresp compra": "uasgvinc",
    "nome uresp compra": "uasg",
}

# Tamanhos aceitos para as chaves (fora disso a linha vai para a quarentena).
TAM_CHAVE_DW = 22
TAM_ID_COMPRA_DA = (16, 17)
TAM_ITEM_DA = (1, 5)

# -----------------------------------------------------------------------------
# Layout das abas de saída: (título, tipo, largura)
# -----------------------------------------------------------------------------
COLS_DW = [
    ("IdentififItemCompra", "texto", 24),
    ("Catmat", "texto", 12),
    ("Descrição", "texto", 60),
    ("U.F.", "texto", 18),
    ("Classe", "inteiro", 9),
    ("Órgão Sup Unid Partic", "texto", 32),
    ("Órgão Unid Partic", "texto", 32),
    ("Órgão Vinc UResp Compra", "texto", 32),
    ("Nome UResp Compra", "texto", 36),
    ("Municipio UResp Compra", "texto", 22),
    ("UF UResp Compra", "texto", 8),
    ("Esfera Unid Partic", "texto", 14),
    ("CPF/CNPJ Fornecedor", "texto", 18),
    ("Nome Fornecedor", "texto", 40),
    ("Fabric Material Compra", "texto", 24),
    ("Marca Material Compra", "texto", 24),
    ("Ano Resultado Compra", "inteiro", 10),
    ("Dia Resultado Compra", "data", 14),
    ("Modalidade Compra", "texto", 26),
    ("Qtde Comprada Item", "qtde", 12),
    ("Valor Preço Unit Item", "moeda", 14),
    ("Preço Total", "moeda", 14),
]

COLS_DA = [
    ("IdentififItemCompra", "texto", 24),
    ("Catmat", "texto", 12),
    ("Descrição", "texto", 60),
    ("U.F.", "texto", 18),
    ("Classe", "inteiro", 9),
    ("nomeUasg", "texto", 36),
    ("municipio", "texto", 22),
    ("estado", "texto", 8),
    ("nomeOrgao", "texto", 32),
    ("poder", "texto", 8),
    ("esfera", "texto", 8),
    ("niFornecedor", "texto", 18),
    ("nomeFornecedor", "texto", 40),
    ("marca", "texto", 24),
    ("Ano", "inteiro", 8),
    ("dataCompra", "data", 14),
    ("modalidade", "texto", 26),
    ("quantidade", "qtde", 12),
    ("precoUnitario", "moeda", 14),
    ("Preço Total", "moeda", 14),
]



# =============================================================================
# UTILIDADES
# =============================================================================

def norm(s) -> str:
    """Normaliza nome de coluna: sem acento, minúsculo, espaços colapsados."""
    if s is None:
        return ""
    s = str(s).replace("\ufeff", "").strip()
    s = unicodedata.normalize("NFKD", s)
    s = "".join(c for c in s if not unicodedata.combining(c))
    return " ".join(s.lower().split())


def txt(v) -> str:
    """Converte qualquer valor de célula em texto limpo, sem '.0' de float."""
    if v is None:
        return ""
    if isinstance(v, float):
        if v != v:                      # NaN
            return ""
        if v.is_integer():
            return str(int(v))
        return repr(v)
    if isinstance(v, (datetime, date)):
        return v.strftime("%Y-%m-%d")
    s = str(v).strip()
    return "" if s.lower() in ("nan", "none", "null") else s


def num(v):
    """Converte texto em número, aceitando '1.234,56', '1234.56', '165,00', '1 '."""
    if v is None or v == "":
        return None
    if isinstance(v, (int, float)) and not isinstance(v, bool):
        return float(v)
    s = str(v).strip().replace(" ", "").replace("\xa0", "")
    if not s or s.lower() in ("nan", "none", "null"):
        return None
    neg = s.startswith("-")
    s = s.lstrip("+-").replace("R$", "")
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")       # 1.234,56
    elif "," in s:
        s = s.replace(",", ".")                        # 1234,56
    elif s.count(".") == 1 and len(s.split(".")[1]) == 3 and len(s.split(".")[0]) <= 3:
        s = s.replace(".", "")                         # 1.234 -> milhar
    try:
        f = float(s)
    except ValueError:
        return None
    return -f if neg else f


def inteiro(v):
    f = num(v)
    return int(f) if f is not None else None


def data_dw(v):
    """'19 Out 1999' -> date(1999,10,19). Aceita também 19/10/1999 e ISO."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = txt(v)
    if not s:
        return None
    partes = s.replace("/", " ").replace("-", " ").split()
    if len(partes) == 3:
        d, m, a = partes
        mes = _MESES_PT.get(norm(m)[:3])
        if mes is None:
            try:
                mes = int(m)
            except ValueError:
                mes = None
        try:
            if mes and len(d) == 4:                    # formato ISO: aaaa mm dd
                return date(int(d), mes, int(a))
            if mes:
                return date(int(a), mes, int(d))
        except ValueError:
            return None
    return None


def data_iso(v):
    """'2025-07-04' ou '2025-07-04T12:00:00' -> date."""
    if isinstance(v, datetime):
        return v.date()
    if isinstance(v, date):
        return v
    s = txt(v)
    if len(s) >= 10:
        try:
            return date(int(s[0:4]), int(s[5:7]), int(s[8:10]))
        except ValueError:
            pass
    return None


def so_digitos(s: str) -> str:
    return "".join(c for c in s if c.isdigit())


# =============================================================================
# LEITURA DE ARQUIVOS (CSV e XLSX) EM STREAMING
# =============================================================================

try:
    csv.field_size_limit(sys.maxsize)
except OverflowError:
    csv.field_size_limit(2 ** 31 - 1)


def _cp1252_ou_byte(erro):
    """Os 5 bytes que o Windows-1252 não define viram o caractere Latin-1."""
    trecho = erro.object[erro.start:erro.end]
    return "".join(chr(b) for b in trecho), erro.end


codecs.register_error("cp1252_ou_byte", _cp1252_ou_byte)


def _detectar_encoding(caminho: Path) -> str:
    with open(caminho, "rb") as fb:
        amostra = fb.read(1_048_576)
    if amostra.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    for corte in (0, 1, 2, 3):                 # evita erro por caractere partido
        try:
            (amostra[: len(amostra) - corte]).decode("utf-8")
            return "utf-8"
        except UnicodeDecodeError:
            continue
    # Windows-1252, não Latin-1: os CSVs do DA e o Catálogo trazem “aspas
    # curvas” e travessões –, que em Latin-1 viram caracteres de controle.
    return "cp1252"


def _detectar_separador(linha: str) -> str:
    # O DW exporta com ";" e o Extrator de CATMATs usa "@" nos CSVs do DA.
    candidatos = {sep: linha.count(sep) for sep in ("@", ";", "\t", "|", ",")}
    sep = max(candidatos, key=candidatos.get)
    return sep if candidatos[sep] > 0 else ";"


def ler_tabela(caminho: Path):
    """Gerador: entrega o cabeçalho (lista) e, em seguida, cada linha (lista)."""
    if caminho.suffix.lower() in (".xlsx", ".xlsm"):
        wb = load_workbook(caminho, read_only=True, data_only=True)
        ws = wb["Dados CATMAT"] if "Dados CATMAT" in wb.sheetnames else wb[wb.sheetnames[0]]
        try:
            for linha in ws.iter_rows(values_only=True):
                yield list(linha)
        finally:
            wb.close()
    else:
        enc = _detectar_encoding(caminho)
        erros = "cp1252_ou_byte" if enc == "cp1252" else "replace"
        with open(caminho, "r", encoding=enc, newline="", errors=erros) as f:
            primeira = f.readline()
            sep = _detectar_separador(primeira)
            f.seek(0)
            for linha in csv.reader(f, delimiter=sep):
                yield linha


def indices(cabecalho) -> dict:
    """{nome_normalizado: posição}, reconstruindo cabeçalhos vazios do DW."""
    nomes = [norm(c) for c in cabecalho]
    for i in range(1, len(nomes)):
        if not nomes[i]:
            nomes[i] = _DW_CODIGO_SEGUINTE.get(nomes[i - 1], "")
    idx = {}
    for i, nome in enumerate(nomes):
        if nome and nome not in idx:
            idx[nome] = i
    return idx


def detectar_fonte(idx: dict) -> Optional[str]:
    if "identif item compra" in idx:
        return "DW"
    if "idcompra" in idx and "numeroitemcompra" in idx:
        return "DA"
    return None


# =============================================================================
# TRANSFORMAÇÃO DAS LINHAS
# =============================================================================

def _leitor(linha, idx):
    def g(nome, padrao=""):
        i = idx.get(nome)
        if i is None or i >= len(linha):
            return padrao
        return txt(linha[i])
    return g


TAM_CATMAT = 6

# Quantas vezes cada prefixo excedente foi removido do CATMAT do DW. Serve para
# o log: se aparecer algo diferente de "1000", é sinal de que a origem mudou e
# vale conferir antes de confiar no resultado.
_CATMAT_PREFIXOS = {}


def _catmat(valor: str):
    if CATMAT_COMO_TEXTO or not valor:
        return valor
    return inteiro(valor) if so_digitos(valor) == valor else valor


def _catmat_dw(valor: str):
    """No DW o CATMAT às vezes vem com dígitos a mais à esquerda (ex.: 1000610972
    para o CATMAT 610972). Fica só com os 6 da direita.

    Só mexe quando o valor é todo numérico e tem mais de 6 dígitos: qualquer
    coisa com letra, símbolo ou 6 dígitos ou menos passa intacta, para não
    inventar truncamento onde não há. O DA não passa por aqui — vem limpo da API.
    """
    if valor and len(valor) > TAM_CATMAT and so_digitos(valor) == valor:
        prefixo = valor[:-TAM_CATMAT]
        _CATMAT_PREFIXOS[prefixo] = _CATMAT_PREFIXOS.get(prefixo, 0) + 1
        valor = valor[-TAM_CATMAT:]
    return _catmat(valor)


def _chave_catmat(valor) -> str:
    """CATMAT como chave de comparação: só dígitos, sem zeros à esquerda
    ("000183", "183" e 183 são o mesmo item)."""
    s = txt(valor)
    return (s.lstrip("0") or "0") if s.isdigit() else ""


def _descricao_do_da(saida, descricoes: dict, troca: dict):
    """Põe na linha do DW o descritivo que o DA usa para o mesmo CATMAT.
    Sem o CATMAT no DA, a descrição do próprio DW é mantida."""
    cod = _chave_catmat(saida[1])
    nova = descricoes.get(cod)
    if nova is None:
        troca["mantidas"] += 1
        if cod:
            troca["sem_da"].add(cod)
        return
    saida[2] = nova
    troca["do_da"] += 1


def _troca_vazia() -> dict:
    return {"do_da": 0, "mantidas": 0, "sem_da": set()}


def transformar_dw(linha, idx):
    """Devolve (chave22, classe, linha_de_saida, erro) a partir de uma linha do DW."""
    g = _leitor(linha, idx)

    chave = g("identif item compra")
    classe = g("classe")
    erro = ""
    if not (chave.isdigit() and len(chave) == TAM_CHAVE_DW):
        erro = f"'Identif Item Compra' deveria ter {TAM_CHAVE_DW} dígitos: {chave!r}"
    elif classe and not classe.isdigit():
        erro = f"coluna 'Classe' não numérica ({classe!r}) - provável desalinhamento"

    qtde = num(g("qtde comprada item"))
    preco = num(g("valor preco unit item"))
    total = round(qtde * preco, 2) if (qtde is not None and preco is not None) else None
    saida = [
        chave,
        _catmat_dw(g("catmat")),
        g("descricao material servico"),
        g("unidade fornecimento"),
        inteiro(classe),
        g("orgao sup unid partic"),
        g("orgao unid partic"),
        g("orgao vinc uresp compra"),
        g("nome uresp compra"),
        g("municipio uresp compra"),
        g("uf uresp compra"),
        g("esfera unid partic"),
        g("cpf/cnpj fornecedor"),
        g("nome fornecedor"),
        g("fabric material compra"),
        g("marca material compra"),
        inteiro(g("ano resultado compra")),
        data_dw(g("dia resultado compra")),
        g("modalidade compra"),
        qtde,
        preco,
        total,
    ]
    return chave, classe, saida, erro


def _unidade_fornecimento_da(g):
    """Reproduz a coluna 'Unidade de Fornecimento' do Extrator de CATMATs."""
    pronta = g("unidade de fornecimento")
    if pronta:
        return pronta
    nome = g("nomeunidadefornecimento")
    sigla = g("siglaunidadefornecimento")
    cap = g("capacidadeunidadefornecimento")
    medida = g("siglaunidademedida")
    if not nome and sigla:
        nome = _SIGLA_NOME.get(sigla.upper(), sigla)
    cap_num = num(cap) or 0
    partes = [nome] if nome else []
    if cap and cap_num != 0:
        partes.append(cap)
        if medida:
            partes.append(medida)
    return " ".join(partes)


def transformar_da(linha, idx, modalidades_desconhecidas):
    """Devolve (chave22, classe, linha_de_saida, erro) a partir de uma linha do DA."""
    g = _leitor(linha, idx)

    id_compra = g("idcompra")
    n_item = g("numeroitemcompra")
    classe = g("codigoclasse")
    chave, erro = "", ""

    # Validação estrita: é ela que separa a linha boa da linha corrompida.
    # Linhas desalinhadas costumam trazer o idItemCompra no lugar do idCompra,
    # o que geraria uma chave errada (e uma duplicata não detectada).
    if not (id_compra.isdigit() and TAM_ID_COMPRA_DA[0] <= len(id_compra) <= TAM_ID_COMPRA_DA[1]):
        erro = (f"'idCompra' deveria ter {TAM_ID_COMPRA_DA[0]} ou "
                f"{TAM_ID_COMPRA_DA[1]} dígitos: {id_compra!r}")
    elif not (n_item.isdigit() and TAM_ITEM_DA[0] <= len(n_item) <= TAM_ITEM_DA[1]):
        erro = f"'numeroItemCompra' inválido: {n_item!r}"
    elif classe and not classe.isdigit():
        erro = f"'codigoClasse' não numérico ({classe!r}) - provável desalinhamento"
    else:
        chave = id_compra.zfill(17) + n_item.zfill(5)

    dt = data_iso(g(norm(CAMPO_DATA_DA))) or data_iso(g("datacompra")) or data_iso(g("dataresultado"))
    qtde = num(g("quantidade"))
    preco = num(g("precounitario"))
    total = round(qtde * preco, 2) if (qtde is not None and preco is not None) else None

    cod_mod = g("modalidade")
    modalidade = MAPA_MODALIDADE_DA.get(cod_mod, "")
    if not modalidade and cod_mod:
        if not erro:                       # linha corrompida não conta como código novo
            modalidades_desconhecidas.add(cod_mod)
        modalidade = f"Modalidade {cod_mod}"

    saida = [
        chave,
        _catmat(g("codigoitemcatalogo")),
        g("descricaoitem"),
        _unidade_fornecimento_da(g),
        inteiro(classe),
        g("nomeuasg"),
        g("municipio"),
        g("estado"),
        g("nomeorgao"),
        g("poder"),
        g("esfera"),
        g("nifornecedor"),
        g("nomefornecedor"),
        g("marca"),
        dt.year if dt else None,
        dt,
        modalidade,
        qtde,
        preco,
        total,
    ]
    return chave, classe, saida, erro


# =============================================================================
# ESCRITA DAS PLANILHAS
# =============================================================================

_FONTE_CAB = Font(bold=True, color="FFFFFF")
_FUNDO_CAB = PatternFill("solid", fgColor="1F4E79")
_ALINHA_CAB = Alignment(vertical="center", horizontal="center", wrap_text=True)


def _letra(i: int) -> str:
    letra = ""
    while i >= 0:
        letra = chr(ord("A") + i % 26) + letra
        i = i // 26 - 1
    return letra


# =============================================================================
# SAÍDA: uma pasta de trabalho por classe, com exatamente as abas DW e DA
# =============================================================================

_CAB_XLSX = {"bold": True, "bg_color": "1F4E79", "font_color": "FFFFFF",
             "align": "center", "valign": "vcenter", "text_wrap": True}
_NUM_XLSX = {"data": FORMATO_DATA, "qtde": FORMATO_QTDE, "moeda": FORMATO_MOEDA}


class SaidaParte:
    """Uma pasta de trabalho .xlsx com exatamente duas abas: DW e DA.

    Quando uma classe não cabe em um único arquivo (limite de 1.048.576 linhas
    por aba do Excel), o excedente vai para "... - Parte 2", "... - Parte 3" —
    nunca para uma terceira aba. O corte é feito em fronteira de ano, de modo
    que um ano nunca fica dividido entre dois arquivos (a menos que o ano
    sozinho estoure o limite, caso raro que é avisado no log).
    """

    celulas_higienizadas = 0            # zerado a cada consolidar()

    def __init__(self, caminho: Path, linhas_previstas: dict):
        self.caminho = caminho
        self.wb = xlsxwriter.Workbook(str(caminho), {
            "constant_memory": True,           # escreve linha a linha, sem inchar a RAM
            "default_date_format": FORMATO_DATA,
            "strings_to_numbers": False,       # "000183" continua texto
            "strings_to_urls": False,          # descrição longa não vira hyperlink
        })
        fmt_cab = self.wb.add_format(_CAB_XLSX)
        self.abas = {}
        for fonte, cols in (("dw", COLS_DW), ("da", COLS_DA)):
            ws = self.wb.add_worksheet(fonte.upper())
            for i, (_titulo, tipo_col, largura) in enumerate(cols):
                # No xlsxwriter o formato de coluna já vale para as células
                # escritas sem formato próprio — não é preciso criar um objeto
                # de célula por data, como o openpyxl exigia.
                if tipo_col in _NUM_XLSX:
                    ws.set_column(i, i, largura,
                                  self.wb.add_format({"num_format": _NUM_XLSX[tipo_col]}))
                else:
                    ws.set_column(i, i, largura)
            ws.freeze_panes(1, 0)
            ws.write_row(0, 0, [t for t, _x, _l in cols], fmt_cab)
            n = max(linhas_previstas.get(fonte, 0), 1)
            ws.autofilter(0, 0, n, len(cols) - 1)
            self.abas[fonte] = [ws, 0]

    def append(self, fonte: str, valores: list) -> bool:
        """Grava uma linha. Devolve False se a aba encheu (não coube)."""
        registro = self.abas[fonte]
        if registro[1] >= LIMITE_LINHAS_PLANILHA - 1:      # -1 por causa do cabeçalho
            return False
        # Caracteres de controle (\x00-\x1f) vêm no texto livre da origem. O
        # openpyxl levantava IllegalCharacterError e matava a aba; o xlsxwriter
        # é pior, escreve "MICRODONT_x0001_ STERIL" em silêncio. Por isso a
        # limpeza é preventiva. O search() antes do sub() custa ~1,6s por milhão
        # de linhas, contra ~110s que a escrita dessas linhas leva.
        for i, v in enumerate(valores):
            if type(v) is str and _CTRL_ILEGAIS.search(v):
                valores[i] = _CTRL_ILEGAIS.sub(" ", v)
                SaidaParte.celulas_higienizadas += 1
        registro[1] += 1
        registro[0].write_row(registro[1], 0, valores)
        return True

    def fechar(self):
        self.wb.close()

    def descartar(self):
        """Cancelamento: fecha os temporários e apaga o arquivo pela metade."""
        try:
            self.wb.close()
        except Exception:
            pass
        try:
            if self.caminho.exists():
                self.caminho.unlink()
        except Exception:
            pass


# =============================================================================
# PLANEJAMENTO DAS PARTES
# =============================================================================

def _rotulo_ano(ano):
    return str(ano) if ano else "sem ano"


def planejar_partes(contagens: dict, prefixo: str, sufixo: str,
                    log=print) -> tuple:
    """Decide quantos arquivos cada classe terá e qual ano vai em qual.

    contagens: {(classe, ano): {"dw": n, "da": n}}  -- ano é int ou None
    Devolve (plano, roteador):
        plano[classe]                 = [{"parte":1,"nome":...,"anos":[...],
                                          "dw":n,"da":n}, ...]
        roteador[(classe, ano, fonte)] = [(quantidade, indice_parte), ...]
    O roteador é uma lista porque um único ano grande demais precisa ser
    dividido entre partes; no caso normal ela tem um elemento só.
    """
    limite = LIMITE_LINHAS_PLANILHA - 1
    classes = sorted({c for c, _a in contagens})
    plano, roteador = {}, {}

    for classe in classes:
        anos = sorted({a for c, a in contagens if c == classe},
                      key=lambda a: (a is None, a or 0))   # "sem ano" por último
        partes = [{"parte": 1, "anos": [], "dw": 0, "da": 0}]
        for ano in anos:
            q = contagens[(classe, ano)]
            atual = partes[-1]
            # Cabe inteiro na parte corrente?
            if (atual["dw"] + q["dw"] <= limite and atual["da"] + q["da"] <= limite):
                if q["dw"] or q["da"]:
                    atual["anos"].append(ano)
                    atual["dw"] += q["dw"]; atual["da"] += q["da"]
                    for fonte in ("dw", "da"):
                        if q[fonte]:
                            roteador[(classe, ano, fonte)] = [(q[fonte], len(partes) - 1)]
                continue
            # Não cabe: abre parte nova, salvo se o ano sozinho estoura o limite
            if q["dw"] <= limite and q["da"] <= limite:
                if not atual["anos"]:                     # parte vazia, evita buraco
                    partes.pop()
                partes.append({"parte": len(partes) + 1, "anos": [ano],
                               "dw": q["dw"], "da": q["da"]})
                for fonte in ("dw", "da"):
                    if q[fonte]:
                        roteador[(classe, ano, fonte)] = [(q[fonte], len(partes) - 1)]
                continue
            # Caso raro: um único ano maior que o limite -> divide o ano
            log(f"  ! Classe {classe}, ano {_rotulo_ano(ano)}: "
                f"dw={q['dw']:,} da={q['da']:,} — o ano sozinho passa do limite "
                f"do Excel e precisou ser dividido entre arquivos."
                .replace(",", "."))
            idx_inicial = len(partes) - 1
            for fonte in ("dw", "da"):
                restante, trechos = q[fonte], []
                idx = idx_inicial      # cada fonte enche a partir da mesma parte,
                                       # senão o DA começaria depois do DW e
                                       # deixaria abas DA vazias nas primeiras
                while restante > 0:
                    livre = limite - partes[idx][fonte]
                    if livre <= 0:
                        partes.append({"parte": len(partes) + 1, "anos": [],
                                       "dw": 0, "da": 0})
                        idx = len(partes) - 1
                        continue
                    usa = min(livre, restante)
                    partes[idx][fonte] += usa
                    if ano not in partes[idx]["anos"]:
                        partes[idx]["anos"].append(ano)
                    trechos.append((usa, idx))
                    restante -= usa
                    idx += 1
                    if restante and idx >= len(partes):
                        partes.append({"parte": len(partes) + 1, "anos": [],
                                       "dw": 0, "da": 0})
                if trechos:
                    roteador[(classe, ano, fonte)] = trechos

        partes = [p for p in partes if p["dw"] or p["da"]]
        for i, p in enumerate(partes, start=1):
            p["parte"] = i
            p["nome"] = (f"{prefixo}{classe}{sufixo}"
                         + (f" - Parte {i}" if i > 1 else "") + ".xlsx")
        plano[classe] = partes

    return plano, roteador


class Roteador:
    """Diz em qual parte cada linha deve cair, na ordem em que ela aparece.

    A contagem da 1ª passada e a gravação da 2ª leem os mesmos arquivos na
    mesma ordem, então o n-ésimo registro de (classe, ano, fonte) é sempre o
    mesmo nas duas — é isso que torna o plano determinístico.
    """

    def __init__(self, roteador: dict, partes_abertas: dict):
        self._plano = roteador
        self._abertas = partes_abertas          # {(classe, indice): SaidaParte}
        self._vistas = {}

    def destino(self, classe, ano, fonte):
        chave = (classe, ano, fonte)
        trechos = self._plano.get(chave)
        if not trechos:
            return None
        n = self._vistas.get(chave, 0)
        self._vistas[chave] = n + 1
        for quantidade, idx in trechos:
            if n < quantidade:
                return self._abertas.get((classe, idx))
            n -= quantidade
        return self._abertas.get((classe, trechos[-1][1]))


# =============================================================================
# PROCESSAMENTO
# =============================================================================

class Cancelado(Exception):
    """Interrompe a consolidação a pedido de quem chamou (botão Cancelar da GUI)."""


# Só é consultado de tempos em tempos: a checagem por linha custaria caro em
# arquivos de milhões de registros.
_INTERVALO_CANCELAMENTO = 2000


def coletar_arquivos(entradas, padroes=("*.csv", "*.CSV", "*.xlsx", "*.xlsm")) -> list:
    achados = []
    for entrada in entradas:
        p = Path(entrada)
        if p.is_dir():
            for padrao in padroes:
                achados.extend(sorted(p.rglob(padrao)))
        elif p.exists():
            achados.append(p)
        else:
            achados.extend(sorted(Path(x) for x in glob.glob(entrada)))
    vistos, unicos = set(), []
    for a in achados:
        chave = str(a.resolve()).lower()
        if chave not in vistos and a.suffix.lower() in (".csv", ".xlsx", ".xlsm"):
            vistos.add(chave)
            unicos.append(a)
    return unicos


def _est(estatisticas, classe):
    return estatisticas.setdefault(classe, {
        "dw": 0, "da_lidas": 0, "da_dup": 0, "da_mantidas": 0,
        "dw_invalidas": 0, "da_invalidas": 0, "da_dup_interna": 0,
    })


class Quarentena:
    """Guarda as linhas corrompidas em CSV, sem perder nada do conteúdo original."""

    def __init__(self, caminho: Path):
        self.caminho = caminho
        self._arq = None
        self._csv = None
        self.total = 0

    def registrar(self, fonte, arquivo, n_linha, motivo, linha):
        if self._arq is None:
            self._arq = open(self.caminho, "w", encoding="utf-8-sig", newline="")
            self._csv = csv.writer(self._arq, delimiter=";")
            self._csv.writerow(["Fonte", "Arquivo", "Linha", "Motivo", "Conteúdo original ->"])
        self._csv.writerow([fonte, arquivo, n_linha, motivo] + [txt(v) for v in linha])
        self.total += 1

    def fechar(self):
        if self._arq:
            self._arq.close()


def processar_dw(arquivos, estatisticas, chaves_dw, ano_min, ano_max,
                 quarentena, modo="contar", contagens=None, roteador=None,
                 verbose=True, log=print, cancelado=None, progresso=None,
                 ao_gravar=None, descricoes=None, troca_desc=None):
    """Percorre os arquivos do DW.

    modo="contar": monta o índice de chaves, as estatísticas, a quarentena e a
        contagem por (classe, ano) que alimenta o plano de partes.
    modo="gravar": repete exatamente a mesma triagem, mas só escreve nas
        planilhas já abertas. Com `descricoes` ({catmat: texto do DA}), a
        descrição de cada linha é trocada pela do DA e contada em `troca_desc`.

    As duas passadas leem os mesmos arquivos na mesma ordem, então a n-ésima
    linha de cada (classe, ano) é a mesma nas duas. É isso que faz o plano
    fechar: sem contar antes, não há como saber em que ano cortar o arquivo,
    porque as linhas não chegam em ordem de ano.
    """
    contando = (modo == "contar")
    total = len(arquivos)
    for i_arq, arq in enumerate(arquivos, start=1):
        if progresso:
            progresso(i_arq - 1, total, f"DW · {arq.name}")
        if cancelado and cancelado():
            raise Cancelado()
        gen = ler_tabela(arq)
        try:
            idx = indices(next(gen))
        except StopIteration:
            continue
        if contando and "classe" not in idx:
            log(f"  ! {arq.name}: coluna 'Classe' não encontrada - as linhas irão "
                f"para SEM_CLASSE")
        gravadas = ignoradas = invalidas = 0
        for n_linha, linha in enumerate(gen, start=2):
            if cancelado and n_linha % _INTERVALO_CANCELAMENTO == 0 and cancelado():
                gen.close()
                raise Cancelado()
            if not linha or all(v in (None, "") for v in linha):
                continue
            chave, classe, saida, erro = transformar_dw(linha, idx)
            if erro:
                if contando:
                    invalidas += 1
                    _est(estatisticas,
                         classe if classe.isdigit() else "SEM_CLASSE")["dw_invalidas"] += 1
                    quarentena.registrar("DW", arq.name, n_linha, erro, linha)
                continue
            classe = classe or "SEM_CLASSE"
            if contando:
                chaves_dw.add(int(chave))      # int ocupa menos memória que str
            ano = saida[16]                    # Ano Resultado Compra
            if (ano_min and ano and ano < ano_min) or (ano_max and ano and ano > ano_max):
                ignoradas += 1
                continue
            if contando:
                contagens.setdefault((classe, ano), {"dw": 0, "da": 0})["dw"] += 1
                _est(estatisticas, classe)["dw"] += 1
            else:
                destino = roteador.destino(classe, ano, "dw")
                if destino is not None:
                    if descricoes is not None:
                        _descricao_do_da(saida, descricoes, troca_desc)
                    destino.append("dw", saida)
                    if ao_gravar:
                        ao_gravar()
            gravadas += 1
        if verbose and contando:
            extra = f" | {ignoradas} fora do filtro de ano" if ignoradas else ""
            extra += f" | {invalidas} em quarentena" if invalidas else ""
            log(f"  [DW] {arq.name}: {gravadas:,} linhas{extra}".replace(",", "."))
    if progresso:
        progresso(total, total, "DW concluído" if contando else "DW gravado")



def processar_da(arquivos, estatisticas, chaves_dw, ano_min, ano_max,
                 dedup_interno, escritor_dup, modalidades_desconhecidas,
                 quarentena, modo="contar", contagens=None, roteador=None,
                 verbose=True, log=print, cancelado=None, progresso=None,
                 ao_gravar=None, pular=None, pular_registro=None,
                 descricoes=None):
    """Percorre os arquivos do DA. Mesmos dois modos do processar_dw.

    Na contagem, `pular_registro` recebe o número das linhas descartadas como
    duplicata. Na gravação, `pular` traz essa mesma lista de volta e as linhas
    são puladas sem consultar o índice de chaves — que aí nem precisa existir.
    É o que permite gravar em outro processo sem replicar centenas de MB.

    Na contagem, `descricoes` recebe {catmat: (dataHoraAtualizacaoItem, texto)}
    de toda linha válida — duplicatas e anos fora do filtro inclusive, porque
    servem para dar ao DW o descritivo do DA. Vale o texto mais recente.
    """
    contando = (modo == "contar")
    vistas_da = set() if dedup_interno else None
    total = len(arquivos)
    for i_arq, arq in enumerate(arquivos, start=1):
        pular_aqui = (pular or {}).get(str(arq)) if pular is not None else None
        cursor = 0
        if progresso:
            progresso(i_arq - 1, total, f"DA · {arq.name}")
        if cancelado and cancelado():
            raise Cancelado()
        gen = ler_tabela(arq)
        try:
            idx = indices(next(gen))
        except StopIteration:
            continue
        i_data = idx.get("datahoraatualizacaoitem")
        lidas = dup = mantidas = ignoradas = invalidas = 0
        for n_linha, linha in enumerate(gen, start=2):
            if cancelado and n_linha % _INTERVALO_CANCELAMENTO == 0 and cancelado():
                gen.close()
                raise Cancelado()
            if not linha or all(v in (None, "") for v in linha):
                continue
            primeira = txt(linha[0]).lower()
            if primeira.startswith(("totalregistros", "totalpaginas", "idcompra")):
                continue                        # rodapé da API ou cabeçalho repetido
            lidas += 1
            chave, classe, saida, erro = transformar_da(linha, idx,
                                                        modalidades_desconhecidas)
            if erro:
                if contando:
                    invalidas += 1
                    _est(estatisticas,
                         classe if classe.isdigit() else "SEM_CLASSE")["da_invalidas"] += 1
                    quarentena.registrar("DA", arq.name, n_linha, erro, linha)
                continue
            classe = classe or "SEM_CLASSE"
            if contando:
                _est(estatisticas, classe)["da_lidas"] += 1
                if descricoes is not None and saida[2]:
                    cod = _chave_catmat(saida[1])
                    data = txt(linha[i_data]) if i_data is not None and i_data < len(linha) else ""
                    atual = descricoes.get(cod)
                    if cod and (atual is None or data > atual[0]):
                        descricoes[cod] = (data, saida[2])

            if contando:
                if int(chave) in chaves_dw:
                    dup += 1
                    _est(estatisticas, classe)["da_dup"] += 1
                    if escritor_dup:
                        escritor_dup.writerow([classe, chave, arq.name, saida[1],
                                               saida[15] or "", saida[12], saida[18]])
                    if pular_registro is not None:
                        pular_registro.setdefault(str(arq), array("l")).append(n_linha)
                    continue
                if vistas_da is not None:
                    if int(chave) in vistas_da:
                        _est(estatisticas, classe)["da_dup_interna"] += 1
                        if pular_registro is not None:
                            pular_registro.setdefault(str(arq), array("l")).append(n_linha)
                        continue
                    vistas_da.add(int(chave))
            elif pular_aqui is not None:
                if cursor < len(pular_aqui) and pular_aqui[cursor] == n_linha:
                    cursor += 1
                    dup += 1
                    continue

            ano = saida[14]                     # Ano, derivado de dataCompra
            if (ano_min and ano and ano < ano_min) or (ano_max and ano and ano > ano_max):
                ignoradas += 1
                continue
            if contando:
                contagens.setdefault((classe, ano), {"dw": 0, "da": 0})["da"] += 1
                _est(estatisticas, classe)["da_mantidas"] += 1
            else:
                destino = roteador.destino(classe, ano, "da")
                if destino is not None:
                    destino.append("da", saida)
                    if ao_gravar:
                        ao_gravar()
            mantidas += 1
        if verbose and contando:
            extra = f" | {ignoradas} fora do filtro de ano" if ignoradas else ""
            extra += f" | {invalidas} em quarentena" if invalidas else ""
            log(f"  [DA] {arq.name}: {lidas:,} lidas | {dup:,} duplicadas removidas | "
                f"{mantidas:,} mantidas{extra}".replace(",", "."))
    if progresso:
        progresso(total, total, "DA concluído" if contando else "DA gravado")


def gravar_relatorio(caminho: Path, estatisticas: dict, arquivos_gerados: dict):
    wb = Workbook()
    ws = wb.active
    ws.title = "Resumo"
    cabecalho = ["Classe", "Linhas DW", "DA lidas", "DA duplicadas (removidas)",
                 "DA mantidas", "% removido do DA", "DW em quarentena",
                 "DA em quarentena", "Duplicatas internas DA", "Arquivo gerado"]
    ws.append(cabecalho)
    for c in ws[1]:
        c.font, c.fill, c.alignment = _FONTE_CAB, _FUNDO_CAB, _ALINHA_CAB
    for classe in sorted(estatisticas):
        e = estatisticas[classe]
        pct = (e["da_dup"] / e["da_lidas"] * 100) if e["da_lidas"] else 0
        ws.append([classe, e["dw"], e["da_lidas"], e["da_dup"], e["da_mantidas"],
                   round(pct, 2), e["dw_invalidas"], e["da_invalidas"],
                   e["da_dup_interna"], arquivos_gerados.get(classe, "")])
    larguras = [14, 14, 14, 26, 14, 18, 18, 18, 22, 44]
    for i, w in enumerate(larguras):
        ws.column_dimensions[_letra(i)].width = w
    ws.freeze_panes = "A2"
    ws.auto_filter.ref = f"A1:{_letra(len(cabecalho) - 1)}{ws.max_row}"
    wb.save(caminho)


# =============================================================================
# GRAVAÇÃO EM PARALELO (um processo por grupo de classes)
# =============================================================================
#
# Só a 2ª passada é paralelizada, e é onde está o tempo: ler e transformar são
# ~8% do trabalho, gravar o .xlsx é ~92%. Cada processo abre e fecha as próprias
# pastas de trabalho, então não há estado compartilhado — o que cruza a fronteira
# entre processos é só o plano (pequeno) e as linhas do DA a pular.
#
# Os trabalhadores NÃO recebem o índice de chaves do DW: seriam centenas de MB
# replicados. Em vez disso a 1ª passada anota o número das linhas do DA que
# foram descartadas como duplicata, e o trabalhador simplesmente as pula. Isso
# ainda libera o índice antes da parte pesada do serviço.


def _dividir_classes(plano: dict, n_grupos: int) -> list:
    """Distribui as classes entre os processos equilibrando o total de linhas.

    Guloso, da maior para a menor: a classe mais pesada vai sempre para o grupo
    mais folgado. Sem isso, uma classe grande sozinha num grupo faria todos os
    outros processos terminarem cedo e ficarem esperando.
    """
    pesos = {c: sum(p["dw"] + p["da"] for p in partes)
             for c, partes in plano.items()}
    grupos = [{"classes": [], "peso": 0} for _ in range(max(1, n_grupos))]
    for classe in sorted(pesos, key=lambda c: -pesos[c]):
        g = min(grupos, key=lambda g: g["peso"])
        g["classes"].append(classe)
        g["peso"] += pesos[classe]
    return [g["classes"] for g in grupos if g["classes"]]


def _tarefa_gravar(tarefa: dict, fila, evento_cancelar):
    """Executado em processo separado: grava as partes das classes recebidas."""
    saidas = {}
    try:
        SaidaParte.celulas_higienizadas = 0
        dir_saida = Path(tarefa["saida"])
        for classe, partes in tarefa["plano"].items():
            for i, p in enumerate(partes):
                saidas[(classe, i)] = SaidaParte(dir_saida / p["nome"],
                                                 {"dw": p["dw"], "da": p["da"]})
        roteador = Roteador(tarefa["rotas"], saidas)
        arquivos_dw = [Path(a) for a in tarefa["arquivos_dw"]]
        arquivos_da = [Path(a) for a in tarefa["arquivos_da"]]

        escritas = [0]
        def _contar(n=1):
            escritas[0] += n
            if escritas[0] % 20000 == 0:
                fila.put(("prog", 20000))

        cancelado = evento_cancelar.is_set
        troca = _troca_vazia()
        processar_dw(arquivos_dw, {}, None, tarefa["ano_min"], tarefa["ano_max"],
                     None, modo="gravar", roteador=roteador, verbose=False,
                     cancelado=cancelado, ao_gravar=_contar,
                     descricoes=tarefa["descricoes_da"], troca_desc=troca)
        processar_da(arquivos_da, {}, None, tarefa["ano_min"], tarefa["ano_max"],
                     False, None, set(), None, modo="gravar", roteador=roteador,
                     pular=tarefa["pular_da"], verbose=False,
                     cancelado=cancelado, ao_gravar=_contar)

        for (classe, _i), parte in sorted(saidas.items()):
            parte.fechar()
            fila.put(("log", f"  ✓ {parte.caminho.name}"))
        fila.put(("prog", escritas[0] % 20000))
        fila.put(("ok", {"celulas": SaidaParte.celulas_higienizadas,
                         "classes": list(tarefa["plano"]),
                         "troca": troca}))
    except Cancelado:
        for parte in saidas.values():
            parte.descartar()
        fila.put(("cancelado", None))
    except Exception as e:
        for parte in saidas.values():
            parte.descartar()
        fila.put(("erro", f"{type(e).__name__}: {e}"))


def gravar_em_paralelo(plano, rotas, arquivos_dw, arquivos_da, dir_saida,
                       ano_min, ano_max, pular_da, n_processos, total_linhas,
                       log=print, progresso=None, cancelado=None,
                       descricoes=None) -> dict:
    """Dispara os processos e vai repassando log e progresso para a interface."""
    # spawn em todo lugar: é o único modo do Windows, e usá-lo também no Linux
    # evita que um bug só apareça na máquina do usuário.
    ctx = multiprocessing.get_context("spawn")
    fila, evento = ctx.Queue(), ctx.Event()
    grupos = _dividir_classes(plano, n_processos)

    processos = []
    for classes in grupos:
        tarefa = {
            "saida": str(dir_saida),
            "arquivos_dw": [str(a) for a in arquivos_dw],
            "arquivos_da": [str(a) for a in arquivos_da],
            "plano": {c: plano[c] for c in classes},
            "rotas": {k: v for k, v in rotas.items() if k[0] in set(classes)},
            "pular_da": pular_da,
            "descricoes_da": descricoes,
            "ano_min": ano_min, "ano_max": ano_max,
        }
        p = ctx.Process(target=_tarefa_gravar, args=(tarefa, fila, evento),
                        daemon=True)
        p.start()
        processos.append(p)
        log(f"  processo {p.pid}: classe(s) {', '.join(classes)}")

    resumo = {"celulas": 0, "erro": None, "cancelado": False, "troca": _troca_vazia()}
    escritas, pendentes = 0, len(processos)
    while pendentes:
        if cancelado and cancelado() and not evento.is_set():
            evento.set()
        try:
            tipo, dado = fila.get(timeout=0.2)
        except queue.Empty:
            # Um processo pode morrer sem conseguir avisar (falta de memória,
            # por exemplo). Sem esta checagem o laço esperaria para sempre.
            vivos = sum(1 for p in processos if p.is_alive())
            if vivos == 0 and fila.empty():
                if pendentes:
                    resumo["erro"] = resumo["erro"] or (
                        "um processo de gravação terminou sem responder")
                break
            continue
        if tipo == "prog":
            escritas += dado
            if progresso and total_linhas:
                progresso(min(escritas / total_linhas, 1.0),
                          f"Gravando ({escritas:,} linhas)".replace(",", "."))
        elif tipo == "log":
            log(dado)
        elif tipo == "ok":
            resumo["celulas"] += dado["celulas"]
            resumo["troca"]["do_da"] += dado["troca"]["do_da"]
            resumo["troca"]["mantidas"] += dado["troca"]["mantidas"]
            resumo["troca"]["sem_da"] |= dado["troca"]["sem_da"]
            pendentes -= 1
        elif tipo == "cancelado":
            resumo["cancelado"] = True
            pendentes -= 1
        elif tipo == "erro":
            resumo["erro"] = dado
            evento.set()
            pendentes -= 1

    for p in processos:
        p.join(timeout=60)
        if p.is_alive():
            p.terminate()
    return resumo


# =============================================================================
# MOTOR REUTILIZÁVEL
# =============================================================================

def consolidar(entradas=(), dw=(), da=(), saida=".",
               prefixo="DA-DW Classe ", sufixo="",
               ano_min=None, ano_max=None,
               salvar_duplicatas=False, dedup_interno_da=False, processos=1,
               descricao_do_da=True,
               log=print, progresso=None, cancelado=None) -> dict:
    """Consolida DW + DA e devolve um resumo do que foi feito.

    Com descricao_do_da=True, a descrição de cada linha do DW é trocada pelo
    descritivo que o DA usa para o mesmo CATMAT (o DA é lido depois do DW na
    1ª passada, então o mapa já está completo quando a 2ª passada grava o DW).

    São duas passadas pelos arquivos de entrada. A primeira só conta (e monta
    o índice de chaves, a quarentena e a auditoria de duplicatas); a segunda
    grava. A contagem é necessária porque cada classe vira UMA pasta de
    trabalho com exatamente as abas DW e DA, e o excedente vai para "Parte 2",
    "Parte 3" — cortando em fronteira de ano. Como as linhas não chegam em
    ordem de ano, não há como decidir o corte sem contar antes. A releitura
    custa pouco: ler e transformar é ~8% do tempo, gravar o xlsx é ~92%.

    Os três callbacks existem para a interface gráfica:
        log(msg)                   -> uma linha de texto para o usuário
        progresso(fracao, rotulo)  -> fracao entre 0.0 e 1.0
        cancelado() -> bool        -> True interrompe (levanta Cancelado)
    """
    dir_saida = Path(saida)
    dir_saida.mkdir(parents=True, exist_ok=True)

    arquivos_dw = coletar_arquivos(dw)
    arquivos_da = coletar_arquivos(da)
    indefinidos = [a for a in coletar_arquivos(entradas)
                   if a not in arquivos_dw and a not in arquivos_da]

    resultado = {
        "cancelado": False, "arquivos_dw": 0, "arquivos_da": 0, "ignorados": [],
        "estatisticas": {}, "gerados": {}, "pasta_saida": str(dir_saida),
        "totais": {"dw": 0, "da_lidas": 0, "da_dup": 0, "da_mantidas": 0},
        "quarentena": 0, "arquivo_quarentena": None, "arquivo_duplicatas": None,
        "celulas_higienizadas": 0, "arquivos": 0, "partes": {},
        "catmat_prefixos": {}, "descricao_dw": None,
        "modalidades_desconhecidas": [], "relatorio": None,
    }

    if not (arquivos_dw or arquivos_da or indefinidos):
        log("Nenhum arquivo .csv/.xlsx encontrado nas entradas informadas.")
        return resultado

    def _fracao(base, peso):
        def _cb(feito, total, rotulo=""):
            if progresso:
                progresso(base + peso * (feito / total if total else 1), rotulo)
        return _cb

    saidas = {}                       # {(classe, indice_parte): SaidaParte}
    estatisticas, chaves_dw, contagens = {}, set(), {}
    pular_da = {}                     # {arquivo: linhas do DA a descartar}
    descricoes_da = {} if descricao_do_da else None   # {catmat: (data, texto)}
    mapa_desc = None                  # {catmat: texto}, montado após a 1ª passada
    troca_desc = _troca_vazia()
    modalidades_desconhecidas = set()
    quarentena = Quarentena(dir_saida / "linhas_em_quarentena.csv")
    SaidaParte.celulas_higienizadas = 0
    _CATMAT_PREFIXOS.clear()
    prefixos_catmat = {}
    arq_dup = escritor_dup = None

    try:
        log("Identificando a origem de cada arquivo...")
        ignorados = []
        for arq in indefinidos:
            if cancelado and cancelado():
                raise Cancelado()
            gen = ler_tabela(arq)
            try:
                fonte = detectar_fonte(indices(next(gen)))
            except StopIteration:
                fonte = None
            gen.close()
            if fonte == "DW":
                arquivos_dw.append(arq)
            elif fonte == "DA":
                arquivos_da.append(arq)
            else:
                ignorados.append(arq)
        for arq in ignorados:
            log(f"  ! ignorado (cabeçalho não reconhecido): {arq.name}")
        log(f"  {len(arquivos_dw)} arquivo(s) do DW | "
            f"{len(arquivos_da)} arquivo(s) do DA")
        resultado["arquivos_dw"] = len(arquivos_dw)
        resultado["arquivos_da"] = len(arquivos_da)
        resultado["ignorados"] = [a.name for a in ignorados]

        # ── 1ª passada: contar ───────────────────────────────────────────────
        log("\n[1/4] Lendo o DW e montando o índice de chaves...")
        processar_dw(arquivos_dw, estatisticas, chaves_dw, ano_min, ano_max,
                     quarentena, modo="contar", contagens=contagens, log=log,
                     cancelado=cancelado, progresso=_fracao(0.00, 0.05))
        log(f"  -> {len(chaves_dw):,} chaves únicas no DW".replace(",", "."))
        prefixos_catmat = dict(_CATMAT_PREFIXOS)
        if prefixos_catmat:
            detalhe = " | ".join(f"{pre} ({n:,} linhas)".replace(",", ".")
                                 for pre, n in sorted(prefixos_catmat.items(),
                                                      key=lambda kv: -kv[1]))
            log(f"  CATMAT do DW com dígitos a mais à esquerda, mantidos os "
                f"{TAM_CATMAT} da direita: {detalhe}")

        if salvar_duplicatas:
            arq_dup = open(dir_saida / "duplicatas_removidas.csv", "w",
                           encoding="utf-8-sig", newline="")
            escritor_dup = csv.writer(arq_dup, delimiter=";")
            escritor_dup.writerow(["Classe", "IdentififItemCompra", "Arquivo origem",
                                   "Catmat", "dataCompra", "nomeFornecedor",
                                   "precoUnitario"])

        log("\n[2/4] Lendo o DA e removendo as duplicatas...")
        processar_da(arquivos_da, estatisticas, chaves_dw, ano_min, ano_max,
                     dedup_interno_da, escritor_dup, modalidades_desconhecidas,
                     quarentena, modo="contar", contagens=contagens, log=log,
                     pular_registro=pular_da, descricoes=descricoes_da,
                     cancelado=cancelado, progresso=_fracao(0.05, 0.05))
        if arq_dup:
            arq_dup.close(); arq_dup = None
        quarentena.fechar()
        if descricoes_da is not None:
            mapa_desc = {cod: texto for cod, (_data, texto) in descricoes_da.items()}
            descricoes_da.clear()
            log(f"  -> {len(mapa_desc):,} CATMATs com descritivo no DA "
                f"(usado também na aba DW)".replace(",", "."))

        if not contagens:
            log("\n⚠ Nenhuma linha válida sobrou para gravar.")
            resultado["estatisticas"] = estatisticas
            resultado["quarentena"] = quarentena.total
            resultado["arquivo_quarentena"] = (quarentena.caminho.name
                                               if quarentena.total else None)
            return resultado

        # ── Plano das partes ─────────────────────────────────────────────────
        log("\n[3/4] Planejando os arquivos...")
        plano, rotas = planejar_partes(contagens, prefixo, sufixo, log=log)
        n_arquivos = sum(len(p) for p in plano.values())
        for classe, partes in plano.items():
            if len(partes) > 1:
                log(f"  Classe {classe}: {len(partes)} arquivos "
                    + " | ".join(f"Parte {p['parte']}: "
                                 f"{_rotulo_ano(p['anos'][0])}–{_rotulo_ano(p['anos'][-1])}"
                                 for p in partes))
        log(f"  {n_arquivos} arquivo(s) a gravar, "
            f"{len(plano)} classe(s).")

        # O índice de chaves já cumpriu seu papel: as duplicatas viraram lista
        # de linhas a pular. Liberá-lo aqui devolve centenas de MB justamente
        # antes da fase pesada — e é o que permite gravar em outros processos.
        chaves_dw.clear()

        total_linhas = sum(p["dw"] + p["da"]
                           for partes in plano.values() for p in partes)
        n_proc = min(max(1, int(processos or 1)), len(plano))

        log("\n[4/4] Gravando as planilhas...")
        usar_sequencial = n_proc <= 1
        if n_proc > 1:
            log(f"  {n_proc} processos em paralelo "
                f"({len(plano)} classe(s) a distribuir)")
            try:
                resumo = gravar_em_paralelo(
                    plano, rotas, arquivos_dw, arquivos_da, dir_saida,
                    ano_min, ano_max, pular_da, n_proc, total_linhas,
                    log=log, progresso=(lambda f, r="": progresso(0.10 + 0.75 * f, r))
                    if progresso else None, cancelado=cancelado,
                    descricoes=mapa_desc)
            except Exception as e:                 # não conseguiu nem iniciar
                resumo = {"celulas": 0, "cancelado": False,
                          "erro": f"{type(e).__name__}: {e}", "troca": _troca_vazia()}
            if resumo["cancelado"]:
                raise Cancelado()
            if resumo["erro"]:
                # A 1ª passada pode ter levado dezenas de minutos; jogá-la fora
                # por causa do multiprocessing seria cruel. Refaz sequencial.
                log(f"  ⚠ A gravação em paralelo falhou ({resumo['erro']}).")
                log("    Refazendo em um processo só — vai demorar mais, mas o "
                    "resultado é o mesmo.")
                for partes in plano.values():
                    for pt in partes:
                        alvo = dir_saida / pt["nome"]
                        if alvo.exists():
                            try:
                                alvo.unlink()
                            except OSError:
                                pass
                usar_sequencial = True
            else:
                SaidaParte.celulas_higienizadas += resumo["celulas"]
                troca_desc = resumo["troca"]
        if usar_sequencial:
            for classe, partes in plano.items():
                for i, pt in enumerate(partes):
                    saidas[(classe, i)] = SaidaParte(dir_saida / pt["nome"],
                                                     {"dw": pt["dw"], "da": pt["da"]})
            roteador = Roteador(rotas, saidas)
            processar_dw(arquivos_dw, estatisticas, None, ano_min, ano_max,
                         None, modo="gravar", roteador=roteador, log=log,
                         verbose=False, cancelado=cancelado,
                         progresso=_fracao(0.10, 0.45),
                         descricoes=mapa_desc, troca_desc=troca_desc)
            processar_da(arquivos_da, estatisticas, None, ano_min, ano_max,
                         False, None, modalidades_desconhecidas,
                         None, modo="gravar", roteador=roteador, log=log,
                         verbose=False, pular=pular_da, cancelado=cancelado,
                         progresso=_fracao(0.55, 0.30))
    except Cancelado:
        resultado["cancelado"] = True
        for parte in saidas.values():
            parte.descartar()
        saidas.clear()
        if arq_dup:
            arq_dup.close()
        quarentena.fechar()
        log("\n🛑 Consolidação cancelada — nenhuma planilha foi gravada.")
        return resultado

    # ── Fechamento (é aqui que o .xlsx é de fato montado no disco) ───────────
    # No caminho paralelo cada processo já fechou as suas; aqui `saidas` está
    # vazio e só o plano descreve o que foi gravado.
    total_partes = len(saidas)
    for i, (_chave, parte) in enumerate(sorted(saidas.items()), start=1):
        parte.fechar()
        if progresso:
            progresso(0.85 + 0.14 * (i / total_partes),
                      f"Fechando {parte.caminho.name}")
    gerados = {classe: [p["nome"] for p in partes]
               for classe, partes in plano.items()}
    for classe, partes in plano.items():
        for p in partes:
            log(f"  {p['nome']}: dw={p['dw']:,} | da={p['da']:,} "
                f"| anos {_rotulo_ano(p['anos'][0])}–{_rotulo_ano(p['anos'][-1])}"
                .replace(",", "."))
    descricao_dw = None
    if mapa_desc is not None:
        descricao_dw = {"do_da": troca_desc["do_da"], "mantidas": troca_desc["mantidas"],
                        "catmats_sem_da": len(troca_desc["sem_da"])}
        log(f"  Descrição do DW: {descricao_dw['do_da']:,} linha(s) com o descritivo "
            f"do DA | {descricao_dw['mantidas']:,} mantida(s) do DW "
            f"({descricao_dw['catmats_sem_da']:,} CATMAT(s) sem compra no DA)"
            .replace(",", "."))

    nome_relatorio = "Relatorio_Consolidacao.xlsx"
    gravar_relatorio(dir_saida / nome_relatorio, estatisticas,
                     {c: " | ".join(n) for c, n in gerados.items()})

    resultado.update({
        "estatisticas": estatisticas,
        "gerados": {c: " | ".join(n) for c, n in gerados.items()},
        "partes": {c: [p["nome"] for p in ps] for c, ps in plano.items()},
        "arquivos": sum(len(v) for v in gerados.values()),
        "relatorio": nome_relatorio,
        "totais": {
            "dw":          sum(e["dw"] for e in estatisticas.values()),
            "da_lidas":    sum(e["da_lidas"] for e in estatisticas.values()),
            "da_dup":      sum(e["da_dup"] for e in estatisticas.values()),
            "da_mantidas": sum(e["da_mantidas"] for e in estatisticas.values()),
        },
        "quarentena": quarentena.total,
        "arquivo_quarentena": quarentena.caminho.name if quarentena.total else None,
        "arquivo_duplicatas": "duplicatas_removidas.csv" if salvar_duplicatas else None,
        "celulas_higienizadas": SaidaParte.celulas_higienizadas,
        "catmat_prefixos": prefixos_catmat,
        "descricao_dw": descricao_dw,
        "modalidades_desconhecidas": sorted(modalidades_desconhecidas),
    })
    if progresso:
        progresso(1.0, "Concluído")
    return resultado


# =============================================================================
# MAIN
# =============================================================================

def _cli_consolidacao():
    ap = argparse.ArgumentParser(
        description="Consolida DW (SIASG) + DA (Compras.gov) por classe, "
                    "removendo do DA os registros já existentes no DW.")
    ap.add_argument("-e", "--entrada", action="append", default=[],
                    help="pasta (ou arquivo/curinga) com os CSVs do DW e do DA. "
                         "Pode repetir. A origem é detectada pelo cabeçalho.")
    ap.add_argument("--dw", action="append", default=[], help="entradas que são do DW")
    ap.add_argument("--da", action="append", default=[], help="entradas que são do DA")
    ap.add_argument("-s", "--saida", required=True, help="pasta de saída")
    ap.add_argument("--prefixo", default="bps_dw_da__Classe_",
                    help="prefixo do nome dos arquivos gerados")
    ap.add_argument("--sufixo", default="", help="sufixo do nome (ex.: _2020_a_2026)")
    ap.add_argument("--ano-min", type=int, help="descarta registros anteriores a este ano")
    ap.add_argument("--ano-max", type=int, help="descarta registros posteriores a este ano")
    ap.add_argument("--salvar-duplicatas", action="store_true",
                    help="gera CSV de auditoria com as linhas do DA removidas")
    ap.add_argument("--dedup-interno-da", action="store_true",
                    help="também remove repetições da mesma chave dentro do próprio DA "
                         "(atenção: costumam ser registros distintos, de fornecedores "
                         "e preços diferentes)")
    ap.add_argument("--processos", type=int, default=1,
                    help="processos paralelos na gravação (1 = sequencial)")
    args = ap.parse_args()

    r = consolidar(entradas=args.entrada, dw=args.dw, da=args.da,
                   processos=args.processos,
                   saida=args.saida, prefixo=args.prefixo, sufixo=args.sufixo,
                   ano_min=args.ano_min, ano_max=args.ano_max,
                   salvar_duplicatas=args.salvar_duplicatas,
                   dedup_interno_da=args.dedup_interno_da)

    if not r["gerados"] and not r["estatisticas"]:
        return 1

    t = r["totais"]
    print("\n" + "=" * 62)
    print(f"DW gravado ..................... {t['dw']:,}".replace(",", "."))
    print(f"DA lido (linhas válidas) ....... {t['da_lidas']:,}".replace(",", "."))
    print(f"DA removido (já estava no DW) .. {t['da_dup']:,}".replace(",", "."))
    print(f"DA mantido ..................... {t['da_mantidas']:,}".replace(",", "."))
    if r["descricao_dw"]:
        d = r["descricao_dw"]
        print(f"DW com descritivo do DA ........ {d['do_da']:,}".replace(",", "."))
        print(f"DW com descrição própria ....... {d['mantidas']:,} "
              f"({d['catmats_sem_da']:,} CATMATs sem compra no DA)".replace(",", "."))
    print(f"Relatório ...................... {r['relatorio']}")
    if r["quarentena"]:
        print(f"\n! {r['quarentena']:,} linha(s) corrompida(s) não entraram nas planilhas."
              .replace(",", "."))
        print(f"  Conteúdo preservado em: {r['arquivo_quarentena']}")
    if r["catmat_prefixos"]:
        print("\n! CATMAT do DW com dígitos a mais à esquerda: "
              + " | ".join(f"{k} ({v:,})".replace(",", ".")
                           for k, v in r["catmat_prefixos"].items()))
    if r["celulas_higienizadas"]:
        print(f"\n! {r['celulas_higienizadas']:,} célula(s) com caracteres de controle "
              f"foram higienizadas.".replace(",", "."))
    if r["modalidades_desconhecidas"]:
        print(f"\n! Códigos de modalidade não mapeados: "
              f"{', '.join(r['modalidades_desconhecidas'])}"
              f"\n  Complete o dicionário MAPA_MODALIDADE_DA neste arquivo.")
    print("=" * 62)
    return 0


# =============================================================================
if __name__ == "__main__":
    # PRECISA ser a primeira coisa: no Windows os processos filhos reexecutam o
    # programa, e sem isto um .exe congelado abriria uma janela nova a cada
    # processo, sem parar.
    multiprocessing.freeze_support()
    # Com argumentos, roda a consolidação em linha de comando; sem eles,
    # abre a interface. Ex.: python ExtratorCatmat.py -e ENTRADA -s SAIDA
    if len(sys.argv) > 1:
        sys.exit(_cli_consolidacao())
    app = App()
    app.mainloop()
