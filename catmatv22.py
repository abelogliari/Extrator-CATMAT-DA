import re
import requests
import pandas as pd
from io import StringIO
from typing import Tuple, Dict, List, Optional
import os
import FreeSimpleGUI as sg
import time
from openpyxl import Workbook
import shutil
import json
import threading
import math

pausar_extracao = threading.Event()
pausar_busca_catmat = threading.Event()


requests.packages.urllib3.disable_warnings(requests.packages.urllib3.exceptions.InsecureRequestWarning)

# --- SUBSTITUA A CLASSE ExcelChunkWriter ANTIGA POR ESTAS DUAS ---

class ExcelChunkWriter:
    def __init__(self, base_filename: str, sheet_name: str = "Dados CATMAT", max_rows_per_file: int = 1_000_000):
        self.base_filename = base_filename
        self.sheet_name = sheet_name
        self.max_rows = max_rows_per_file
        self.part = 1
        self.header: List[str] = []
        self.current_row_count = 0
        self.files_saved = []
        self._new_workbook()

    def _filepath(self) -> str:
        base, ext = os.path.splitext(self.base_filename)
        # Garante a extensão .xlsx
        if not ext or ext.lower() != '.xlsx': ext = '.xlsx'
        return f"{base}_part{self.part}{ext}"

    def _new_workbook(self):
        self.wb = Workbook()
        self.ws = self.wb.active
        self.ws.title = self.sheet_name
        self.header_written = False
        self.current_row_count = 0

    def _ensure_header(self, columns: List[str]):
        if not self.header: self.header = list(columns)
        if not self.header_written:
            self.ws.append(self.header)
            self.header_written = True

    def _rollover_if_needed(self, rows_to_write: int = 1):
        if self.current_row_count + rows_to_write > self.max_rows:
            path = self._filepath()
            self.wb.save(path)
            self.files_saved.append(path)
            self.part += 1
            self._new_workbook()
            if self.header: 
                self.ws.append(self.header)
                self.header_written = True

    def write_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty: return
        self._ensure_header(list(df.columns))
        
        # Alinha colunas
        for col in self.header:
            if col not in df.columns: df[col] = pd.NA
        df = df[self.header]
        
        for _, row in df.iterrows():
            self._rollover_if_needed(1)
            self.ws.append([None if pd.isna(value) else value for value in row])
            self.current_row_count += 1

    def finalize(self) -> List[str]:
        if self.header_written and self.current_row_count > 0:
            path = self._filepath()
            self.wb.save(path)
            if path not in self.files_saved: self.files_saved.append(path)
        return self.files_saved

class CSVChunkWriter:
    def __init__(self, base_filename: str, sep: str = ';', encoding: str = 'utf-8-sig', max_rows_per_file: int = 1_000_000):
        self.base_filename = base_filename
        self.sep = sep
        self.encoding = encoding
        self.max_rows = max_rows_per_file
        self.part = 1
        self.current_row_count = 0
        self.files_saved = []
        self.header_written_current_file = False

    def _filepath(self) -> str:
        base, ext = os.path.splitext(self.base_filename)
        # Garante a extensão .csv
        if not ext or ext.lower() != '.csv': ext = '.csv'
        return f"{base}_part{self.part}{ext}"

    def write_dataframe(self, df: pd.DataFrame):
        if df is None or df.empty: return
        
        rows_in_df = len(df)
        
        # Se adicionar esse DF estourar o limite, salvamos e vamos pro próximo (lógica simplificada para CSV em bloco)
        if self.current_row_count + rows_in_df > self.max_rows:
            self.part += 1
            self.current_row_count = 0
            self.header_written_current_file = False
        
        path = self._filepath()
        mode = 'a' if self.header_written_current_file else 'w'
        header = not self.header_written_current_file
        
        df.to_csv(path, sep=self.sep, index=False, mode=mode, header=header, encoding=self.encoding)
        
        self.header_written_current_file = True
        self.current_row_count += rows_in_df
        
        if path not in self.files_saved:
            self.files_saved.append(path)

    def finalize(self) -> List[str]:
        return self.files_saved

def parse_csv_text(csv_text: str) -> pd.DataFrame:
    lines = [ln for ln in csv_text.splitlines() if ln.strip()]
    if not lines: return pd.DataFrame()
    try:
        return pd.read_csv(StringIO("\n".join(lines)), sep=";", dtype=str, engine="python", on_bad_lines='warn', quoting=3)
    except Exception as e:
        sg.popup_error(f"⚠ Erro ao ler CSV: {e}")
        return pd.DataFrame()

def ler_pagina_catmat(codigo: int, pagina: int, URL_BASE, TAMANHO_PAGINA, TIMEOUT) -> Tuple[Optional[pd.DataFrame], Optional[str]]:
    URL = f"{URL_BASE}/modulo-pesquisa-preco/1.1_consultarMaterial_CSV"
    params = {"tamanhoPagina": TAMANHO_PAGINA, "codigoItemCatalogo": int(codigo), "pagina": int(pagina)}
    tentativas = 0
    while tentativas < 2:
        try:
            resp = requests.get(URL, params=params, timeout=TIMEOUT, verify=False)
            if resp.status_code == 429:
                wait = 30 if tentativas == 0 else 60
                time.sleep(wait)
                tentativas += 1
                continue
            resp.raise_for_status()
            csv_text = resp.content.decode("utf-8-sig", errors="replace")
            return None, csv_text
        except requests.exceptions.ConnectionError as e:
            return None, f"ERRO_CONEXAO: Falha de conexão ao buscar CATMAT {codigo}. Verifique sua rede. ({e})"
        except requests.exceptions.RequestException as e:
            return None, f"ERRO_REQUISICAO: Erro de rede ao buscar CATMAT {codigo}: {e}"
            
    return None, f"ERRO_REQUISICAO: Erro 429 (Too Many Requests) persistente para CATMAT {codigo}"


def buscar_pdms_por_classe(codigo_classe: int, URL_BASE: str, TIMEOUT: int) -> Optional[Tuple[pd.DataFrame, int]]:
    URL = f"{URL_BASE}/modulo-material/3_consultarPdmMaterial"
    all_pdms = []
    pagina_atual = 1
    total_paginas = 1  # Valor inicial provisório
    total_registros_api = 0
    TAMANHO_PAGINA = 500 # Definimos uma constante para garantir consistência

    while pagina_atual <= total_paginas:
        params = {
            "codigoClasse": codigo_classe, 
            "pagina": pagina_atual, 
            "tamanhoPagina": TAMANHO_PAGINA, 
            "bps": "false"
        }
        
        try:
            resp = requests.get(URL, params=params, timeout=TIMEOUT, verify=False)
            resp.raise_for_status()
            data = resp.json()

            if "resultado" in data:
                all_pdms.extend(data["resultado"])

            # Lógica de cálculo manual de páginas
            if pagina_atual == 1:
                total_registros_api = int(data.get("totalRegistros", 0))
                
                # SE a API diz que tem mais de 500 registros, calculamos as páginas na mão
                if total_registros_api > 0:
                    # Ex: 1875 / 500 = 3.75 -> Teto = 4 páginas
                    total_paginas = math.ceil(total_registros_api / TAMANHO_PAGINA)
                else:
                    total_paginas = 1
                
                # Log de depuração (opcional, aparece no print do console)
                print(f"DEBUG: Registros: {total_registros_api} | Páginas Calculadas: {total_paginas}")

            pagina_atual += 1
            time.sleep(0.5)

        except requests.exceptions.RequestException as e:
            sg.popup_error(f"Erro de rede ao buscar PDMs da classe {codigo_classe} (página {pagina_atual}):\n{e}")
            return None
        except json.JSONDecodeError:
            sg.popup_error(f"Falha ao decodificar a resposta JSON da API para a classe {codigo_classe}.")
            return None

    if not all_pdms:
        return None

    df = pd.DataFrame(all_pdms)
    
    # Validação final para garantir que não duplicamos dados se a API se comportar de forma estranha
    df = df.drop_duplicates(subset=['codigoPdm'])
    
    return df, total_registros_api

def buscar_catmats_por_pdm(codigos_pdm: List[int], URL_BASE: str, TIMEOUT: int, window: sg.Window) -> Tuple[Optional[pd.DataFrame], List[int]]:
    global cancelar_busca_catmat
    URL = f"{URL_BASE}/modulo-material/4_consultarItemMaterial"
    all_catmats = []
    pdms_com_erro = []
    total_pdms = len(codigos_pdm)

    for i, pdm_code in enumerate(codigos_pdm):
        pausar_busca_catmat.wait() 
        if cancelar_busca_catmat:
            # CORREÇÃO: Usar write_event_value em vez de update direto
            window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', "Busca cancelada pelo usuário.")
            break

        pagina_atual = 1
        total_paginas = 1
        
        # CORREÇÃO: Enviar o texto para o loop principal atualizar
        window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', f"Buscando CATMATs do PDM {pdm_code} ({i+1}/{total_pdms})...")
        # REMOVIDO: window.refresh() (Isso quebra threads, não use refresh em threads)

        while True: 
            try:
                while pagina_atual <= total_paginas:
                    if cancelar_busca_catmat: raise InterruptedError
                    
                    params = {"codigoPdm": pdm_code, "pagina": pagina_atual, "tamanhoPagina": 500, "bps": "false"}
                    resp = requests.get(URL, params=params, timeout=TIMEOUT, verify=False)
                    
                    if resp.status_code == 429:
                        raise requests.exceptions.HTTPError("429 Client Error: Too Many Requests")

                    resp.raise_for_status()
                    data = resp.json()

                    if "resultado" in data:
                        all_catmats.extend(data["resultado"])

                    if pagina_atual == 1:
                        total_paginas = data.get("totalPaginas", 1)
                    
                    pagina_atual += 1
                    time.sleep(0.5)
                
                break 

            except requests.exceptions.RequestException as e:
                # CORREÇÃO: write_event_value
                window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', f"⚠️ Erro no PDM {pdm_code}. Adicionado à fila de nova tentativa.")
                pdms_com_erro.append(pdm_code)
                time.sleep(1)
                break 

            except json.JSONDecodeError:
                # CORREÇÃO: write_event_value
                window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', f"⚠️ Erro de JSON no PDM {pdm_code}. Adicionado à fila de nova tentativa.")
                pdms_com_erro.append(pdm_code)
                time.sleep(1)
                break
            except InterruptedError:
                break
        if cancelar_busca_catmat: break

    df = pd.DataFrame(all_catmats) if all_catmats else None
    
    return df, pdms_com_erro

def pagina_corrompida(csv_text: str) -> Tuple[bool, str]:
    if not csv_text:
        return False, csv_text

    linhas_originais = [ln for ln in csv_text.splitlines() if ln.strip()]
    if not linhas_originais:
        return False, csv_text
    
    num_colunas_esperado = 0
    header_index = -1
    header_line = ""

    try:
        header_index = next(i for i, ln in enumerate(linhas_originais) if not ln.lower().startswith(("totalregistros:", "totalpaginas:")))
        header_line = linhas_originais[header_index]
        num_colunas_esperado = len(header_line.split(';'))
    except StopIteration:
        return False, csv_text

    if num_colunas_esperado == 0:
        return False, csv_text

    linhas_corrigidas = []
    buffer_linha = ""
    foi_corrigido = False

    linhas_corrigidas.extend(linhas_originais[:header_index])
    linhas_corrigidas.append(header_line)

    for linha in linhas_originais[header_index + 1:]:
        if linha.lower().startswith(("totalregistros:", "totalpaginas:")):
            if buffer_linha:
                linhas_corrigidas.append(buffer_linha)
                buffer_linha = ""
            linhas_corrigidas.append(linha)
            continue
        
        linha_atual = buffer_linha + linha.replace("\r", "").replace("\n", "")
        num_colunas_atual = len(linha_atual.split(';'))

        if num_colunas_atual < num_colunas_esperado:
            buffer_linha = linha_atual + " " 
            foi_corrigido = True
        else:
            linhas_corrigidas.append(linha_atual)
            buffer_linha = ""
    
    if buffer_linha:
        linhas_corrigidas.append(buffer_linha)
    
    csv_corrigido_final = "\n".join(linhas_corrigidas)
    
    return foi_corrigido, csv_corrigido_final

def processar_dataframe_final(df: pd.DataFrame, ordem_colunas: List[str]) -> pd.DataFrame:
    if df.empty:
        return df

    primeira_coluna = df.columns[0]
    df = df[~df[primeira_coluna].astype(str).str.contains("totalRegistros|totalPaginas", case=False, na=False)].copy()
    if df.empty:
        return df

    def criar_unidade_fornecimento(row):
        p1 = row.get('nomeUnidadeFornecimento')
        p2 = row.get('capacidadeUnidadeFornecimento')
        p3 = row.get('siglaUnidadeMedida')
        
        partes_validas = [str(p) for p in [p1, p2, p3] if pd.notna(p) and str(p).strip()]
        
        if len(partes_validas) == 3:
            return " ".join(partes_validas)
        else:
            return ""
    
    df['Unidade de Fornecimento'] = df.apply(criar_unidade_fornecimento, axis=1)

    def converter_para_float(valor):
        if pd.isna(valor): return 0.0
        
        valor_str = str(valor)
        try:
            return float(valor_str.replace('.', '').replace(',', '.'))
        except (ValueError, TypeError):
            return 0.0

    preco_num = df['precoUnitario'].apply(converter_para_float)
    quantidade_num = df['quantidade'].apply(converter_para_float)
    df['Preço Total'] = preco_num * quantidade_num

    for col in ["nomeUnidadeMedida", "percentualMaiorDesconto"]:
        if col in df.columns:
            if df[col].isnull().all() or df[col].astype(str).str.strip().eq('').all():
                df = df.drop(columns=[col])

    colunas_existentes_na_ordem = [col for col in ordem_colunas if col in df.columns]
    colunas_extras = [col for col in df.columns if col not in colunas_existentes_na_ordem]
    df = df[colunas_existentes_na_ordem + colunas_extras]

    return df

sg.theme('SystemDefaultForReal')

welcome_message = """Olá! Bem-vindo ao Extrator de CATMATs Pro.

Sua ferramenta para extrair e descobrir dados no Portal de Compras Governamentais!

O que este programa faz?
Este programa possui duas funções principais em abas separadas:

1.  Extração por CATMAT (Aba 1): Se você já tem uma lista de códigos de materiais (CATMATs), esta aba busca todas as informações de compras, corrige problemas nos dados e consolida tudo em um arquivo Excel.

2.  Explorador de Classes (Aba 2): Se você quer descobrir novos itens, pode começar com o código de uma Classe, encontrar todos os Padrões Descritivos de Materiais (PDMs) dentro dela e, em seguida, listar todos os CATMATs relacionados para extração.

Primeiros Passos:
- Para uma extração direta com uma lista pronta, use a primeira aba, o arquivo excel deve iniciar a lista dos CATMATs com o cabeçalho codigoItemCatalogo.
- Para descobrir itens, comece pela segunda aba e, ao final, envie os CATMATs encontrados para a extração na primeira aba.

Acompanhe todo o processo em tempo real aqui neste log. Bom trabalho!
"""

layout_extracao_config = [
    [sg.Text("Arquivo de Códigos:", size=(20,1)), sg.Input(key="-ARQUIVO-", enable_events=True, expand_x=True), sg.FileBrowse(button_text='Procurar', file_types=(("Excel Files", "*.xlsx"), ("CSV Files", "*.csv")))],
    
    # --- NOVO: SELEÇÃO DE FORMATO ---
    [sg.Text("Formato de Saída:", size=(20,1)), 
     sg.Radio('Excel (.xlsx)', "GROUP_FMT", default=True, key='-FMT_XLSX-'), 
     sg.Radio('CSV (.csv)', "GROUP_FMT", key='-FMT_CSV-')],
    # --------------------------------
    
    [sg.Checkbox('Salvar cópias dos arquivos CSV corrompidos?', key='-SALVAR_CORROMPIDOS-', default=False, enable_events=True)],
    [sg.Column([[sg.Text("Pasta para Corrompidos:", size=(20,1)), sg.Input(key="-PASTA-", enable_events=True, expand_x=True), sg.FolderBrowse(button_text='Procurar')]], key='-SECAO_PASTA_CORROMPIDOS-', visible=False)],
]
layout_extracao_stats = [
    [sg.Text("Códigos Processados:", size=(20,1)), sg.Text("0 / 0", key="-CONT_PROCESSADOS-", font=("Helvetica", 10, "bold"))],
    [sg.Text("Registros Consolidados:", size=(20,1)), sg.Text("0", key="-CONT_REGISTROS-", font=("Helvetica", 10, "bold"))],
    [sg.Text("Páginas Corrigidas:", size=(20,1)), sg.Text("0", key="-CONT_CORRIGIDAS-", font=("Helvetica", 10, "bold"), text_color="orange")],
    [sg.Text("Códigos sem Dados:", size=(20,1)), sg.Text("0", key="-CONT_VAZIOS-", font=("Helvetica", 10, "bold"), text_color="#FF6347")],
]
layout_extracao_execucao = [
    [sg.Text("Status: Ocioso", key='-STATUS-', expand_x=True, font=("Helvetica", 10, "italic"))],
    [sg.ProgressBar(max_value=1000, orientation='h', size=(50, 20), key='-PROGRESS-', expand_x=True), sg.Text('0%', size=(5,1), key='-PERCENT-', font=("Helvetica", 10, "bold"))],
    [sg.Multiline(default_text=welcome_message, size=(80,20), key="-OUTPUT-", autoscroll=True, expand_x=True, expand_y=True, background_color='black', text_color='white')],
]
tab1_layout = [
    [sg.Frame('1. Configurações de Entrada', layout_extracao_config, expand_x=True)],
    [sg.Frame('2. Resumo da Execução', layout_extracao_stats, expand_x=True)],
    [sg.Frame('3. Log e Progresso', layout_extracao_execucao, expand_x=True, expand_y=True)],
    [sg.Button("Iniciar Extração", key="-START-", disabled=True), sg.Button("Cancelar", key="-CANCEL-", disabled=True), 
     sg.Button('Pausar', key='-PAUSE_EXTRACTION-', button_color=('white', 'darkblue'), disabled=True), 
     sg.Button("Salvar Log", key="-SAVE_LOG-", disabled=True)]
]

COR_BOTAO_SELECIONADO = ('white', 'green')
COR_BOTAO_PADRAO = ('white', 'grey')
FILTRO_BOTOES = ['-FILTRO_TODOS-', '-FILTRO_ATIVOS-', '-FILTRO_INATIVOS-']

pdm_headings = ['Código PDM', 'Descrição', 'Status']
layout_explorador = [
    [sg.Frame('1. Buscar PDMs por Classe', [[
        sg.Text('Código da Classe:'), 
        sg.Input(key='-INPUT_CLASSE-', size=(10,1)), 
        sg.Button('Buscar PDMs', key='-BUSCAR_PDMS-'),
        sg.Push(),
        sg.Text("", key="-PDM_COUNT_DISPLAY-", font=("Helvetica", 10, "bold"))
    ]], expand_x=True)],
    [sg.Frame('2. Resultados da Busca de PDMs', [[
        sg.Text("Filtro:"), 
        sg.Button('Todos', key='-FILTRO_TODOS-', button_color=COR_BOTAO_SELECIONADO), 
        sg.Button('Apenas Ativos', key='-FILTRO_ATIVOS-', button_color=COR_BOTAO_PADRAO), 
        sg.Button('Apenas Inativos', key='-FILTRO_INATIVOS-', button_color=COR_BOTAO_PADRAO),
        sg.Push(),
        sg.Button("Exportar PDMs Visíveis", key="-EXPORTAR_PDMS-"), 
        sg.Checkbox('Selecionar Todos Visíveis', key='-SELECIONAR_TODOS_PDM-', enable_events=True)
        ], [
        sg.Table(values=[], headings=pdm_headings, num_rows=15, key='-TABELA_PDMS-', enable_events=True, justification='left', auto_size_columns=False, col_widths=[10, 50, 8], expand_x=True, select_mode='extended')
    ]], expand_x=True, expand_y=True)],
    
    [sg.Frame('Busca Avulsa por PDMs', [
        [sg.Text("Cole os códigos PDM (um por linha):"), sg.Push(), sg.Button('Buscar CATMATs (PDMs da Lista)', key='-BUSCAR_PDMS_ESPECIFICOS-')],
        [sg.Multiline(size=(40, 8), key='-INPUT_PDMS_ESPECIFICOS-')]
    ], expand_x=True)],
    
    [sg.Frame('3. Ações', [[
        sg.Button('Buscar CATMATs (PDMs da Tabela)', key='-BUSCAR_CATMATS-'),
        sg.Button('Buscar CATMATs e Iniciar Extração', key='-BUSCAR_E_EXTRAIR-', button_color=('white', 'blue'), tooltip='Busca os CATMATs dos PDMs selecionados e inicia a extração de dados para eles automaticamente.'),
        sg.Button('Exportar Lista de CATMATs', key='-EXPORTAR_CATMATS-', disabled=True),
        sg.Column([[
            sg.Text("", key="-STATUS_EXPLORADOR-", font=("Helvetica", 10, "italic")),
            sg.Button('Pausar Busca', key='-PAUSE_SEARCH-', button_color=('white', 'darkblue')), 
            sg.Button('Cancelar Busca', key='-CANCELAR_BUSCA_CATMAT-', button_color=('white', 'red'))
        ]], key='-COLUNA_CANCELAR_BUSCA-', visible=False)
        ],[
        sg.Column([[
            sg.Button('Iniciar Extração com CATMATs Encontrados', key='-INICIAR_EXTRACAO_EXPLORADOR-', disabled=True, button_color=('white', 'green'), tooltip='Clique aqui para enviar a lista de CATMATs encontrados para a aba de extração e iniciar o processo.')
        ]], key='-COLUNA_INICIAR_EXTRACAO-', visible=False)
    ]], expand_x=True)],
]

layout = [[sg.TabGroup([[
    sg.Tab('Extração por CATMAT', tab1_layout, key='-TAB_EXTRACAO-'),
    sg.Tab('Explorador de Classes', layout_explorador, key='-TAB_EXPLORADOR-')
]], key='-TAB_GROUP-', expand_x=True, expand_y=True)]]

window = sg.Window("Extrator de CATMATs Pro", layout, resizable=True, finalize=True)
window.set_min_size((800, 700))

window['-INPUT_CLASSE-'].bind('<Return>', '_Enter')

URL_BASE = "https://dadosabertos.compras.gov.br"
TIMEOUT = 120
ordem_final_colunas = [
    "idCompra", "idItemCompra", "forma", "modalidade", "criterioJulgamento",
    "numeroItemCompra", "descricaoItem", "codigoItemCatalogo", "nomeUnidadeFornecimento",
    "siglaUnidadeFornecimento", "nomeUnidadeMedida",  "capacidadeUnidadeFornecimento", "siglaUnidadeMedida",
    "Unidade de Fornecimento", "capacidade", "quantidade", "precoUnitario", "Preço Total", "percentualMaiorDesconto",
    "niFornecedor", "nomeFornecedor", "marca", "codigoUasg", "nomeUasg",
    "codigoMunicipio", "municipio", "estado", "codigoOrgao", "nomeOrgao",
    "poder", "esfera", "dataCompra", "dataHoraAtualizacaoCompra", "dataHoraAtualizacaoItem",
    "dataResultado", "dataHoraAtualizacaoUasg", "codigoClasse", "nomeClasse"
]

processing = False; codes_iterator = None; writer = None
codigos_para_processar, paginas_corrompidas, registros_esperados, registros_baixados = [], {}, {}, {}
total_registros_baixados, paginas_corrigidas_count, codigos_vazios_count = 0, 0, 0

lista_pdms_completa = pd.DataFrame()
lista_catmats_descobertos = []
cancelar_busca_catmat = False

def buscar_catmats_thread(pdms_selecionados, window, acao_final='apenas_buscar'):
    window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', "Iniciando busca... (1ª tentativa)")
    df_passo1, pdms_para_tentar_novamente = buscar_catmats_por_pdm(pdms_selecionados, URL_BASE, TIMEOUT, window)
    
    df_passo2 = None
    pdms_falha_final = []

    if pdms_para_tentar_novamente and not cancelar_busca_catmat:
        window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', f"Aguardando 5s antes da 2ª tentativa para {len(pdms_para_tentar_novamente)} PDMs...")
        time.sleep(5)
        window.write_event_value('-UPDATE_STATUS_EXPLORADOR-', "Iniciando 2ª tentativa...")
        df_passo2, pdms_falha_final = buscar_catmats_por_pdm(pdms_para_tentar_novamente, URL_BASE, TIMEOUT, window)

    dfs_finais = []
    if df_passo1 is not None and not df_passo1.empty:
        dfs_finais.append(df_passo1)
    if df_passo2 is not None and not df_passo2.empty:
        dfs_finais.append(df_passo2)
    
    df_catmats_final = pd.concat(dfs_finais, ignore_index=True) if dfs_finais else None
    
    resultado = {
        'dataframe': df_catmats_final,
        'falhas': pdms_falha_final,
        'acao_final': acao_final
    }
    window.write_event_value('-THREAD_CATMAT_CONCLUIDA-', resultado)

def atualizar_cores_filtro(botao_ativo: str, window: sg.Window):
    for botao in FILTRO_BOTOES:
        if botao == botao_ativo:
            window[botao].update(button_color=COR_BOTAO_SELECIONADO)
        else:
            window[botao].update(button_color=COR_BOTAO_PADRAO)

def iniciar_processo_extracao(lista_codigos, formato_saida='xlsx'):
    global processing, codes_iterator, writer, paginas_corrompidas, registros_esperados, registros_baixados, codigos_para_processar
    global total_registros_baixados, paginas_corrigidas_count, codigos_vazios_count
    
    if not lista_codigos:
        sg.popup_error("Nenhum código para processar.")
        return

    processing = True
    codigos_para_processar = lista_codigos
    window["-START-"].update(disabled=True); window["-CANCEL-"].update(disabled=False); window["-SAVE_LOG-"].update(disabled=True)
    window['-PAUSE_EXTRACTION-'].update(disabled=False, text='Pausar')
    pausar_extracao.set()

    window["-OUTPUT-"].update(""); window["-PROGRESS-"].update(0); window["-PERCENT-"].update('0%')
    window["-CONT_PROCESSADOS-"].update(f"0 / {len(codigos_para_processar)}"); window["-CONT_REGISTROS-"].update("0")
    window["-CONT_CORRIGIDAS-"].update("0"); window["-CONT_VAZIOS-"].update("0")

    # --- LÓGICA DE ESCOLHA DO ESCRITOR ---
    if formato_saida == 'csv':
        writer = CSVChunkWriter("dados_completos_extraidos.csv")
        window["-OUTPUT-"].print(f"💾 Configurado para salvar em CSV.", text_color='lightgreen')
    else:
        writer = ExcelChunkWriter("dados_completos_extraidos.xlsx")
        window["-OUTPUT-"].print(f"💾 Configurado para salvar em Excel.", text_color='lightgreen')
    # -------------------------------------

    paginas_corrompidas, registros_esperados, registros_baixados = {}, {}, {}
    total_registros_baixados, paginas_corrigidas_count, codigos_vazios_count = 0, 0, 0
    
    codes_iterator = iter(enumerate(codigos_para_processar, 1))
    window["-STATUS-"].update("Status: Processando...")

while True:
    event, values = window.read(timeout=120)

    if event == '-UPDATE_STATUS_EXPLORADOR-':
        window['-STATUS_EXPLORADOR-'].update(values[event])

    if event == sg.WIN_CLOSED: break

    if not processing:
        arquivo_ok = bool(values['-ARQUIVO-'])
        pasta_ok = not values['-SALVAR_CORROMPIDOS-'] or (values['-SALVAR_CORROMPIDOS-'] and values['-PASTA-'])
        if arquivo_ok and pasta_ok: window['-START-'].update(disabled=False)
        else: window['-START-'].update(disabled=True)

    if event == '-SALVAR_CORROMPIDOS-':
        window['-SECAO_PASTA_CORROMPIDOS-'].update(visible=values['-SALVAR_CORROMPIDOS-'])
    
    if event == "-START-" and not processing:
        try:
            ARQUIVO_CODIGOS = values["-ARQUIVO-"]
            df_codigos = pd.read_excel(ARQUIVO_CODIGOS) if ARQUIVO_CODIGOS.lower().endswith(".xlsx") else pd.read_csv(ARQUIVO_CODIGOS, sep=";")
            if "codigoItemCatalogo" not in df_codigos.columns:
                sg.popup_error("Erro: O arquivo de entrada deve ter a coluna 'codigoItemCatalogo'.")
                continue
            lista_codigos = pd.Series(df_codigos["codigoItemCatalogo"]).dropna().astype(int).drop_duplicates().tolist()
            window["-OUTPUT-"].print(f"🔎 {len(lista_codigos)} códigos únicos carregados do arquivo.", text_color='lightblue')
            
            # --- CAPTURA O FORMATO ESCOLHIDO ---
            formato = 'csv' if values['-FMT_CSV-'] else 'xlsx'
            iniciar_processo_extracao(lista_codigos, formato_saida=formato)
            # -----------------------------------
            
        except Exception as e:
            sg.popup_error(f"Ocorreu um erro ao ler o arquivo de códigos:\n{e}")

    if event == '-INICIAR_EXTRACAO_EXPLORADOR-':
        if not lista_catmats_descobertos:
            sg.popup_error("Nenhum CATMAT foi encontrado ou selecionado.")
        else:
            window['-TAB_EXTRACAO-'].select()
            window["-OUTPUT-"].print(f"🔎 {len(lista_catmats_descobertos)} códigos descobertos via explorador.", text_color='lightblue')
            
            # --- CAPTURA O FORMATO DA ABA 1 ---
            formato = 'csv' if values['-FMT_CSV-'] else 'xlsx'
            iniciar_processo_extracao(lista_catmats_descobertos, formato_saida=formato)
            # ----------------------------------

    if event == "-CANCEL-" and processing:
        processing = False; codes_iterator = None
        window["-STATUS-"].update("Status: Cancelando..."); window["-OUTPUT-"].print("\n🛑 Processo cancelado. Finalizando arquivos...", text_color='yellow')
        if writer:
            saved_parts = writer.finalize()
            if saved_parts: window["-OUTPUT-"].print(f"💾 Dados parciais salvos em: {', '.join(saved_parts)}", text_color='yellow')
        window["-PROGRESS-"].update(0); window["-PERCENT-"].update('0%');
        window["-START-"].update(disabled=False); window["-CANCEL-"].update(disabled=True); window["-SAVE_LOG-"].update(disabled=False)
        window["-PAUSE_EXTRACTION-"].update(disabled=True)
        window["-STATUS-"].update("Status: Cancelado")

    if event == '-PAUSE_EXTRACTION-':
        if pausar_extracao.is_set():
            pausar_extracao.clear()
            window['-PAUSE_EXTRACTION-'].update(text='Retomar')
            window['-STATUS-'].update("Status: Pausado.")
        else:
            pausar_extracao.set()
            window['-PAUSE_EXTRACTION-'].update(text='Pausar')
            window['-STATUS-'].update("Status: Retomando extração...")

    if event == "-SAVE_LOG-":
        filepath = sg.popup_get_file("Salvar Log de Execução", save_as=True, no_window=True, default_extension=".txt", file_types=(("Text Files", "*.txt"),))
        if filepath:
            try:
                with open(filepath, 'w', encoding='utf-8') as f: f.write(values["-OUTPUT-"])
                sg.popup_quick("Log salvo com sucesso!")
            except Exception as e: sg.popup_error(f"Não foi possível salvar o log.\nErro: {e}")

    if event in ('-BUSCAR_PDMS-', '-INPUT_CLASSE-_Enter'):
        codigo_classe = values['-INPUT_CLASSE-']
        if codigo_classe and codigo_classe.isdigit():
            window['-PDM_COUNT_DISPLAY-'].update("")
            window.refresh()
            
            resultado_busca = buscar_pdms_por_classe(int(codigo_classe), URL_BASE, TIMEOUT)
            
            if resultado_busca is not None:
                df_pdms, total_api = resultado_busca
                
                df_processado = df_pdms.rename(columns={'codigoPdm': 'Código PDM', 'nomePdm': 'Descrição', 'statusPdm': 'Status'})
                df_processado['Status'] = df_processado['Status'].apply(lambda x: 'Ativo' if x else 'Inativo')
                lista_pdms_completa = df_processado
                
                window['-TABELA_PDMS-'].update(values=lista_pdms_completa[['Código PDM', 'Descrição', 'Status']].values.tolist())
                
                total_encontrados = len(lista_pdms_completa)
                window['-PDM_COUNT_DISPLAY-'].update(f"{total_encontrados} de {total_api} PDMs encontrados.")
                window['-STATUS_EXPLORADOR-'].update("")
                
                atualizar_cores_filtro('-FILTRO_TODOS-', window)
                window['-SELECIONAR_TODOS_PDM-'].update(value=False)
                window['-EXPORTAR_CATMATS-'].update(disabled=True)
                window['-COLUNA_INICIAR_EXTRACAO-'].update(visible=False)
                window['-INICIAR_EXTRACAO_EXPLORADOR-'].update('Iniciar Extração com CATMATs Encontrados')
            else:
                window['-STATUS_EXPLORADOR-'].update("Nenhum PDM encontrado ou erro na busca.")
                window['-PDM_COUNT_DISPLAY-'].update("")
                lista_pdms_completa = pd.DataFrame()
                window['-TABELA_PDMS-'].update(values=[])
        elif event == '-BUSCAR_PDMS-':
            sg.popup_error("Por favor, insira um código de Classe válido (apenas números).")
            
    if event == '-EXPORTAR_PDMS-':
        dados_visiveis = window['-TABELA_PDMS-'].Values
        if not dados_visiveis:
            sg.popup_error("Não há PDMs na tabela para exportar.")
        else:
            filepath = sg.popup_get_file("Salvar Lista de PDMs", save_as=True, no_window=True, default_extension=".csv", default_path="PDMs_exportados.csv", file_types=(("CSV Files", "*.csv"),))
            if filepath:
                try:
                    df_export = pd.DataFrame(dados_visiveis, columns=pdm_headings)
                    df_export.to_csv(filepath, index=False, sep=';', encoding='utf-8-sig')
                    sg.popup_ok(f"Lista de PDMs visíveis salva com sucesso em:\n{filepath}")
                except Exception as e:
                    sg.popup_error(f"Não foi possível salvar a lista de PDMs.\nErro: {e}")

    if event in FILTRO_BOTOES:
        if not lista_pdms_completa.empty:
            atualizar_cores_filtro(event, window)
            df_filtrado = lista_pdms_completa
            if event == '-FILTRO_ATIVOS-': df_filtrado = lista_pdms_completa[lista_pdms_completa['Status'] == 'Ativo']
            elif event == '-FILTRO_INATIVOS-': df_filtrado = lista_pdms_completa[lista_pdms_completa['Status'] == 'Inativo']
            window['-TABELA_PDMS-'].update(values=df_filtrado[['Código PDM', 'Descrição', 'Status']].values.tolist())
            window['-SELECIONAR_TODOS_PDM-'].update(value=False)

    if event == '-SELECIONAR_TODOS_PDM-':
        if values['-SELECIONAR_TODOS_PDM-']:
            num_rows = len(window['-TABELA_PDMS-'].Values)
            window['-TABELA_PDMS-'].update(select_rows=list(range(num_rows)))
        else:
            window['-TABELA_PDMS-'].update(select_rows=[])

    if event in ('-BUSCAR_CATMATS-', '-BUSCAR_E_EXTRAIR-', '-BUSCAR_PDMS_ESPECIFICOS-'):
        pdms_selecionados = []
        if event in ('-BUSCAR_CATMATS-', '-BUSCAR_E_EXTRAIR-'):
            indices_selecionados = window['-TABELA_PDMS-'].SelectedRows
            if not indices_selecionados: sg.popup_error("Selecione pelo menos um PDM na tabela.")
            else:
                dados_tabela = window['-TABELA_PDMS-'].Values
                pdms_selecionados = [int(dados_tabela[i][0]) for i in indices_selecionados if i < len(dados_tabela)]
        else:
            pdms_texto = values['-INPUT_PDMS_ESPECIFICOS-']
            if not pdms_texto.strip():
                sg.popup_error("Por favor, insira pelo menos um código PDM na caixa de busca avulsa.")
            else:
                pdms_invalidos = []
                for linha in pdms_texto.strip().split('\n'):
                    try:
                        pdms_selecionados.append(int(linha.strip()))
                    except ValueError:
                        if linha.strip(): pdms_invalidos.append(linha.strip())
                if pdms_invalidos:
                    sg.popup_error(f"Os seguintes valores são inválidos e foram ignorados:\n{', '.join(pdms_invalidos)}")
        
        if pdms_selecionados:
            cancelar_busca_catmat = False
            pausar_busca_catmat.set() 
            window['-PAUSE_SEARCH-'].update(text='Pausar Busca')
            window['-BUSCAR_CATMATS-'].update(disabled=True); window['-BUSCAR_E_EXTRAIR-'].update(disabled=True)
            window['-BUSCAR_PDMS_ESPECIFICOS-'].update(disabled=True)
            window['-COLUNA_CANCELAR_BUSCA-'].update(visible=True)
            
            acao = 'extrair' if event == '-BUSCAR_E_EXTRAIR-' else 'apenas_buscar'
            threading.Thread(target=buscar_catmats_thread, args=(pdms_selecionados, window, acao), daemon=True).start()

    if event == '-CANCELAR_BUSCA_CATMAT-':
        cancelar_busca_catmat = True

    if event == '-PAUSE_SEARCH-':
        if pausar_busca_catmat.is_set():
            pausar_busca_catmat.clear()
            window['-PAUSE_SEARCH-'].update(text='Retomar Busca')
            window['-STATUS_EXPLORADOR-'].update("Busca pausada.")
        else:
            pausar_busca_catmat.set()
            window['-PAUSE_SEARCH-'].update(text='Pausar Busca')
            window['-STATUS_EXPLORADOR-'].update("Retomando busca...")

    if event == '-THREAD_CATMAT_CONCLUIDA-':
        resultado = values[event]
        df_catmats = resultado.get('dataframe')
        falhas_finais = resultado.get('falhas', [])
        acao_final = resultado.get('acao_final')

        window['-COLUNA_CANCELAR_BUSCA-'].update(visible=False)
        window['-BUSCAR_CATMATS-'].update(disabled=False); window['-BUSCAR_E_EXTRAIR-'].update(disabled=False)
        window['-BUSCAR_PDMS_ESPECIFICOS-'].update(disabled=False)

        if df_catmats is not None and 'codigoItem' in df_catmats.columns:
            lista_catmats_descobertos = df_catmats['codigoItem'].dropna().astype(int).tolist()
            num_catmats = len(lista_catmats_descobertos)

            status_msg = f"✅ {num_catmats} CATMATs encontrados!"
            if falhas_finais:
                status_msg += f" ⚠ Falha ao buscar {len(falhas_finais)} PDMs."
                sg.popup_warning(f"A busca foi concluída, mas não foi possível obter dados dos seguintes PDMs, mesmo após duas tentativas:\n\n{', '.join(map(str, falhas_finais))}")

            window['-STATUS_EXPLORADOR-'].update(status_msg)
            window['-INICIAR_EXTRACAO_EXPLORADOR-'].update(f'Iniciar Extração com {num_catmats} CATMATs Encontrados')
            window['-EXPORTAR_CATMATS-'].update(disabled=False)
            window['-COLUNA_INICIAR_EXTRACAO-'].update(visible=True)
            window['-INICIAR_EXTRACAO_EXPLORADOR-'].update(disabled=False)

            if acao_final == 'extrair':
                window.write_event_value('-INICIAR_EXTRACAO_EXPLORADOR-', None)
        else:
            lista_catmats_descobertos = []
            if not cancelar_busca_catmat:
                window['-STATUS_EXPLORADOR-'].update("Nenhum CATMAT encontrado.")
            window['-EXPORTAR_CATMATS-'].update(disabled=True)
            window['-COLUNA_INICIAR_EXTRACAO-'].update(visible=False)
            window['-INICIAR_EXTRACAO_EXPLORADOR-'].update(disabled=True)
            
    if event == '-EXPORTAR_CATMATS-':
        if not lista_catmats_descobertos:
            sg.popup_error("Não há CATMATs na lista para exportar. Realize uma busca primeiro.")
        else:
            filepath = sg.popup_get_file(
                "Salvar Lista de CATMATs",
                save_as=True,
                no_window=True,
                default_extension=".csv",
                default_path="CATMATs_descobertos.csv",
                file_types=(("CSV Files", "*.csv"),)
            )
            if filepath:
                try:
                    df_export = pd.DataFrame(lista_catmats_descobertos, columns=['codigoItemCatalogo'])
                    df_export.to_csv(filepath, index=False, sep=';')
                    sg.popup_ok(f"Lista com {len(df_export)} CATMATs salva com sucesso em:\n{filepath}")
                except Exception as e:
                    sg.popup_error(f"Não foi possível salvar a lista.\nErro: {e}")

    if processing:
        pausar_extracao.wait() 
        try:
            idx, codigo = next(codes_iterator)
            total_codigos = len(codigos_para_processar)
            window["-STATUS-"].update(f"Status: Processando código {codigo} ({idx}/{total_codigos})")
            window["-CONT_PROCESSADOS-"].update(f"{idx} / {total_codigos}")
            
            pagina_atual, total_paginas, baixados_do_codigo = 1, None, 0
            
            try:
                _, csv_text = ler_pagina_catmat(codigo, pagina_atual, URL_BASE, 500, TIMEOUT)
                if csv_text and csv_text.startswith("ERRO_CONEXAO"):
                    sg.popup_error(csv_text)
                    processing = False 
                    raise StopIteration
                if csv_text is None or csv_text.startswith("ERRO_REQUISICAO"):
                    window["-OUTPUT-"].print(f"ℹ️ Código {codigo}: Falha ou nenhum registro. {csv_text or ''}", text_color='lightblue'); codigos_vazios_count += 1; window["-CONT_VAZIOS-"].update(codigos_vazios_count)
                    raise StopIteration 
                
                m_reg = re.search(r"totalRegistros\s*:\s*(\d+)", csv_text, re.IGNORECASE)
                registros_esperados[codigo] = int(m_reg.group(1)) if m_reg else 0
                if registros_esperados[codigo] == 0:
                     window["-OUTPUT-"].print(f"ℹ️ Código {codigo}: API informa 0 registros.", text_color='lightblue'); codigos_vazios_count += 1; window["-CONT_VAZIOS-"].update(codigos_vazios_count)
                     raise StopIteration

                while True: 
                    is_corrupt, csv_corrigido = pagina_corrompida(csv_text)
                    df_final_pagina = parse_csv_text(csv_corrigido)
                    
                    if is_corrupt:
                        paginas_corrompidas.setdefault(codigo, []).append(str(pagina_atual))
                        if values['-SALVAR_CORROMPIDOS-'] and values['-PASTA-']:
                            path = os.path.join(values['-PASTA-'], f"cod_{codigo}_pag_{pagina_atual}_corrompido.csv")
                            with open(path, 'w', encoding='utf-8-sig') as f: f.write(csv_text)
                        
                        window["-OUTPUT-"].print(f"⚠️ Cód {codigo}, Pág {pagina_atual}: Corrigida.", text_color='orange')
                        paginas_corrigidas_count += 1; window["-CONT_CORRIGIDAS-"].update(paginas_corrigidas_count)
                    else:
                        window["-OUTPUT-"].print(f"✅ Cód {codigo}, Pág {pagina_atual}: Lida com sucesso.", text_color='lightgreen')

                    if df_final_pagina is not None and not df_final_pagina.empty:
                        df_final_pagina.loc[:, "codigoItemCatalogo"] = str(codigo)
                        
                        df_processado = processar_dataframe_final(df_final_pagina, ordem_final_colunas)
                        
                        registros_na_pagina = len(df_processado)
                        baixados_do_codigo += registros_na_pagina
                        total_registros_baixados += registros_na_pagina
                        window["-CONT_REGISTROS-"].update(f"{total_registros_baixados:,}".replace(",", "."))
                        writer.write_dataframe(df_processado)
                    
                    if total_paginas is None:
                        m = re.search(r"total\s*p[áa]ginas?\s*:\s*(\d+)", csv_text, re.IGNORECASE)
                        total_paginas = int(m.group(1)) if m else 1
                    
                    pagina_atual += 1
                    time.sleep(0.5)
                    if pagina_atual > total_paginas: break
                    _, csv_text = ler_pagina_catmat(codigo, pagina_atual, URL_BASE, 500, TIMEOUT)
                    if csv_text is None or csv_text.startswith("ERRO_"): break

            except StopIteration: pass
            except Exception as e: window["-OUTPUT-"].print(f"❌ Erro crítico no código {codigo}: {e}", text_color='#FF6347')
            registros_baixados[codigo] = baixados_do_codigo
            
            percent_complete = int((idx / total_codigos) * 100)
            window['-PROGRESS-'].update(percent_complete * 10)
            window['-PERCENT-'].update(f'{percent_complete}%')

        except StopIteration: 
            window['-PROGRESS-'].update(1000); window['-PERCENT-'].update('100%')
            window["-STATUS-"].update("Status: Processamento concluído! Gerando relatório...")
            window.refresh()
            
            processing = False
            window["-OUTPUT-"].print("\n🎉 Download concluído! Gerando arquivo final...", text_color='lightblue')

            saved_parts = writer.finalize()
            if not saved_parts:
                sg.popup("Nenhum dado válido foi baixado. O relatório não será gerado.")
                window["-START-"].update(disabled=False); window["-CANCEL-"].update(disabled=True); window["-SAVE_LOG-"].update(disabled=False)
                window["-PAUSE_EXTRACTION-"].update(disabled=True)
                window["-STATUS-"].update("Status: Ocioso")
                
            else: 
                window["-OUTPUT-"].print(f"💾 Arquivos de dados salvos em: {', '.join(saved_parts)}", text_color='lightblue')
                
                wb_rel = Workbook()
                ws_rel = wb_rel.active
                ws_rel.title = "Relatório Integridade"
                ws_rel.append(["codigoItemCatalogo", "registros_esperados_api", "registros_baixados_reais", "paginas_com_problemas", "status"])
                for c in codigos_para_processar:
                    baixados = int(registros_baixados.get(c, 0))
                    esperados = int(registros_esperados.get(c, 0))
                    paginas = paginas_corrompidas.get(c, [])
                    
                    diferenca = abs(esperados - baixados)
                    status = "OK"
                    if diferenca == 0:
                        status = "OK"
                    elif diferenca <= 2:
                        status = f"OK (Pequena divergência API: {baixados}/{esperados})"
                    else:
                        status = f"Inconsistência Grave (Baixados: {baixados} / Esperados: {esperados})"

                    ws_rel.append([c, esperados, baixados, ", ".join(map(str, paginas)), status])
                
                report_filename = "Relatorio_Integridade.xlsx"
                try:
                    wb_rel.save(report_filename)
                    window["-OUTPUT-"].print(f"📊 Relatório de integridade salvo em arquivo separado: {report_filename}", text_color='lightblue')
                except Exception as e:
                    sg.popup_error(f"Não foi possível salvar o arquivo de relatório:\n{e}")
                
                resumo_final = f"""
    ✅ Processo Concluído!
    -------------------------------------------
      - Códigos Processados: {len(codigos_para_processar)}
      - Registros Consolidados: {f"{total_registros_baixados:,}".replace(",", ".")}
      - Páginas Corrigidas: {paginas_corrigidas_count}
      - Códigos sem Registros: {codigos_vazios_count}
    -------------------------------------------"""
                sg.popup('Resumo da Extração', resumo_final)
                
                ultimo_arquivo = saved_parts[-1]
                extensao_final = os.path.splitext(ultimo_arquivo)[1] # Pega .csv ou .xlsx automaticamente
                tipos_arquivo = (("Excel Files", "*.xlsx"),) if extensao_final == '.xlsx' else (("CSV Files", "*.csv"),)
                caminho_destino = sg.popup_get_file(
                    "Escolha onde salvar o arquivo de DADOS principal", 
                    save_as=True, 
                    no_window=True, 
                    default_path=os.path.basename(ultimo_arquivo), 
                    file_types=tipos_arquivo
            )
            
            if caminho_destino:
                if not caminho_destino.lower().endswith(extensao_final): caminho_destino += extensao_final
                shutil.copy(ultimo_arquivo, caminho_destino)
                sg.popup("✅ Sucesso!", f"Arquivo de DADOS salvo em:\n{caminho_destino}\n\nO relatório foi salvo na pasta do programa.")
            else: 
                sg.popup("⚠ Atenção", f"Nenhum local escolhido. O arquivo de DADOS permanece em:\n{ultimo_arquivo}")
                
                window["-STATUS-"].update("Status: Concluído!"); window["-START-"].update(disabled=False); window["-CANCEL-"].update(disabled=True)
                window["-PAUSE_EXTRACTION-"].update(disabled=True)
                window["-SAVE_LOG-"].update(disabled=False)

window.close()