"""
Tratamento do Catálogo Oficial (CATMAT)  —  padroniza o "descricaoItem"
para ficar igual ao "descricaoCATMAT" da extração BPS (extracao_bps.sql).

Regras aplicadas (as MESMAS do SQL, na mesma ordem):
    1. "¿" vira "°"                        (grau que chega corrompido)
    2. remove todo "*"                     (APLICAÇÃO*:  ->  APLICAÇÃO:)
    3. dois-pontos sempre como ": "        (MATERIAL:PMMA -> MATERIAL: PMMA)
    4. remove espaço antes da vírgula      (LUZ LED , TIPO -> LUZ LED, TIPO)
    5. espaços repetidos viram um só e tira espaço do início/fim

Só as colunas listadas em COLUNAS_TRATAR mudam; todas as outras são gravadas
exatamente como estavam. O arquivo é lido linha a linha, então funciona com o
catálogo inteiro (centenas de MB) sem carregar tudo na memória.

    python tratar_catalogo.py                       -> usa os caminhos abaixo
    python tratar_catalogo.py ENTRADA.csv SAIDA.csv -> sobrescreve os caminhos
"""

import re
import csv
import codecs
import sys
import time
from pathlib import Path

# ══════════════════════════════════════════════════════════════════════════════
#  CONFIGURAÇÃO  —  ajuste aqui
# ══════════════════════════════════════════════════════════════════════════════
# Caminho do catálogo original (pode ser o arquivo completo).
ARQUIVO_ENTRADA = r"C:\Users\abel.ogliari\OneDrive - Ministério da Saúde\Área de Trabalho\Extrator-CATMAT-DA\Catalogo Oficial.CSV"
# Caminho do arquivo tratado. Vazio ("") = mesmo nome com sufixo "_tratado".
ARQUIVO_SAIDA   = r""
# Colunas a tratar (nome exato do cabeçalho).
COLUNAS_TRATAR  = ["descricaoItem"]
# Separador de colunas do CSV.
DELIMITADOR     = "@"
# "auto" detecta UTF-8 ou ISO-8859-1 (latin-1). A saída usa a mesma codificação.
ENCODING        = "auto"
# ══════════════════════════════════════════════════════════════════════════════

_RE_DOIS_PONTOS = re.compile(r"\s*:\s*")
_RE_ESPACO_VIRG = re.compile(r"\s+,")
_RE_ESPACOS     = re.compile(r"\s+")


def tratar_texto(txt: str) -> str:
    """Aplica as regras 1 a 5 (espelho do bloco ds_catmat do SQL)."""
    if not txt:
        return txt
    txt = txt.replace("¿", "°").replace("*", "")
    txt = _RE_DOIS_PONTOS.sub(": ", txt)
    txt = _RE_ESPACO_VIRG.sub(",", txt)
    txt = _RE_ESPACOS.sub(" ", txt)
    return txt.strip()


def _milhar(n: int) -> str:
    return f"{n:,}".replace(",", ".")


def detectar_encoding(caminho: Path) -> str:
    """UTF-8 se o início do arquivo decodificar como UTF-8; senão latin-1.
    latin-1 nunca falha e regrava os bytes originais sem perda."""
    with open(caminho, "rb") as f:
        amostra = f.read(4 * 1024 * 1024)
    if amostra.startswith(b"\xef\xbb\xbf"):
        return "utf-8-sig"
    try:
        # final=False: tolera um caractere cortado no fim da amostra
        codecs.getincrementaldecoder("utf-8")().decode(amostra, final=False)
        return "utf-8"
    except UnicodeDecodeError:
        return "latin-1"


def detectar_fim_de_linha(caminho: Path, encoding: str) -> str:
    with open(caminho, "r", encoding=encoding, newline="") as f:
        primeira = f.readline()
    return "\r\n" if primeira.endswith("\r\n") else "\n"


def main() -> int:
    entrada = Path(sys.argv[1] if len(sys.argv) > 1 else ARQUIVO_ENTRADA)
    if len(sys.argv) > 2:
        saida = Path(sys.argv[2])
    elif ARQUIVO_SAIDA:
        saida = Path(ARQUIVO_SAIDA)
    else:
        saida = entrada.with_name(f"{entrada.stem}_tratado{entrada.suffix}")

    if not entrada.is_file():
        print(f"[ERRO] Arquivo de entrada não encontrado: {entrada}")
        return 1
    if saida.resolve() == entrada.resolve():
        print("[ERRO] A saída não pode ser o próprio arquivo de entrada.")
        return 1

    encoding = detectar_encoding(entrada) if ENCODING == "auto" else ENCODING
    fim_linha = detectar_fim_de_linha(entrada, encoding)
    csv.field_size_limit(2**31 - 1)

    print(f"Entrada : {entrada}")
    print(f"Saída   : {saida}")
    print(f"Encoding: {encoding} | fim de linha: {fim_linha!r}")

    inicio = time.time()
    total = alteradas = 0
    with open(entrada, "r", encoding=encoding, newline="") as fin, \
         open(saida, "w", encoding=encoding, newline="") as fout:
        leitor = csv.reader(fin, delimiter=DELIMITADOR, quotechar='"')
        escritor = csv.writer(fout, delimiter=DELIMITADOR, quotechar='"',
                              quoting=csv.QUOTE_MINIMAL, lineterminator=fim_linha)

        cabecalho = next(leitor)
        faltando = [c for c in COLUNAS_TRATAR if c not in cabecalho]
        if faltando:
            print(f"[ERRO] Coluna(s) não encontrada(s): {faltando}")
            print(f"       Colunas do arquivo: {cabecalho}")
            return 1
        indices = [cabecalho.index(c) for c in COLUNAS_TRATAR]
        escritor.writerow(cabecalho)

        for linha in leitor:
            total += 1
            mudou = False
            for i in indices:
                if i < len(linha):
                    novo = tratar_texto(linha[i])
                    if novo != linha[i]:
                        linha[i] = novo
                        mudou = True
            alteradas += mudou
            escritor.writerow(linha)
            if total % 200_000 == 0:
                print(f"  ... {_milhar(total)} linhas")

    seg = time.time() - inicio
    print(f"Concluído: {_milhar(total)} linhas lidas, "
          f"{_milhar(alteradas)} alteradas em {seg:.1f}s")
    return 0


if __name__ == "__main__":
    sys.exit(main())
