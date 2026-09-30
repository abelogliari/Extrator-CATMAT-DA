# Extrator de CATMATs Pro

Ferramenta desktop para extração de registros de preços praticados do **Portal de Compras Governamentais** (`dadosabertos.compras.gov.br`), desenvolvida para o **BPS/DESID — Ministério da Saúde**.

O programa percorre a hierarquia do catálogo de materiais do Governo Federal e consolida os registros em planilhas prontas para análise:

```
Classe  →  PDMs  →  CATMATs  →  Registros de Preços
```

**Arquivo principal:** `ExtratorCatmat.py` · Python 3.8+ · CustomTkinter

---

## Índice

- [Instalação e dependências](#instalação-e-dependências)
- [Extração sem interface](#extração-sem-interface)
- [As duas abas](#as-duas-abas)
- [Funcionalidades](#funcionalidades)
- [Arquitetura](#arquitetura)
- [Desempenho](#desempenho)
- [Histórico do projeto](#histórico-do-projeto)
- [Testes](#testes)
- [Pontos em aberto](#pontos-em-aberto)
- [Requisitos originais](#requisitos-originais)

---

## Instalação e dependências

```bash
pip install requests pandas openpyxl customtkinter lxml
python ExtratorCatmat.py
```

O `lxml` é opcional, mas **fortemente recomendado**: o `openpyxl` o usa como acelerador de serialização quando disponível, e a diferença na gravação de planilhas grandes é significativa.

Distribuição em produção: executável gerado com PyInstaller e instalador criado no **Inno Setup**.

---

## Extração sem interface

`extrator_comprasgov.py` é só a parte que conecta no Compras.gov, isolada do `ExtratorCatmat.py`: o mesmo motor (cota de requisições, parser de CSV, paginação, writers em streaming, relatório de integridade), sem CustomTkinter, sem a aba BPS e sem a consolidação DW + DA. Depende apenas de `requests`, `pandas` e `openpyxl`.

Como na interface ("Usar a descrição do BPS", marcada por padrão), o `descricaoItem` de cada registro é trocado pela descrição do mesmo CATMAT no BPS, limpa com as regras do `extracao_bps.sql`. CATMAT sem compra no BPS fica com o texto do Compras.gov. A consulta é uma chamada leve por CATMAT, sem filtro de data. O relatório de integridade ganha a coluna `descricao BPS`, que indica os CATMATs sem compra no BPS e os que o BPS não respondeu. `--sem-descricao-bps` desliga a troca.

```bash
python extrator_comprasgov.py codigos.xlsx                    # CATMATs do arquivo
python extrator_comprasgov.py codigos.xlsx -t PDM --por-classe
python extrator_comprasgov.py -c "451234;451235" -i 01-01-2025 -f 31-12-2025
python extrator_comprasgov.py --classes "6505;6515" -s saida  # Classe → PDMs → CATMATs → registros
python extrator_comprasgov.py -h                              # todas as opções
```

A saída é a mesma da interface (`dados_completos_extraidos_part1.xlsx` ou um `classe_XXXX_part1` por classe, mais o relatório de integridade). Diferenças: códigos com erro de API passam pela fila de retry (15 s → 30 s) também na extração por lista de códigos, e o relatório marca `ERRO_API_PERSISTENTE` e `NAO PROCESSADO (cancelado)` em vez de mostrar esses códigos como `OK`. Ctrl+C cancela e salva o que já foi baixado; um segundo Ctrl+C encerra na hora.

As funções foram copiadas, não importadas: o `ExtratorCatmat.py` continua autossuficiente para o PyInstaller. Uma correção no motor precisa ser feita nos dois arquivos.

---

## As duas abas

### 1. Extração por CATMAT

Recebe um `.xlsx` ou `.csv` com a lista de códigos e extrai os registros de preços.

| Campo | Descrição |
|---|---|
| Arquivo de Códigos | Planilha com a coluna `codigoItemCatalogo`, `codigoPdm` ou a genérica `codigo` |
| Buscar por | `CATMAT` ou `PDM` — espelha o parâmetro `tipo` da API |
| Salvar um arquivo por classe | Separa a saída em `classe_XXXX` |
| Pasta de destino | Exigida quando o modo por classe está ativo |
| Data de Início / Data Final | Filtro `DD-MM-AAAA` sobre a data da compra |
| Formato de Saída | `.xlsx` ou `.csv` |
| Salvar cópias dos CSV corrompidos | Guarda as páginas com perda para inspeção |

### 2. Extração por Classes

Informa uma ou mais classes (separadas por `;`) e o programa executa o pipeline completo — **sem interação intermediária**:

```
Classe → PDMs → CATMATs → Registros → classe_XXXX.xlsx + Relatorio_Integridade_XXXX.xlsx
```

Opções da aba: um arquivo por classe, pasta de destino, e **extração direta por PDM** (`tipo=codigoPdm`), que dispensa a expansão PDM → CATMAT e reduz drasticamente o número de requisições.

---

## Funcionalidades

### Busca por CATMAT ou PDM (parâmetro `tipo`)

A API de Pesquisa de Preço passou a exigir os parâmetros `tipo` e `codigo`:

```
/modulo-pesquisa-preco/1.1_consultarMaterial_CSV
    ?pagina=1&tamanhoPagina=500
    &tipo={codigoItemCatalogo|codigoPdm}
    &codigo={codigo}
```

O extrator envia essa assinatura e, se o servidor recusar (HTTP 400/404), refaz a chamada no formato antigo (`codigoItemCatalogo=`) automaticamente. A detecção acontece **uma única vez por execução** e fica memorizada em `_API_ACEITA_TIPO` — sem custo de requisição dupla nas páginas seguintes.

Na busca por PDM, o `codigoItemCatalogo` devolvido pela API é **preservado** (a resposta traz vários CATMATs) e o PDM de origem é gravado na coluna `codigoPdm`.

### Um arquivo por classe

Disponível nas duas abas. A classe é resolvida em duas etapas:

1. **Coluna `classe` no arquivo de entrada** (aceita `Classe` ou `codigoClasse`), quando presente. Permite agrupar por qualquer critério próprio, não só pela classe oficial.
2. **Campo `codigoClasse` dos próprios registros**, quando o arquivo não traz a coluna. A API já devolve `codigoClasse` e `nomeClasse` em cada registro de preço, então o agrupamento **não custa nenhuma requisição adicional**.

Registros sem classe identificável vão para `classe_sem_classe`. Caracteres inválidos para nome de arquivo são substituídos por `_`.

**Pasta de destino.** É exigida *antes* do início: quem clicar em Iniciar sem preenchê-la recebe um pop-up para escolher a pasta, e a extração só começa depois disso. Os arquivos de cada classe são gravados direto nessa pasta ao longo da execução, junto com o relatório de integridade — nenhum diálogo ao final.

### Coluna "Unidade de Fornecimento"

Campo derivado, montado com estas regras:

- Usa `nomeUnidadeFornecimento` quando presente, ignorando `siglaUnidadeFornecimento`
- Se o nome estiver vazio, traduz `siglaUnidadeFornecimento` pelo mapa de abreviações (`FR-AM` → `Frasco-Ampola`, com hífen)
- Ignora `capacidadeUnidadeFornecimento` quando vale `0,00`
- Quando a capacidade está ausente ou zerada, **omite também** `siglaUnidadeMedida` — a unidade de medida não significa nada sem a quantidade

### Tratamento de CSVs corrompidos

O detector original era heurístico: contava `;` por linha com `str.split()` sobre linhas obtidas de `str.splitlines()`. Isso produzia três classes de falha:

1. `str.splitlines()` quebra em `\x0b \x0c \x1c-\x1e \x85 \u2028 \u2029`, que o parser CSV e o servidor tratam como texto comum — fragmentos fantasma.
2. Contar `;` ignorando aspas marcava campos citados com `;` interno como "excesso de colunas" e remontava a linha torta.
3. Linhas contendo apenas `"` eram descartadas — isso apagava o fechamento de um campo multilinha e o parser engolia os registros seguintes.

A versão atual usa `csv.reader` sobre o texto bruto (que já resolve quebras dentro de campos citados), remonta no **nível de campo** — quando a quebra cai dentro de um campo, o último campo do fragmento e o primeiro do seguinte são as duas metades do mesmo campo — e recolhe `;` excedente de volta para `descricaoItem` em vez de descartar a linha.

O diagnóstico final não é heurístico: compara o número de registros lidos com o esperado da página. É essa conferência que garante que nenhuma página problemática passe batido.

Numa bateria de 11 páginas sintéticas cobrindo os modos de falha conhecidos, o parser antigo perdia ou desalinhava registros em **7**; o novo acerta as **11**.

O painel distingue dois números:

| | significado |
|---|---|
| **Reparadas** | a página precisou de conserto, mas todos os registros foram recuperados |
| **Com Perda** | não foi possível reconstituir a página — registros faltando |

O contador antigo misturava os dois e, além disso, acusava falsos positivos (campo citado com `;`, `\x0b`, `\u2028` eram marcados como corrompidos com os dados intactos).

### Paginação

O rodapé da resposta (`totalPaginas`) é a fonte primária. O cálculo `ceil(totalRegistros / 500)` entra como conferência e, quando o rodapé falta, como substituto — o código antigo caía em `1` nesse caso e truncava a extração nos 500 primeiros registros em silêncio.

A leitura tolera separador de milhar: `\d+` sozinho capturaria apenas o `1` em `totalRegistros: 1.234`.

Quando o rodapé de páginas não vem e a última página volta cheia (500 registros), o extrator faz uma requisição extra para confirmar que acabou. Com o rodapé presente, nenhuma requisição a mais é feita.

### Log de erros de API

Todo erro de rede ou de API é registrado com a **fonte** onde aconteceu:

| fonte | onde |
|---|---|
| `DA` | Compras.gov, Pesquisa de Preço (registros de preço) |
| `CATÁLOGO` | Compras.gov, catálogo de materiais (PDMs e CATMATs) |
| `BPS` | API de Dados Abertos do Ministério da Saúde |
| `PROGRAMA` | erro interno do extrator, não da API (mostra função e linha) |

Cada ocorrência aparece no log da tela (🔴) com o endpoint, o código, a página, o tipo de erro traduzido (HTTP 400/403/404/429/500/502/503/504, timeout, falha de DNS, conexão recusada/interrompida, erro de SSL/proxy, resposta que não é CSV/JSON) e o trecho da resposta da API. Também é gravada em `Log_Erros_API_<data>.csv`, na pasta de destino ou na pasta do programa, com a URL completa para reproduzir no navegador. O arquivo só é criado se houver erro.

Ao final da extração, a **apuração** agrupa as ocorrências por fonte e tipo, com a quantidade e os códigos afetados, e informa quantas vezes o Compras.gov pediu pausa por limite de requisições (HTTP 429). No relatório de integridade, os códigos com erro saem com o motivo (`ERRO_API_PERSISTENTE — DA: HTTP 503 (...)`) em vez de `OK`.

Uma resposta HTTP 200 que não é o CSV da Pesquisa de Preço, como uma página HTML de manutenção, deixou de ser contada como "0 registros": vira erro e entra no retry.

### Relatório de integridade

Ao final de cada extração é gerado um `Relatorio_Integridade.xlsx` (ou `Relatorio_Integridade_XXXX.xlsx` por classe) com uma linha por código:

| coluna | conteúdo |
|---|---|
| `codigoItemCatalogo` / `codigoPdm` | cabeçalho acompanha o tipo de busca |
| `esperados` | `totalRegistros` informado pela API |
| `baixados` | registros efetivamente gravados |
| `paginas` | páginas com perda |
| `status` | `OK`, `OK (divergencia: n/m)` até 2 registros, ou `Inconsistencia Grave` |

---

## Arquitetura

**Threading.** A extração roda em `threading.Thread`; toda atualização de interface passa por `self.after(0, fn)`. Variáveis Tk (`StringVar`, `BooleanVar`) são capturadas na thread principal antes do disparo — Tk não é thread-safe.

**Concorrência.** `ThreadPoolExecutor` com 5 workers para as buscas PDM → CATMAT. A extração de registros de preços é **sequencial** (1 worker): a versão paralela causava erros em massa na API.

**Rede.** `requests.Session` compartilhada com `pool_maxsize=12`, reaproveitando conexões TCP/TLS (~150–350 ms por requisição). Intervalos: `0,2 s` entre páginas de PDM/CATMAT, `0,5 s` entre páginas de registros de preços.

**Retry em três níveis.**

| nível | política |
|---|---|
| Classes sem PDMs | reprocessadas após as demais — 3 s → 8 s |
| PDMs sem CATMATs | até 3 tentativas — 3 s → 8 s |
| CATMATs com erro de API | fila de retry ao fim da classe — 15 s → 30 s |
| HTTP 429 | espera 15 s, depois 30 s |

**Pausa e cancelamento.** `threading.Event` inicializado como liberado — sem isso, um `wait()` bloqueia para sempre em qualquer fluxo que esqueça de chamar `.set()`. A checagem acontece **entre páginas**, não só entre códigos: um CATMAT com 40 páginas responde ao botão em segundos, não em minutos.

**Queda de rede.** `ERRO_CONEXAO` pausa automaticamente com contagem regressiva de 60 s — não cancela. A extração retoma de onde parou.

**Saída.** `utf-8-sig` (UTF-8 com BOM) e separador `;` em todos os CSVs. Rollover automático a cada 1 milhão de linhas (`_part1`, `_part2`…). O `CSVChunkWriter` reindexa cada página pelo cabeçalho da primeira — sem isso, uma página com menos colunas grava valores sob o cabeçalho errado.

---

## Desempenho

### Escrita de `.xlsx` em streaming

O modo padrão do `openpyxl` mantém a planilha inteira em memória e serializa tudo no `save()`, concentrando o custo no encerramento — justamente onde o usuário espera parado. O `ExcelChunkWriter` usa `Workbook(write_only=True)`, que envia as linhas para disco conforme chegam.

Medido com 100 mil linhas × 39 colunas:

| | durante | **no final** | pico de RAM |
|---|---|---|---|
| in-memory (anterior) | 28,4 s | **40,2 s** | 825 MB |
| streaming (atual) | 38,8 s | **3,0 s** | ~0 MB |

O tempo "durante" é absorvido pelas esperas de rede; o "no final" é espera pura. Extrapolando para 230 mil registros: a espera no encerramento cai de **~92 s para ~7 s**, e o pico de memória de **~1,9 GB para praticamente nada**. O tempo total do writer também melhora: 68,6 s → 41,8 s.

Comparação de motores (30 mil linhas): `openpyxl` normal 16,6 s · `write_only` 10,3 s · `xlsxwriter` com `constant_memory` 7,5 s. O `xlsxwriter` é mais rápido no total, mas o `write_only` ganha onde importa (3,0 s contra 1,7 s de fechamento é irrelevante perto de 40 s) e não exige dependência nova no instalador.

### Proteção contra queda

Em `write_only` o arquivo só pode ser salvo uma vez, então não há como reescrevê-lo periodicamente. No lugar disso, cada `.xlsx` em andamento mantém um espelho `<nome>.parcial.csv` gravado em append, **apagado quando o `.xlsx` fecha com sucesso** e preservado se algo falhar. O custo é indistinguível de zero: 18,5 s com espelho contra 18,8 s sem, em 50 mil linhas.

> Se um `.parcial.csv` sobrar na pasta ao final de uma execução, é sinal de que o `save()` falhou — e esse arquivo contém tudo o que havia sido baixado.

Para saída em CSV nada disso é necessário: a escrita sempre foi em append (~16 ms por página de 500 linhas), o que a torna mais resiliente a queda em extrações longas.

---

## Histórico do projeto

### 2025 — Primeira geração (`catmatvNN.py`)

| Data | Marco |
|---|---|
| **15/09/2025** | Commit inicial (`catmatv14.py`, `catmatv15.py`) e primeiro executável |
| **21/09/2025** | Extração por PDM e Classe (`catmatv15` → `v18`); correção da coluna Preço Total |
| **22/09/2025** | Versões `v19` e `v20`; executável v18 |
| **23–24/09/2025** | Ajustes voltados à classe 6505 (Drogas e Medicamentos) |
| **25/09/2025** | Novo instalador (Inno Setup) e limpeza do repositório |
| **24/10/2025** | Versão em espanhol (`catmatv20 espanhol`) |

### 2026 — Segunda geração (`ExtratorCatmat.py`)

| Data | Marco |
|---|---|
| **29/01/2026** | Escolha de formato de saída: CSV ou Excel |
| **15/04/2026** | **Nova interface gráfica** em CustomTkinter; código reestruturado e versões antigas removidas. Nasce o `ExtratorCatmat.py` |
| **15/04/2026** | Paralelismo nas requisições e otimização geral — e, no mesmo dia, **remoção do paralelismo** na busca de CATMATs: causava erros de acesso a arquivos sem ganho real de desempenho |
| **16/04/2026** | Primeiro tratamento de páginas corrompidas |
| **17/04/2026** | Correção de Pausar/Retomar, retomada automática após queda de rede e do bug de `NoneType` em listas vazias |
| **21/04/2026** | Sessão longa de refatoração: `_extracao_thread` em `threading.Thread` (a UI congelava com `_tick` na thread principal), restauração do layout da aba 2, `pausar_busca_catmat` que bloqueava silenciosamente o fluxo automatizado, retry em três níveis, `requests.Session` compartilhada, e a lógica definitiva da coluna Unidade de Fornecimento |
| **22/04/2026** | Commit "Unidade de Fornecimento corrigida" |
| **16/07/2026** | Auditoria de encoding dos CSVs: confirmado `utf-8-sig` + `;` como padrão; identificada a inconsistência do `CATMATs_descobertos.csv`, que omitia o BOM |
| **11/08/2026** | Estudo de viabilidade de migração para web (arquitetura de jobs, rate limiter global compartilhado, cache da hierarquia Classe → PDM → CATMAT). Conclusão: o desafio não é trocar Tkinter por HTML, é o modelo de execução — processo longo com estado em memória e disco local |
| **11/08/2026** | **Suporte ao parâmetro `tipo`** (CATMAT ou PDM) com fallback automático para a assinatura antiga; **reescrita do parser de CSV**; remoção de um bloco de ~545 linhas duplicado que sobrescrevia silenciosamente a versão nova da lógica de negócio; correção de desalinhamento de colunas no `CSVChunkWriter`; sanitização de caracteres de controle no Excel |
| **12/08/2026** | Separação dos contadores **Reparadas · Com Perda**; paginação com rodapé como fonte primária e tolerância a separador de milhar; **um arquivo por classe na aba 1**; pasta de destino exigida no início; **escrita em streaming** (`write_only`) com espelho `.parcial.csv` |

### Bugs relevantes encontrados no caminho

- **Bloco duplicado (11/08/2026).** As linhas 2099–2643 repetiam toda a camada de negócio numa versão *mais antiga*. Como Python executa de cima para baixo, a segunda cópia sobrescrevia a primeira: o que rodava era a versão velha, sem o retry com backoff da busca de PDMs. Qualquer correção feita na primeira cópia seria silenciosamente ignorada.
- **Desalinhamento no CSV.** `processar_dataframe_final` descarta colunas 100% vazias *por página*. Sem reindexar no `to_csv(mode="a")`, uma página com menos colunas gravava valores sob cabeçalho errado — `precoUnitario` caindo em `marca`.
- **`IllegalCharacterError` em massa.** Um único `\x0b` no texto livre da API derrubaria a extração inteira no `wb.save()`.
- **`cancelar_busca_catmat` sem reset.** Um cancelamento no explorador silenciava os retries de PDM de todas as classes seguintes na mesma sessão.
- **`except Exception` que mente.** Um `KeyError` interno em `processar_dataframe_final` era reportado como "sem resposta após 3 tentativas", como se fosse falha da API.
- **Contador enganoso.** Depois da reescrita do parser, "Páginas Corrigidas" foi a zero em 229.951 registros — não porque o detector estivesse morto, mas porque o significado do contador havia mudado sem o nome mudar junto.

---

## Pontos em aberto

- **Memória na busca por PDM.** O worker acumula todas as páginas antes de escrever. Com `tipo=codigoPdm`, um único código pode trazer dezenas de milhares de registros — gravar por página evitaria o pico.
- **`_http.verify = False`** com warnings suprimidos, desde o início do projeto. Vale tentar `verify=True` e só cair para `False` se o certificado do gov.br falhar de fato.
- **`processar_dataframe_final` descarta colunas por página.** Mesmo com o writer corrigido, a decisão certa seria descartar só no final, olhando o conjunto todo.
- **`except Exception` genérico no worker** mascara erros locais como falha de API.
- **Duplicação menor:** `_pdms_selecionados_codigos` e `_pdms_sel` são funções idênticas.
- **Migração para web** (ver 11/08/2026): exige arquitetura de jobs, rate limiter global e cache da hierarquia.

---

## Requisitos originais

**Prioritários**

- [x] Consertar os CSVs corrompidos
- [x] Criação do instalador (Inno Setup)
- [x] Extração por PDM e Classe
- [x] Busca por PDM **ou** CATMAT via parâmetro `tipo` da API
- [x] Checkbox para salvar ou não os arquivos corrompidos
- [x] Excluir colunas `nomeUnidadeMedida` e `percentualMaiorDesconto` quando vazias
- [ ] Avaliar migração da interface para PySide6/PyQt6 ou web (Flask + HTML)
- [ ] Gerar PDF oficial para licitações (modelo BPS-Legado)

**Desejáveis**

- [x] Texto de boas-vindas na interface

**Possíveis**

- [ ] Formatar a tabela final em "Verde Claro, Estilo de Tabela Média 28" no Excel
