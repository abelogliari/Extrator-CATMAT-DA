-- =====================================================================================
-- BPS - EXTRACAO DE COMPRAS (schema dbbps)
-- -------------------------------------------------------------------------------------
-- 1) Filtro de classes: preencha a lista no bloco "filtro_classe" logo abaixo.
-- 2) Aliases em ASCII/camelCase: nomes validos como elemento XML (sem acento/espaco).
-- 3) Codigos (BR, PDM, ano, grupo, classe) saem como TEXTO -> evita "2,024" / "8,231".
-- 4) Saida com exatamente as 33 colunas da planilha final, na ordem 1..33.
--    As colunas de Grupo ficaram comentadas no fim do SELECT (nao sao exportadas).
-- 5) LIMPEZA EM DOIS ESTAGIOS:
--    a) bloco "ent" - generico, vale para os 15 campos de texto: remove tags HTML,
--       decodifica QUALQUER entidade numerica (&#NNN;) via CHR() e as nomeadas
--       (case-insensitive: pega &amp; e tambem &AMP;), converte espacos invisiveis
--       e normaliza espacos em branco.
--    b) bloco "lim" - especifico do descricaoCATMAT, mesmas regras do
--       tratar_catalogo.py: "¿" -> "°", sem "*", ": " apos dois-pontos, sem espaco
--       antes de virgula. Se mudar uma regra aqui, mude tambem no script Python.
--    ATENCAO: o Python ainda usa tabela fixa de entidades. Como o SQL agora
--    decodifica todas, os dois podem divergir em codigos fora daquela tabela.
-- OBS DBeaver: nao insira linhas em branco dentro da query (SQL Error [42601]).
-- =====================================================================================
WITH filtro_classe AS (
    -- Codigo de 4 digitos = grupo (2) + classe (2). Ex.: 6505, 6510, 6515...
    -- Escreva os codigos DENTRO das aspas simples da linha marcada, separados
    -- por virgula. Espacos sao ignorados. Aspas vazias ('') = TODAS as classes.
    --   uma classe .... '6505'
    --   16 classes ..... '6505, 6510, 6515, 6520, 6525, 6530, 6532, 6540,
    --                     6545, 6550, 6630, 6640, 6665, 6685, 7195, 8415'
    SELECT TRIM(x) AS cod_classe
    FROM UNNEST(STRING_TO_ARRAY(
        -- >>>>>>>>>>>>>>>>>>>  PREENCHA AS CLASSES NESTA LINHA ABAIXO NO ' ' <<<<<<<<<<<<<<<<<<
        '6505'
    , ',')) AS x
    WHERE TRIM(x) <> ''
)
SELECT
    -- ---------------------------------------------------------------- 1 a 6: ITEM
    tb_codigo_br.co_codigo_br::TEXT                     AS "codigoBR",
    lim.ds_catmat                                       AS "descricaoCATMAT",
    ent.ds_unidade_fornecimento                         AS "unidadeFornecimento",
    REPLACE(
        TO_CHAR(tb_unidade_fornecimento.nu_capacidade, 'FM999999990.00'),
        '.', ','
    )                                                   AS "capacidade",
    ent.no_unidade_medida                               AS "unidadeMedida",
    ent.ds_unidade_fornecimento
        || CASE WHEN tb_unidade_fornecimento.nu_capacidade IS NOT NULL
                THEN ' ' || REPLACE(
                        TO_CHAR(tb_unidade_fornecimento.nu_capacidade, 'FM999999990.00'),
                        '.', ','
                     )
                ELSE '' END
        || CASE WHEN ent.no_unidade_medida IS NOT NULL
                THEN ' ' || ent.no_unidade_medida
                ELSE '' END                             AS "unidadeFornecimentoCapacidade",
    -- ------------------------------------------------------ 7 a 10: CLASSE E PDM
    cls.cod_classe                                      AS "codigoClasse",
    ent.ds_classe                                       AS "descricaoClasse",
    tb_inc_pdm.co_inc::TEXT                             AS "pdm",
    ent.ds_pdm                                          AS "descricaoPDM",
    -- ---------------------------------------------------------- 11 a 12: REGISTRO
    tb_item.ds_registro_anvisa                          AS "anvisa",
    tb_item.st_generico                                 AS "generico",
    -- ------------------------------------------------------------ 13 a 17: COMPRA
    EXTRACT(YEAR FROM tb_compra.dt_homologacao)::INTEGER::TEXT AS "anoCompra",
    tb_compra.dt_homologacao                            AS "compra",
    tb_compra.dt_hora_compra_salva                      AS "insercao",
    ent.ds_modalidade                                   AS "modalidadeCompra",
    tb_compra.tp_compra                                 AS "tipoCompra",
    -- ------------------------------------------------------- 18 a 22: INSTITUICAO
    ent.no_instituicao                                  AS "nomeInstituicao",
    REGEXP_REPLACE(
        LPAD(tb_compra.ds_cnpj_usuario::TEXT, 14, '0'),
        '(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})',
        '\1.\2.\3/\4-\5'
    )                                                   AS "cnpjInstituicao",
    ent.no_municipio                                    AS "municipioInstituicao",
    tb_compra.sg_uf_cnpj                                AS "uf",
    ent.ds_esfera                                       AS "esfera",
    -- --------------------------------------------- 23 a 26: FORNECEDOR/FABRICANTE
    REGEXP_REPLACE(
        LPAD(rl_compra_item.ds_cnpj_fornecedor::TEXT, 14, '0'),
        '(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})',
        '\1.\2.\3/\4-\5'
    )                                                   AS "cnpjFornecedor",
    ent.no_fornecedor                                   AS "fornecedor",
    REGEXP_REPLACE(
        LPAD(rl_compra_item.ds_cnpj_fabricante::TEXT, 14, '0'),
        '(\d{2})(\d{3})(\d{3})(\d{4})(\d{2})',
        '\1.\2.\3/\4-\5'
    )                                                   AS "cnpjFabricante",
    ent.no_fabricante                                   AS "fabricante",
    -- ----------------------------------------------------------- 27 a 29: VALORES
    -- Se a quantidade sair como "1,000", troque a linha abaixo por:
    -- REPLACE(REPLACE(REPLACE(TO_CHAR(rl_compra_item.nu_quantidade,'FM999G999G999G990'),'.','#'),',','.'),'#',',')
    rl_compra_item.nu_quantidade                        AS "qtdItensComprados",
    REPLACE(
        REPLACE(
            REPLACE(
                TO_CHAR(rl_compra_item.nu_preco_unitario, 'FM999G999G999G990D0000'),
                '.', '#'
            ),
            ',', '.'
        ),
        '#', ','
    )                                                   AS "precoUnitario",
    REPLACE(
        REPLACE(
            REPLACE(
                TO_CHAR(
                    rl_compra_item.nu_quantidade * rl_compra_item.nu_preco_unitario,
                    'FM999G999G999G990D0000'
                ),
                '.', '#'
            ),
            ',', '.'
        ),
        '#', ','
    )                                                   AS "precoTotal",
    -- ---------------------------------------------- 30 a 33: PROCESSO/OBSERVACOES
    ent.ds_processo                                     AS "numeroProcesso",
    ent.ds_ata                                          AS "numeroAtaPrecos",
    tb_compra.nu_validade_compra                        AS "validadeCompra",
    ent.ds_observacoes                                  AS "observacoes"
    -- ----------------------------------------------------------------------------
    -- COLUNAS DE GRUPO: fora da exportacao. O codigo de classe de 4 digitos ja e
    -- montado internamente (bloco "cls" mais abaixo), entao o grupo nao precisa
    -- sair na planilha. Para trazer de volta, basta descomentar as duas linhas.
    -- , tb_grupo.co_codigo::TEXT                       AS "codigoGrupo"
    -- , ent.ds_grupo                                   AS "descricaoGrupo"
FROM dbbps.rl_compra_item
JOIN dbbps.tb_compra
    ON tb_compra.co_seq_compra = rl_compra_item.co_compra
JOIN dbbps.tb_modalidade_compra
    ON tb_modalidade_compra.co_seq_modalidade_compra = tb_compra.co_modalidade
JOIN dbbps.tb_codigo_br
    ON tb_codigo_br.co_seq_codigo_br = rl_compra_item.co_codigo_br
JOIN dbbps.tb_unidade_fornecimento
    ON tb_unidade_fornecimento.co_seq_unidade_fornecimento = rl_compra_item.co_unidade_fornecimento
LEFT JOIN dbbps.tb_item
    ON tb_item.co_seq_item = rl_compra_item.co_item
LEFT JOIN dbbps.tb_unidade_medida
    ON tb_unidade_medida.co_seq_unidade_medida = tb_unidade_fornecimento.co_unidade_medida
-- HIERARQUIA CATMAT: Codigo BR -> PDM -> Classe -> Grupo
LEFT JOIN dbbps.tb_inc_pdm
    ON tb_inc_pdm.co_seq_inc_pdm = tb_codigo_br.co_inc_pdm
   AND tb_inc_pdm.st_registro_ativo = 'S'
LEFT JOIN dbbps.tb_classe
    ON tb_classe.co_seq_classe = tb_inc_pdm.co_classe
   AND tb_classe.st_registro_ativo = 'S'
LEFT JOIN dbbps.tb_grupo
    ON tb_grupo.co_seq_grupo = tb_classe.co_grupo
   AND tb_grupo.st_registro_ativo = 'S'
JOIN dbbps.tb_instituicao AS forn
    ON forn.co_seq_instituicao = rl_compra_item.co_fornecedor
   AND forn.st_registro_ativo = 'S'
JOIN dbbps.tb_instituicao AS fabr
    ON fabr.co_seq_instituicao = rl_compra_item.co_fabricante
   AND fabr.st_registro_ativo = 'S'
LEFT JOIN dbbps.tb_instituicao AS inst_compradora
    ON inst_compradora.nu_cnpj = tb_compra.ds_cnpj_usuario
   AND inst_compradora.st_registro_ativo = 'S'
-- CODIGO DE CLASSE COMPOSTO (grupo + classe), calculado uma unica vez
CROSS JOIN LATERAL (
    SELECT CASE
             WHEN tb_grupo.co_codigo IS NOT NULL AND tb_classe.co_codigo IS NOT NULL
             THEN LPAD(TRIM(tb_grupo.co_codigo::TEXT),  2, '0')
               || LPAD(TRIM(tb_classe.co_codigo::TEXT), 2, '0')
             ELSE NULL
           END AS cod_classe
) AS cls
-- =====================================================================================
-- ESTAGIO A - LIMPEZA GENERICA ("ent0" monta o array, "ent" da nome aos itens)
--   1) tags de BLOCO (br, p, div, td, li, ...) viram ESPACO -> nao cola palavras
--   2) qualquer outra tag (font, u, sup, o:p, desconhecidas) e removida
--   3) entidade numerica: QUALQUER &#NNN; vira o caractere via CHR()
--   4) entidades nomeadas, case-insensitive (a base tem "&LT;" e "&AMP;" em CAIXA
--      ALTA; REPLACE simples nao pegaria). O "&amp;" fica por ULTIMO, senao
--      "&amp;#205;" viraria letra em vez do literal "&#205;".
--      Detalhe: em REGEXP_REPLACE o "&" no texto de substituicao significa "a
--      correspondencia inteira", por isso o &amp; passa por CHR(1) e so depois
--      vira "&" com REPLACE.
--   5) TRANSLATE dos espacos INVISIVEIS: CHR(160) = NBSP (732 ocorrencias no
--      ds_catmat) e CHR(8239) = espaco estreito. Eles NAO sao capturados por
--      [[:space:]] na maioria dos locales e chegariam na planilha como caractere
--      invisivel, quebrando PROCV e comparacao exata.
--   6) colapso de espacos/tabs/quebras + TRIM (pega CHR(9) e CHR(10) decodificados)
-- O teste POSITION('&#' ...) = 0 manda as linhas sem entidade pelo caminho rapido.
-- PARA ACRESCENTAR UM CAMPO: some ao ARRAY[...] e crie o apelido no bloco "ent".
-- -------------------------------------------------------------------------------------
-- NORMALIZACOES OPCIONAIS: descomente SEMPRE AOS PARES (a linha da lista de origem
-- e a linha correspondente da lista de destino, na mesma ordem). Conferidas contra
-- o inventario real de entidades do ds_catmat:
--   micro:      CHR(181) sinal micro -> CHR(956) mu grego   (26 e 31 ocorrencias,
--               visualmente identicos; sem isso "µG" e "μG" nao casam entre si)
--   ponto:      CHR(8901) operador   -> CHR(183) ponto medio  (4 e 16)
--   tipografia: CHR(8220) CHR(8221) aspas curvas -> aspas retas, CHR(8211) -> hifen
--   subscritos: CHR(8320..8329) formulas quimicas -> digitos normais (~191 no total)
--   gregas:     CHR(924) M grego e CHR(913) A grego -> M e A latinos (18 e 1
--               ocorrencias; CONFIRA as linhas antes, pode ser intencional)
-- =====================================================================================
CROSS JOIN LATERAL (
    SELECT ARRAY(
        SELECT CASE WHEN d.v IS NULL THEN NULL ELSE
                    TRIM(REGEXP_REPLACE(
                        TRANSLATE(
                            REPLACE(
                                REGEXP_REPLACE(
                                    REGEXP_REPLACE(
                                        REGEXP_REPLACE(
                                            REGEXP_REPLACE(
                                                REGEXP_REPLACE(
                                                    REGEXP_REPLACE(d.v, '&nbsp;', ' ', 'gi'),
                                                '&quot;', '"', 'gi'),
                                            '&apos;', '''', 'gi'),
                                        '&lt;', '<', 'gi'),
                                    '&gt;', '>', 'gi'),
                                '&amp;', CHR(1), 'gi'),
                            CHR(1), '&'),
                            CHR(160) || CHR(8239)
                            --  || CHR(181)
                            --  || CHR(8901)
                            --  || CHR(8220) || CHR(8221) || CHR(8211)
                            --  || CHR(8320) || CHR(8321) || CHR(8322) || CHR(8323) || CHR(8324)
                            --  || CHR(8325) || CHR(8326) || CHR(8327) || CHR(8328) || CHR(8329)
                            --  || CHR(924) || CHR(913)
                            ,
                            '  '
                            --  || CHR(956)
                            --  || CHR(183)
                            --  || '""-'
                            --  || '01234'
                            --  || '56789'
                            --  || 'MA'
                        ),
                    '[[:space:]]+', ' ', 'g'))
               END
        FROM (
            SELECT u.i AS i,
                   CASE
                     WHEN u.raw IS NULL THEN NULL
                     WHEN POSITION('&#' IN u.raw) = 0 THEN tg.v
                     ELSE COALESCE((
                       SELECT STRING_AGG(
                                CASE WHEN tk.m[1] ~ '^&#[0-9]{1,7};$'
                                      AND SUBSTRING(tk.m[1] FROM 3 FOR LENGTH(tk.m[1]) - 3)::INT
                                          BETWEEN 1 AND 1114111
                                     THEN CHR(SUBSTRING(tk.m[1] FROM 3 FOR LENGTH(tk.m[1]) - 3)::INT)
                                     ELSE tk.m[1]
                                END, '' ORDER BY tk.ord)
                       FROM REGEXP_MATCHES(tg.v, '&#[0-9]{1,7};|[^&]+|&', 'g')
                            WITH ORDINALITY AS tk(m, ord)
                     ), '')
                   END AS v
            FROM UNNEST(ARRAY[
                     tb_codigo_br.ds_catmat::TEXT,
                     tb_inc_pdm.ds_pdm::TEXT,
                     tb_classe.ds_classe::TEXT,
                     tb_grupo.ds_grupo::TEXT,
                     tb_unidade_fornecimento.ds_unidade_fornecimento::TEXT,
                     tb_unidade_medida.no_nome::TEXT,
                     tb_compra.no_instituicao::TEXT,
                     tb_compra.no_municipio::TEXT,
                     COALESCE(tb_compra.ds_esfera, inst_compradora.ds_esfera)::TEXT,
                     tb_modalidade_compra.ds_modalidade::TEXT,
                     forn.ds_razao_social::TEXT,
                     fabr.ds_razao_social::TEXT,
                     tb_compra.ds_processo::TEXT,
                     tb_compra.ds_ata::TEXT,
                     tb_compra.ds_observacoes::TEXT
                 ]) WITH ORDINALITY AS u(raw, i)
            CROSS JOIN LATERAL (
                SELECT REGEXP_REPLACE(
                         REGEXP_REPLACE(u.raw,
                           '</?(br|p|div|tr|td|th|li|ul|ol|table|h[1-6])[^>]*>', ' ', 'gi'),
                         '<[^>]+>', '', 'g') AS v
            ) AS tg
        ) AS d
        ORDER BY d.i
    ) AS t
) AS ent0
CROSS JOIN LATERAL (
    SELECT ent0.t[1]  AS ds_catmat,
           ent0.t[2]  AS ds_pdm,
           ent0.t[3]  AS ds_classe,
           ent0.t[4]  AS ds_grupo,
           ent0.t[5]  AS ds_unidade_fornecimento,
           ent0.t[6]  AS no_unidade_medida,
           ent0.t[7]  AS no_instituicao,
           ent0.t[8]  AS no_municipio,
           ent0.t[9]  AS ds_esfera,
           ent0.t[10] AS ds_modalidade,
           ent0.t[11] AS no_fornecedor,
           ent0.t[12] AS no_fabricante,
           ent0.t[13] AS ds_processo,
           ent0.t[14] AS ds_ata,
           ent0.t[15] AS ds_observacoes
) AS ent
-- =====================================================================================
-- ESTAGIO B - REGRAS DO CATALOGO OFICIAL, so no descricaoCATMAT.
-- Espelhar qualquer mudanca daqui no tratar_catalogo.py.
-- OBS 1: "¿" -> "°" trata o grau corrompido na origem. A base tambem tem 449
-- ocorrencias de "&#176;", que o estagio A ja decodifica direto para "°" -- os
-- dois caminhos convergem no mesmo caractere.
-- OBS 2: se algum registro trouxer "&#191;" (interrogacao invertida legitima), o
-- estagio A decodifica para "¿" e esta regra converte para "°". Nao aparece no
-- inventario atual; se aparecer, restringir o REPLACE aqui.
-- =====================================================================================
CROSS JOIN LATERAL (
    SELECT TRIM(REGEXP_REPLACE(REGEXP_REPLACE(REGEXP_REPLACE(
               REPLACE(REPLACE(ent.ds_catmat, '¿', '°'), '*', ''),
           '[[:space:]]*:[[:space:]]*', ': ', 'g'),
           '[[:space:]]+,', ',', 'g'),
           '[[:space:]]+', ' ', 'g'))                   AS ds_catmat
) AS lim
WHERE rl_compra_item.st_registro_ativo = 'S'
  AND tb_compra.st_registro_ativo = 'S'
  -- PERIODO: 01/01/2021 ate 31/12/2026 (altere as duas datas se precisar)
  AND tb_compra.dt_homologacao >= DATE '2021-01-01'
  AND tb_compra.dt_homologacao <  DATE '2027-01-01'
  -- FILTRO DE CLASSES: se a lista do topo estiver vazia, traz todas
  AND (
        NOT EXISTS (SELECT 1 FROM filtro_classe)
        OR cls.cod_classe IN (SELECT cod_classe FROM filtro_classe)
      )
;