# -*- coding: utf-8 -*-
"""
Etapa 10 - Endereços declarados por presidiário.

Refatoração RMC:
- script puro para execução por etapa;
- sem notebook;
- sem consulta API;
- sem amostras pesadas;
- normalização de endereço;
- enriquecimento por CEP local em bronze.tbl_cep_es;
- transporte de complemento/referência/observação para a ponte pessoa-endereço;
- criação de endereço único pela hierarquia: CEP+NÚMERO, LOGRADOURO+NÚMERO+BAIRRO+MUNICÍPIO, LOGRADOURO+BAIRRO+MUNICÍPIO;
- materialização física controlada em gold.tmp_* entre blocos críticos;
- publicação final em gold.sinp_ent_endereco, gold.sinp_pnt_pessoa_endereco e gold.sinp_endereco_sem_chave;
- envio ao Postgres somente no final, com log isolado e arquivos finais reparticionados para evitar estouro no toLocalIterator.
"""

from contexto import *
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.storagelevel import StorageLevel
import os


def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

    # ============================================================
    # 00 - REFRESH DAS ORIGENS
    # ============================================================

    tabelas_origem = [
        "bronze.infopen_enderecos_preso",
        "bronze.infopen_geral_municipios",
        "bronze.tbl_cep_es",
        "gold.sinp_pnt_pessoa_preso",
    ]

    for tabela_origem in tabelas_origem:
        spark.sql(f"REFRESH TABLE {tabela_origem}")

    spark.catalog.clearCache()

    # ============================================================
    # 00.1 - LIMPEZA INICIAL DAS TEMPORÁRIAS
    # ============================================================

    temporarias = [
        "tmp_sinp_end_norm_01",
        "tmp_sinp_endereco_trabalho_01",
        "tmp_sinp_endereco_cep_ref_es",
        "tmp_sinp_endereco_base_cep",
        "tmp_sinp_endereco_municipio_cep_map",
        "tmp_sinp_endereco_cep_candidatos",
        "tmp_sinp_endereco_cep_escolhido",
        "tmp_sinp_endereco_trabalho_02",
        "tmp_sinp_endereco_cep_geo_ref",
        "tmp_sinp_endereco_pessoa_preso",
        "tmp_sinp_endereco_base_chave",
        "tmp_sinp_endereco_sem_chave_01",
        "tmp_sinp_endereco_unico_01",
        "tmp_sinp_pnt_pessoa_endereco_01",
    ]

    for tabela in temporarias:
        spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    spark.catalog.clearCache()



    # ============================================================
    # 01 - NORMALIZAÇÃO COMPLETA
    # ============================================================

    # =============================================================================
    # NORMALIZAÇÃO COMPLETA - ENDEREÇOS DECLARADOS POR PRESIDIÁRIO
    # Origem principal : bronze.infopen_enderecos_preso
    # Apoio município  : bronze.infopen_geral_municipios
    # Saída            : df_end_norm_01
    #
    # Objetivo:
    # - Normalizar endereço declarado pelo preso
    # - Enriquecer com município/UF
    # - Separar logradouro, tipo, número e complemento
    # - Preservar campos brutos
    # - Gerar chaves preliminares para análise de compartilhamento
    # =============================================================================

    # -----------------------------------------------------------------------------
    # Refresh das tabelas
    # -----------------------------------------------------------------------------

    spark.sql("refresh table bronze.infopen_enderecos_preso")
    spark.sql("refresh table bronze.infopen_geral_municipios")

    df_raw = spark.table("bronze.infopen_enderecos_preso")

    # -----------------------------------------------------------------------------
    # Função de normalização textual
    # -----------------------------------------------------------------------------

    def normaliza_expr(expr):
        c = F.upper(F.trim(expr.cast("string")))

        c = F.translate(
            c,
            "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ",
            "AAAAAEEEEIIIIOOOOOUUUUCN"
        )

        c = F.regexp_replace(c, r"[\r\n\t]+", " ")
        c = F.regexp_replace(c, r"\s+", " ")
        c = F.trim(c)

        c = F.when(
            c.isNull()
            | (c == "")
            | (c.isin("NULL", "[NULL]", "NULO", "NA", "N/A", "-", ".", "0")),
            F.lit(None)
        ).otherwise(c)

        return c


    # -----------------------------------------------------------------------------
    # Município normalizado
    # -----------------------------------------------------------------------------

    df_municipio = (
        spark.table("bronze.infopen_geral_municipios")
        .select(
            F.col("id_municipio").cast("long").alias("id_municipio"),
            F.upper(F.trim(F.col("municipio_siglauf").cast("string"))).alias("municipio_uf"),
            F.upper(F.trim(F.col("municipio_nome").cast("string"))).alias("municipio_nome_raw")
        )
        .withColumn(
            "municipio_nome_norm",
            F.translate(
                F.col("municipio_nome_raw"),
                "ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇÑ",
                "AAAAAEEEEIIIIOOOOOUUUUCN"
            )
        )
        .withColumn("municipio_nome_norm", F.regexp_replace(F.col("municipio_nome_norm"), r"[\r\n\t]+", " "))
        .withColumn("municipio_nome_norm", F.regexp_replace(F.col("municipio_nome_norm"), r"\s+", " "))
        .withColumn("municipio_nome_norm", F.trim(F.col("municipio_nome_norm")))
        .dropDuplicates(["id_municipio"])
    )

    # -----------------------------------------------------------------------------
    # Base bruta preservada + campos textuais normalizados
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_raw
        .select(
            F.col("id_enderecopreso").cast("long").alias("id_enderecopreso"),
            F.col("id_preso").cast("long").alias("id_preso"),
            F.col("id_municipio").cast("long").alias("id_municipio"),

            F.col("enderecopreso_bairro").alias("bairro_raw"),
            F.col("enderecopreso_logradouro").alias("logradouro_raw"),
            F.col("enderecopreso_complemento").alias("complemento_raw"),
            F.col("enderecopreso_referencia").alias("referencia_raw"),
            F.col("enderecopreso_observacao").alias("observacao_raw"),
            F.col("enderecopreso_cep").alias("cep_raw")
        )
        .join(df_municipio, "id_municipio", "left")
        .withColumn("bairro_norm", normaliza_expr(F.col("bairro_raw")))
        .withColumn("logradouro_base", normaliza_expr(F.col("logradouro_raw")))
        .withColumn("complemento_base", normaliza_expr(F.col("complemento_raw")))
        .withColumn("referencia_norm", normaliza_expr(F.col("referencia_raw")))
        .withColumn("observacao_norm", normaliza_expr(F.col("observacao_raw")))
    )

    # -----------------------------------------------------------------------------
    # Classificação do tipo de endereço declarado
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "tp_endereco_declarado",
            F.when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DA\s+MAE\b"), F.lit("MAE"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DO\s+PAI\b"), F.lit("PAI"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DA\s+ESPOSA\b"), F.lit("ESPOSA"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DO\s+PADRINHO\b"), F.lit("PADRINHO"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DA\s+AVO\b"), F.lit("AVO"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DO\s+AVO\b"), F.lit("AVO"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DA\s+IRMA\b"), F.lit("IRMA"))
             .when(F.col("logradouro_base").rlike(r"^ENDERECO\s+DO\s+IRMAO\b"), F.lit("IRMAO"))
             .when(F.col("observacao_norm").rlike(r"CASA DA MAE|RESIDENCIA MATERNA|ENDERECO DA MAE"), F.lit("MAE"))
             .when(F.col("observacao_norm").rlike(r"CASA DO PAI|RESIDENCIA PATERNA|ENDERECO DO PAI"), F.lit("PAI"))
             .when(F.col("observacao_norm").rlike(r"RESIDENCIA ESPOSA|CASA DA ESPOSA|ENDERECO DA ESPOSA"), F.lit("ESPOSA"))
             .otherwise(F.lit("DECLARADO"))
        )
    )

    # -----------------------------------------------------------------------------
    # Remoção de prefixos operacionais e relacionais do logradouro
    # -----------------------------------------------------------------------------

    regex_prefixo_relacional = r"^ENDERECO\s+(DA|DO|DE)\s+[^:]{1,100}:\s*"

    regex_prefixo_operacional = (
        r"^("
        r"ENDERECO\s+RESIDENCIAL|"
        r"ENDERECO|"
        r"END\.|"
        r"ENDER\.|"
        r"LOCALIZACAO|"
        r"LOGRADOURO"
        r")\s*[:\-]?\s*"
    )

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_base"), regex_prefixo_relacional, ""))
        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_norm"), regex_prefixo_operacional, ""))

        # Normalização de prefixos de tipo mal digitados no início
        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_norm"), r"^RUA\s*:\s*", "RUA "))
        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_norm"), r"^R\s*:\s*", "RUA "))
        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_norm"), r"^AV\s*\.?\s*:\s*", "AVENIDA "))
        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_norm"), r"^AVENIDA\s*:\s*", "AVENIDA "))

        .withColumn("logradouro_norm", F.regexp_replace(F.col("logradouro_norm"), r"\s+", " "))
        .withColumn("logradouro_norm", F.trim(F.col("logradouro_norm")))

        # Bairro
        .withColumn("bairro_norm", F.regexp_replace(F.col("bairro_norm"), r"^BAIRRO\s*[:\-]?\s*", ""))
        .withColumn("bairro_norm", F.regexp_replace(F.col("bairro_norm"), r"\s+", " "))
        .withColumn("bairro_norm", F.trim(F.col("bairro_norm")))

        # Complemento
        .withColumn("complemento_norm", F.col("complemento_base"))
        .withColumn("complemento_norm", F.regexp_replace(F.col("complemento_norm"), r"^COMPLEMENTO\s*[:\-]?\s*", ""))
        .withColumn("complemento_norm", F.regexp_replace(F.col("complemento_norm"), r"\s+", " "))
        .withColumn("complemento_norm", F.trim(F.col("complemento_norm")))

        # Referência e observação
        .withColumn("referencia_norm", F.regexp_replace(F.col("referencia_norm"), r"^REFERENCIA\s*[:\-]?\s*", ""))
        .withColumn("observacao_norm", F.regexp_replace(F.col("observacao_norm"), r"^OBSERVACAO\s*[:\-]?\s*", ""))
    )

    # -----------------------------------------------------------------------------
    # CEP
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn("cep_num", F.regexp_replace(F.col("cep_raw").cast("string"), r"[^0-9]", ""))
        .withColumn(
            "cep_norm",
            F.when(
                (F.length("cep_num") == 8)
                & (~F.col("cep_num").isin("00000000", "99999999")),
                F.col("cep_num")
            )
        )
        .drop("cep_num")
    )

    # -----------------------------------------------------------------------------
    # Flags de qualidade / situação do endereço
    # -----------------------------------------------------------------------------

    regex_morador_rua = r"(MORADOR DE RUA|MORADORA DE RUA|MORADO DE RUA|SITUACAO DE RUA|EM SITUACAO DE RUA)"
    regex_zona_rural = r"(ZONA RURAL|ZONA RUAL|ZONA RUARAL|AREA RURAL|CORREGO|SITIO|FAZENDA|ASSENTAMENTO|COMUNIDADE|INTERIOR)"
    regex_nao_informado = r"(NAO INFORMADO|NAO INFORMADA|NAO CADASTRADO|NAO CADASTRADA|NAO SABE|NAO SOUBE|NAO INFORMOU|NAO DECLARADO|PREJUDICADO|SEM COMPROVANTE|INCERTO)"
    regex_generico = r"(RUA PROJETADA|RUA PRINCIPAL|RUA NAO CADASTRADA|RUA NAO INFORMADA|ZONA RURAL|MORADOR DE RUA|SITUACAO DE RUA|ENDERECO INCERTO)"

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "fl_morador_rua",
            F.when(F.col("logradouro_norm").rlike(regex_morador_rua), F.lit("S")).otherwise(F.lit("N"))
        )
        .withColumn(
            "fl_zona_rural",
            F.when(
                F.col("bairro_norm").rlike(regex_zona_rural)
                | F.col("logradouro_norm").rlike(regex_zona_rural)
                | F.col("complemento_norm").rlike(regex_zona_rural)
                | F.col("referencia_norm").rlike(regex_zona_rural),
                F.lit("S")
            ).otherwise(F.lit("N"))
        )
        .withColumn(
            "fl_logradouro_nao_informado",
            F.when(
                F.col("logradouro_norm").isNull()
                | F.col("logradouro_norm").rlike(regex_nao_informado),
                F.lit("S")
            ).otherwise(F.lit("N"))
        )
        .withColumn(
            "fl_logradouro_generico",
            F.when(F.col("logradouro_norm").rlike(regex_generico), F.lit("S")).otherwise(F.lit("N"))
        )
    )

    # -----------------------------------------------------------------------------
    # Normalização segura de S/N e marcadores de número
    # Regra crítica:
    # - S/N antes de número
    # - N, Nº, N°, NR, NRO, NUMERO só viram NUMERO se vierem antes de dígito
    # -----------------------------------------------------------------------------

    regex_numero_marcador = r"\b(?:NUMERO|NRO|NR|N)\s*[º°\.]*\s*[:=\.]?\s*(?=[0-9])"

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn("logradouro_work", F.col("logradouro_norm"))
        .withColumn("complemento_work", F.col("complemento_norm"))

        # S/N no logradouro
        .withColumn("logradouro_work", F.regexp_replace(F.col("logradouro_work"), r"\bS\s*/\s*N[º°\.]*\b", " SEM_NUMERO "))
        .withColumn("logradouro_work", F.regexp_replace(F.col("logradouro_work"), r"\bSN\b", " SEM_NUMERO "))
        .withColumn("logradouro_work", F.regexp_replace(F.col("logradouro_work"), r"\bS\s+N\b", " SEM_NUMERO "))

        # S/N no complemento
        .withColumn("complemento_work", F.regexp_replace(F.col("complemento_work"), r"\bS\s*/\s*N[º°\.]*\b", " SEM_NUMERO "))
        .withColumn("complemento_work", F.regexp_replace(F.col("complemento_work"), r"\bSN\b", " SEM_NUMERO "))
        .withColumn("complemento_work", F.regexp_replace(F.col("complemento_work"), r"\bS\s+N\b", " SEM_NUMERO "))

        # Marcadores de número
        .withColumn("logradouro_work", F.regexp_replace(F.col("logradouro_work"), regex_numero_marcador, " NUMERO "))
        .withColumn("complemento_work", F.regexp_replace(F.col("complemento_work"), regex_numero_marcador, " NUMERO "))

        .withColumn("logradouro_work", F.regexp_replace(F.col("logradouro_work"), r"\s+", " "))
        .withColumn("logradouro_work", F.trim(F.col("logradouro_work")))

        .withColumn("complemento_work", F.regexp_replace(F.col("complemento_work"), r"\s+", " "))
        .withColumn("complemento_work", F.trim(F.col("complemento_work")))
    )

    # -----------------------------------------------------------------------------
    # Flag sem número
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "fl_sem_numero",
            F.when(
                F.col("logradouro_work").rlike(r"\bSEM_NUMERO\b")
                | F.col("complemento_work").rlike(r"\bSEM_NUMERO\b")
                | F.col("logradouro_work").rlike(r"\bSEM NUMERO\b")
                | F.col("complemento_work").rlike(r"\bSEM NUMERO\b"),
                F.lit("S")
            ).otherwise(F.lit("N"))
        )
    )

    # -----------------------------------------------------------------------------
    # Extração de número do imóvel
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "numero_exp_logradouro",
            F.regexp_extract(F.col("logradouro_work"), r"\bNUMERO\s+([0-9]{1,6})\b", 1)
        )
        .withColumn(
            "numero_virgula_logradouro",
            F.regexp_extract(F.col("logradouro_work"), r",\s*([0-9]{1,6})(?=\s*(?:$|[-,;/ ]))", 1)
        )
        .withColumn(
            "numero_exp_complemento",
            F.regexp_extract(F.col("complemento_work"), r"\bNUMERO\s+([0-9]{1,6})\b", 1)
        )
        .withColumn(
            "numero_complemento_solto",
            F.regexp_extract(F.col("complemento_work"), r"^([0-9]{1,6})$", 1)
        )
        .withColumn(
            "numero_norm",
            F.when(F.col("numero_exp_logradouro") != "", F.col("numero_exp_logradouro"))
             .when(F.col("numero_virgula_logradouro") != "", F.col("numero_virgula_logradouro"))
             .when(F.col("numero_exp_complemento") != "", F.col("numero_exp_complemento"))
             .when(F.col("numero_complemento_solto") != "", F.col("numero_complemento_solto"))
        )
        .withColumn(
            "numero_norm",
            F.when(F.col("fl_sem_numero") == "S", F.lit(None)).otherwise(F.col("numero_norm"))
        )
    )

    # -----------------------------------------------------------------------------
    # Extração estruturada de complemento
    # A extração considera complemento_raw e complemento embutido no logradouro
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01

        # QUADRA
        .withColumn(
            "quadra_complemento",
            F.regexp_extract(
                F.col("complemento_work"),
                r"\b(?:QUADRA|QD)\.?\s*[:.,\-]?\s*[\"“”']?\s*([A-Z0-9]+)",
                1
            )
        )
        .withColumn(
            "quadra_logradouro",
            F.regexp_extract(
                F.col("logradouro_work"),
                r"\b(?:QUADRA|QD)\.?\s*[:.,\-]?\s*[\"“”']?\s*([A-Z0-9]+)",
                1
            )
        )

        # LOTE
        .withColumn(
            "lote_complemento",
            F.regexp_extract(
                F.col("complemento_work"),
                r"\b(?:LOTE|LT)\.?\s*[:.,\-]?\s*[\"“”']?\s*([A-Z0-9]+)",
                1
            )
        )
        .withColumn(
            "lote_logradouro",
            F.regexp_extract(
                F.col("logradouro_work"),
                r"\b(?:LOTE|LT)\.?\s*[:.,\-]?\s*[\"“”']?\s*([A-Z0-9]+)",
                1
            )
        )

        # BLOCO
        .withColumn(
            "bloco_complemento",
            F.regexp_extract(
                F.col("complemento_work"),
                r"\b(?:BLOCO|BLQ)\.?\s*[:.,\-]?\s*[\"“”']?\s*([A-Z0-9]+)",
                1
            )
        )
        .withColumn(
            "bloco_logradouro",
            F.regexp_extract(
                F.col("logradouro_work"),
                r"\b(?:BLOCO|BLQ)\.?\s*[:.,\-]?\s*[\"“”']?\s*([A-Z0-9]+)",
                1
            )
        )

        # CASA
        .withColumn(
            "casa_complemento",
            F.regexp_extract(
                F.col("complemento_work"),
                r"\bCASA\.?\s*[:.,\-]?\s*([A-Z0-9]+)",
                1
            )
        )
        .withColumn(
            "casa_logradouro",
            F.regexp_extract(
                F.col("logradouro_work"),
                r"\bCASA\.?\s*[:.,\-]?\s*([A-Z0-9]+)",
                1
            )
        )

        # APTO / APT / APARTAMENTO / AP
        .withColumn(
            "apto_complemento",
            F.regexp_extract(
                F.col("complemento_work"),
                r"\b(?:APTO|APT|APARTAMENTO|AP)\.?\s*[:.,\-]?\s*([A-Z0-9]+)",
                1
            )
        )
        .withColumn(
            "apto_logradouro",
            F.regexp_extract(
                F.col("logradouro_work"),
                r"\b(?:APTO|APT|APARTAMENTO|AP)\.?\s*[:.,\-]?\s*([A-Z0-9]+)",
                1
            )
        )

        # ANDAR
        .withColumn(
            "andar_complemento",
            F.regexp_extract(
                F.col("complemento_work"),
                r"\b([0-9]{1,2})\s*[º°]?\s*ANDAR\b",
                1
            )
        )
        .withColumn(
            "andar_logradouro",
            F.regexp_extract(
                F.col("logradouro_work"),
                r"\b([0-9]{1,2})\s*[º°]?\s*ANDAR\b",
                1
            )
        )
    )

    # -----------------------------------------------------------------------------
    # Converter strings vazias de complemento para null
    # -----------------------------------------------------------------------------

    campos_extraidos_complemento = [
        "quadra_complemento", "quadra_logradouro",
        "lote_complemento", "lote_logradouro",
        "bloco_complemento", "bloco_logradouro",
        "casa_complemento", "casa_logradouro",
        "apto_complemento", "apto_logradouro",
        "andar_complemento", "andar_logradouro"
    ]

    for c in campos_extraidos_complemento:
        df_end_norm_01 = df_end_norm_01.withColumn(
            c,
            F.when(F.col(c) != "", F.col(c))
        )

    # -----------------------------------------------------------------------------
    # Consolidar complemento estruturado
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn("quadra_norm", F.coalesce(F.col("quadra_complemento"), F.col("quadra_logradouro")))
        .withColumn("lote_norm", F.coalesce(F.col("lote_complemento"), F.col("lote_logradouro")))
        .withColumn("bloco_norm", F.coalesce(F.col("bloco_complemento"), F.col("bloco_logradouro")))
        .withColumn("casa_norm", F.coalesce(F.col("casa_complemento"), F.col("casa_logradouro")))
        .withColumn("apto_norm", F.coalesce(F.col("apto_complemento"), F.col("apto_logradouro")))
        .withColumn("andar_norm", F.coalesce(F.col("andar_complemento"), F.col("andar_logradouro")))
        .withColumn(
            "fl_fundos",
            F.when(
                F.col("complemento_work").rlike(r"\bFUNDOS\b")
                | F.col("logradouro_work").rlike(r"\bFUNDOS\b"),
                F.lit("S")
            ).otherwise(F.lit("N"))
        )
        .withColumn(
            "complemento_estruturado_norm",
            F.concat_ws(
                " | ",
                F.when(F.col("quadra_norm").isNotNull(), F.concat(F.lit("QUADRA "), F.col("quadra_norm"))),
                F.when(F.col("lote_norm").isNotNull(), F.concat(F.lit("LOTE "), F.col("lote_norm"))),
                F.when(F.col("bloco_norm").isNotNull(), F.concat(F.lit("BLOCO "), F.col("bloco_norm"))),
                F.when(F.col("casa_norm").isNotNull(), F.concat(F.lit("CASA "), F.col("casa_norm"))),
                F.when(F.col("apto_norm").isNotNull(), F.concat(F.lit("APTO "), F.col("apto_norm"))),
                F.when(F.col("andar_norm").isNotNull(), F.concat(F.lit("ANDAR "), F.col("andar_norm"))),
                F.when(F.col("fl_fundos") == "S", F.lit("FUNDOS"))
            )
        )
        .withColumn(
            "complemento_estruturado_norm",
            F.when(F.col("complemento_estruturado_norm") != "", F.col("complemento_estruturado_norm"))
        )
        .withColumn(
            "complemento_final_norm",
            F.coalesce(F.col("complemento_estruturado_norm"), F.col("complemento_norm"))
        )
    )

    # -----------------------------------------------------------------------------
    # Construção do logradouro sem número e sem complemento embutido
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn("logradouro_sem_numero", F.col("logradouro_work"))

        # Remove número explícito
        .withColumn(
            "logradouro_sem_numero",
            F.regexp_replace(
                F.col("logradouro_sem_numero"),
                r"\bNUMERO\s+[0-9]{1,6}\s*[-/]?\s*[A-Z]?\b",
                " "
            )
        )

        # Remove número por vírgula
        .withColumn(
            "logradouro_sem_numero",
            F.regexp_replace(
                F.col("logradouro_sem_numero"),
                r",\s*[0-9]{1,6}(?=\s*(?:$|[-,;/ ]))",
                " "
            )
        )

        # Remove S/N
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bSEM_NUMERO\b", " "))

        # Remove trechos acessórios
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r",?\s*BAIRRO\s+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r",?\s*MUNICIPIO\s+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r",?\s*PROX\.?\s+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r",?\s*PROXIMO\s+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r",?\s*PX\s+.*$", " "))

        # Remove complemento embutido
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bQUADRA\s*[:\.]?\s*[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bQD\.?\s*[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bLOTE\s*[:\.]?\s*[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bLT\.?\s*[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bBLOCO\s*[-:]?\s*[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bBLQ\s+[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bCASA\s+[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bAPTO\s+[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bAPT\s+[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bAP\s+[A-Z0-9]+.*$", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\bAPARTAMENTO\s+[A-Z0-9]+.*$", " "))

        # Limpeza final
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"[^A-Z0-9 ]", " "))
        .withColumn("logradouro_sem_numero", F.regexp_replace(F.col("logradouro_sem_numero"), r"\s+", " "))
        .withColumn("logradouro_sem_numero", F.trim(F.col("logradouro_sem_numero")))
    )

    # -----------------------------------------------------------------------------
    # Tipo de logradouro
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "tp_logradouro_norm",
            F.when(F.col("logradouro_sem_numero").rlike(r"^RUA\b|^R\b"), F.lit("RUA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^AVENIDA\b|^AV\b"), F.lit("AVENIDA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^BECO\b"), F.lit("BECO"))
             .when(F.col("logradouro_sem_numero").rlike(r"^ESCADARIA\b"), F.lit("ESCADARIA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^TRAVESSA\b|^TV\b"), F.lit("TRAVESSA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^RODOVIA\b|^ROD\b"), F.lit("RODOVIA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^ESTRADA\b"), F.lit("ESTRADA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^ALAMEDA\b"), F.lit("ALAMEDA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^PRACA\b"), F.lit("PRACA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^LADEIRA\b"), F.lit("LADEIRA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^VIELA\b"), F.lit("VIELA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^CORREGO\b"), F.lit("CORREGO"))
             .when(F.col("logradouro_sem_numero").rlike(r"^SITIO\b"), F.lit("SITIO"))
             .when(F.col("logradouro_sem_numero").rlike(r"^FAZENDA\b"), F.lit("FAZENDA"))
             .when(F.col("logradouro_sem_numero").rlike(r"^BR\s+[0-9]+"), F.lit("RODOVIA"))
        )
    )

    # -----------------------------------------------------------------------------
    # Nome do logradouro
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "logradouro_nome_norm",
            F.regexp_replace(
                F.col("logradouro_sem_numero"),
                r"^(RUA|R|AVENIDA|AV|BECO|ESCADARIA|TRAVESSA|TV|RODOVIA|ROD|ESTRADA|ALAMEDA|PRACA|LADEIRA|VIELA|CORREGO|SITIO|FAZENDA)\b\s*",
                ""
            )
        )
        .withColumn("logradouro_nome_norm", F.regexp_replace(F.col("logradouro_nome_norm"), r"[^A-Z0-9 ]", " "))
        .withColumn("logradouro_nome_norm", F.regexp_replace(F.col("logradouro_nome_norm"), r"\s+", " "))
        .withColumn("logradouro_nome_norm", F.trim(F.col("logradouro_nome_norm")))
        .withColumn(
            "logradouro_nome_norm",
            F.when(F.col("logradouro_nome_norm") == "", F.lit(None)).otherwise(F.col("logradouro_nome_norm"))
        )
    )

    # -----------------------------------------------------------------------------
    # Flag de ausência de endereço real
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "fl_endereco_sem_dado_real",
            F.when(
                (F.col("fl_logradouro_nao_informado") == "S")
                | F.col("logradouro_nome_norm").isin(
                    "INCERTO",
                    "DO PAI",
                    "DA MAE",
                    "DA ESPOSA",
                    "DO PADRINHO",
                    "SEM ENDERECO FIXO",
                    "SEM RESIDENCIA FIXA",
                    "OUTRO LOCAL"
                ),
                F.lit("S")
            ).otherwise(F.lit("N"))
        )
    )

    # -----------------------------------------------------------------------------
    # Chaves preliminares
    # -----------------------------------------------------------------------------

    df_end_norm_01 = (
        df_end_norm_01
        .withColumn(
            "chave_endereco_norm_01",
            F.md5(
                F.concat_ws(
                    "|",
                    F.coalesce(F.col("id_municipio").cast("string"), F.lit("[NULL]")),
                    F.coalesce(F.col("bairro_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("tp_logradouro_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("logradouro_nome_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("numero_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("cep_norm"), F.lit("[NULL]"))
                )
            )
        )
        .withColumn(
            "chave_endereco_unidade_norm_01",
            F.md5(
                F.concat_ws(
                    "|",
                    F.coalesce(F.col("id_municipio").cast("string"), F.lit("[NULL]")),
                    F.coalesce(F.col("bairro_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("tp_logradouro_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("logradouro_nome_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("numero_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("quadra_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("lote_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("bloco_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("casa_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("apto_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("andar_norm"), F.lit("[NULL]")),
                    F.coalesce(F.col("cep_norm"), F.lit("[NULL]"))
                )
            )
        )
    )


    # ============================================================
    # 02 - MATERIALIZAÇÃO DA NORMALIZAÇÃO
    # ============================================================

    tabela = "tmp_sinp_end_norm_01"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_end_norm_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_end_norm_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_end_norm_01 = spark.table(f"gold.{tabela}")
    df_end_norm_01.createOrReplaceTempView("vw_end_norm_01")


    # ============================================================
    # 03 - TABELA INICIAL DE TRABALHO
    # ============================================================

    from pyspark.storagelevel import StorageLevel

    # =============================================================================
    # TABELA NORMALIZADA INICIAL DE TRABALHO - ENDEREÇOS DECLARADOS
    #
    # Origem lógica : df_end_norm_01
    # Base          : Amostra Final (2) + CEP
    # Saída         : df_endereco_trabalho_01
    # View temp     : vw_endereco_trabalho_01
    #
    # NÃO grava em gold.
    # NÃO envia ao Postgres.
    # NÃO usa schema sinp.
    # =============================================================================

    # -----------------------------------------------------------------------------
    # Validação de dependência
    # -----------------------------------------------------------------------------

    campos_obrigatorios = [
        "id_enderecopreso",
        "id_preso",
        "id_municipio",
        "municipio_uf",
        "municipio_nome_norm",
        "bairro_norm",
        "tp_logradouro_norm",
        "logradouro_nome_norm",
        "numero_norm",
        "complemento_final_norm",
        "referencia_norm",
        "observacao_norm",
        "cep_raw",
        "cep_norm"
    ]

    campos_ausentes = [c for c in campos_obrigatorios if c not in df_end_norm_01.columns]

    if campos_ausentes:
        raise Exception(f"Campos ausentes em df_end_norm_01: {campos_ausentes}")

    # -----------------------------------------------------------------------------
    # Montagem da tabela inicial de trabalho
    # -----------------------------------------------------------------------------

    df_endereco_trabalho_01 = (
        df_end_norm_01
        .select(
            F.col("id_enderecopreso").cast("long").alias("id_enderecopreso"),
            F.col("id_preso").cast("long").alias("id_preso"),
            F.col("id_municipio").cast("long").alias("id_municipio"),

            F.col("municipio_uf").cast("string").alias("municipio_uf"),
            F.col("municipio_nome_norm").cast("string").alias("municipio_nome_norm"),

            F.col("bairro_norm").cast("string").alias("bairro_norm"),
            F.col("tp_logradouro_norm").cast("string").alias("tp_logradouro_norm"),
            F.col("logradouro_nome_norm").cast("string").alias("logradouro_nome_norm"),
            F.col("numero_norm").cast("string").alias("numero_norm"),

            # CEP presente mesmo que majoritariamente nulo
            F.col("cep_raw").cast("string").alias("cep_raw"),
            F.col("cep_norm").cast("string").alias("cep_norm"),

            F.col("complemento_final_norm").cast("string").alias("complemento_final_norm"),
            F.col("referencia_norm").cast("string").alias("referencia_norm"),
            F.col("observacao_norm").cast("string").alias("observacao_norm")
        )
        .withColumn(
            "id_endereco_trabalho_01",
            F.substring(
                F.md5(
                    F.concat_ws(
                        "|",
                        F.coalesce(F.col("id_enderecopreso").cast("string"), F.lit("[NULL]")),
                        F.coalesce(F.col("id_preso").cast("string"), F.lit("[NULL]")),
                        F.coalesce(F.col("id_municipio").cast("string"), F.lit("[NULL]"))
                    )
                ),
                1,
                30
            )
        )
        .withColumn(
            "ds_endereco_trabalho",
            F.concat_ws(
                " | ",
                F.when(F.col("municipio_uf").isNotNull(), F.concat(F.lit("UF: "), F.col("municipio_uf"))),
                F.when(F.col("municipio_nome_norm").isNotNull(), F.concat(F.lit("MUNICIPIO: "), F.col("municipio_nome_norm"))),
                F.when(F.col("bairro_norm").isNotNull(), F.concat(F.lit("BAIRRO: "), F.col("bairro_norm"))),
                F.when(
                    F.col("tp_logradouro_norm").isNotNull() | F.col("logradouro_nome_norm").isNotNull(),
                    F.concat_ws(" ", F.col("tp_logradouro_norm"), F.col("logradouro_nome_norm"))
                ),
                F.when(F.col("numero_norm").isNotNull(), F.concat(F.lit("N: "), F.col("numero_norm"))),
                F.when(F.col("cep_norm").isNotNull(), F.concat(F.lit("CEP: "), F.col("cep_norm"))),
                F.when(F.col("complemento_final_norm").isNotNull(), F.concat(F.lit("COMPL: "), F.col("complemento_final_norm"))),
                F.when(F.col("referencia_norm").isNotNull(), F.concat(F.lit("REF: "), F.col("referencia_norm"))),
                F.when(F.col("observacao_norm").isNotNull(), F.concat(F.lit("OBS: "), F.col("observacao_norm")))
            )
        )
        .select(
            "id_endereco_trabalho_01",
            "id_enderecopreso",
            "id_preso",
            "id_municipio",
            "municipio_uf",
            "municipio_nome_norm",
            "bairro_norm",
            "tp_logradouro_norm",
            "logradouro_nome_norm",
            "numero_norm",
            "cep_raw",
            "cep_norm",
            "complemento_final_norm",
            "referencia_norm",
            "observacao_norm",
            "ds_endereco_trabalho"
        )
    )

    # -----------------------------------------------------------------------------
    # Persistência apenas em memória/disco no Jupyter para análise
    # -----------------------------------------------------------------------------

    tabela = "tmp_sinp_endereco_trabalho_01"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_endereco_trabalho_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_endereco_trabalho_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_endereco_trabalho_01 = spark.table(f"gold.{tabela}")
    df_endereco_trabalho_01.createOrReplaceTempView("vw_endereco_trabalho_01")


    # ============================================================
    # 04 - ENRIQUECIMENTO DE CEP LOCAL
    # ============================================================

    from pyspark.storagelevel import StorageLevel

    # =============================================================================
    # CONSTRUÇÃO - ENRIQUECIMENTO DE CEP DOS ENDEREÇOS DECLARADOS
    #
    # Entrada:
    #   df_endereco_trabalho_01
    #   bronze.tbl_cep_es
    #
    # Saída:
    #   df_endereco_trabalho_02
    #   vw_endereco_trabalho_02
    #
    # Não grava em gold.
    # Não envia Postgres.
    # Não chama API.
    # =============================================================================

    # -----------------------------------------------------------------------------
    # Dependências
    # -----------------------------------------------------------------------------

    spark.sql("REFRESH TABLE bronze.tbl_cep_es")

    # -----------------------------------------------------------------------------
    # Campos obrigatórios da base de endereços
    # -----------------------------------------------------------------------------

    campos_obrigatorios_endereco = [
        "id_endereco_trabalho_01",
        "id_enderecopreso",
        "id_preso",
        "id_municipio",
        "municipio_uf",
        "municipio_nome_norm",
        "bairro_norm",
        "tp_logradouro_norm",
        "logradouro_nome_norm",
        "numero_norm",
        "cep_raw",
        "cep_norm",
        "complemento_final_norm",
        "referencia_norm",
        "observacao_norm",
        "ds_endereco_trabalho"
    ]

    campos_ausentes_endereco = [
        c for c in campos_obrigatorios_endereco
        if c not in df_endereco_trabalho_01.columns
    ]

    if campos_ausentes_endereco:
        raise Exception(f"Campos ausentes em df_endereco_trabalho_01: {campos_ausentes_endereco}")

    # -----------------------------------------------------------------------------
    # Referência CEP Aberto ES
    # -----------------------------------------------------------------------------

    df_cep_ref_es = (
        spark.table("bronze.tbl_cep_es")
        .select(
            F.col("id_cepaberto_es").cast("string").alias("id_cepaberto_es"),
            F.col("cep").cast("string").alias("cep_ref"),
            F.col("bairro_norm").cast("string").alias("bairro_ref_norm"),
            F.col("tp_logradouro_norm").cast("string").alias("tp_logradouro_ref_norm"),
            F.col("logradouro_nome_norm").cast("string").alias("logradouro_ref_nome_norm"),
            F.col("cidade_cepaberto_id").cast("string").alias("cidade_cepaberto_id"),
            F.col("estado_cepaberto_id").cast("string").alias("estado_cepaberto_id"),
            F.col("cep_lado").cast("string").alias("cep_lado"),
            F.col("cep_num_ini").cast("int").alias("cep_num_ini"),
            F.col("cep_num_fim").cast("int").alias("cep_num_fim"),
            F.col("fl_cep_tem_faixa_numero").cast("string").alias("fl_cep_tem_faixa_numero")
        )
        .filter(F.length("cep_ref") == 8)
        .dropDuplicates()
    )

    tabela = "tmp_sinp_endereco_cep_ref_es"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_cep_ref_es.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_cep_ref_es, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_cep_ref_es = spark.table(f"gold.{tabela}")
    df_cep_ref_es.createOrReplaceTempView("vw_cep_ref_es")

    # -----------------------------------------------------------------------------
    # Base de endereços preparada
    # -----------------------------------------------------------------------------

    df_end_base_cep = (
        df_endereco_trabalho_01
        .withColumn(
            "cep_original_norm",
            F.when(F.length(F.regexp_replace(F.col("cep_norm").cast("string"), r"[^0-9]", "")) == 8,
                   F.regexp_replace(F.col("cep_norm").cast("string"), r"[^0-9]", ""))
        )
        .withColumn(
            "numero_norm_int",
            F.when(
                F.length(F.regexp_replace(F.col("numero_norm").cast("string"), r"[^0-9]", "")) > 0,
                F.regexp_replace(F.col("numero_norm").cast("string"), r"[^0-9]", "").cast("int")
            )
        )
        .withColumn("municipio_uf", F.upper(F.trim(F.col("municipio_uf"))))
        .withColumn("municipio_nome_norm", F.upper(F.trim(F.col("municipio_nome_norm"))))
        .withColumn("bairro_norm", F.upper(F.trim(F.col("bairro_norm"))))
        .withColumn("tp_logradouro_norm", F.upper(F.trim(F.col("tp_logradouro_norm"))))
        .withColumn("logradouro_nome_norm", F.upper(F.trim(F.col("logradouro_nome_norm"))))
    )

    tabela = "tmp_sinp_endereco_base_cep"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_end_base_cep.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_end_base_cep, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_end_base_cep = spark.table(f"gold.{tabela}")
    df_end_base_cep.createOrReplaceTempView("vw_end_base_cep")

    # =============================================================================
    # 1. MAPEAR id_municipio -> cidade_cepaberto_id
    # -----------------------------------------------------------------------------
    # Como bronze.tbl_cep_es não traz nome do município, o vínculo mais seguro
    # é inferido pelos registros da nossa base que já possuem CEP original.
    # =============================================================================

    df_municipio_cep_map_base = (
        df_end_base_cep.alias("e")
        .filter(F.col("e.municipio_uf") == "ES")
        .filter(F.col("e.cep_original_norm").isNotNull())
        .join(
            df_cep_ref_es.select(
                F.col("cep_ref").alias("cep_original_norm"),
                "cidade_cepaberto_id"
            ).dropDuplicates(),
            "cep_original_norm",
            "inner"
        )
        .groupBy(
            "e.id_municipio",
            "e.municipio_uf",
            "e.municipio_nome_norm",
            "cidade_cepaberto_id"
        )
        .agg(
            F.count("*").alias("qtd_vinculos_cep")
        )
    )

    w_mun = Window.partitionBy("id_municipio").orderBy(
        F.desc("qtd_vinculos_cep"),
        F.asc("cidade_cepaberto_id")
    )

    df_municipio_cep_map_01 = (
        df_municipio_cep_map_base
        .withColumn("rn", F.row_number().over(w_mun))
        .filter(F.col("rn") == 1)
        .drop("rn")
    )

    tabela = "tmp_sinp_endereco_municipio_cep_map"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_municipio_cep_map_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_municipio_cep_map_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_municipio_cep_map_01 = spark.table(f"gold.{tabela}")
    df_municipio_cep_map_01.createOrReplaceTempView("vw_municipio_cep_map_01")

    # -----------------------------------------------------------------------------
    # Endereços com cidade CEP Aberto mapeada
    # -----------------------------------------------------------------------------

    df_end_base_cep = (
        df_end_base_cep
        .join(
            df_municipio_cep_map_01.select(
                "id_municipio",
                F.col("cidade_cepaberto_id").alias("cidade_cepaberto_id_mapeado")
            ),
            "id_municipio",
            "left"
        )
    )

    # =============================================================================
    # 2. FUNÇÃO LÓGICA DE COMPATIBILIDADE DE NÚMERO
    # =============================================================================

    cond_numero_compativel = (
        (F.col("c.fl_cep_tem_faixa_numero") == "N")
        |
        (
            F.col("e.numero_norm_int").isNotNull()
            &
            (F.col("c.cep_num_ini").isNull() | (F.col("e.numero_norm_int") >= F.col("c.cep_num_ini")))
            &
            (F.col("c.cep_num_fim").isNull() | (F.col("e.numero_norm_int") <= F.col("c.cep_num_fim")))
            &
            (
                F.col("c.cep_lado").isNull()
                | ((F.col("c.cep_lado") == "PAR") & ((F.col("e.numero_norm_int") % 2) == 0))
                | ((F.col("c.cep_lado") == "IMPAR") & ((F.col("e.numero_norm_int") % 2) == 1))
            )
        )
    )

    cond_tipo_compativel = (
        (F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"))
        | F.col("e.tp_logradouro_norm").isNull()
        | F.col("c.tp_logradouro_ref_norm").isNull()
    )

    # =============================================================================
    # 3. MATCH NÍVEL 1
    # Município mapeado + bairro + logradouro + tipo compatível + número compatível
    # =============================================================================

    df_match_n1 = (
        df_end_base_cep.alias("e")
        .filter(F.col("e.municipio_uf") == "ES")
        .filter(F.col("e.cep_original_norm").isNull())
        .filter(F.col("e.cidade_cepaberto_id_mapeado").isNotNull())
        .filter(F.col("e.bairro_norm").isNotNull())
        .filter(F.col("e.logradouro_nome_norm").isNotNull())
        .join(
            df_cep_ref_es.alias("c"),
            (
                (F.col("e.cidade_cepaberto_id_mapeado") == F.col("c.cidade_cepaberto_id"))
                & (F.col("e.bairro_norm") == F.col("c.bairro_ref_norm"))
                & (F.col("e.logradouro_nome_norm") == F.col("c.logradouro_ref_nome_norm"))
                & cond_tipo_compativel
                & cond_numero_compativel
            ),
            "inner"
        )
        .withColumn(
            "score_cep_candidato",
            F.when(
                (F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"))
                & (F.col("c.fl_cep_tem_faixa_numero") == "S"),
                F.lit(100)
            )
            .when(F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"), F.lit(95))
            .otherwise(F.lit(90))
        )
        .select(
            F.col("e.id_endereco_trabalho_01"),
            F.col("c.id_cepaberto_es"),
            F.col("c.cep_ref").alias("cep_candidato"),
            F.lit("N1_MUNICIPIO_BAIRRO_LOGRADOURO").alias("fonte_cep_candidato"),
            "score_cep_candidato"
        )
    )

    # =============================================================================
    # 4. MATCH NÍVEL 2
    # Município mapeado + logradouro + tipo compatível + número compatível
    # Usado quando bairro diverge ou está ausente.
    # Só será aceito depois se não houver ambiguidade.
    # =============================================================================

    df_match_n2 = (
        df_end_base_cep.alias("e")
        .filter(F.col("e.municipio_uf") == "ES")
        .filter(F.col("e.cep_original_norm").isNull())
        .filter(F.col("e.cidade_cepaberto_id_mapeado").isNotNull())
        .filter(F.col("e.logradouro_nome_norm").isNotNull())
        .join(
            df_cep_ref_es.alias("c"),
            (
                (F.col("e.cidade_cepaberto_id_mapeado") == F.col("c.cidade_cepaberto_id"))
                & (F.col("e.logradouro_nome_norm") == F.col("c.logradouro_ref_nome_norm"))
                & cond_tipo_compativel
                & cond_numero_compativel
            ),
            "inner"
        )
        .withColumn(
            "score_cep_candidato",
            F.when(
                (F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"))
                & (F.col("c.fl_cep_tem_faixa_numero") == "S"),
                F.lit(85)
            )
            .when(F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"), F.lit(80))
            .otherwise(F.lit(75))
        )
        .select(
            F.col("e.id_endereco_trabalho_01"),
            F.col("c.id_cepaberto_es"),
            F.col("c.cep_ref").alias("cep_candidato"),
            F.lit("N2_MUNICIPIO_LOGRADOURO").alias("fonte_cep_candidato"),
            "score_cep_candidato"
        )
    )

    # =============================================================================
    # 5. MATCH NÍVEL 3
    # Bairro + logradouro + tipo compatível + número compatível, sem município mapeado
    # Só usado quando a cidade CEP Aberto não foi inferida.
    # Só será aceito depois se não houver ambiguidade.
    # =============================================================================

    df_match_n3 = (
        df_end_base_cep.alias("e")
        .filter(F.col("e.municipio_uf") == "ES")
        .filter(F.col("e.cep_original_norm").isNull())
        .filter(F.col("e.cidade_cepaberto_id_mapeado").isNull())
        .filter(F.col("e.bairro_norm").isNotNull())
        .filter(F.col("e.logradouro_nome_norm").isNotNull())
        .join(
            df_cep_ref_es.alias("c"),
            (
                (F.col("e.bairro_norm") == F.col("c.bairro_ref_norm"))
                & (F.col("e.logradouro_nome_norm") == F.col("c.logradouro_ref_nome_norm"))
                & cond_tipo_compativel
                & cond_numero_compativel
            ),
            "inner"
        )
        .withColumn(
            "score_cep_candidato",
            F.when(
                (F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"))
                & (F.col("c.fl_cep_tem_faixa_numero") == "S"),
                F.lit(70)
            )
            .when(F.col("e.tp_logradouro_norm") == F.col("c.tp_logradouro_ref_norm"), F.lit(65))
            .otherwise(F.lit(60))
        )
        .select(
            F.col("e.id_endereco_trabalho_01"),
            F.col("c.id_cepaberto_es"),
            F.col("c.cep_ref").alias("cep_candidato"),
            F.lit("N3_BAIRRO_LOGRADOURO_SEM_MUNICIPIO_MAPEADO").alias("fonte_cep_candidato"),
            "score_cep_candidato"
        )
    )

    # =============================================================================
    # 6. CONSOLIDAÇÃO DOS CANDIDATOS
    # Regra:
    # - pega maior score por endereço;
    # - se houver mais de um CEP no maior score, não preenche.
    # =============================================================================

    df_cep_candidatos_todos = (
        df_match_n1
        .unionByName(df_match_n2)
        .unionByName(df_match_n3)
    )

    tabela = "tmp_sinp_endereco_cep_candidatos"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_cep_candidatos_todos.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_cep_candidatos_todos, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_cep_candidatos_todos = spark.table(f"gold.{tabela}")
    df_cep_candidatos_todos.createOrReplaceTempView("vw_cep_candidatos_todos")

    df_cep_rank = (
        df_cep_candidatos_todos
        .withColumn(
            "score_max",
            F.max("score_cep_candidato").over(Window.partitionBy("id_endereco_trabalho_01"))
        )
        .filter(F.col("score_cep_candidato") == F.col("score_max"))
    )

    df_cep_escolhido_01 = (
        df_cep_rank
        .groupBy("id_endereco_trabalho_01", "score_max")
        .agg(
            F.countDistinct("cep_candidato").alias("qtd_ceps_candidatos_score_max"),
            F.countDistinct("id_cepaberto_es").alias("qtd_refs_candidatas_score_max"),
            F.first("cep_candidato", ignorenulls=True).alias("cep_enriquecido_norm"),
            F.first("fonte_cep_candidato", ignorenulls=True).alias("fonte_cep_enriquecido")
        )
        .filter(F.col("qtd_ceps_candidatos_score_max") == 1)
        .select(
            "id_endereco_trabalho_01",
            "cep_enriquecido_norm",
            "fonte_cep_enriquecido",
            F.col("score_max").alias("score_cep_enriquecido"),
            "qtd_refs_candidatas_score_max"
        )
    )

    tabela = "tmp_sinp_endereco_cep_escolhido"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_cep_escolhido_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_cep_escolhido_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_cep_escolhido_01 = spark.table(f"gold.{tabela}")
    df_cep_escolhido_01.createOrReplaceTempView("vw_cep_escolhido_01")

    # =============================================================================
    # 7. DATAFRAME FINAL DE TRABALHO COM CEP FINAL
    # =============================================================================

    df_endereco_trabalho_02 = (
        df_end_base_cep
        .join(df_cep_escolhido_01, "id_endereco_trabalho_01", "left")
        .withColumn(
            "cep_final_norm",
            F.coalesce(F.col("cep_original_norm"), F.col("cep_enriquecido_norm"))
        )
        .withColumn(
            "fonte_cep_final",
            F.when(F.col("cep_original_norm").isNotNull(), F.lit("ORIGINAL_INFOPEN"))
             .when(F.col("cep_enriquecido_norm").isNotNull(), F.col("fonte_cep_enriquecido"))
             .otherwise(F.lit("NAO_PREENCHIDO"))
        )
        .withColumn(
            "score_cep_final",
            F.when(F.col("cep_original_norm").isNotNull(), F.lit(100))
             .when(F.col("cep_enriquecido_norm").isNotNull(), F.col("score_cep_enriquecido"))
             .otherwise(F.lit(0))
        )
        .withColumn(
            "ds_endereco_trabalho",
            F.concat_ws(
                " | ",
                F.when(F.col("municipio_uf").isNotNull(), F.concat(F.lit("UF: "), F.col("municipio_uf"))),
                F.when(F.col("municipio_nome_norm").isNotNull(), F.concat(F.lit("MUNICIPIO: "), F.col("municipio_nome_norm"))),
                F.when(F.col("bairro_norm").isNotNull(), F.concat(F.lit("BAIRRO: "), F.col("bairro_norm"))),
                F.when(
                    F.col("tp_logradouro_norm").isNotNull() | F.col("logradouro_nome_norm").isNotNull(),
                    F.concat_ws(" ", F.col("tp_logradouro_norm"), F.col("logradouro_nome_norm"))
                ),
                F.when(F.col("numero_norm").isNotNull(), F.concat(F.lit("N: "), F.col("numero_norm"))),
                F.when(F.col("cep_final_norm").isNotNull(), F.concat(F.lit("CEP: "), F.col("cep_final_norm"))),
                F.when(F.col("complemento_final_norm").isNotNull(), F.concat(F.lit("COMPL: "), F.col("complemento_final_norm"))),
                F.when(F.col("referencia_norm").isNotNull(), F.concat(F.lit("REF: "), F.col("referencia_norm"))),
                F.when(F.col("observacao_norm").isNotNull(), F.concat(F.lit("OBS: "), F.col("observacao_norm")))
            )
        )
        .select(
            "id_endereco_trabalho_01",
            "id_enderecopreso",
            "id_preso",
            "id_municipio",
            "municipio_uf",
            "municipio_nome_norm",
            "bairro_norm",
            "tp_logradouro_norm",
            "logradouro_nome_norm",
            "numero_norm",
            "cep_raw",
            "cep_original_norm",
            "cep_enriquecido_norm",
            "cep_final_norm",
            "fonte_cep_final",
            "score_cep_final",
            "qtd_refs_candidatas_score_max",
            "complemento_final_norm",
            "referencia_norm",
            "observacao_norm",
            "ds_endereco_trabalho"
        )
    )

    tabela = "tmp_sinp_endereco_trabalho_02"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_endereco_trabalho_02.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_endereco_trabalho_02, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_endereco_trabalho_02 = spark.table(f"gold.{tabela}")
    df_endereco_trabalho_02.createOrReplaceTempView("vw_endereco_trabalho_02")

    print("[OK] df_endereco_trabalho_02 criado.")
    print("[OK] vw_endereco_trabalho_02 criada.")


    # ============================================================
    # 05 - ENDEREÇOS ÚNICOS E PONTE PESSOA-ENDEREÇO
    # ============================================================

    from pyspark.storagelevel import StorageLevel

    # =============================================================================
    # CONSTRUÇÃO - ENDEREÇOS ÚNICOS + PONTE PESSOA-ENDEREÇO
    #
    # Entrada:
    #   df_endereco_trabalho_02
    #   bronze.tbl_cep_es
    #   gold.sinp_pnt_pessoa_preso
    #
    # Hierarquia da chave de endereço único:
    #   1) CEP + NUMERO
    #   2) LOGRADOURO + NUMERO + BAIRRO + MUNICIPIO
    #   3) LOGRADOURO + BAIRRO + MUNICIPIO
    #
    # Regras:
    # - lat/long vêm da bronze.tbl_cep_es pelo CEP final
    # - complemento, referência e observação ficam na ponte pessoa-endereço
    # - entidade endereço único não carrega complemento/observação
    # - ponte traz id_pessoa via id_preso
    #
    # Saídas:
    #   df_endereco_unico_01
    #   df_pnt_pessoa_endereco_01
    #   df_endereco_sem_chave_01
    #
    # Views:
    #   vw_endereco_unico_01
    #   vw_pnt_pessoa_endereco_01
    #   vw_endereco_sem_chave_01
    #
    # Não grava gold.
    # Não envia Postgres.
    # =============================================================================

    # -----------------------------------------------------------------------------
    # Dependências
    # -----------------------------------------------------------------------------

    tabela_cep_geo = "bronze.tbl_cep_es"
    tabela_pessoa_preso = "gold.sinp_pnt_pessoa_preso"

    spark.sql(f"REFRESH TABLE {tabela_cep_geo}")
    spark.sql(f"REFRESH TABLE {tabela_pessoa_preso}")

    # -----------------------------------------------------------------------------
    # Validação de campos da base de endereços
    # -----------------------------------------------------------------------------

    campos_obrigatorios_endereco = [
        "id_endereco_trabalho_01",
        "id_enderecopreso",
        "id_preso",
        "id_municipio",
        "municipio_uf",
        "municipio_nome_norm",
        "bairro_norm",
        "tp_logradouro_norm",
        "logradouro_nome_norm",
        "numero_norm",
        "cep_raw",
        "cep_original_norm",
        "cep_enriquecido_norm",
        "cep_final_norm",
        "fonte_cep_final",
        "score_cep_final",
        "complemento_final_norm",
        "referencia_norm",
        "observacao_norm",
        "ds_endereco_trabalho"
    ]

    campos_ausentes_endereco = [
        c for c in campos_obrigatorios_endereco
        if c not in df_endereco_trabalho_02.columns
    ]

    if campos_ausentes_endereco:
        raise Exception(f"Campos ausentes em df_endereco_trabalho_02: {campos_ausentes_endereco}")

    # -----------------------------------------------------------------------------
    # Referência CEP + GEO
    # Garante campos lat/long/metadados mesmo se ainda não existirem na bronze.tbl_cep_es
    # -----------------------------------------------------------------------------

    df_cep_raw = spark.table(tabela_cep_geo)

    if "cep" not in df_cep_raw.columns:
        raise Exception(f"Campo cep ausente em {tabela_cep_geo}.")

    # Lat
    if "lat" not in df_cep_raw.columns:
        if "latitude" in df_cep_raw.columns:
            df_cep_raw = df_cep_raw.withColumn("lat", F.col("latitude").cast("double"))
        else:
            df_cep_raw = df_cep_raw.withColumn("lat", F.lit(None).cast("double"))

    # Long
    if "long" not in df_cep_raw.columns:
        if "longitude" in df_cep_raw.columns:
            df_cep_raw = df_cep_raw.withColumn("long", F.col("longitude").cast("double"))
        else:
            df_cep_raw = df_cep_raw.withColumn("long", F.lit(None).cast("double"))

    # Metadados geo opcionais
    campos_geo_string = [
        "fl_geo_encontrada",
        "precisao_geo",
        "geo_bairro",
        "geo_logradouro",
        "geo_cidade",
        "geo_uf",
        "dt_consulta_geo"
    ]

    for c in campos_geo_string:
        if c not in df_cep_raw.columns:
            df_cep_raw = df_cep_raw.withColumn(c, F.lit(None).cast("string"))

    df_cep_geo_ref_01 = (
        df_cep_raw
        .select(
            F.regexp_replace(F.col("cep").cast("string"), r"[^0-9]", "").alias("cep_geo"),
            F.col("lat").cast("double").alias("lat"),
            F.col("long").cast("double").alias("long"),
            F.col("fl_geo_encontrada").cast("string").alias("fl_geo_encontrada"),
            F.col("precisao_geo").cast("string").alias("precisao_geo"),
            F.col("geo_bairro").cast("string").alias("geo_bairro"),
            F.col("geo_logradouro").cast("string").alias("geo_logradouro"),
            F.col("geo_cidade").cast("string").alias("geo_cidade"),
            F.col("geo_uf").cast("string").alias("geo_uf"),
            F.col("dt_consulta_geo").cast("string").alias("dt_consulta_geo")
        )
        .filter(F.length("cep_geo") == 8)
        .groupBy("cep_geo")
        .agg(
            F.first("lat", ignorenulls=True).alias("lat"),
            F.first("long", ignorenulls=True).alias("long"),
            F.first("fl_geo_encontrada", ignorenulls=True).alias("fl_geo_encontrada"),
            F.first("precisao_geo", ignorenulls=True).alias("precisao_geo"),
            F.first("geo_bairro", ignorenulls=True).alias("geo_bairro"),
            F.first("geo_logradouro", ignorenulls=True).alias("geo_logradouro"),
            F.first("geo_cidade", ignorenulls=True).alias("geo_cidade"),
            F.first("geo_uf", ignorenulls=True).alias("geo_uf"),
            F.first("dt_consulta_geo", ignorenulls=True).alias("dt_consulta_geo")
        )
    )

    tabela = "tmp_sinp_endereco_cep_geo_ref"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_cep_geo_ref_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_cep_geo_ref_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_cep_geo_ref_01 = spark.table(f"gold.{tabela}")
    df_cep_geo_ref_01.createOrReplaceTempView("vw_cep_geo_ref_01")

    # -----------------------------------------------------------------------------
    # Mapa id_preso -> id_pessoa
    # -----------------------------------------------------------------------------

    df_map_raw = spark.table(tabela_pessoa_preso)

    for campo in ["id_preso", "id_pessoa"]:
        if campo not in df_map_raw.columns:
            raise Exception(f"Campo {campo} ausente em {tabela_pessoa_preso}.")

    df_pessoa_preso_01 = (
        df_map_raw
        .select(
            F.col("id_preso").cast("string").alias("id_preso_join"),
            F.col("id_pessoa").cast("string").alias("id_pessoa")
        )
        .filter(F.col("id_preso_join").isNotNull())
        .filter(F.col("id_pessoa").isNotNull())
        .groupBy("id_preso_join")
        .agg(
            F.first("id_pessoa", ignorenulls=True).alias("id_pessoa"),
            F.countDistinct("id_pessoa").alias("qtd_pessoas_por_preso")
        )
    )

    tabela = "tmp_sinp_endereco_pessoa_preso"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_pessoa_preso_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pessoa_preso_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_pessoa_preso_01 = spark.table(f"gold.{tabela}")
    df_pessoa_preso_01.createOrReplaceTempView("vw_pessoa_preso_01")

    # -----------------------------------------------------------------------------
    # Base de endereços com CEP + GEO
    # -----------------------------------------------------------------------------

    df_endereco_base_chave_01 = (
        df_endereco_trabalho_02
        .select(
            "id_endereco_trabalho_01",
            "id_enderecopreso",
            "id_preso",
            "id_municipio",
            "municipio_uf",
            "municipio_nome_norm",
            "bairro_norm",
            "tp_logradouro_norm",
            "logradouro_nome_norm",
            "numero_norm",
            "cep_raw",
            "cep_original_norm",
            "cep_enriquecido_norm",
            "cep_final_norm",
            "fonte_cep_final",
            "score_cep_final",
            "complemento_final_norm",
            "referencia_norm",
            "observacao_norm",
            "ds_endereco_trabalho"
        )
        .withColumn("id_preso_join", F.col("id_preso").cast("string"))
        .withColumn("municipio_uf", F.upper(F.trim(F.col("municipio_uf"))))
        .withColumn("municipio_nome_norm", F.upper(F.trim(F.col("municipio_nome_norm"))))
        .withColumn("bairro_norm", F.upper(F.trim(F.col("bairro_norm"))))
        .withColumn("tp_logradouro_norm", F.upper(F.trim(F.col("tp_logradouro_norm"))))
        .withColumn("logradouro_nome_norm", F.upper(F.trim(F.col("logradouro_nome_norm"))))
        .withColumn(
            "numero_norm_limpo",
            F.regexp_replace(
                F.upper(F.trim(F.col("numero_norm").cast("string"))),
                r"[^0-9A-Z]",
                ""
            )
        )
        .withColumn(
            "numero_norm_limpo",
            F.when(F.col("numero_norm_limpo") != "", F.col("numero_norm_limpo"))
        )
        .withColumn(
            "cep_final_norm",
            F.regexp_replace(F.col("cep_final_norm").cast("string"), r"[^0-9]", "")
        )
        .withColumn(
            "cep_final_norm",
            F.when(F.length(F.col("cep_final_norm")) == 8, F.col("cep_final_norm"))
        )
        .join(
            df_cep_geo_ref_01,
            F.col("cep_final_norm") == F.col("cep_geo"),
            "left"
        )
        .drop("cep_geo")
    )

    # -----------------------------------------------------------------------------
    # Hierarquia da chave de endereço único
    # -----------------------------------------------------------------------------

    df_endereco_base_chave_01 = (
        df_endereco_base_chave_01
        .withColumn(
            "nivel_chave_endereco",
            F.when(
                F.col("cep_final_norm").isNotNull()
                & F.col("numero_norm_limpo").isNotNull(),
                F.lit("N1_CEP_NUMERO")
            )
            .when(
                F.col("logradouro_nome_norm").isNotNull()
                & F.col("numero_norm_limpo").isNotNull()
                & F.col("bairro_norm").isNotNull()
                & F.col("id_municipio").isNotNull(),
                F.lit("N2_LOGRADOURO_NUMERO_BAIRRO_MUNICIPIO")
            )
            .when(
                F.col("logradouro_nome_norm").isNotNull()
                & F.col("bairro_norm").isNotNull()
                & F.col("id_municipio").isNotNull(),
                F.lit("N3_LOGRADOURO_BAIRRO_MUNICIPIO")
            )
        )
        .withColumn(
            "chave_endereco_unico_texto",
            F.when(
                F.col("nivel_chave_endereco") == "N1_CEP_NUMERO",
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("NIVEL: "), F.col("nivel_chave_endereco")),
                    F.concat(F.lit("CEP: "), F.col("cep_final_norm")),
                    F.concat(F.lit("NUMERO: "), F.col("numero_norm_limpo"))
                )
            )
            .when(
                F.col("nivel_chave_endereco") == "N2_LOGRADOURO_NUMERO_BAIRRO_MUNICIPIO",
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("NIVEL: "), F.col("nivel_chave_endereco")),
                    F.concat(F.lit("MUNICIPIO: "), F.col("id_municipio").cast("string")),
                    F.concat(F.lit("BAIRRO: "), F.col("bairro_norm")),
                    F.concat(
                        F.lit("LOGRADOURO: "),
                        F.concat_ws(" ", F.col("tp_logradouro_norm"), F.col("logradouro_nome_norm"))
                    ),
                    F.concat(F.lit("NUMERO: "), F.col("numero_norm_limpo"))
                )
            )
            .when(
                F.col("nivel_chave_endereco") == "N3_LOGRADOURO_BAIRRO_MUNICIPIO",
                F.concat_ws(
                    " | ",
                    F.concat(F.lit("NIVEL: "), F.col("nivel_chave_endereco")),
                    F.concat(F.lit("MUNICIPIO: "), F.col("id_municipio").cast("string")),
                    F.concat(F.lit("BAIRRO: "), F.col("bairro_norm")),
                    F.concat(
                        F.lit("LOGRADOURO: "),
                        F.concat_ws(" ", F.col("tp_logradouro_norm"), F.col("logradouro_nome_norm"))
                    )
                )
            )
        )
        .withColumn(
            "id_endereco_unico_01",
            F.when(
                F.col("nivel_chave_endereco").isNotNull(),
                F.substring(
                    F.md5(F.coalesce(F.col("chave_endereco_unico_texto"), F.lit("[NULL]"))),
                    1,
                    30
                )
            )
        )
    )

    tabela = "tmp_sinp_endereco_base_chave"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_endereco_base_chave_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_endereco_base_chave_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_endereco_base_chave_01 = spark.table(f"gold.{tabela}")
    df_endereco_base_chave_01.createOrReplaceTempView("vw_endereco_base_chave_01")

    # -----------------------------------------------------------------------------
    # Registros sem chave suficiente
    # -----------------------------------------------------------------------------

    df_endereco_sem_chave_01 = (
        df_endereco_base_chave_01
        .filter(F.col("id_endereco_unico_01").isNull())
        .select(
            "id_endereco_trabalho_01",
            "id_enderecopreso",
            "id_preso",
            "id_municipio",
            "municipio_uf",
            "municipio_nome_norm",
            "bairro_norm",
            "tp_logradouro_norm",
            "logradouro_nome_norm",
            F.col("numero_norm_limpo").alias("numero_norm"),
            "cep_final_norm",
            "complemento_final_norm",
            "referencia_norm",
            "observacao_norm",
            "ds_endereco_trabalho"
        )
    )

    tabela = "tmp_sinp_endereco_sem_chave_01"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_endereco_sem_chave_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_endereco_sem_chave_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_endereco_sem_chave_01 = spark.table(f"gold.{tabela}")
    df_endereco_sem_chave_01.createOrReplaceTempView("vw_endereco_sem_chave_01")

    # -----------------------------------------------------------------------------
    # Endereços únicos
    # Complemento, referência e observação NÃO entram aqui.
    # -----------------------------------------------------------------------------

    df_endereco_unico_01 = (
        df_endereco_base_chave_01
        .filter(F.col("id_endereco_unico_01").isNotNull())
        .groupBy(
            "id_endereco_unico_01",
            "nivel_chave_endereco",
            "chave_endereco_unico_texto"
        )
        .agg(
            F.count("*").alias("qtd_registros_origem"),
            F.countDistinct("id_enderecopreso").alias("qtd_enderecos_preso_origem"),
            F.countDistinct("id_preso").alias("qtd_presos_distintos"),

            F.first("id_municipio", ignorenulls=True).alias("id_municipio"),
            F.first("municipio_uf", ignorenulls=True).alias("municipio_uf"),
            F.first("municipio_nome_norm", ignorenulls=True).alias("municipio_nome_norm"),
            F.first("bairro_norm", ignorenulls=True).alias("bairro_norm"),
            F.first("tp_logradouro_norm", ignorenulls=True).alias("tp_logradouro_norm"),
            F.first("logradouro_nome_norm", ignorenulls=True).alias("logradouro_nome_norm"),
            F.first("numero_norm_limpo", ignorenulls=True).alias("numero_norm"),
            F.first("cep_final_norm", ignorenulls=True).alias("cep_final_norm"),

            F.first("lat", ignorenulls=True).alias("lat"),
            F.first("long", ignorenulls=True).alias("long"),
            F.first("fl_geo_encontrada", ignorenulls=True).alias("fl_geo_encontrada"),
            F.first("precisao_geo", ignorenulls=True).alias("precisao_geo"),
            F.first("geo_bairro", ignorenulls=True).alias("geo_bairro"),
            F.first("geo_logradouro", ignorenulls=True).alias("geo_logradouro"),
            F.first("geo_cidade", ignorenulls=True).alias("geo_cidade"),
            F.first("geo_uf", ignorenulls=True).alias("geo_uf"),
            F.first("dt_consulta_geo", ignorenulls=True).alias("dt_consulta_geo"),

            F.max("score_cep_final").alias("score_cep_final_max"),
            F.first("fonte_cep_final", ignorenulls=True).alias("fonte_cep_final"),

            F.countDistinct("bairro_norm").alias("qtd_bairros_distintos_no_grupo"),
            F.countDistinct("logradouro_nome_norm").alias("qtd_logradouros_distintos_no_grupo"),
            F.countDistinct("cep_final_norm").alias("qtd_ceps_distintos_no_grupo")
        )
        .withColumn(
            "fl_endereco_geolocalizado",
            F.when(
                F.col("lat").isNotNull() & F.col("long").isNotNull(),
                F.lit("S")
            ).otherwise(F.lit("N"))
        )
        .withColumn(
            "ds_endereco_unico",
            F.concat_ws(
                " | ",
                F.concat(F.lit("NIVEL: "), F.col("nivel_chave_endereco")),
                F.when(F.col("municipio_uf").isNotNull(), F.concat(F.lit("UF: "), F.col("municipio_uf"))),
                F.when(F.col("municipio_nome_norm").isNotNull(), F.concat(F.lit("MUNICIPIO: "), F.col("municipio_nome_norm"))),
                F.when(F.col("bairro_norm").isNotNull(), F.concat(F.lit("BAIRRO: "), F.col("bairro_norm"))),
                F.when(
                    F.col("tp_logradouro_norm").isNotNull() | F.col("logradouro_nome_norm").isNotNull(),
                    F.concat_ws(" ", F.col("tp_logradouro_norm"), F.col("logradouro_nome_norm"))
                ),
                F.when(F.col("numero_norm").isNotNull(), F.concat(F.lit("N: "), F.col("numero_norm"))),
                F.when(F.col("cep_final_norm").isNotNull(), F.concat(F.lit("CEP: "), F.col("cep_final_norm"))),
                F.when(
                    F.col("lat").isNotNull() & F.col("long").isNotNull(),
                    F.concat(
                        F.lit("GEO: "),
                        F.col("lat").cast("string"),
                        F.lit(","),
                        F.col("long").cast("string")
                    )
                )
            )
        )
        .select(
            "id_endereco_unico_01",
            "nivel_chave_endereco",
            "chave_endereco_unico_texto",
            "id_municipio",
            "municipio_uf",
            "municipio_nome_norm",
            "bairro_norm",
            "tp_logradouro_norm",
            "logradouro_nome_norm",
            "numero_norm",
            "cep_final_norm",
            "lat",
            "long",
            "fl_endereco_geolocalizado",
            "fl_geo_encontrada",
            "precisao_geo",
            "geo_bairro",
            "geo_logradouro",
            "geo_cidade",
            "geo_uf",
            "dt_consulta_geo",
            "fonte_cep_final",
            "score_cep_final_max",
            "qtd_registros_origem",
            "qtd_enderecos_preso_origem",
            "qtd_presos_distintos",
            "qtd_bairros_distintos_no_grupo",
            "qtd_logradouros_distintos_no_grupo",
            "qtd_ceps_distintos_no_grupo",
            "ds_endereco_unico"
        )
    )

    tabela = "tmp_sinp_endereco_unico_01"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_endereco_unico_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_endereco_unico_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_endereco_unico_01 = spark.table(f"gold.{tabela}")
    df_endereco_unico_01.createOrReplaceTempView("vw_endereco_unico_01")

    # -----------------------------------------------------------------------------
    # Ponte pessoa-endereço
    # Complemento, referência e observação ficam aqui.
    # -----------------------------------------------------------------------------

    df_pnt_pessoa_endereco_01 = (
        df_endereco_base_chave_01.alias("e")
        .filter(F.col("e.id_endereco_unico_01").isNotNull())
        .join(
            df_pessoa_preso_01.alias("p"),
            F.col("e.id_preso_join") == F.col("p.id_preso_join"),
            "left"
        )
        .withColumn(
            "fl_id_pessoa_encontrado",
            F.when(F.col("p.id_pessoa").isNotNull(), F.lit("S")).otherwise(F.lit("N"))
        )
        .withColumn(
            "id_pessoa_endereco_01",
            F.substring(
                F.md5(
                    F.concat_ws(
                        "|",
                        F.coalesce(F.col("p.id_pessoa"), F.lit("[NULL]")),
                        F.coalesce(F.col("e.id_preso").cast("string"), F.lit("[NULL]")),
                        F.coalesce(F.col("e.id_enderecopreso").cast("string"), F.lit("[NULL]")),
                        F.coalesce(F.col("e.id_endereco_unico_01"), F.lit("[NULL]"))
                    )
                ),
                1,
                30
            )
        )
        .select(
            "id_pessoa_endereco_01",
            F.col("p.id_pessoa").alias("id_pessoa"),
            F.col("e.id_preso").alias("id_preso"),
            "fl_id_pessoa_encontrado",
            F.col("p.qtd_pessoas_por_preso").alias("qtd_pessoas_por_preso"),

            F.col("e.id_endereco_unico_01").alias("id_endereco_unico_01"),
            F.col("e.nivel_chave_endereco").alias("nivel_chave_endereco"),
            F.col("e.chave_endereco_unico_texto").alias("chave_endereco_unico_texto"),

            F.col("e.id_endereco_trabalho_01").alias("id_endereco_trabalho_01"),
            F.col("e.id_enderecopreso").alias("id_enderecopreso"),

            F.col("e.complemento_final_norm").alias("complemento_pessoa_endereco_norm"),
            F.col("e.referencia_norm").alias("referencia_pessoa_endereco_norm"),
            F.col("e.observacao_norm").alias("observacao_pessoa_endereco_norm"),

            F.col("e.cep_raw").alias("cep_raw"),
            F.col("e.cep_original_norm").alias("cep_original_norm"),
            F.col("e.cep_enriquecido_norm").alias("cep_enriquecido_norm"),
            F.col("e.cep_final_norm").alias("cep_final_norm"),
            F.col("e.fonte_cep_final").alias("fonte_cep_final"),
            F.col("e.score_cep_final").alias("score_cep_final"),

            F.col("e.lat").alias("lat"),
            F.col("e.long").alias("long"),
            F.col("e.fl_geo_encontrada").alias("fl_geo_encontrada"),
            F.col("e.precisao_geo").alias("precisao_geo"),

            F.col("e.ds_endereco_trabalho").alias("ds_endereco_declarado")
        )
        .dropDuplicates(["id_pessoa_endereco_01"])
    )

    tabela = "tmp_sinp_pnt_pessoa_endereco_01"
    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_pnt_pessoa_endereco_01.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pnt_pessoa_endereco_01, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_pnt_pessoa_endereco_01 = spark.table(f"gold.{tabela}")
    df_pnt_pessoa_endereco_01.createOrReplaceTempView("vw_pnt_pessoa_endereco_01")

    print("[OK] df_endereco_unico_01 criado.")
    print("[OK] vw_endereco_unico_01 criada.")
    print("[OK] df_pnt_pessoa_endereco_01 criado.")
    print("[OK] vw_pnt_pessoa_endereco_01 criada.")
    print("[OK] df_endereco_sem_chave_01 criado.")
    print("[OK] vw_endereco_sem_chave_01 criada.")


    # ============================================================
    # 06 - PUBLICAÇÃO FINAL
    # ============================================================

    tabela = "sinp_ent_endereco"

    df_sinp_ent_endereco = spark.sql(r"""
        select
            id_endereco_unico_01 as id_endereco,
            nivel_chave_endereco,
            chave_endereco_unico_texto,
            id_municipio,
            municipio_uf,
            municipio_nome_norm,
            bairro_norm,
            tp_logradouro_norm,
            logradouro_nome_norm,
            numero_norm,
            cep_final_norm,
            lat,
            long,
            fl_endereco_geolocalizado,
            fl_geo_encontrada,
            precisao_geo,
            geo_bairro,
            geo_logradouro,
            geo_cidade,
            geo_uf,
            dt_consulta_geo,
            fonte_cep_final,
            score_cep_final_max,
            qtd_registros_origem,
            qtd_enderecos_preso_origem,
            qtd_presos_distintos,
            qtd_bairros_distintos_no_grupo,
            qtd_logradouros_distintos_no_grupo,
            qtd_ceps_distintos_no_grupo,
            ds_endereco_unico
        from gold.tmp_sinp_endereco_unico_01
    """)

    df_sinp_ent_endereco_pg = df_sinp_ent_endereco.repartition(800, F.col("id_endereco"))

    df_sinp_ent_endereco_pg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 2000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_ent_endereco_pg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_ent_endereco")

    tabela = "sinp_pnt_pessoa_endereco"

    df_sinp_pnt_pessoa_endereco = spark.sql(r"""
        select
            id_pessoa_endereco_01 as id_pessoa_endereco,
            id_pessoa,
            id_preso,
            fl_id_pessoa_encontrado,
            qtd_pessoas_por_preso,
            id_endereco_unico_01 as id_endereco,
            nivel_chave_endereco,
            chave_endereco_unico_texto,
            id_endereco_trabalho_01,
            id_enderecopreso,
            complemento_pessoa_endereco_norm,
            referencia_pessoa_endereco_norm,
            observacao_pessoa_endereco_norm,
            cep_raw,
            cep_original_norm,
            cep_enriquecido_norm,
            cep_final_norm,
            fonte_cep_final,
            score_cep_final,
            lat,
            long,
            fl_geo_encontrada,
            precisao_geo,
            ds_endereco_declarado
        from gold.tmp_sinp_pnt_pessoa_endereco_01
    """)

    df_sinp_pnt_pessoa_endereco_pg = df_sinp_pnt_pessoa_endereco.repartition(1200, F.col("id_pessoa_endereco"))

    df_sinp_pnt_pessoa_endereco_pg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 2000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_pnt_pessoa_endereco_pg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_pnt_pessoa_endereco")

    tabela = "sinp_endereco_sem_chave"

    df_sinp_endereco_sem_chave = spark.sql(r"""
        select
            id_endereco_trabalho_01,
            id_enderecopreso,
            id_preso,
            id_municipio,
            municipio_uf,
            municipio_nome_norm,
            bairro_norm,
            tp_logradouro_norm,
            logradouro_nome_norm,
            numero_norm,
            cep_final_norm,
            complemento_final_norm,
            referencia_norm,
            observacao_norm,
            ds_endereco_trabalho
        from gold.tmp_sinp_endereco_sem_chave_01
    """)

    df_sinp_endereco_sem_chave_pg = df_sinp_endereco_sem_chave.repartition(400, F.col("id_endereco_trabalho_01"))

    df_sinp_endereco_sem_chave_pg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 2000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_endereco_sem_chave_pg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_endereco_sem_chave")

    # ============================================================
    # 07 - ENVIO FINAL PARA POSTGRES
    # ============================================================
    # Regra operacional:
    # - as três tabelas finais já foram materializadas em gold antes deste ponto;
    # - o envio ao Postgres é isolado por tabela;
    # - erro em uma tabela não impede tentativa das demais;
    # - ao final, se qualquer envio falhar, a etapa falha com lista objetiva.

    envios_postgres = [
        ("gold.sinp_pnt_pessoa_endereco", "id_pessoa_endereco"),
        ("gold.sinp_ent_endereco", "id_endereco"),
        ("gold.sinp_endereco_sem_chave", "id_endereco_trabalho_01"),
    ]

    erros_postgres = []

    for origem_pg, pk_pg in envios_postgres:
        print(f"[POSTGRES][INICIO] {origem_pg} | pk={pk_pg}", flush=True)

        try:
            enviar_gold_para_postgres(origem_pg, pk_pg)
            print(f"[POSTGRES][FIM] {origem_pg}", flush=True)
        except Exception as e:
            msg_erro = f"{origem_pg} | pk={pk_pg} | erro={str(e)}"
            erros_postgres.append(msg_erro)
            print(f"[POSTGRES][ERRO] {msg_erro}", flush=True)

    if len(erros_postgres) > 0:
        raise Exception("Falha no envio ao Postgres: " + " || ".join(erros_postgres))

    # ============================================================
    # 08 - LIMPEZA FINAL DAS TEMPORÁRIAS
    # ============================================================

    for tabela in temporarias:
        spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    spark.catalog.clearCache()
