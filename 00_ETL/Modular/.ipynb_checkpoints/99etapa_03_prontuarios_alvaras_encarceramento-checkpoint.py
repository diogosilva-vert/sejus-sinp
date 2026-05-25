# -*- coding: utf-8 -*-
"""Prontuários, alvarás e fato de encarceramento."""

import os
import hashlib
from datetime import datetime, timedelta, date

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

    """Etapa extraída do notebook original."""

    # ===== CELL 17 =====
    # ============================================================
    # CORREÇÃO DEVIDO ERRO DE TIPO DE DADO EM PRESO_PRONTUARIO_SOCIAL
    # APÓS CORRIGIDO REMOVER
    # ============================================================

    origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_SOCIAL.parquet"
    destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_SOCIAL_CORR.parquet"

    df_corr = (
        spark.read.parquet(origem)
        .withColumn("dt_cadastro_ficha", F.col("dt_cadastro_ficha").cast("timestamp"))
    )

    os.system(f"hdfs dfs -rm -r -skipTrash {destino} >/dev/null 2>&1")

    df_corr.write         .mode("overwrite")         .option("compression", "snappy")         .parquet(destino)

    spark.sql("DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_social_corr")

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_social_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_social_corr")

    # ============================================================
    # CORRECAO DEVIDO ERRO DE TIPO DE DADO EM PRESO_PRONTUARIO_PSICO
    # APOS CORRIGIDO REMOVER
    # ============================================================

    origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_PSICO.parquet"
    destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_PSICO_CORR.parquet"

    df_corr = (
        spark.read.parquet(origem)
        .withColumn("dt_cadastro_ficha", F.col("dt_cadastro_ficha").cast("timestamp"))
        .withColumn("dt_ultima_alteracao", F.col("dt_ultima_alteracao").cast("timestamp"))
    )

    os.system(f"hdfs dfs -rm -r -skipTrash {destino} >/dev/null 2>&1")

    df_corr.write         .mode("overwrite")         .option("compression", "snappy")         .parquet(destino)

    spark.sql("DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_psico_corr")

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_psico_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_psico_corr")

    # ============================================================
    # CORRECAO DEVIDO ERRO DE TIPO DE DADO EM PRESO_PRONTUARIO_CRIMINAL
    # APOS CORRIGIDO REMOVER
    # ============================================================

    origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_CRIMINAL.parquet"
    destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_CRIMINAL_CORR.parquet"

    df_corr = (
        spark.read.parquet(origem)
        .withColumn("dt_cadastro_ficha", F.col("dt_cadastro_ficha").cast("timestamp"))
        .withColumn("dt_ultima_alteracao", F.col("dt_ultima_alteracao").cast("timestamp"))
    )

    os.system(f"hdfs dfs -rm -r -skipTrash {destino} >/dev/null 2>&1")

    df_corr.write         .mode("overwrite")         .option("compression", "snappy")         .parquet(destino)

    spark.sql("DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_criminal_corr")

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_criminal_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_criminal_corr")

    # ============================================================
    # CORRECAO DEVIDO ERRO DE TIPO DE DADO EM PRESO_PRONTUARIO_PROFISSIONAL
    # APOS CORRIGIDO REMOVER
    # ============================================================

    origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_PROFISSIONAL.parquet"
    destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_PRONTUARIO_PROFISSIONAL_CORR.parquet"

    df_corr = (
        spark.read.parquet(origem)
        .withColumn("dt_cadastro_ficha", F.col("dt_cadastro_ficha").cast("timestamp"))
        .withColumn("dt_ultima_alteracao", F.col("dt_ultima_alteracao").cast("timestamp"))
    )

    os.system(f"hdfs dfs -rm -r -skipTrash {destino} >/dev/null 2>&1")

    df_corr.write         .mode("overwrite")         .option("compression", "snappy")         .parquet(destino)

    spark.sql("DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_profissional_corr")

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_profissional_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_profissional_corr")

    # ===== CELL 18 =====
    # ============================================================
    # REFRESH PRONTUARIOS CORRIGIDOS E REFERENCIA PESSOA/PRESO
    # ============================================================

    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_social_corr")
    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_criminal_corr")
    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_profissional_corr")
    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_psico_corr")
    spark.sql("REFRESH TABLE gold.sinp_pnt_pessoa_preso")
    spark.catalog.clearCache()

    tabela = "tmp_ref_pessoa_preso_prontuario"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_ref_pessoa_preso_prontuario = spark.sql("""
        select distinct
            cast(id_preso as string) as id_preso_str,
            id_pessoa,
            nome_pessoa
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
    """)

    df_ref_pessoa_preso_prontuario.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_ref_pessoa_preso_prontuario, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # PRONTUARIO SOCIAL
    # ============================================================

    prontsoc = spark.sql("""
    SELECT
        md5(concat(cast(ps.id_prontuario_social as string), cast(p.id_pessoa as string))) as id_pessoa_prontsoc,
        p.id_pessoa,
        p.nome_pessoa,
        ps.*
    FROM bronze.infopen_preso_prontuario_social_corr ps
    inner join gold.tmp_ref_pessoa_preso_prontuario p
        on cast(ps.id_preso as string) = p.id_preso_str
    """)

    tabela = "sinp_fil_pront_soc"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    prontsoc.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontsoc, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_soc", "id_pessoa_prontsoc")

    # ============================================================
    # PRONTUARIO CRIMINAL
    # ============================================================

    prontcrim = spark.sql("""
    SELECT
        md5(concat(cast(pc.id_prontuario_criminal as string), cast(p.id_pessoa as string))) as id_pessoa_prontcrim,
        p.id_pessoa,
        p.nome_pessoa,
        pc.*
    FROM bronze.infopen_preso_prontuario_criminal_corr pc
    inner join gold.tmp_ref_pessoa_preso_prontuario p
        on cast(pc.id_preso as string) = p.id_preso_str
    """)

    tabela = "sinp_fil_pront_crim"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    prontcrim.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontcrim, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_crim", "id_pessoa_prontcrim")

    # ============================================================
    # PRONTUARIO PROFISSIONAL
    # ============================================================

    prontprof = spark.sql("""
    SELECT
        md5(concat(cast(pp.id_prontuario_profissional as string), cast(p.id_pessoa as string))) as id_pessoa_prontprof,
        p.id_pessoa,
        p.nome_pessoa,
        pp.*
    FROM bronze.infopen_preso_prontuario_profissional_corr pp
    inner join gold.tmp_ref_pessoa_preso_prontuario p
        on cast(pp.id_preso as string) = p.id_preso_str
    """)

    tabela = "sinp_fil_pront_prof"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    prontprof.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontprof, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_prof", "id_pessoa_prontprof")

    # ============================================================
    # PRONTUARIO PSICO
    # ============================================================

    prontpsico = spark.sql("""
    SELECT
        md5(concat(cast(pps.id_prontuario_psico as string), cast(p.id_pessoa as string))) as id_pessoa_prontpsico,
        p.id_pessoa,
        p.nome_pessoa,
        pps.*
    FROM bronze.infopen_preso_prontuario_psico_corr pps
    inner join gold.tmp_ref_pessoa_preso_prontuario p
        on cast(pps.id_preso as string) = p.id_preso_str
    """)

    tabela = "sinp_fil_pront_psico"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    prontpsico.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontpsico, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_psico", "id_pessoa_prontpsico")

    # ===== CELL 19 =====
    # ============================================================
    # REFRESH
    # ============================================================

    spark.sql("REFRESH TABLE bronze.infopen_vw_alvaras")
    spark.sql("REFRESH TABLE bronze.infopen_minio_beneficios")
    spark.sql("REFRESH TABLE gold.sinp_pnt_pessoa_preso")
    spark.sql("REFRESH TABLE gold.sinp_ent_pessoa")
    spark.catalog.clearCache()

    # ============================================================
    # HELPERS
    # ============================================================

    def first_existing_col(cols, candidates):
        mapa = {c.lower(): c for c in cols}
        for cand in candidates:
            if cand.lower() in mapa:
                return mapa[cand.lower()]
        return None

    def add_ts_from_candidates(df, new_col, candidates):
        col = first_existing_col(df.columns, candidates)
        if col:
            return df.withColumn(new_col, F.to_timestamp(F.col(col))), col
        return df.withColumn(new_col, F.lit(None).cast("timestamp")), None

    def add_dt_from_ts(df, new_col, source_ts_col):
        return df.withColumn(new_col, F.to_date(F.col(source_ts_col)))

    def lit_str_or_null(valor):
        return F.lit(valor).cast("string")

    # ============================================================
    # REFERENCIA DE PESSOA
    # ============================================================

    df_ref_pessoa_alvara = spark.sql("""
        select distinct
            cast(pp.id_preso as string) as id_preso_str,
            pp.id_pessoa,
            coalesce(ep.nome_pessoa, pp.nome_pessoa) as nome_pessoa,
            ep.documento as documento_pessoa,
            ep.origem as origem_pessoa,
            ep.sexo_pessoa,
            ep.data_nascimento_pessoa,
            ep.nome_mae,
            ep.nome_pai,
            ep.etnia
        from gold.sinp_pnt_pessoa_preso pp
        left join gold.sinp_ent_pessoa ep
            on pp.id_pessoa = ep.id_pessoa
    """)

    tabela = "tmp_ref_pessoa_alvara"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_ref_pessoa_alvara.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_ref_pessoa_alvara, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_ref_pessoa_alvara")

    # ============================================================
    # BASE ALVARAS NORMALIZADA
    # ============================================================

    df_alvaras_normalizada = spark.sql("""
        select
            *,
            cast(id_preso as string) as id_preso_str
        from bronze.infopen_vw_alvaras
    """)

    tabela = "tmp_alvaras_normalizada"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_alvaras_normalizada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_alvaras_normalizada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_alvaras_normalizada")

    # ============================================================
    # BASE ALVARAS
    # ============================================================

    df_alvaras_base = spark.sql("""
        select
            a.*,
            p.id_pessoa,
            b.beneficio_nome,
            b.beneficio_descricao,
            p.nome_pessoa,
            p.documento_pessoa,
            p.origem_pessoa,
            p.sexo_pessoa,
            p.data_nascimento_pessoa,
            p.nome_mae,
            p.nome_pai,
            p.etnia
        from gold.tmp_alvaras_normalizada a
        left join gold.tmp_ref_pessoa_alvara p
            on a.id_preso_str = p.id_preso_str
        inner join bronze.infopen_minio_beneficios b
            on a.id_beneficio = b.id_beneficio
    """)

    tabela = "tmp_base_alvaras"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_alvaras_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_alvaras_base, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_alvaras")

    # ============================================================
    # ENRIQUECIMENTO DINAMICO
    # ============================================================

    df_alvaras = spark.table("gold.tmp_base_alvaras")

    df_alvaras, src_dt_emissao = add_ts_from_candidates(df_alvaras, "dt_emissao_alvara", [
        "dt_emissao", "data_emissao", "dt_alvara", "data_alvara", "dt_expedicao", "data_expedicao"
    ])

    df_alvaras, src_dt_cadastro = add_ts_from_candidates(df_alvaras, "dt_cadastro_alvara", [
        "dt_cadastro", "data_cadastro", "dt_registro", "data_registro", "dt_inclusao", "data_inclusao"
    ])

    df_alvaras, src_dt_cumprimento = add_ts_from_candidates(df_alvaras, "dt_cumprimento_alvara", [
        "dt_cumprimento", "data_cumprimento", "dt_baixa", "data_baixa"
    ])

    df_alvaras, src_dt_revogacao = add_ts_from_candidates(df_alvaras, "dt_revogacao_alvara", [
        "dt_revogacao", "data_revogacao", "dt_cancelamento", "data_cancelamento"
    ])

    df_alvaras, src_dt_validade = add_ts_from_candidates(df_alvaras, "dt_validade_alvara", [
        "dt_validade", "data_validade", "dt_vencimento", "data_vencimento", "dt_prazo", "data_prazo"
    ])

    df_alvaras = df_alvaras.withColumn(
        "dt_referencia_alvara",
        F.coalesce(
            F.col("dt_emissao_alvara"),
            F.col("dt_cadastro_alvara"),
            F.col("dt_cumprimento_alvara")
        )
    )

    df_alvaras = add_dt_from_ts(df_alvaras, "dt_referencia_alvara_ref", "dt_referencia_alvara")
    df_alvaras = add_dt_from_ts(df_alvaras, "dt_emissao_alvara_ref", "dt_emissao_alvara")
    df_alvaras = add_dt_from_ts(df_alvaras, "dt_cadastro_alvara_ref", "dt_cadastro_alvara")
    df_alvaras = add_dt_from_ts(df_alvaras, "dt_cumprimento_alvara_ref", "dt_cumprimento_alvara")
    df_alvaras = add_dt_from_ts(df_alvaras, "dt_revogacao_alvara_ref", "dt_revogacao_alvara")
    df_alvaras = add_dt_from_ts(df_alvaras, "dt_validade_alvara_ref", "dt_validade_alvara")

    df_alvaras = df_alvaras.withColumn(
        "txt_beneficio",
        F.trim(
            F.concat_ws(
                " - ",
                F.coalesce(F.col("beneficio_nome"), F.lit("")),
                F.coalesce(F.col("beneficio_descricao"), F.lit(""))
            )
        )
    )

    df_alvaras = df_alvaras.withColumn(
        "txt_beneficio_upper",
        F.upper(F.coalesce(F.col("txt_beneficio"), F.lit("")))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_tem_pessoa",
        F.when(F.col("id_pessoa").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_tem_beneficio",
        F.when(F.col("id_beneficio").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_tem_documento_pessoa",
        F.when(F.col("documento_pessoa").isNotNull(), F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_beneficio_liberatorio",
        F.when(
            F.col("txt_beneficio_upper").rlike(
                "SOLTURA|LIBERDADE|ALVARA DE SOLTURA|ALVARÁ DE SOLTURA|RELAXAMENTO|REVOGA.?.? DE PRISAO|REVOGA.?.? DE PRISÃO|PRISAO DOMICILIAR|PRISÃO DOMICILIAR"
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_beneficio_temporario",
        F.when(
            F.col("txt_beneficio_upper").rlike(
                "SAIDA TEMPORARIA|SAÍDA TEMPORÁRIA|VISITA PERIODICA|VISITA PERIÓDICA|TRABALHO EXTERNO"
            ),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_beneficio_regime",
        F.when(
            F.col("txt_beneficio_upper").rlike("REGIME|PROGRESSAO|PROGRESSÃO"),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "status_alvara",
        F.when(F.col("dt_revogacao_alvara").isNotNull(), F.lit("REVOGADO"))
         .when(F.col("dt_cumprimento_alvara").isNotNull(), F.lit("CUMPRIDO"))
         .when(
             F.col("dt_validade_alvara_ref").isNotNull() &
             (F.col("dt_validade_alvara_ref") < F.current_date()),
             F.lit("VENCIDO")
         )
         .when(F.col("dt_referencia_alvara").isNotNull(), F.lit("ATIVO_OU_EM_ABERTO"))
         .otherwise(F.lit("SEM_STATUS"))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_alvara_ativo",
        F.when(F.col("status_alvara") == "ATIVO_OU_EM_ABERTO", F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_alvara_cumprido",
        F.when(F.col("status_alvara") == "CUMPRIDO", F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_alvara_revogado",
        F.when(F.col("status_alvara") == "REVOGADO", F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_alvara_vencido",
        F.when(F.col("status_alvara") == "VENCIDO", F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn(
        "idade_alvara_dias",
        F.when(
            F.col("dt_referencia_alvara_ref").isNotNull(),
            F.datediff(F.current_date(), F.col("dt_referencia_alvara_ref"))
        ).otherwise(F.lit(None).cast("int"))
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_alvara_recente_30d",
        F.when(
            F.col("idade_alvara_dias").between(0, 30),
            F.lit(1)
        ).otherwise(F.lit(0))
    )

    w_preso = Window.partitionBy("id_preso_str")
    w_pessoa = Window.partitionBy("id_pessoa")

    df_alvaras = df_alvaras.withColumn(
        "qtd_alvaras_preso",
        F.count(F.lit(1)).over(w_preso)
    )

    df_alvaras = df_alvaras.withColumn(
        "qtd_alvaras_pessoa",
        F.when(
            F.col("id_pessoa").isNotNull(),
            F.count(F.lit(1)).over(w_pessoa)
        ).otherwise(F.lit(None).cast("int"))
    )

    df_alvaras = df_alvaras.withColumn(
        "dt_primeiro_alvara_preso",
        F.min("dt_referencia_alvara").over(w_preso)
    )

    df_alvaras = df_alvaras.withColumn(
        "dt_ultimo_alvara_preso",
        F.max("dt_referencia_alvara").over(w_preso)
    )

    df_alvaras = df_alvaras.withColumn(
        "flag_multiplos_alvaras_preso",
        F.when(F.col("qtd_alvaras_preso") > 1, F.lit(1)).otherwise(F.lit(0))
    )

    df_alvaras = df_alvaras.withColumn("src_dt_emissao_alvara", lit_str_or_null(src_dt_emissao))
    df_alvaras = df_alvaras.withColumn("src_dt_cadastro_alvara", lit_str_or_null(src_dt_cadastro))
    df_alvaras = df_alvaras.withColumn("src_dt_cumprimento_alvara", lit_str_or_null(src_dt_cumprimento))
    df_alvaras = df_alvaras.withColumn("src_dt_revogacao_alvara", lit_str_or_null(src_dt_revogacao))
    df_alvaras = df_alvaras.withColumn("src_dt_validade_alvara", lit_str_or_null(src_dt_validade))

    # ============================================================
    # PERSISTENCIA INTERMEDIARIA POS-WINDOW
    # ============================================================

    tabela = "tmp_base_alvaras_enriquecida"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_alvaras.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_alvaras, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_alvaras_enriquecida")

    # ============================================================
    # ENTIDADE FINAL
    # ============================================================

    df_alvaras_final = spark.table("gold.tmp_base_alvaras_enriquecida")

    tabela = "sinp_ent_alvaras"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_alvaras_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_alvaras_final, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres(f"gold.{tabela}", "id_alvara")

# ===== CELL 20 =====
    # ============================================================
    # FATO DE ENCARCERAMENTO
    # Refatorado para execução distribuída em Spark SQL.
    # Remove processamento Python/RDD por preso e evita montagem manual
    # de múltiplas saídas em mapPartitions.
    # ============================================================

    def persistir_gold(df, tabela, pk_postgres=None, coluna_id_contagem=None):
        destino_tabela = f"{path}{tabela}"

        spark.sql(f"drop table if exists gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {destino_tabela} >/dev/null 2>&1")

        df.write \
            .mode("overwrite") \
            .option("maxRecordsPerFile", 1_000_000) \
            .option("compression", "snappy") \
            .parquet(destino_tabela)

        write_impala_table_partioned(
            df,
            "gold",
            tabela,
            destino_tabela
        )

        spark.catalog.clearCache()
        spark.sql(f"refresh table gold.{tabela}")

        if pk_postgres is not None:
            enviar_gold_para_postgres(
                f"gold.{tabela}",
                pk_postgres
            )

        if coluna_id_contagem is not None:
            spark.sql(f"""
                select
                    '{tabela}' as tabela,
                    count(*) as total_registros,
                    count(distinct {coluna_id_contagem}) as total_ids
                from gold.{tabela}
            """).show(truncate=False)

        return spark.table(f"gold.{tabela}")

    # ============================================================
    # 1. BASE UNIFICADA DE EVENTOS
    # ============================================================

    spark.sql("REFRESH TABLE gold.sinp_ent_mov_entrada")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_saida")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_interna")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_externa")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_saidinha")
    spark.catalog.clearCache()

    df_eventos_macro = spark.sql("""
    select
        cast(id_preso as string) as id_preso,
        cast(id_pessoa as string) as id_pessoa,
        nome_pessoa,
        cast(id_movimentacao as string) as id_movimentacao,
        cast(id_tipomovimentacao as string) as id_tipomovimentacao,
        ds_tipo_mov,
        cast(movimentacao_data as timestamp) as dt_evento,
        cast(id_estabelecimentosecurity as string) as id_estabelecimento,
        observacao,
        categoria_movimentacao,
        cast(null as string) as subcategoria_evento,
        cast(ids_alvara as string) as ids_alvara,
        cast(qtd_alvaras as bigint) as qtd_alvaras,
        cast(ids_artigo as string) as ids_artigo,
        cast(ds_tipificacao_penal as string) as ds_tipificacao_penal,
        cast(ds_tipificacao_penal_principal as string) as ds_tipificacao_penal_principal,
        cast(qtd_tipificacoes_penais as bigint) as qtd_tipificacoes_penais,
        cast(ids_estabelecimento_externo as string) as ids_estabelecimento_externo,
        cast(ds_estabelecimento_externo as string) as ds_estabelecimento_externo,
        cast(ids_estabelecimento_security as string) as ids_estabelecimento_security,
        cast(ids_estabelecimento_anterior as string) as ids_estabelecimento_anterior,
        cast(ids_tipo_obito as string) as ids_tipo_obito,
        cast(ds_tipo_obito as string) as ds_tipo_obito,
        cast(ids_tipo_saida_temporaria as string) as ids_tipo_saida_temporaria,
        cast(ds_tipo_saida_temporaria as string) as ds_tipo_saida_temporaria,
        cast(dt_retorno_saida_temporaria as timestamp) as dt_retorno_saida_temporaria,
        1 as prioridade_categoria
    from gold.sinp_ent_mov_entrada
    where id_preso is not null
      and movimentacao_data is not null

    union all

    select
        cast(id_preso as string) as id_preso,
        cast(id_pessoa as string) as id_pessoa,
        nome_pessoa,
        cast(id_movimentacao as string) as id_movimentacao,
        cast(id_tipomovimentacao as string) as id_tipomovimentacao,
        ds_tipo_mov,
        cast(movimentacao_data as timestamp) as dt_evento,
        cast(id_estabelecimentosecurity as string) as id_estabelecimento,
        observacao,
        categoria_movimentacao,
        cast(null as string) as subcategoria_evento,
        cast(ids_alvara as string) as ids_alvara,
        cast(qtd_alvaras as bigint) as qtd_alvaras,
        cast(ids_artigo as string) as ids_artigo,
        cast(ds_tipificacao_penal as string) as ds_tipificacao_penal,
        cast(ds_tipificacao_penal_principal as string) as ds_tipificacao_penal_principal,
        cast(qtd_tipificacoes_penais as bigint) as qtd_tipificacoes_penais,
        cast(ids_estabelecimento_externo as string) as ids_estabelecimento_externo,
        cast(ds_estabelecimento_externo as string) as ds_estabelecimento_externo,
        cast(ids_estabelecimento_security as string) as ids_estabelecimento_security,
        cast(ids_estabelecimento_anterior as string) as ids_estabelecimento_anterior,
        cast(ids_tipo_obito as string) as ids_tipo_obito,
        cast(ds_tipo_obito as string) as ds_tipo_obito,
        cast(ids_tipo_saida_temporaria as string) as ids_tipo_saida_temporaria,
        cast(ds_tipo_saida_temporaria as string) as ds_tipo_saida_temporaria,
        cast(dt_retorno_saida_temporaria as timestamp) as dt_retorno_saida_temporaria,
        2 as prioridade_categoria
    from gold.sinp_ent_mov_saida
    where id_preso is not null
      and movimentacao_data is not null
    """)

    persistir_gold(df_eventos_macro, "tmp_eventos_encarceramento_macro")

    df_eventos_menores = spark.sql("""
    select
        cast(id_preso as string) as id_preso,
        cast(id_pessoa as string) as id_pessoa,
        nome_pessoa,
        cast(id_movimentacao as string) as id_movimentacao,
        cast(id_tipomovimentacao as string) as id_tipomovimentacao,
        ds_tipo_mov,
        cast(movimentacao_data as timestamp) as dt_evento,
        cast(id_estabelecimentosecurity as string) as id_estabelecimento,
        observacao,
        categoria_movimentacao,
        cast(subcategoria_saidinha as string) as subcategoria_evento,
        cast(ids_alvara as string) as ids_alvara,
        cast(qtd_alvaras as bigint) as qtd_alvaras,
        cast(ids_artigo as string) as ids_artigo,
        cast(ds_tipificacao_penal as string) as ds_tipificacao_penal,
        cast(ds_tipificacao_penal_principal as string) as ds_tipificacao_penal_principal,
        cast(qtd_tipificacoes_penais as bigint) as qtd_tipificacoes_penais,
        cast(ids_estabelecimento_externo as string) as ids_estabelecimento_externo,
        cast(ds_estabelecimento_externo as string) as ds_estabelecimento_externo,
        cast(ids_estabelecimento_security as string) as ids_estabelecimento_security,
        cast(ids_estabelecimento_anterior as string) as ids_estabelecimento_anterior,
        cast(ids_tipo_obito as string) as ids_tipo_obito,
        cast(ds_tipo_obito as string) as ds_tipo_obito,
        cast(ids_tipo_saida_temporaria as string) as ids_tipo_saida_temporaria,
        cast(ds_tipo_saida_temporaria as string) as ds_tipo_saida_temporaria,
        cast(dt_retorno_saida_temporaria as timestamp) as dt_retorno_saida_temporaria,
        case
            when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then 3
            when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then 4
            when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then 5
            else 9
        end as prioridade_categoria
    from (
        select * from gold.sinp_ent_mov_interna
        union all
        select * from gold.sinp_ent_mov_externa
        union all
        select * from gold.sinp_ent_mov_saidinha
    ) x
    where id_preso is not null
      and movimentacao_data is not null
    """)

    persistir_gold(df_eventos_menores, "tmp_eventos_encarceramento_menores")

    df_eventos_base = spark.sql("""
    select
        md5(
            concat_ws(
                '||',
                coalesce(cast(id_preso as string), ''),
                coalesce(cast(id_movimentacao as string), ''),
                coalesce(cast(categoria_movimentacao as string), ''),
                coalesce(cast(dt_evento as string), ''),
                coalesce(cast(prioridade_categoria as string), '')
            )
        ) as id_evento_ref,
        x.*,
        case
            when categoria_movimentacao = 'ENTRADA' then 1
            when categoria_movimentacao = 'SAIDA' then 2
            when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then 3
            when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then 4
            when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then 5
            else 9
        end as ord_tp_evento
    from (
        select * from gold.tmp_eventos_encarceramento_macro
        union all
        select * from gold.tmp_eventos_encarceramento_menores
    ) x
    """)

    persistir_gold(df_eventos_base, "tmp_eventos_encarceramento_base")

    # ============================================================
    # 2. ENTRADAS, SAIDAS E PERIODOS
    # ============================================================

    df_entradas = spark.sql("""
    select
        e.*,
        row_number() over (
            partition by e.id_preso
            order by
                e.dt_evento,
                e.ord_tp_evento,
                e.prioridade_categoria,
                coalesce(e.id_movimentacao, '')
        ) as nr_periodo_encarceramento,
        lead(e.dt_evento) over (
            partition by e.id_preso
            order by
                e.dt_evento,
                e.ord_tp_evento,
                e.prioridade_categoria,
                coalesce(e.id_movimentacao, '')
        ) as dt_proxima_entrada,
        lead(e.id_movimentacao) over (
            partition by e.id_preso
            order by
                e.dt_evento,
                e.ord_tp_evento,
                e.prioridade_categoria,
                coalesce(e.id_movimentacao, '')
        ) as id_movimentacao_proxima_entrada
    from gold.tmp_eventos_encarceramento_base e
    where e.categoria_movimentacao = 'ENTRADA'
    """)

    persistir_gold(df_entradas, "tmp_enc_entradas_ordenadas")

    df_saida_primeira_periodo = spark.sql("""
    select
        id_preso,
        nr_periodo_encarceramento,
        id_evento_ref_saida,
        id_movimentacao_saida,
        id_tipomovimentacao_saida,
        ds_tipo_mov_saida,
        dt_saida,
        id_estabelecimento_saida,
        observacao_saida
    from (
        select
            e.id_preso,
            e.nr_periodo_encarceramento,
            s.id_evento_ref as id_evento_ref_saida,
            s.id_movimentacao as id_movimentacao_saida,
            s.id_tipomovimentacao as id_tipomovimentacao_saida,
            s.ds_tipo_mov as ds_tipo_mov_saida,
            s.dt_evento as dt_saida,
            s.id_estabelecimento as id_estabelecimento_saida,
            s.observacao as observacao_saida,
            row_number() over (
                partition by e.id_preso, e.nr_periodo_encarceramento
                order by
                    s.dt_evento,
                    s.ord_tp_evento,
                    s.prioridade_categoria,
                    coalesce(s.id_movimentacao, '')
            ) as rn_saida
        from gold.tmp_enc_entradas_ordenadas e
        inner join gold.tmp_eventos_encarceramento_base s
            on e.id_preso = s.id_preso
           and s.categoria_movimentacao = 'SAIDA'
           and s.dt_evento >= e.dt_evento
           and (
                e.dt_proxima_entrada is null
                or s.dt_evento < e.dt_proxima_entrada
           )
    ) x
    where rn_saida = 1
    """)

    persistir_gold(df_saida_primeira_periodo, "tmp_enc_saida_primeira_periodo")

    df_periodos_base = spark.sql("""
    select
        md5(
            concat_ws(
                '||',
                coalesce(cast(e.id_preso as string), ''),
                coalesce(cast(e.nr_periodo_encarceramento as string), ''),
                coalesce(cast(e.id_movimentacao as string), ''),
                coalesce(cast(e.dt_evento as string), '')
            )
        ) as id_encarceramento,

        e.id_preso,
        e.id_pessoa,
        e.nome_pessoa,
        cast(e.nr_periodo_encarceramento as int) as nr_periodo_encarceramento,

        e.id_movimentacao as id_movimentacao_entrada,
        e.id_tipomovimentacao as id_tipomovimentacao_entrada,
        e.ds_tipo_mov as ds_tipo_mov_entrada,
        e.dt_evento as dt_entrada,
        e.id_estabelecimento as id_estabelecimento_entrada,
        e.observacao as observacao_entrada,
        e.ids_artigo as ids_artigo_entrada,
        e.ds_tipificacao_penal as ds_tipificacao_penal_entrada,
        e.ds_tipificacao_penal_principal as ds_tipificacao_penal_principal_entrada,

        s.id_movimentacao_saida,
        s.id_tipomovimentacao_saida,
        s.ds_tipo_mov_saida,
        case
            when s.dt_saida is not null then s.dt_saida
            when e.dt_proxima_entrada is not null then cast(from_unixtime(unix_timestamp(e.dt_proxima_entrada) - 1) as timestamp)
            else cast(null as timestamp)
        end as dt_saida,
        s.id_estabelecimento_saida,
        s.observacao_saida,
        case
            when s.dt_saida is not null then 'SAIDA'
            when e.dt_proxima_entrada is not null then 'AJUSTE_NOVA_ENTRADA'
            else cast(null as string)
        end as tp_fechamento,

        e.dt_proxima_entrada,
        e.id_movimentacao_proxima_entrada
    from gold.tmp_enc_entradas_ordenadas e
    left join gold.tmp_enc_saida_primeira_periodo s
        on e.id_preso = s.id_preso
       and e.nr_periodo_encarceramento = s.nr_periodo_encarceramento
    """)

    persistir_gold(df_periodos_base, "tmp_enc_periodos_base")

    # ============================================================
    # 3. EVENTOS MENORES ASSOCIADOS AOS PERIODOS
    # ============================================================

    df_eventos_periodo_base = spark.sql("""
    select
        md5(
            concat_ws(
                '||',
                coalesce(cast(p.id_encarceramento as string), ''),
                coalesce(cast(e.id_movimentacao as string), ''),
                coalesce(cast(e.categoria_movimentacao as string), '')
            )
        ) as id_encarceramento_evento,

        e.id_evento_ref,
        p.id_encarceramento,
        p.id_preso,
        p.id_pessoa,
        p.nome_pessoa,
        p.nr_periodo_encarceramento,

        e.categoria_movimentacao,
        e.subcategoria_evento,
        e.id_movimentacao,
        e.id_tipomovimentacao,
        e.ds_tipo_mov,
        e.dt_evento,
        e.id_estabelecimento,
        e.observacao,
        e.ids_alvara,
        e.qtd_alvaras,
        e.ids_artigo,
        e.ds_tipificacao_penal,
        e.ds_tipificacao_penal_principal,
        e.qtd_tipificacoes_penais,
        e.ids_estabelecimento_externo,
        e.ds_estabelecimento_externo,
        e.ids_estabelecimento_security,
        e.ids_estabelecimento_anterior,
        e.ids_tipo_obito,
        e.ds_tipo_obito,
        e.ids_tipo_saida_temporaria,
        e.ds_tipo_saida_temporaria,
        e.dt_retorno_saida_temporaria
    from gold.tmp_eventos_encarceramento_base e
    inner join gold.tmp_enc_periodos_base p
        on e.id_preso = p.id_preso
       and e.dt_evento >= p.dt_entrada
       and (
            p.dt_saida is null
            or e.dt_evento < p.dt_saida
       )
    where e.categoria_movimentacao not in ('ENTRADA', 'SAIDA')
    """)

    persistir_gold(df_eventos_periodo_base, "tmp_enc_eventos_periodo_base")

    df_agregados_periodo = spark.sql("""
    select
        id_encarceramento,

        cast(sum(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then 1 else 0 end) as int) as qtd_mov_internas,
        cast(sum(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then 1 else 0 end) as int) as qtd_mov_externas,
        cast(sum(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then 1 else 0 end) as int) as qtd_mov_saidinha,

        cast(sum(case when subcategoria_evento = 'SAIDA_SAIDINHA' then 1 else 0 end) as int) as qtd_saida_saidinha,
        cast(sum(case when subcategoria_evento = 'RETORNO_SAIDINHA' then 1 else 0 end) as int) as qtd_retorno_saidinha,

        cast(sum(case when qtd_alvaras is not null then qtd_alvaras else 0 end) as int) as qtd_alvaras_periodo,

        min(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then dt_evento else null end) as dt_primeira_mov_interna,
        max(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then dt_evento else null end) as dt_ultima_mov_interna,

        min(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then dt_evento else null end) as dt_primeira_mov_externa,
        max(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then dt_evento else null end) as dt_ultima_mov_externa,

        min(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then dt_evento else null end) as dt_primeira_saidinha,
        max(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then dt_evento else null end) as dt_ultima_saidinha,

        case
            when sum(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' and ds_tipo_mov is not null then 1 else 0 end) > 0
            then concat_ws(',', sort_array(collect_set(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then ds_tipo_mov else null end)))
            else cast(null as string)
        end as ds_eventos_internos,

        case
            when sum(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' and ds_tipo_mov is not null then 1 else 0 end) > 0
            then concat_ws(',', sort_array(collect_set(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then ds_tipo_mov else null end)))
            else cast(null as string)
        end as ds_eventos_externos,

        case
            when sum(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' and ds_tipo_mov is not null then 1 else 0 end) > 0
            then concat_ws(',', sort_array(collect_set(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then ds_tipo_mov else null end)))
            else cast(null as string)
        end as ds_eventos_saidinha
    from gold.tmp_enc_eventos_periodo_base
    group by id_encarceramento
    """)

    persistir_gold(df_agregados_periodo, "tmp_enc_agregados_periodo")

    # ============================================================
    # 4. FATO FINAL DE ENCARCERAMENTO
    # ============================================================

    df_fat_encarceramento = spark.sql("""
    select
        p.id_encarceramento,
        p.id_preso,
        p.id_pessoa,
        p.nome_pessoa,
        p.nr_periodo_encarceramento,

        p.id_movimentacao_entrada,
        p.id_tipomovimentacao_entrada,
        p.ds_tipo_mov_entrada,
        p.dt_entrada,
        p.id_estabelecimento_entrada,
        p.observacao_entrada,
        p.ids_artigo_entrada,
        p.ds_tipificacao_penal_entrada,
        p.ds_tipificacao_penal_principal_entrada,

        p.id_movimentacao_saida,
        p.id_tipomovimentacao_saida,
        p.ds_tipo_mov_saida,
        p.dt_saida,
        p.id_estabelecimento_saida,
        p.observacao_saida,
        p.tp_fechamento,

        case
            when p.dt_saida is not null then 'FECHADO'
            else 'ABERTO'
        end as st_encarceramento,

        cast(datediff(coalesce(to_date(p.dt_saida), current_date()), to_date(p.dt_entrada)) as int) as qt_dias_encarceramento,

        coalesce(a.qtd_mov_internas, 0) as qtd_mov_internas,
        coalesce(a.qtd_mov_externas, 0) as qtd_mov_externas,
        coalesce(a.qtd_mov_saidinha, 0) as qtd_mov_saidinha,
        coalesce(a.qtd_saida_saidinha, 0) as qtd_saida_saidinha,
        coalesce(a.qtd_retorno_saidinha, 0) as qtd_retorno_saidinha,
        coalesce(a.qtd_alvaras_periodo, 0) as qtd_alvaras_periodo,

        a.dt_primeira_mov_interna,
        a.dt_ultima_mov_interna,
        a.dt_primeira_mov_externa,
        a.dt_ultima_mov_externa,
        a.dt_primeira_saidinha,
        a.dt_ultima_saidinha,

        a.ds_eventos_internos,
        a.ds_eventos_externos,
        a.ds_eventos_saidinha,

        cast(
            coalesce(a.qtd_mov_internas, 0) * 1.0
            + coalesce(a.qtd_mov_externas, 0) * 2.0
            + coalesce(a.qtd_mov_saidinha, 0) * 1.5
            + coalesce(a.qtd_alvaras_periodo, 0) * 2.0
            as double
        ) as score_comportamental,

        case
            when (
                coalesce(a.qtd_mov_internas, 0) * 1.0
                + coalesce(a.qtd_mov_externas, 0) * 2.0
                + coalesce(a.qtd_mov_saidinha, 0) * 1.5
                + coalesce(a.qtd_alvaras_periodo, 0) * 2.0
            ) >= 20 then 'ALTA_DINAMICA'
            when (
                coalesce(a.qtd_mov_internas, 0) * 1.0
                + coalesce(a.qtd_mov_externas, 0) * 2.0
                + coalesce(a.qtd_mov_saidinha, 0) * 1.5
                + coalesce(a.qtd_alvaras_periodo, 0) * 2.0
            ) >= 8 then 'MEDIA_DINAMICA'
            else 'BAIXA_DINAMICA'
        end as perfil_comportamental,

        'N' as fl_inconsistente
    from gold.tmp_enc_periodos_base p
    left join gold.tmp_enc_agregados_periodo a
        on p.id_encarceramento = a.id_encarceramento
    """)

    persistir_gold(
        df_fat_encarceramento.dropDuplicates(["id_encarceramento"]),
        "sinp_fat_encarceramento",
        pk_postgres="id_encarceramento",
        coluna_id_contagem="id_encarceramento"
    )

    df_fat_encarceramento_evento = (
        spark.table("gold.tmp_enc_eventos_periodo_base")
        .select(
            "id_encarceramento_evento",
            "id_encarceramento",
            "id_preso",
            "id_pessoa",
            "nome_pessoa",
            "nr_periodo_encarceramento",
            "categoria_movimentacao",
            "subcategoria_evento",
            "id_movimentacao",
            "id_tipomovimentacao",
            "ds_tipo_mov",
            "dt_evento",
            "id_estabelecimento",
            "observacao",
            "ids_alvara",
            "qtd_alvaras",
            "ids_artigo",
            "ds_tipificacao_penal",
            "ds_tipificacao_penal_principal",
            "qtd_tipificacoes_penais",
            "ids_estabelecimento_externo",
            "ds_estabelecimento_externo",
            "ids_estabelecimento_security",
            "ids_estabelecimento_anterior",
            "ids_tipo_obito",
            "ds_tipo_obito",
            "ids_tipo_saida_temporaria",
            "ds_tipo_saida_temporaria",
            "dt_retorno_saida_temporaria"
        )
        .dropDuplicates(["id_encarceramento_evento"])
    )

    persistir_gold(
        df_fat_encarceramento_evento,
        "sinp_fat_encarceramento_evento",
        pk_postgres="id_encarceramento_evento",
        coluna_id_contagem="id_encarceramento_evento"
    )

    # ============================================================
    # 5. INCONSISTENCIAS
    # ============================================================

    df_incons_entrada_sem_saida = spark.sql("""
    select
        md5(
            concat_ws(
                '||',
                coalesce(cast(n.id_preso as string), ''),
                coalesce(cast(n.id_movimentacao as string), ''),
                'ENTRADA_SEM_SAIDA_ANTERIOR'
            )
        ) as id_encarceramento_inconsistencia,
        n.id_preso,
        n.id_pessoa,
        n.nome_pessoa,
        cast(p.nr_periodo_encarceramento as int) as nr_periodo_encarceramento,
        n.id_movimentacao as id_movimentacao_ref,
        n.categoria_movimentacao as categoria_movimentacao_ref,
        n.ds_tipo_mov as ds_tipo_mov_ref,
        n.dt_evento as dt_evento_ref,
        'ENTRADA_SEM_SAIDA_ANTERIOR' as tp_inconsistencia,
        'Periodo anterior fechado artificialmente em nova entrada - 1 segundo' as detalhe_inconsistencia
    from gold.tmp_enc_periodos_base p
    inner join gold.tmp_enc_entradas_ordenadas n
        on p.id_preso = n.id_preso
       and n.nr_periodo_encarceramento = p.nr_periodo_encarceramento + 1
    where p.tp_fechamento = 'AJUSTE_NOVA_ENTRADA'
    """)

    persistir_gold(df_incons_entrada_sem_saida, "tmp_enc_incons_entrada_sem_saida")

    df_incons_saida_sem_entrada = spark.sql("""
    select
        md5(
            concat_ws(
                '||',
                coalesce(cast(s.id_preso as string), ''),
                coalesce(cast(s.id_movimentacao as string), ''),
                'SAIDA_SEM_ENTRADA'
            )
        ) as id_encarceramento_inconsistencia,
        s.id_preso,
        s.id_pessoa,
        s.nome_pessoa,
        cast(null as int) as nr_periodo_encarceramento,
        s.id_movimentacao as id_movimentacao_ref,
        s.categoria_movimentacao as categoria_movimentacao_ref,
        s.ds_tipo_mov as ds_tipo_mov_ref,
        s.dt_evento as dt_evento_ref,
        'SAIDA_SEM_ENTRADA' as tp_inconsistencia,
        'Saida encontrada sem periodo aberto' as detalhe_inconsistencia
    from gold.tmp_eventos_encarceramento_base s
    left join gold.tmp_enc_saida_primeira_periodo sp
        on s.id_evento_ref = sp.id_evento_ref_saida
    where s.categoria_movimentacao = 'SAIDA'
      and sp.id_evento_ref_saida is null
    """)

    persistir_gold(df_incons_saida_sem_entrada, "tmp_enc_incons_saida_sem_entrada")

    df_incons_evento_menor = spark.sql("""
    select
        md5(
            concat_ws(
                '||',
                coalesce(cast(e.id_preso as string), ''),
                coalesce(cast(e.id_movimentacao as string), ''),
                'EVENTO_MENOR_FORA_PERIODO'
            )
        ) as id_encarceramento_inconsistencia,
        e.id_preso,
        e.id_pessoa,
        e.nome_pessoa,
        cast(null as int) as nr_periodo_encarceramento,
        e.id_movimentacao as id_movimentacao_ref,
        e.categoria_movimentacao as categoria_movimentacao_ref,
        e.ds_tipo_mov as ds_tipo_mov_ref,
        e.dt_evento as dt_evento_ref,
        'EVENTO_MENOR_FORA_PERIODO' as tp_inconsistencia,
        'Evento menor sem encarceramento aberto para agrupamento' as detalhe_inconsistencia
    from gold.tmp_eventos_encarceramento_base e
    left join gold.tmp_enc_eventos_periodo_base ep
        on e.id_evento_ref = ep.id_evento_ref
    where e.categoria_movimentacao not in ('ENTRADA', 'SAIDA')
      and ep.id_evento_ref is null
    """)

    persistir_gold(df_incons_evento_menor, "tmp_enc_incons_evento_menor")

    df_fat_encarceramento_inconsistencia = spark.sql("""
    select * from gold.tmp_enc_incons_entrada_sem_saida
    union all
    select * from gold.tmp_enc_incons_saida_sem_entrada
    union all
    select * from gold.tmp_enc_incons_evento_menor
    """)

    persistir_gold(
        df_fat_encarceramento_inconsistencia.dropDuplicates(["id_encarceramento_inconsistencia"]),
        "sinp_fat_encarceramento_inconsistencia",
        pk_postgres="id_encarceramento_inconsistencia",
        coluna_id_contagem="id_encarceramento_inconsistencia"
    )


    #====================================================================================================================
    # PONTE SERVIDOR PRONTUARIO
    #====================================================================================================================

    spark.sql("refresh table bronze.livros_acesso_unidade_historicalpolicial")
    spark.sql("refresh table gold.sinp_ent_pessoa")

    spark.sql("refresh table gold.sinp_fil_pront_crim")
    spark.sql("refresh table gold.sinp_fil_pront_prof")
    spark.sql("refresh table gold.sinp_fil_pront_psico")
    spark.sql("refresh table gold.sinp_fil_pront_soc")
    spark.catalog.clearCache()

    # ============================================================
    # BASE HISTORICAL POLICIAL NORMALIZADA
    # ============================================================

    tabela = "tmp_sh_pront_base"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_sh_pront_base = spark.sql("""
        select distinct
            trim(regexp_extract(coalesce(documento, ''), '^([^ ]+)', 1)) as documento,
            cast(history_id as bigint) as id_usuario_preenchimento,
            cast(trim(regexp_extract(coalesce(documento, ''), '^([^ ]+)', 1)) as bigint) as id_preso_num
        from bronze.livros_acesso_unidade_historicalpolicial
        where documento is not null
          and trim(documento) <> ''
          and history_id is not null
          and trim(regexp_extract(coalesce(documento, ''), '^([^ ]+)', 1)) rlike '^[0-9]+$'
    """)

    df_sh_pront_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sh_pront_base, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # REFERENCIA PESSOA POR ID_PRESO
    # ============================================================

    tabela = "tmp_sh_pront_pessoa"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_sh_pront_pessoa = spark.sql("""
        select distinct
            cast(id_preso as bigint) as id_preso_num,
            id_pessoa
        from gold.sinp_ent_pessoa
        where id_preso is not null
          and id_pessoa is not null
    """)

    df_sh_pront_pessoa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sh_pront_pessoa, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # UNION DE PRONTUARIOS
    # ============================================================

    tabela = "tmp_sh_pront_prontuario_union"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_sh_pront_prontuario_union = spark.sql("""
        select distinct
            cast(id_usuario_preenchimento as bigint) as id_usuario_preenchimento,
            cast(id_pessoa_prontcrim as string) as id_pessoa_prontcrim
        from gold.sinp_fil_pront_crim
        where id_usuario_preenchimento is not null
          and id_pessoa_prontcrim is not null

        union all

        select distinct
            cast(id_usuario_preenchimento as bigint) as id_usuario_preenchimento,
            cast(id_pessoa_prontprof as string) as id_pessoa_prontcrim
        from gold.sinp_fil_pront_prof
        where id_usuario_preenchimento is not null
          and id_pessoa_prontprof is not null

        union all

        select distinct
            cast(id_usuario_preenchimento as bigint) as id_usuario_preenchimento,
            cast(id_pessoa_prontpsico as string) as id_pessoa_prontcrim
        from gold.sinp_fil_pront_psico
        where id_usuario_preenchimento is not null
          and id_pessoa_prontpsico is not null

        union all

        select distinct
            cast(id_usuario_preenchimento as bigint) as id_usuario_preenchimento,
            cast(id_pessoa_prontsoc as string) as id_pessoa_prontcrim
        from gold.sinp_fil_pront_soc
        where id_usuario_preenchimento is not null
          and id_pessoa_prontsoc is not null
    """)

    df_sh_pront_prontuario_union.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sh_pront_prontuario_union, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # DEDUP DE PRONTUARIOS
    # ============================================================

    tabela = "tmp_sh_pront_prontuario_dedup"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_sh_pront_prontuario_dedup = spark.sql("""
        select distinct
            id_usuario_preenchimento,
            id_pessoa_prontcrim
        from gold.tmp_sh_pront_prontuario_union
    """)

    df_sh_pront_prontuario_dedup.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sh_pront_prontuario_dedup, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # PONTE FINAL
    # ============================================================

    tabela = "sinp_pnt_sh_pront"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    pnt_sh_pront = spark.sql("""
        select distinct
            substr(
                md5(
                    concat_ws(
                        '|',
                        cast(b.id_preso_num as string),
                        cast(b.id_usuario_preenchimento as string),
                        coalesce(p.id_pessoa, ''),
                        coalesce(pr.id_pessoa_prontcrim, '')
                    )
                ),
                1,
                30
            ) as id_pnt_sh_pront,

            b.documento,
            b.id_usuario_preenchimento,
            b.id_preso_num as id_preso,
            p.id_pessoa,
            pr.id_pessoa_prontcrim

        from gold.tmp_sh_pront_base b

        left join gold.tmp_sh_pront_pessoa p
            on b.id_preso_num = p.id_preso_num

        left join gold.tmp_sh_pront_prontuario_dedup pr
            on b.id_usuario_preenchimento = pr.id_usuario_preenchimento
    """)

    pnt_sh_pront.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        pnt_sh_pront,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres(
        f"gold.{tabela}",
        "id_pnt_sh_pront"
    )

    spark.sql(f"""
        select
            count(*) as total_registros,
            count(distinct id_pnt_sh_pront) as total_ids_distintos,
            count(distinct documento) as total_documentos_distintos,
            count(distinct id_usuario_preenchimento) as total_usuarios_preenchimento_distintos,
            count(distinct id_preso) as total_id_preso_distintos,
            count(distinct id_pessoa) as total_id_pessoa_distintos,
            count(distinct id_pessoa_prontcrim) as total_id_pessoa_prontcrim_distintos,
            sum(case when id_pessoa is not null then 1 else 0 end) as registros_com_id_pessoa,
            sum(case when id_pessoa_prontcrim is not null then 1 else 0 end) as registros_com_id_pessoa_prontcrim,
            max(length(id_pnt_sh_pront)) as tamanho_max_id
        from gold.{tabela}
    """).show(truncate=False)
