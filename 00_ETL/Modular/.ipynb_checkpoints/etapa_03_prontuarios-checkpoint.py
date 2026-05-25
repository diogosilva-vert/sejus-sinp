# -*- coding: utf-8 -*-
"""Etapa 03 - Prontuários e ponte servidor-prontuário."""

import os
import hashlib
from datetime import datetime, timedelta, date

from contexto import *


def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

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
