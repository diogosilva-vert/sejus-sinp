# -*- coding: utf-8 -*-
"""Etapa 04 - Alvarás."""

import os
import hashlib
from datetime import datetime, timedelta, date

from contexto import *


def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

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
