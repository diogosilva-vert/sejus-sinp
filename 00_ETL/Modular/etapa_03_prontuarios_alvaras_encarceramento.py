# -*- coding: utf-8 -*-
"""Prontuários, alvarás e fato de encarceramento."""

from contexto import *

def executar(spark, path=path):
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

    df_corr.write.mode("overwrite").parquet(destino)

    df_valid = spark.read.parquet(destino)
    df_valid.createOrReplaceTempView("tmp_preso_prontuario_social_corr")

    spark.sql("""
    DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_social_corr
    """)

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_social_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

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

    df_corr.write.mode("overwrite").parquet(destino)

    df_valid = spark.read.parquet(destino)
    df_valid.createOrReplaceTempView("tmp_preso_prontuario_psico_corr")

    spark.sql("""
    DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_psico_corr
    """)

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_psico_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

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

    df_corr.write.mode("overwrite").parquet(destino)

    df_valid = spark.read.parquet(destino)
    df_valid.createOrReplaceTempView("tmp_preso_prontuario_criminal_corr")

    spark.sql("""
    DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_criminal_corr
    """)

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_criminal_corr
    USING PARQUET
    LOCATION '{destino}'
    """)


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

    df_corr.write.mode("overwrite").parquet(destino)

    df_valid = spark.read.parquet(destino)
    df_valid.createOrReplaceTempView("tmp_preso_prontuario_profissional_corr")

    spark.sql("""
    DROP TABLE IF EXISTS bronze.infopen_preso_prontuario_profissional_corr
    """)

    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_prontuario_profissional_corr
    USING PARQUET
    LOCATION '{destino}'
    """)


    # ===== CELL 18 =====
    prontsoc = spark.sql("""
    SELECT
        md5(concat(cast(ps.id_prontuario_social as string), cast(p.id_pessoa as string))) as id_pessoa_prontsoc,
        p.id_pessoa,p.nome_pessoa,
        ps.*
    FROM bronze.infopen_preso_prontuario_social_corr ps
    inner join gold.sinp_pnt_pessoa_preso p
        on cast(ps.id_preso as string) = cast(p.id_preso as string)
    """)

    tabela = "sinp_fil_pront_soc"
    prontsoc.write.mode("overwrite").option("maxRecordsPerFile", 1_000_000).option("compression", "snappy").parquet(f"{path}{tabela}")
    write_impala_table_partioned(prontsoc, "gold", tabela, f"{path}{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_soc", "id_pessoa_prontsoc")

    tabela = "sinp_fil_pront_soc"

    # ============================================================
    # PRONTUARIO CRIMINAL
    # ============================================================

    prontcrim = spark.sql("""
    SELECT
        md5(concat(cast(pc.id_prontuario_criminal as string), cast(p.id_pessoa as string))) as id_pessoa_prontcrim,
        p.id_pessoa,p.nome_pessoa,
        pc.*
    FROM bronze.infopen_preso_prontuario_criminal_corr pc
    inner join gold.sinp_pnt_pessoa_preso p
        on cast(pc.id_preso as string) = cast(p.id_preso as string)
    """)

    tabela = "sinp_fil_pront_crim"

    prontcrim.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontcrim, "gold", tabela, f"{path}{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_crim", "id_pessoa_prontcrim")


    # ============================================================
    # PRONTUARIO PROFISSIONAL
    # ============================================================

    prontprof = spark.sql("""
    SELECT
        md5(concat(cast(pp.id_prontuario_profissional as string), cast(p.id_pessoa as string))) as id_pessoa_prontprof,
        p.id_pessoa,p.nome_pessoa,
        pp.*
    FROM bronze.infopen_preso_prontuario_profissional_corr pp
    inner join gold.sinp_pnt_pessoa_preso p
        on cast(pp.id_preso as string) = cast(p.id_preso as string)
    """)

    tabela = "sinp_fil_pront_prof"

    prontprof.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontprof, "gold", tabela, f"{path}{tabela}")

    enviar_gold_para_postgres("gold.sinp_fil_pront_prof", "id_pessoa_prontprof")

    # ============================================================
    # PRONTUARIO PSICO
    # ============================================================

    prontpsico = spark.sql("""
    SELECT
        md5(concat(cast(pps.id_prontuario_psico as string), cast(p.id_pessoa as string))) as id_pessoa_prontpsico,
        p.id_pessoa,p.nome_pessoa,
        pps.*
    FROM bronze.infopen_preso_prontuario_psico_corr pps
    inner join gold.sinp_pnt_pessoa_preso p
        on cast(pps.id_preso as string) = cast(p.id_preso as string)
    """)

    tabela = "sinp_fil_pront_psico"

    prontpsico.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(prontpsico, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres("gold.sinp_fil_pront_psico", "id_pessoa_prontpsico")


    # ===== CELL 19 =====
    import os
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

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
    # BASE ALVARAS
    # ============================================================

    df_alvaras_base = spark.sql("""
        with alvaras as (
            select
                *,
                cast(id_preso as string) as id_preso_str
            from bronze.infopen_vw_alvaras
        )
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
        from alvaras a
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

    # Datas canônicas
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

    # Texto consolidado do benefício
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

    # Flags básicas
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

    # Tipificação do benefício
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

    # Status operacional
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

    # Métricas temporais
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

    # Métricas por preso/pessoa
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

    # Colunas de rastreabilidade - CORRIGIDAS PARA NAO GERAR VOID
    df_alvaras = df_alvaras.withColumn("src_dt_emissao_alvara", lit_str_or_null(src_dt_emissao))
    df_alvaras = df_alvaras.withColumn("src_dt_cadastro_alvara", lit_str_or_null(src_dt_cadastro))
    df_alvaras = df_alvaras.withColumn("src_dt_cumprimento_alvara", lit_str_or_null(src_dt_cumprimento))
    df_alvaras = df_alvaras.withColumn("src_dt_revogacao_alvara", lit_str_or_null(src_dt_revogacao))
    df_alvaras = df_alvaras.withColumn("src_dt_validade_alvara", lit_str_or_null(src_dt_validade))


    # ============================================================
    # PERSISTENCIA INTERMEDIARIA
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
    enviar_gold_para_postgres(f"gold.{tabela}", "id_alvara")


    # ===== CELL 20 =====
    from pyspark.sql import functions as F, types as T, Row
    import hashlib
    from datetime import datetime, timedelta, date

    # ============================================================
    # FUNCOES AUXILIARES
    # ============================================================

    def _hash(*vals):
        txt = "||".join("" if v is None else str(v) for v in vals)
        return hashlib.md5(txt.encode("utf-8")).hexdigest()

    def _to_python_dt(v):
        if v is None:
            return None
        return v

    def _today_midnight():
        now = datetime.now()
        return datetime(now.year, now.month, now.day)

    # ============================================================
    # 1. BASE UNIFICADA DE EVENTOS
    # ============================================================

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

    df_eventos_todos = (
        df_eventos_macro
        .unionByName(df_eventos_menores)
        .withColumn(
            "ord_tp_evento",
            F.when(F.col("categoria_movimentacao") == "ENTRADA", F.lit(1))
             .when(F.col("categoria_movimentacao") == "SAIDA", F.lit(2))
             .when(F.col("categoria_movimentacao") == "MOVIMENTACOES_INTERNAS", F.lit(3))
             .when(F.col("categoria_movimentacao") == "MOVIMENTACOES_EXTERNAS", F.lit(4))
             .when(F.col("categoria_movimentacao") == "MOVIMENTACOES_SAIDINHA", F.lit(5))
             .otherwise(F.lit(9))
        )
        .repartition("id_preso")
        .sortWithinPartitions("id_preso", "dt_evento", "ord_tp_evento", "prioridade_categoria", "id_movimentacao")
    )

    # ============================================================
    # 2. SCHEMAS
    # ============================================================

    schema_encarceramento = T.StructType([
        T.StructField("id_encarceramento", T.StringType(), False),
        T.StructField("id_preso", T.StringType(), True),
        T.StructField("id_pessoa", T.StringType(), True),
        T.StructField("nome_pessoa", T.StringType(), True),
        T.StructField("nr_periodo_encarceramento", T.IntegerType(), True),

        T.StructField("id_movimentacao_entrada", T.StringType(), True),
        T.StructField("id_tipomovimentacao_entrada", T.StringType(), True),
        T.StructField("ds_tipo_mov_entrada", T.StringType(), True),
        T.StructField("dt_entrada", T.TimestampType(), True),
        T.StructField("id_estabelecimento_entrada", T.StringType(), True),
        T.StructField("observacao_entrada", T.StringType(), True),
        T.StructField("ids_artigo_entrada", T.StringType(), True),
        T.StructField("ds_tipificacao_penal_entrada", T.StringType(), True),
        T.StructField("ds_tipificacao_penal_principal_entrada", T.StringType(), True),

        T.StructField("id_movimentacao_saida", T.StringType(), True),
        T.StructField("id_tipomovimentacao_saida", T.StringType(), True),
        T.StructField("ds_tipo_mov_saida", T.StringType(), True),
        T.StructField("dt_saida", T.TimestampType(), True),
        T.StructField("id_estabelecimento_saida", T.StringType(), True),
        T.StructField("observacao_saida", T.StringType(), True),
        T.StructField("tp_fechamento", T.StringType(), True),

        T.StructField("st_encarceramento", T.StringType(), True),
        T.StructField("qt_dias_encarceramento", T.IntegerType(), True),

        T.StructField("qtd_mov_internas", T.IntegerType(), True),
        T.StructField("qtd_mov_externas", T.IntegerType(), True),
        T.StructField("qtd_mov_saidinha", T.IntegerType(), True),
        T.StructField("qtd_saida_saidinha", T.IntegerType(), True),
        T.StructField("qtd_retorno_saidinha", T.IntegerType(), True),
        T.StructField("qtd_alvaras_periodo", T.IntegerType(), True),

        T.StructField("dt_primeira_mov_interna", T.TimestampType(), True),
        T.StructField("dt_ultima_mov_interna", T.TimestampType(), True),
        T.StructField("dt_primeira_mov_externa", T.TimestampType(), True),
        T.StructField("dt_ultima_mov_externa", T.TimestampType(), True),
        T.StructField("dt_primeira_saidinha", T.TimestampType(), True),
        T.StructField("dt_ultima_saidinha", T.TimestampType(), True),

        T.StructField("ds_eventos_internos", T.StringType(), True),
        T.StructField("ds_eventos_externos", T.StringType(), True),
        T.StructField("ds_eventos_saidinha", T.StringType(), True),

        T.StructField("score_comportamental", T.DoubleType(), True),
        T.StructField("perfil_comportamental", T.StringType(), True),

        T.StructField("fl_inconsistente", T.StringType(), True)
    ])

    schema_evento_periodo = T.StructType([
        T.StructField("id_encarceramento_evento", T.StringType(), False),
        T.StructField("id_encarceramento", T.StringType(), True),
        T.StructField("id_preso", T.StringType(), True),
        T.StructField("id_pessoa", T.StringType(), True),
        T.StructField("nome_pessoa", T.StringType(), True),
        T.StructField("nr_periodo_encarceramento", T.IntegerType(), True),
        T.StructField("categoria_movimentacao", T.StringType(), True),
        T.StructField("subcategoria_evento", T.StringType(), True),
        T.StructField("id_movimentacao", T.StringType(), True),
        T.StructField("id_tipomovimentacao", T.StringType(), True),
        T.StructField("ds_tipo_mov", T.StringType(), True),
        T.StructField("dt_evento", T.TimestampType(), True),
        T.StructField("id_estabelecimento", T.StringType(), True),
        T.StructField("observacao", T.StringType(), True),
        T.StructField("ids_alvara", T.StringType(), True),
        T.StructField("qtd_alvaras", T.LongType(), True),
        T.StructField("ids_artigo", T.StringType(), True),
        T.StructField("ds_tipificacao_penal", T.StringType(), True),
        T.StructField("ds_tipificacao_penal_principal", T.StringType(), True),
        T.StructField("qtd_tipificacoes_penais", T.LongType(), True),
        T.StructField("ids_estabelecimento_externo", T.StringType(), True),
        T.StructField("ds_estabelecimento_externo", T.StringType(), True),
        T.StructField("ids_estabelecimento_security", T.StringType(), True),
        T.StructField("ids_estabelecimento_anterior", T.StringType(), True),
        T.StructField("ids_tipo_obito", T.StringType(), True),
        T.StructField("ds_tipo_obito", T.StringType(), True),
        T.StructField("ids_tipo_saida_temporaria", T.StringType(), True),
        T.StructField("ds_tipo_saida_temporaria", T.StringType(), True),
        T.StructField("dt_retorno_saida_temporaria", T.TimestampType(), True)
    ])

    schema_inconsistencia = T.StructType([
        T.StructField("id_encarceramento_inconsistencia", T.StringType(), False),
        T.StructField("id_preso", T.StringType(), True),
        T.StructField("id_pessoa", T.StringType(), True),
        T.StructField("nome_pessoa", T.StringType(), True),
        T.StructField("nr_periodo_encarceramento", T.IntegerType(), True),
        T.StructField("id_movimentacao_ref", T.StringType(), True),
        T.StructField("categoria_movimentacao_ref", T.StringType(), True),
        T.StructField("ds_tipo_mov_ref", T.StringType(), True),
        T.StructField("dt_evento_ref", T.TimestampType(), True),
        T.StructField("tp_inconsistencia", T.StringType(), True),
        T.StructField("detalhe_inconsistencia", T.StringType(), True)
    ])

    # ============================================================
    # 3. PROCESSAMENTO PURO PYTHON POR PRESO
    # ============================================================

    def processar_lista_eventos(eventos):
        eventos = sorted(
            eventos,
            key=lambda r: (
                r["dt_evento"],
                r["ord_tp_evento"],
                r["prioridade_categoria"],
                r["id_movimentacao"] if r["id_movimentacao"] is not None else ""
            )
        )

        encarceramentos = []
        eventos_periodo = []
        inconsistencias = []

        aberto = None
        nr_periodo = 0
        hoje = _today_midnight()

        def novo_periodo(r, nr):
            return {
                "id_preso": r["id_preso"],
                "id_pessoa": r["id_pessoa"],
                "nome_pessoa": r["nome_pessoa"],
                "nr_periodo_encarceramento": int(nr),

                "id_movimentacao_entrada": r["id_movimentacao"],
                "id_tipomovimentacao_entrada": r["id_tipomovimentacao"],
                "ds_tipo_mov_entrada": r["ds_tipo_mov"],
                "dt_entrada": _to_python_dt(r["dt_evento"]),
                "id_estabelecimento_entrada": r["id_estabelecimento"],
                "observacao_entrada": r["observacao"],
                "ids_artigo_entrada": r["ids_artigo"],
                "ds_tipificacao_penal_entrada": r["ds_tipificacao_penal"],
                "ds_tipificacao_penal_principal_entrada": r["ds_tipificacao_penal_principal"],

                "id_movimentacao_saida": None,
                "id_tipomovimentacao_saida": None,
                "ds_tipo_mov_saida": None,
                "dt_saida": None,
                "id_estabelecimento_saida": None,
                "observacao_saida": None,
                "tp_fechamento": None,

                "eventos": [],
                "fl_inconsistente": "N"
            }

        def adicionar_evento(periodo, r):
            periodo["eventos"].append({
                "categoria_movimentacao": r["categoria_movimentacao"],
                "subcategoria_evento": r["subcategoria_evento"],
                "id_movimentacao": r["id_movimentacao"],
                "id_tipomovimentacao": r["id_tipomovimentacao"],
                "ds_tipo_mov": r["ds_tipo_mov"],
                "dt_evento": _to_python_dt(r["dt_evento"]),
                "id_estabelecimento": r["id_estabelecimento"],
                "observacao": r["observacao"],
                "ids_alvara": r["ids_alvara"],
                "qtd_alvaras": int(r["qtd_alvaras"]) if r["qtd_alvaras"] is not None else None,
                "ids_artigo": r["ids_artigo"],
                "ds_tipificacao_penal": r["ds_tipificacao_penal"],
                "ds_tipificacao_penal_principal": r["ds_tipificacao_penal_principal"],
                "qtd_tipificacoes_penais": int(r["qtd_tipificacoes_penais"]) if r["qtd_tipificacoes_penais"] is not None else None,
                "ids_estabelecimento_externo": r["ids_estabelecimento_externo"],
                "ds_estabelecimento_externo": r["ds_estabelecimento_externo"],
                "ids_estabelecimento_security": r["ids_estabelecimento_security"],
                "ids_estabelecimento_anterior": r["ids_estabelecimento_anterior"],
                "ids_tipo_obito": r["ids_tipo_obito"],
                "ds_tipo_obito": r["ds_tipo_obito"],
                "ids_tipo_saida_temporaria": r["ids_tipo_saida_temporaria"],
                "ds_tipo_saida_temporaria": r["ds_tipo_saida_temporaria"],
                "dt_retorno_saida_temporaria": _to_python_dt(r["dt_retorno_saida_temporaria"])
            })

        def fechar_periodo(periodo, r_saida=None, dt_fechamento_forcado=None, tp_fechamento="SAIDA"):
            nonlocal encarceramentos, eventos_periodo

            if r_saida is not None:
                periodo["id_movimentacao_saida"] = r_saida["id_movimentacao"]
                periodo["id_tipomovimentacao_saida"] = r_saida["id_tipomovimentacao"]
                periodo["ds_tipo_mov_saida"] = r_saida["ds_tipo_mov"]
                periodo["dt_saida"] = _to_python_dt(r_saida["dt_evento"])
                periodo["id_estabelecimento_saida"] = r_saida["id_estabelecimento"]
                periodo["observacao_saida"] = r_saida["observacao"]
                periodo["tp_fechamento"] = tp_fechamento
            else:
                periodo["dt_saida"] = dt_fechamento_forcado
                periodo["tp_fechamento"] = tp_fechamento

            id_enc = _hash(
                periodo["id_preso"],
                periodo["nr_periodo_encarceramento"],
                periodo["id_movimentacao_entrada"],
                periodo["dt_entrada"]
            )

            eventos = periodo["eventos"]
            ev_interna = [e for e in eventos if e["categoria_movimentacao"] == "MOVIMENTACOES_INTERNAS"]
            ev_externa = [e for e in eventos if e["categoria_movimentacao"] == "MOVIMENTACOES_EXTERNAS"]
            ev_saidinha = [e for e in eventos if e["categoria_movimentacao"] == "MOVIMENTACOES_SAIDINHA"]

            def min_dt(lst):
                return min([e["dt_evento"] for e in lst]) if lst else None

            def max_dt(lst):
                return max([e["dt_evento"] for e in lst]) if lst else None

            def txt_eventos(lst):
                vals = sorted(set([str(e["ds_tipo_mov"]) for e in lst if e["ds_tipo_mov"] is not None]))
                return ",".join(vals) if vals else None

            qtd_saida_saidinha = sum(1 for e in ev_saidinha if e["subcategoria_evento"] == "SAIDA_SAIDINHA")
            qtd_retorno_saidinha = sum(1 for e in ev_saidinha if e["subcategoria_evento"] == "RETORNO_SAIDINHA")
            qtd_alvaras_periodo = sum(int(e["qtd_alvaras"]) for e in eventos if e["qtd_alvaras"] is not None)

            dt_fim = periodo["dt_saida"] if periodo["dt_saida"] is not None else hoje
            qt_dias = int((dt_fim.date() - periodo["dt_entrada"].date()).days)

            score = (
                len(ev_interna) * 1.0
                + len(ev_externa) * 2.0
                + len(ev_saidinha) * 1.5
                + qtd_alvaras_periodo * 2.0
            )

            if score >= 20:
                perfil = "ALTA_DINAMICA"
            elif score >= 8:
                perfil = "MEDIA_DINAMICA"
            else:
                perfil = "BAIXA_DINAMICA"

            encarceramentos.append((
                id_enc,
                periodo["id_preso"],
                periodo["id_pessoa"],
                periodo["nome_pessoa"],
                int(periodo["nr_periodo_encarceramento"]),

                periodo["id_movimentacao_entrada"],
                periodo["id_tipomovimentacao_entrada"],
                periodo["ds_tipo_mov_entrada"],
                periodo["dt_entrada"],
                periodo["id_estabelecimento_entrada"],
                periodo["observacao_entrada"],
                periodo["ids_artigo_entrada"],
                periodo["ds_tipificacao_penal_entrada"],
                periodo["ds_tipificacao_penal_principal_entrada"],

                periodo["id_movimentacao_saida"],
                periodo["id_tipomovimentacao_saida"],
                periodo["ds_tipo_mov_saida"],
                periodo["dt_saida"],
                periodo["id_estabelecimento_saida"],
                periodo["observacao_saida"],
                periodo["tp_fechamento"],

                "FECHADO" if periodo["dt_saida"] is not None else "ABERTO",
                qt_dias,

                len(ev_interna),
                len(ev_externa),
                len(ev_saidinha),
                qtd_saida_saidinha,
                qtd_retorno_saidinha,
                qtd_alvaras_periodo,

                min_dt(ev_interna),
                max_dt(ev_interna),
                min_dt(ev_externa),
                max_dt(ev_externa),
                min_dt(ev_saidinha),
                max_dt(ev_saidinha),

                txt_eventos(ev_interna),
                txt_eventos(ev_externa),
                txt_eventos(ev_saidinha),

                float(score),
                perfil,

                periodo["fl_inconsistente"]
            ))

            for e in eventos:
                eventos_periodo.append((
                    _hash(id_enc, e["id_movimentacao"], e["categoria_movimentacao"]),
                    id_enc,
                    periodo["id_preso"],
                    periodo["id_pessoa"],
                    periodo["nome_pessoa"],
                    int(periodo["nr_periodo_encarceramento"]),
                    e["categoria_movimentacao"],
                    e["subcategoria_evento"],
                    e["id_movimentacao"],
                    e["id_tipomovimentacao"],
                    e["ds_tipo_mov"],
                    e["dt_evento"],
                    e["id_estabelecimento"],
                    e["observacao"],
                    e["ids_alvara"],
                    e["qtd_alvaras"],
                    e["ids_artigo"],
                    e["ds_tipificacao_penal"],
                    e["ds_tipificacao_penal_principal"],
                    e["qtd_tipificacoes_penais"],
                    e["ids_estabelecimento_externo"],
                    e["ds_estabelecimento_externo"],
                    e["ids_estabelecimento_security"],
                    e["ids_estabelecimento_anterior"],
                    e["ids_tipo_obito"],
                    e["ds_tipo_obito"],
                    e["ids_tipo_saida_temporaria"],
                    e["ds_tipo_saida_temporaria"],
                    e["dt_retorno_saida_temporaria"]
                ))

        for r in eventos:
            cat = r["categoria_movimentacao"]

            if cat == "ENTRADA":
                if aberto is None:
                    nr_periodo += 1
                    aberto = novo_periodo(r, nr_periodo)
                else:
                    dt_forcada = _to_python_dt(r["dt_evento"]) - timedelta(seconds=1)
                    fechar_periodo(
                        aberto,
                        r_saida=None,
                        dt_fechamento_forcado=dt_forcada,
                        tp_fechamento="AJUSTE_NOVA_ENTRADA"
                    )

                    inconsistencias.append((
                        _hash(r["id_preso"], r["id_movimentacao"], "ENTRADA_SEM_SAIDA_ANTERIOR"),
                        r["id_preso"],
                        r["id_pessoa"],
                        r["nome_pessoa"],
                        int(aberto["nr_periodo_encarceramento"]),
                        r["id_movimentacao"],
                        cat,
                        r["ds_tipo_mov"],
                        _to_python_dt(r["dt_evento"]),
                        "ENTRADA_SEM_SAIDA_ANTERIOR",
                        "Periodo anterior fechado artificialmente em nova entrada - 1 segundo"
                    ))

                    nr_periodo += 1
                    aberto = novo_periodo(r, nr_periodo)

            elif cat == "SAIDA":
                if aberto is None:
                    inconsistencias.append((
                        _hash(r["id_preso"], r["id_movimentacao"], "SAIDA_SEM_ENTRADA"),
                        r["id_preso"],
                        r["id_pessoa"],
                        r["nome_pessoa"],
                        None,
                        r["id_movimentacao"],
                        cat,
                        r["ds_tipo_mov"],
                        _to_python_dt(r["dt_evento"]),
                        "SAIDA_SEM_ENTRADA",
                        "Saida encontrada sem periodo aberto"
                    ))
                else:
                    if _to_python_dt(r["dt_evento"]) < aberto["dt_entrada"]:
                        inconsistencias.append((
                            _hash(r["id_preso"], r["id_movimentacao"], "SAIDA_ANTERIOR_ENTRADA"),
                            r["id_preso"],
                            r["id_pessoa"],
                            r["nome_pessoa"],
                            int(aberto["nr_periodo_encarceramento"]),
                            r["id_movimentacao"],
                            cat,
                            r["ds_tipo_mov"],
                            _to_python_dt(r["dt_evento"]),
                            "SAIDA_ANTERIOR_ENTRADA",
                            "Saida com data anterior a entrada do periodo"
                        ))
                    else:
                        fechar_periodo(aberto, r_saida=r, tp_fechamento="SAIDA")
                        aberto = None

            else:
                if aberto is None:
                    inconsistencias.append((
                        _hash(r["id_preso"], r["id_movimentacao"], "EVENTO_MENOR_FORA_PERIODO"),
                        r["id_preso"],
                        r["id_pessoa"],
                        r["nome_pessoa"],
                        None,
                        r["id_movimentacao"],
                        cat,
                        r["ds_tipo_mov"],
                        _to_python_dt(r["dt_evento"]),
                        "EVENTO_MENOR_FORA_PERIODO",
                        "Evento menor sem encarceramento aberto para agrupamento"
                    ))
                else:
                    adicionar_evento(aberto, r)

        if aberto is not None:
            fechar_periodo(aberto, r_saida=None, dt_fechamento_forcado=None, tp_fechamento=None)

        return encarceramentos, eventos_periodo, inconsistencias

    def iter_grouped_by_preso(rows_iter):
        current_id = None
        bucket = []

        for row in rows_iter:
            d = row.asDict(recursive=True)
            k = d["id_preso"]

            if current_id is None:
                current_id = k

            if k != current_id:
                yield current_id, bucket
                current_id = k
                bucket = [d]
            else:
                bucket.append(d)

        if current_id is not None:
            yield current_id, bucket

    def map_encarceramento(rows_iter):
        for _, eventos in iter_grouped_by_preso(rows_iter):
            encs, _, _ = processar_lista_eventos(eventos)
            for x in encs:
                yield x

    def map_eventos(rows_iter):
        for _, eventos in iter_grouped_by_preso(rows_iter):
            _, evs, _ = processar_lista_eventos(eventos)
            for x in evs:
                yield x

    def map_inconsistencias(rows_iter):
        for _, eventos in iter_grouped_by_preso(rows_iter):
            _, _, incs = processar_lista_eventos(eventos)
            for x in incs:
                yield x

    # ============================================================
    # 4. EXECUCAO SEM PANDAS
    # ============================================================

    base_rdd = df_eventos_todos.rdd

    df_fat_encarceramento = spark.createDataFrame(
        base_rdd.mapPartitions(map_encarceramento),
        schema=schema_encarceramento
    )

    df_fat_encarceramento_evento = spark.createDataFrame(
        base_rdd.mapPartitions(map_eventos),
        schema=schema_evento_periodo
    )

    df_fat_encarceramento_inconsistencia = spark.createDataFrame(
        base_rdd.mapPartitions(map_inconsistencias),
        schema=schema_inconsistencia
    )

    # ============================================================
    # 5. AJUSTES FINAIS
    # ============================================================

    df_fat_encarceramento = df_fat_encarceramento.dropDuplicates(["id_encarceramento"])
    df_fat_encarceramento_evento = df_fat_encarceramento_evento.dropDuplicates(["id_encarceramento_evento"])
    df_fat_encarceramento_inconsistencia = df_fat_encarceramento_inconsistencia.dropDuplicates(["id_encarceramento_inconsistencia"])

    # ============================================================
    # 6. PERSISTENCIA
    # ============================================================

    tabela = "sinp_fat_encarceramento"

    df_fat_encarceramento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_encarceramento, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_encarceramento")

    tabela = "sinp_fat_encarceramento_evento"

    df_fat_encarceramento_evento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_encarceramento_evento, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_encarceramento_evento")

    tabela = "sinp_fat_encarceramento_inconsistencia"

    df_fat_encarceramento_inconsistencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_encarceramento_inconsistencia, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_encarceramento_inconsistencia")


