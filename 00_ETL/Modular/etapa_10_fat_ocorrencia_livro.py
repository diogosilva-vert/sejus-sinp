# -*- coding: utf-8 -*-
"""Ocorrência de livro, classificação e risco."""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"
    """Etapa extraída do notebook original."""
    # ===== CELL 26 =====
    import os
    import re

    # ============================================================
    # DESCOBERTA DA TABELA PONTE LIVRO -> INTERNO -> INFOPEN
    # ============================================================

    if spark.sql("show tables in bronze like 'livros_acesso_unidade_interno'").count() == 0:
        raise Exception("Tabela bronze.livros_acesso_unidade_interno não encontrada. Sem ela não é possível resolver a ponte do livro para o preso INFOPEN.")

    cols_interno = spark.table("bronze.livros_acesso_unidade_interno").columns

    def pick_col(cols, candidatos, obrigatoria=True, rotulo="coluna"):
        norm = {re.sub(r'[^a-z0-9]', '', c.lower()): c for c in cols}
        for cand in candidatos:
            k = re.sub(r'[^a-z0-9]', '', cand.lower())
            if k in norm:
                return norm[k]
        if obrigatoria:
            raise Exception(f"Não foi possível localizar {rotulo} em bronze.livros_acesso_unidade_interno. Colunas disponíveis: {cols}")
        return None

    col_id_interno = pick_col(cols_interno, ["id"], True, "id do interno")
    col_infopen = pick_col(cols_interno, ["infopen", "id_preso", "preso_id"], True, "chave INFOPEN do interno")
    col_nome_interno = pick_col(cols_interno, ["nome", "interno", "nome_interno", "descricao"], True, "nome do interno")

    # ============================================================
    # LIMPEZA DEFENSIVA
    # ============================================================

    tabelas_drop = [
        "tmp_raw_livro_ocorrencia",
        "tmp_raw_livro_registrovinculoocorrencia",
        "tmp_raw_livro_vinculacaoocorrencias",
        "tmp_raw_livro_historicalocorrencia",
        "tmp_raw_livro_historicalregistrovinculoocorrencia",
        "tmp_raw_livro_historicalvinculacaoocorrencias",
        "tmp_raw_livro_interno",
        "tmp_base_livro_ocorrencia",
        "tmp_base_livro_registrovinculoocorrencia_agg",
        "tmp_base_livro_vinculacaoocorrencias_raw",
        "tmp_base_livro_vinculacaoocorrencias_agg",
        "tmp_base_livro_historicalocorrencia_agg",
        "tmp_base_livro_historicalregistrovinculoocorrencia_agg",
        "tmp_base_livro_historicalvinculacaoocorrencias_agg",
        "tmp_base_livro_interno_catalogo",
        "tmp_base_livro_interno_catalogo_nome_unico",
        "tmp_base_pessoa_preso_ponte_livro",
        "tmp_base_presidiario_catalogo_livro",
        "tmp_base_livro_interno_tokenizado",
        "tmp_rl_ocorrencia_preso_livro",
        "tmp_base_livro_preso_agg",
        "tmp_fat_ocorrencia_livro",
        "sinp_fat_ocorrencia_livro",
        "sinp_rl_ocorrencia_entidade_livro_raw",
        "sinp_rl_ocorrencia_preso_livro"
    ]

    for t in tabelas_drop:
        spark.sql(f"drop table if exists gold.{t}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{t} >/dev/null 2>&1")

    spark.catalog.clearCache()

    spark.sql("refresh table bronze.livros_acesso_unidade_ocorrencia")
    spark.sql("refresh table bronze.livros_acesso_unidade_registrovinculoocorrencia")
    spark.sql("refresh table bronze.livros_acesso_unidade_vinculacaoocorrencias")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalocorrencia")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalregistrovinculoocorrencia")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalvinculacaoocorrencias")
    spark.sql("refresh table bronze.livros_acesso_unidade_interno")

    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")
    spark.sql("refresh table gold.sinp_ent_pessoa")


    # ============================================================
    # RAW LIVRO OCORRENCIA
    # ============================================================

    df_raw_livro_ocorrencia = spark.sql("""
        select *
        from bronze.livros_acesso_unidade_ocorrencia
    """)

    tabela = "tmp_raw_livro_ocorrencia"

    df_raw_livro_ocorrencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_raw_livro_ocorrencia, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_raw_livro_ocorrencia")


    # ============================================================
    # RAW LIVRO REGISTRO VINCULO OCORRENCIA
    # ============================================================

    df_raw_livro_registrovinculoocorrencia = spark.sql("""
        select *
        from bronze.livros_acesso_unidade_registrovinculoocorrencia
    """)

    tabela = "tmp_raw_livro_registrovinculoocorrencia"

    df_raw_livro_registrovinculoocorrencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_raw_livro_registrovinculoocorrencia, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_raw_livro_registrovinculoocorrencia")


    # ============================================================
    # RAW LIVRO VINCULACAO OCORRENCIAS
    # ============================================================

    df_raw_livro_vinculacaoocorrencias = spark.sql("""
        select *
        from bronze.livros_acesso_unidade_vinculacaoocorrencias
    """)

    tabela = "tmp_raw_livro_vinculacaoocorrencias"

    df_raw_livro_vinculacaoocorrencias.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_raw_livro_vinculacaoocorrencias, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_raw_livro_vinculacaoocorrencias")


    # ============================================================
    # RAW HISTORICOS LIVRO
    # ============================================================

    for origem, destino in [
        ("bronze.livros_acesso_unidade_historicalocorrencia", "tmp_raw_livro_historicalocorrencia"),
        ("bronze.livros_acesso_unidade_historicalregistrovinculoocorrencia", "tmp_raw_livro_historicalregistrovinculoocorrencia"),
        ("bronze.livros_acesso_unidade_historicalvinculacaoocorrencias", "tmp_raw_livro_historicalvinculacaoocorrencias")
    ]:
        df = spark.sql(f"select * from {origem}")
        df.write \
            .mode("overwrite") \
            .option("maxRecordsPerFile", 1_000_000) \
            .option("compression", "snappy") \
            .parquet(f"{path}{destino}")
        write_impala_table_partioned(df, "gold", destino, f"{path}{destino}")
        spark.catalog.clearCache()
        spark.sql(f"refresh table gold.{destino}")


    # ============================================================
    # RAW LIVRO INTERNO (PONTE PARA INFOPEN)
    # ============================================================

    sql_raw_livro_interno = f"""
        select
            cast({col_id_interno} as string) as id_interno_origem,
            cast({col_infopen} as string) as id_preso_infopen,
            trim(regexp_replace(coalesce(cast({col_nome_interno} as string), ''), '\\\\s+', ' ')) as nome_interno,
            upper(trim(regexp_replace(coalesce(cast({col_nome_interno} as string), ''), '\\\\s+', ' '))) as nome_interno_normalizado
        from bronze.livros_acesso_unidade_interno
    """

    df_raw_livro_interno = spark.sql(sql_raw_livro_interno)

    tabela = "tmp_raw_livro_interno"

    df_raw_livro_interno.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_raw_livro_interno, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_raw_livro_interno")


    # ============================================================
    # BASE LIVRO OCORRENCIA
    # ============================================================

    df_base_livro_ocorrencia = spark.sql("""
        select
            cast(id as string) as id_ocorrencia_origem,
            trim(regexp_replace(coalesce(motivo, ''), '\\\\s+', ' ')) as motivo,
            trim(regexp_replace(coalesce(registro, ''), '\\\\s+', ' ')) as registro,
            trim(regexp_replace(coalesce(arquivo, ''), '\\\\s+', ' ')) as arquivo,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem
        from gold.tmp_raw_livro_ocorrencia
    """)

    tabela = "tmp_base_livro_ocorrencia"

    df_base_livro_ocorrencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_ocorrencia, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_ocorrencia")


    # ============================================================
    # REGISTRO VINCULO OCORRENCIA AGREGADO
    # ============================================================

    df_base_livro_registrovinculoocorrencia_agg = spark.sql("""
        select
            cast(ocorrencia_id as string) as id_ocorrencia_origem,
            count(*) as qtd_registrovinculo_livro,
            min(to_timestamp(data_registro)) as dt_primeiro_registrovinculo_livro,
            max(to_timestamp(data_registro)) as dt_ultimo_registrovinculo_livro
        from gold.tmp_raw_livro_registrovinculoocorrencia
        group by cast(ocorrencia_id as string)
    """)

    tabela = "tmp_base_livro_registrovinculoocorrencia_agg"

    df_base_livro_registrovinculoocorrencia_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_registrovinculoocorrencia_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_registrovinculoocorrencia_agg")


    # ============================================================
    # RL RAW OCORRENCIA X ENTIDADE LIVRO - CORRIGIDA
    # ============================================================

    df_base_livro_vinculacaoocorrencias_raw = spark.sql("""
        with base_union as (

            select
                cast(ocorrencia_id as string) as id_ocorrencia_origem,
                'LIVRO' as origem_sistema,
                'INTERNO_RAW' as tipo_entidade_raw,
                trim(regexp_replace(coalesce(interno, ''), '\\\\s+', ' ')) as valor_entidade_raw,
                to_timestamp(data_registro) as dt_registro,
                cast(equipe_id as string) as id_equipe_origem,
                cast(presidio_id as string) as id_presidio_origem
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(interno, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'INTERNOS_RAW',
                trim(regexp_replace(coalesce(internos, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(internos, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'ADVOGADO_RAW',
                trim(regexp_replace(coalesce(advogado, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(advogado, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'ADVOGADOS_RAW',
                trim(regexp_replace(coalesce(advogados, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(advogados, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'ASS_RELIGIOSA_RAW',
                trim(regexp_replace(coalesce(ass_religiosa, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(ass_religiosa, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'ASS_RELIGIOSAS_RAW',
                trim(regexp_replace(coalesce(ass_religiosas, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(ass_religiosas, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'POLICIAL_RAW',
                trim(regexp_replace(coalesce(policial, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(policial, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'POLICIAIS_RAW',
                trim(regexp_replace(coalesce(policiais, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(policiais, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'SERVIDOR_RAW',
                trim(regexp_replace(coalesce(servidor, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(servidor, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'SERVIDORES_RAW',
                trim(regexp_replace(coalesce(servidores, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(servidores, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'VISITANTE_RAW',
                trim(regexp_replace(coalesce(visitante, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(visitante, '')) <> ''

            union all

            select
                cast(ocorrencia_id as string),
                'LIVRO',
                'VISITANTES_RAW',
                trim(regexp_replace(coalesce(visitantes, ''), '\\\\s+', ' ')),
                to_timestamp(data_registro),
                cast(equipe_id as string),
                cast(presidio_id as string)
            from gold.tmp_raw_livro_vinculacaoocorrencias
            where ocorrencia_id is not null
              and trim(coalesce(visitantes, '')) <> ''
        ),
        base_distinct as (
            select distinct
                id_ocorrencia_origem,
                origem_sistema,
                tipo_entidade_raw,
                valor_entidade_raw,
                dt_registro,
                id_equipe_origem,
                id_presidio_origem
            from base_union
            where id_ocorrencia_origem is not null
              and trim(coalesce(valor_entidade_raw, '')) <> ''
        )
        select
            concat(
                'RLRAW_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(id_ocorrencia_origem, ''),
                        coalesce(tipo_entidade_raw, ''),
                        coalesce(valor_entidade_raw, ''),
                        coalesce(cast(dt_registro as string), ''),
                        coalesce(id_equipe_origem, ''),
                        coalesce(id_presidio_origem, '')
                    )
                )
            ) as id_rl_ocorrencia_entidade_raw,
            concat('OCR_LIVRO_', id_ocorrencia_origem) as id_fato_ocorrencia,
            id_ocorrencia_origem,
            origem_sistema,
            tipo_entidade_raw,
            valor_entidade_raw,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem
        from base_distinct
    """)

    tabela = "tmp_base_livro_vinculacaoocorrencias_raw"

    df_base_livro_vinculacaoocorrencias_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_vinculacaoocorrencias_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_vinculacaoocorrencias_raw")


    # ============================================================
    # AGREGADO DE VINCULOS RAW
    # ============================================================

    df_base_livro_vinculacaoocorrencias_agg = spark.sql("""
        select
            id_ocorrencia_origem,
            count(*) as qtd_vinculos_raw_livro,
            sum(case when tipo_entidade_raw in ('INTERNO_RAW', 'INTERNOS_RAW') then 1 else 0 end) as qtd_internos_raw,
            sum(case when tipo_entidade_raw in ('ADVOGADO_RAW', 'ADVOGADOS_RAW') then 1 else 0 end) as qtd_advogados_raw,
            sum(case when tipo_entidade_raw in ('ASS_RELIGIOSA_RAW', 'ASS_RELIGIOSAS_RAW') then 1 else 0 end) as qtd_ass_religiosas_raw,
            sum(case when tipo_entidade_raw in ('POLICIAL_RAW', 'POLICIAIS_RAW') then 1 else 0 end) as qtd_policiais_raw,
            sum(case when tipo_entidade_raw in ('SERVIDOR_RAW', 'SERVIDORES_RAW') then 1 else 0 end) as qtd_servidores_raw,
            sum(case when tipo_entidade_raw in ('VISITANTE_RAW', 'VISITANTES_RAW') then 1 else 0 end) as qtd_visitantes_raw,
            concat_ws(' | ', sort_array(collect_set(case when tipo_entidade_raw in ('INTERNO_RAW', 'INTERNOS_RAW') then valor_entidade_raw end))) as txt_internos_raw,
            concat_ws(' | ', sort_array(collect_set(case when tipo_entidade_raw in ('ADVOGADO_RAW', 'ADVOGADOS_RAW') then valor_entidade_raw end))) as txt_advogados_raw,
            concat_ws(' | ', sort_array(collect_set(case when tipo_entidade_raw in ('ASS_RELIGIOSA_RAW', 'ASS_RELIGIOSAS_RAW') then valor_entidade_raw end))) as txt_ass_religiosas_raw,
            concat_ws(' | ', sort_array(collect_set(case when tipo_entidade_raw in ('POLICIAL_RAW', 'POLICIAIS_RAW') then valor_entidade_raw end))) as txt_policiais_raw,
            concat_ws(' | ', sort_array(collect_set(case when tipo_entidade_raw in ('SERVIDOR_RAW', 'SERVIDORES_RAW') then valor_entidade_raw end))) as txt_servidores_raw,
            concat_ws(' | ', sort_array(collect_set(case when tipo_entidade_raw in ('VISITANTE_RAW', 'VISITANTES_RAW') then valor_entidade_raw end))) as txt_visitantes_raw
        from gold.tmp_base_livro_vinculacaoocorrencias_raw
        group by id_ocorrencia_origem
    """)

    tabela = "tmp_base_livro_vinculacaoocorrencias_agg"

    df_base_livro_vinculacaoocorrencias_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_vinculacaoocorrencias_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_vinculacaoocorrencias_agg")


    # ============================================================
    # HISTORICOS AGREGADOS
    # ============================================================

    df_base_livro_historicalocorrencia_agg = spark.sql("""
        select
            cast(id as string) as id_ocorrencia_origem,
            count(*) as qtd_hist_ocorrencia_livro,
            min(to_timestamp(history_date)) as dt_primeiro_hist_ocorrencia_livro,
            max(to_timestamp(history_date)) as dt_ultimo_hist_ocorrencia_livro
        from gold.tmp_raw_livro_historicalocorrencia
        group by cast(id as string)
    """)

    tabela = "tmp_base_livro_historicalocorrencia_agg"

    df_base_livro_historicalocorrencia_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_historicalocorrencia_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_historicalocorrencia_agg")


    df_base_livro_historicalregistrovinculoocorrencia_agg = spark.sql("""
        select
            cast(ocorrencia_id as string) as id_ocorrencia_origem,
            count(*) as qtd_hist_registrovinculo_livro,
            min(to_timestamp(history_date)) as dt_primeiro_hist_registrovinculo_livro,
            max(to_timestamp(history_date)) as dt_ultimo_hist_registrovinculo_livro
        from gold.tmp_raw_livro_historicalregistrovinculoocorrencia
        group by cast(ocorrencia_id as string)
    """)

    tabela = "tmp_base_livro_historicalregistrovinculoocorrencia_agg"

    df_base_livro_historicalregistrovinculoocorrencia_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_historicalregistrovinculoocorrencia_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_historicalregistrovinculoocorrencia_agg")


    df_base_livro_historicalvinculacaoocorrencias_agg = spark.sql("""
        select
            cast(ocorrencia_id as string) as id_ocorrencia_origem,
            count(*) as qtd_hist_vinculacao_livro,
            min(to_timestamp(history_date)) as dt_primeiro_hist_vinculacao_livro,
            max(to_timestamp(history_date)) as dt_ultimo_hist_vinculacao_livro
        from gold.tmp_raw_livro_historicalvinculacaoocorrencias
        group by cast(ocorrencia_id as string)
    """)

    tabela = "tmp_base_livro_historicalvinculacaoocorrencias_agg"

    df_base_livro_historicalvinculacaoocorrencias_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_historicalvinculacaoocorrencias_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_historicalvinculacaoocorrencias_agg")


    # ============================================================
    # CATALOGO DE INTERNOS DO LIVRO
    # ============================================================

    df_base_livro_interno_catalogo = spark.sql("""
        select
            id_interno_origem,
            id_preso_infopen,
            nome_interno,
            nome_interno_normalizado
        from gold.tmp_raw_livro_interno
        where id_preso_infopen is not null
          and nome_interno_normalizado is not null
          and nome_interno_normalizado <> ''
    """)

    tabela = "tmp_base_livro_interno_catalogo"

    df_base_livro_interno_catalogo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_interno_catalogo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_interno_catalogo")


    df_base_livro_interno_catalogo_nome_unico = spark.sql("""
        select
            nome_interno_normalizado,
            max(id_interno_origem) as id_interno_origem,
            max(id_preso_infopen) as id_preso_infopen
        from gold.tmp_base_livro_interno_catalogo
        group by nome_interno_normalizado
        having count(distinct id_preso_infopen) = 1
    """)

    tabela = "tmp_base_livro_interno_catalogo_nome_unico"

    df_base_livro_interno_catalogo_nome_unico.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_interno_catalogo_nome_unico, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_interno_catalogo_nome_unico")


    # ============================================================
    # PONTE PESSOA X PRESO
    # ============================================================

    df_base_pessoa_preso_ponte_livro = spark.sql("""
        select distinct
            cast(id_preso as string) as id_preso_infopen,
            cast(id_preso as string) as id_preso_origem,
            id_pessoa as id_pessoa_presidiario
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
    """)

    tabela = "tmp_base_pessoa_preso_ponte_livro"

    df_base_pessoa_preso_ponte_livro.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_preso_ponte_livro, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_preso_ponte_livro")


    df_base_presidiario_catalogo_livro = spark.sql("""
        select
            p.id_preso_infopen,
            p.id_pessoa_presidiario,
            e.nome_pessoa as nome_presidiario,
            e.documento as documento_presidiario
        from gold.tmp_base_pessoa_preso_ponte_livro p
        left join gold.sinp_ent_pessoa e
            on p.id_pessoa_presidiario = e.id_pessoa
    """)

    tabela = "tmp_base_presidiario_catalogo_livro"

    df_base_presidiario_catalogo_livro.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_presidiario_catalogo_livro, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_presidiario_catalogo_livro")


    # ============================================================
    # TOKENIZACAO DOS INTERNOS DO LIVRO
    # ============================================================

    df_base_livro_interno_tokenizado = spark.sql("""
        with base as (
            select
                id_fato_ocorrencia,
                id_ocorrencia_origem,
                dt_registro,
                id_equipe_origem,
                id_presidio_origem,
                valor_entidade_raw
            from gold.tmp_base_livro_vinculacaoocorrencias_raw
            where tipo_entidade_raw in ('INTERNO_RAW', 'INTERNOS_RAW')
              and trim(coalesce(valor_entidade_raw, '')) <> ''
        )
        select
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem,
            trim(token) as nome_interno_raw,
            upper(trim(regexp_replace(trim(token), '\\\\s+', ' '))) as nome_interno_normalizado
        from (
            select
                id_fato_ocorrencia,
                id_ocorrencia_origem,
                dt_registro,
                id_equipe_origem,
                id_presidio_origem,
                explode(
                    split(
                        regexp_replace(valor_entidade_raw, '\\\\s*[,;|]+\\\\s*', '|'),
                        '\\\\|'
                    )
                ) as token
            from base
        ) z
        where trim(coalesce(token, '')) <> ''
    """)

    tabela = "tmp_base_livro_interno_tokenizado"

    df_base_livro_interno_tokenizado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_interno_tokenizado, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_interno_tokenizado")


    # ============================================================
    # RL OCORRENCIA X PRESO LIVRO
    # ============================================================

    df_rl_ocorrencia_preso_livro = spark.sql("""
        select
            concat(
                'RLPRESOLIVRO_',
                md5(
                    concat_ws(
                        '|',
                        t.id_ocorrencia_origem,
                        coalesce(c.id_preso_infopen, ''),
                        t.nome_interno_normalizado
                    )
                )
            ) as id_rl_ocorrencia_preso_livro,
            t.id_fato_ocorrencia,
            t.id_ocorrencia_origem,
            c.id_interno_origem,
            c.id_preso_infopen as id_preso_origem,
            p.id_pessoa_presidiario,
            p.nome_presidiario,
            p.documento_presidiario,
            t.nome_interno_raw,
            'NOME_UNICO_CATALOGO_LIVRO' as origem_resolucao
        from gold.tmp_base_livro_interno_tokenizado t
        inner join gold.tmp_base_livro_interno_catalogo_nome_unico c
            on t.nome_interno_normalizado = c.nome_interno_normalizado
        left join gold.tmp_base_presidiario_catalogo_livro p
            on c.id_preso_infopen = p.id_preso_infopen
    """)

    tabela = "tmp_rl_ocorrencia_preso_livro"

    df_rl_ocorrencia_preso_livro.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_preso_livro, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_rl_ocorrencia_preso_livro")


    # ============================================================
    # AGREGADO PRESO LIVRO
    # ============================================================

    df_base_livro_preso_agg = spark.sql("""
        select
            id_ocorrencia_origem,
            count(*) as qtd_presos_resolvidos_livro,
            count(distinct id_preso_origem) as qtd_presos_distintos_livro,
            concat_ws(
                ' | ',
                sort_array(collect_set(id_preso_origem))
            ) as txt_ids_preso_infopen_livro,
            concat_ws(
                ' | ',
                sort_array(collect_set(cast(id_pessoa_presidiario as string)))
            ) as txt_ids_pessoa_presidiario_livro,
            concat_ws(
                ' | ',
                sort_array(collect_set(nome_presidiario))
            ) as txt_nomes_presidiario_livro
        from gold.tmp_rl_ocorrencia_preso_livro
        group by id_ocorrencia_origem
    """)

    tabela = "tmp_base_livro_preso_agg"

    df_base_livro_preso_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_livro_preso_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_livro_preso_agg")


    # ============================================================
    # FATO OCORRENCIA LIVRO
    # ============================================================

    df_fat_ocorrencia_livro = spark.sql("""
        select
            concat('OCR_LIVRO_', o.id_ocorrencia_origem) as id_fato_ocorrencia,
            'LIVRO' as origem_sistema,

            o.id_ocorrencia_origem,
            o.id_presidio_origem,
            o.id_equipe_origem,

            o.dt_registro as dt_evento_referencia,
            o.dt_registro,
            o.motivo,
            o.registro,
            o.arquivo,

            case when coalesce(o.motivo, '') <> '' then 1 else 0 end as flag_tem_motivo,
            case when coalesce(o.registro, '') <> '' then 1 else 0 end as flag_tem_registro,
            case when coalesce(o.arquivo, '') <> '' then 1 else 0 end as flag_tem_arquivo,

            coalesce(rv.qtd_registrovinculo_livro, 0) as qtd_registrovinculo_livro,
            rv.dt_primeiro_registrovinculo_livro,
            rv.dt_ultimo_registrovinculo_livro,

            coalesce(v.qtd_vinculos_raw_livro, 0) as qtd_vinculos_raw_livro,
            coalesce(v.qtd_internos_raw, 0) as qtd_internos_raw,
            coalesce(v.qtd_advogados_raw, 0) as qtd_advogados_raw,
            coalesce(v.qtd_ass_religiosas_raw, 0) as qtd_ass_religiosas_raw,
            coalesce(v.qtd_policiais_raw, 0) as qtd_policiais_raw,
            coalesce(v.qtd_servidores_raw, 0) as qtd_servidores_raw,
            coalesce(v.qtd_visitantes_raw, 0) as qtd_visitantes_raw,

            v.txt_internos_raw,
            v.txt_advogados_raw,
            v.txt_ass_religiosas_raw,
            v.txt_policiais_raw,
            v.txt_servidores_raw,
            v.txt_visitantes_raw,

            coalesce(p.qtd_presos_resolvidos_livro, 0) as qtd_presos_resolvidos_livro,
            coalesce(p.qtd_presos_distintos_livro, 0) as qtd_presos_distintos_livro,
            p.txt_ids_preso_infopen_livro,
            p.txt_ids_pessoa_presidiario_livro,
            p.txt_nomes_presidiario_livro,

            case when coalesce(p.qtd_presos_distintos_livro, 0) > 0 then 1 else 0 end as flag_tem_preso_resolvido,
            case when coalesce(p.qtd_presos_distintos_livro, 0) > 1 then 1 else 0 end as flag_multiplos_presos_resolvidos,

            coalesce(ho.qtd_hist_ocorrencia_livro, 0) as qtd_hist_ocorrencia_livro,
            coalesce(hr.qtd_hist_registrovinculo_livro, 0) as qtd_hist_registrovinculo_livro,
            coalesce(hv.qtd_hist_vinculacao_livro, 0) as qtd_hist_vinculacao_livro,

            ho.dt_primeiro_hist_ocorrencia_livro,
            ho.dt_ultimo_hist_ocorrencia_livro,
            hr.dt_primeiro_hist_registrovinculo_livro,
            hr.dt_ultimo_hist_registrovinculo_livro,
            hv.dt_primeiro_hist_vinculacao_livro,
            hv.dt_ultimo_hist_vinculacao_livro,

            (
                coalesce(rv.qtd_registrovinculo_livro, 0) +
                coalesce(v.qtd_vinculos_raw_livro, 0) +
                coalesce(p.qtd_presos_distintos_livro, 0) +
                coalesce(ho.qtd_hist_ocorrencia_livro, 0) +
                coalesce(hr.qtd_hist_registrovinculo_livro, 0) +
                coalesce(hv.qtd_hist_vinculacao_livro, 0)
            ) as score_complexidade_basica
        from gold.tmp_base_livro_ocorrencia o
        left join gold.tmp_base_livro_registrovinculoocorrencia_agg rv
            on o.id_ocorrencia_origem = rv.id_ocorrencia_origem
        left join gold.tmp_base_livro_vinculacaoocorrencias_agg v
            on o.id_ocorrencia_origem = v.id_ocorrencia_origem
        left join gold.tmp_base_livro_preso_agg p
            on o.id_ocorrencia_origem = p.id_ocorrencia_origem
        left join gold.tmp_base_livro_historicalocorrencia_agg ho
            on o.id_ocorrencia_origem = ho.id_ocorrencia_origem
        left join gold.tmp_base_livro_historicalregistrovinculoocorrencia_agg hr
            on o.id_ocorrencia_origem = hr.id_ocorrencia_origem
        left join gold.tmp_base_livro_historicalvinculacaoocorrencias_agg hv
            on o.id_ocorrencia_origem = hv.id_ocorrencia_origem
    """)

    tabela = "tmp_fat_ocorrencia_livro"

    df_fat_ocorrencia_livro.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_ocorrencia_livro, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_fat_ocorrencia_livro")


    # ============================================================
    # FATO OCORRENCIA LIVRO FINAL
    # ============================================================

    df_fat_ocorrencia_livro_final = spark.sql("""
        select
            id_fato_ocorrencia,
            origem_sistema,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            dt_registro,
            motivo,
            registro,
            arquivo,
            flag_tem_motivo,
            flag_tem_registro,
            flag_tem_arquivo,
            qtd_registrovinculo_livro,
            dt_primeiro_registrovinculo_livro,
            dt_ultimo_registrovinculo_livro,
            qtd_vinculos_raw_livro,
            qtd_internos_raw,
            qtd_advogados_raw,
            qtd_ass_religiosas_raw,
            qtd_policiais_raw,
            qtd_servidores_raw,
            qtd_visitantes_raw,
            txt_internos_raw,
            txt_advogados_raw,
            txt_ass_religiosas_raw,
            txt_policiais_raw,
            txt_servidores_raw,
            txt_visitantes_raw,
            qtd_presos_resolvidos_livro,
            qtd_presos_distintos_livro,
            txt_ids_preso_infopen_livro,
            txt_ids_pessoa_presidiario_livro,
            txt_nomes_presidiario_livro,
            flag_tem_preso_resolvido,
            flag_multiplos_presos_resolvidos,
            qtd_hist_ocorrencia_livro,
            qtd_hist_registrovinculo_livro,
            qtd_hist_vinculacao_livro,
            dt_primeiro_hist_ocorrencia_livro,
            dt_ultimo_hist_ocorrencia_livro,
            dt_primeiro_hist_registrovinculo_livro,
            dt_ultimo_hist_registrovinculo_livro,
            dt_primeiro_hist_vinculacao_livro,
            dt_ultimo_hist_vinculacao_livro,
            score_complexidade_basica
        from gold.tmp_fat_ocorrencia_livro
    """)

    tabela = "sinp_fat_ocorrencia_livro"

    df_fat_ocorrencia_livro_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_ocorrencia_livro_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_ocorrencia")


    # ============================================================
    # RL OCORRENCIA X ENTIDADE RAW LIVRO FINAL
    # ============================================================

    df_rl_ocorrencia_entidade_livro_raw_final = spark.sql("""
        select distinct
            id_rl_ocorrencia_entidade_raw,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            origem_sistema,
            tipo_entidade_raw,
            valor_entidade_raw,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem
        from gold.tmp_base_livro_vinculacaoocorrencias_raw
        where id_rl_ocorrencia_entidade_raw is not null
          and id_fato_ocorrencia is not null
          and id_ocorrencia_origem is not null
    """)

    tabela = "sinp_rl_ocorrencia_entidade_livro_raw"

    df_rl_ocorrencia_entidade_livro_raw_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_entidade_livro_raw_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_ocorrencia_entidade_raw")


    # ============================================================
    # RL OCORRENCIA X PRESO LIVRO FINAL
    # ============================================================

    df_rl_ocorrencia_preso_livro_final = spark.sql("""
        select distinct
            id_rl_ocorrencia_preso_livro,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_interno_origem,
            id_preso_origem,
            id_pessoa_presidiario,
            nome_presidiario,
            documento_presidiario,
            nome_interno_raw,
            origem_resolucao
        from gold.tmp_rl_ocorrencia_preso_livro
    """)

    tabela = "sinp_rl_ocorrencia_preso_livro"

    df_rl_ocorrencia_preso_livro_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_preso_livro_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_ocorrencia_preso_livro")


    # ===== CELL 30 =====
    import os
    import re
    import math
    import json
    import hashlib
    import unicodedata
    from datetime import datetime

    import pandas as pd
    from pyspark.sql import types as T

    # ============================================================
    # TENTATIVA DE CAMADA SEMANTICA LOCAL
    # ============================================================

    try:
        from sklearn.feature_extraction.text import TfidfVectorizer
        from sklearn.metrics.pairwise import cosine_similarity
        SKLEARN_OK = True
    except Exception:
        SKLEARN_OK = False

    from difflib import SequenceMatcher


    # ============================================================
    # VALIDACAO DE ENTRADA
    # ============================================================

    if spark.sql("show tables in gold like 'sinp_fat_ocorrencia_livro'").count() == 0:
        raise Exception("Tabela gold.sinp_fat_ocorrencia_livro não encontrada. Gere a fato do livro antes da classificação semântica.")


    # ============================================================
    # LIMPEZA DEFENSIVA
    # ============================================================

    tabelas_drop = [
        "tmp_ocorrencia_livro_texto_base",
        "tmp_ocorrencia_livro_texto_novo",
        "tmp_dim_classificacao_ocorrencia_livro_novo",
        "tmp_dim_classificacao_ocorrencia_livro_final",
        "sinp_dim_classificacao_ocorrencia_livro",
        "sinp_fat_ocorrencia_livro_classificada"
    ]

    for t in tabelas_drop:
        spark.sql(f"drop table if exists gold.{t}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{t} >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_fat_ocorrencia_livro")


    # ============================================================
    # TAXONOMIA FECHADA
    # ============================================================

    TAXONOMIA = [
        {
            "macroclasse": "SEGURANCA",
            "classe": "FUGA_EVASAO",
            "subclasse": "FUGA_CONSUMADA",
            "grau_risco": 10,
            "criticidade": "CRITICA",
            "patterns": [
                r"\bfuga\b", r"\bevas[aã]o\b", r"\bevadiu\b", r"\bforagid", r"\bempreendeu fuga\b"
            ],
            "examples": [
                "fuga consumada de interno",
                "evasao da unidade prisional",
                "interno evadiu"
            ]
        },
        {
            "macroclasse": "SEGURANCA",
            "classe": "FUGA_EVASAO",
            "subclasse": "TENTATIVA_DE_FUGA",
            "grau_risco": 9,
            "criticidade": "CRITICA",
            "patterns": [
                r"tentativa de fuga", r"tentou fugir", r"tentativa de evas[aã]o",
                r"serra", r"buraco na cela", r"rompimento de grade"
            ],
            "examples": [
                "tentativa de fuga",
                "interno tentou fugir",
                "tentativa de evasao da cela"
            ]
        },
        {
            "macroclasse": "SEGURANCA",
            "classe": "APREENSAO_ILICITO",
            "subclasse": "CELULAR_ELETRONICO",
            "grau_risco": 8,
            "criticidade": "ALTA",
            "patterns": [
                r"\bcelular\b", r"\btelefone\b", r"\bsmartphone\b", r"\bchip\b",
                r"\bcarregador\b", r"\bfone\b", r"\br[aá]dio\b", r"\baparelho eletr[oô]nico\b"
            ],
            "examples": [
                "apreensao de celular",
                "encontrado telefone na cela",
                "chip e carregador apreendidos"
            ]
        },
        {
            "macroclasse": "SEGURANCA",
            "classe": "APREENSAO_ILICITO",
            "subclasse": "DROGA_ENTORPECENTE",
            "grau_risco": 9,
            "criticidade": "CRITICA",
            "patterns": [
                r"\bdroga\b", r"\bentorpec", r"\bmaconha\b", r"\bcoca[ií]na\b",
                r"\bcrack\b", r"\bsubst[aâ]ncia\b", r"\btr[aá]fico\b"
            ],
            "examples": [
                "apreensao de droga",
                "entorpecente encontrado",
                "maconha apreendida"
            ]
        },
        {
            "macroclasse": "SEGURANCA",
            "classe": "APREENSAO_ILICITO",
            "subclasse": "ARMA_OBJETO_PERFUROCORTANTE",
            "grau_risco": 9,
            "criticidade": "CRITICA",
            "patterns": [
                r"\barma\b", r"\bfaca\b", r"\bl[aâ]mina\b", r"\bestoque\b",
                r"\bchucho\b", r"\bperfurocortante\b", r"\bobjeto cortante\b"
            ],
            "examples": [
                "arma artesanal apreendida",
                "faca encontrada na cela",
                "objeto perfurocortante"
            ]
        },
        {
            "macroclasse": "SEGURANCA",
            "classe": "CONTRABANDO",
            "subclasse": "ARREMESSO_OU_INGRESSO_ILICITO",
            "grau_risco": 8,
            "criticidade": "ALTA",
            "patterns": [
                r"\barremesso\b", r"\bcontrabando\b", r"\bingresso il[ií]cito\b",
                r"\bobjeto proibido\b", r"\bmaterial proibido\b"
            ],
            "examples": [
                "arremesso para interior da unidade",
                "entrada de material proibido",
                "contrabando apreendido"
            ]
        },
        {
            "macroclasse": "DISCIPLINA",
            "classe": "VIOLENCIA",
            "subclasse": "AGRESSAO_BRIGA",
            "grau_risco": 9,
            "criticidade": "CRITICA",
            "patterns": [
                r"\bagress[aã]o\b", r"\bagred", r"\bbriga\b", r"\bluta corporal\b",
                r"\bvias de fato\b", r"\bespanc"
            ],
            "examples": [
                "agressao entre internos",
                "briga na cela",
                "vias de fato"
            ]
        },
        {
            "macroclasse": "DISCIPLINA",
            "classe": "VIOLENCIA",
            "subclasse": "AMEACA_COACAO",
            "grau_risco": 8,
            "criticidade": "ALTA",
            "patterns": [
                r"\bamea[cç]a\b", r"\bamea[cç]ou\b", r"\bintimid", r"\bcoa[cç][aã]o\b"
            ],
            "examples": [
                "ameaca contra servidor",
                "coacao entre internos",
                "interno intimidou outro"
            ]
        },
        {
            "macroclasse": "DISCIPLINA",
            "classe": "INDISCIPLINA",
            "subclasse": "DESOBEDIENCIA_TUMULTO",
            "grau_risco": 7,
            "criticidade": "ALTA",
            "patterns": [
                r"\bindisciplina\b", r"\bdesobedi", r"\binsubordina", r"\bdesacato\b",
                r"\btumulto\b", r"\bdesordem\b", r"\bmotim\b", r"\brebeli[aã]o\b"
            ],
            "examples": [
                "ato de indisciplina",
                "desobediencia a ordem legal",
                "tumulto no pavilhao"
            ]
        },
        {
            "macroclasse": "PATRIMONIO",
            "classe": "DANO",
            "subclasse": "DANO_AO_PATRIMONIO",
            "grau_risco": 7,
            "criticidade": "ALTA",
            "patterns": [
                r"\bdano\b", r"\bdepreda", r"\bquebra\b", r"\bdestrui", r"\binc[eê]ndio\b"
            ],
            "examples": [
                "dano ao patrimonio publico",
                "depredacao da cela",
                "quebra de estrutura"
            ]
        },
        {
            "macroclasse": "SAUDE",
            "classe": "AUTOLESAO",
            "subclasse": "AUTOLESAO_OU_SUICIDIO",
            "grau_risco": 8,
            "criticidade": "ALTA",
            "patterns": [
                r"\bautoles", r"\bautomutil", r"\bsuic[ií]dio\b", r"\btentativa de suic[ií]dio\b",
                r"\benforc", r"\bauto exterm"
            ],
            "examples": [
                "autolesao em cela",
                "tentativa de suicidio",
                "interno se automutilou"
            ]
        },
        {
            "macroclasse": "VISITA",
            "classe": "VISITA_IRREGULAR",
            "subclasse": "IRREGULARIDADE_DE_VISITA",
            "grau_risco": 6,
            "criticidade": "MEDIA",
            "patterns": [
                r"\bvisit", r"\bvisita irregular\b", r"\bvisitante\b", r"\bentrada irregular\b"
            ],
            "examples": [
                "irregularidade em visita",
                "visitante com material nao autorizado",
                "entrada irregular de visitante"
            ]
        },
        {
            "macroclasse": "SERVIDOR",
            "classe": "CONDUTA_FUNCIONAL",
            "subclasse": "SERVIDOR_ENVOLVIDO",
            "grau_risco": 8,
            "criticidade": "ALTA",
            "patterns": [
                r"\bservidor\b", r"\bpolicial penal\b", r"\bagente penitenci[aá]rio\b",
                r"\bfuncion[aá]rio\b", r"\bconduta funcional\b"
            ],
            "examples": [
                "servidor envolvido na ocorrencia",
                "conduta irregular de servidor",
                "policial penal citado"
            ]
        },
        {
            "macroclasse": "OPERACIONAL",
            "classe": "REVISTA_FISCALIZACAO",
            "subclasse": "REVISTA_OU_INSPECAO",
            "grau_risco": 4,
            "criticidade": "MEDIA",
            "patterns": [
                r"\brevista\b", r"\binspe[cç][aã]o\b", r"\bfiscaliza", r"\bvarredura\b", r"\bbusca\b"
            ],
            "examples": [
                "revista de cela",
                "inspecao de rotina",
                "busca em pavilhao"
            ]
        },
        {
            "macroclasse": "OPERACIONAL",
            "classe": "PROCEDIMENTO_ADMINISTRATIVO",
            "subclasse": "REGISTRO_OPERACIONAL",
            "grau_risco": 2,
            "criticidade": "BAIXA",
            "patterns": [
                r"\bprocedimento\b", r"\bregistro\b", r"\bcomunica[cç][aã]o\b",
                r"\bapoio\b", r"\bacompanhamento\b", r"\borienta[cç][aã]o\b"
            ],
            "examples": [
                "registro operacional",
                "procedimento administrativo",
                "apoio a atividade"
            ]
        },
        {
            "macroclasse": "OUTROS",
            "classe": "OUTROS",
            "subclasse": "NAO_CLASSIFICADO",
            "grau_risco": 1,
            "criticidade": "BAIXA",
            "patterns": [],
            "examples": [
                "outros",
                "nao classificado",
                "diversos"
            ]
        }
    ]


    # ============================================================
    # NORMALIZACAO
    # ============================================================

    MAPA_SUBSTITUICAO = {
        "cel.": "celular",
        "cel ": "celular ",
        "tel ": "telefone ",
        "apreensao": "apreensao",
        "entorpec.": "entorpecente",
        "entorpec ": "entorpecente ",
        "subst ": "substancia ",
        "obj ": "objeto ",
        "perfuro cortante": "perfurocortante",
        "pol penal": "policial penal",
        "ag penitenciario": "agente penitenciario",
        "ag penitenciária": "agente penitenciario",
        "ag penit": "agente penitenciario",
        "evasao": "evasao",
        "rebelião": "rebelião",
        "desob.": "desobediencia",
        "auto lesao": "autolesao",
        "auto-exterm": "auto exterm",
    }

    def remover_acentos(txt):
        if txt is None:
            return ""
        return "".join(
            c for c in unicodedata.normalize("NFKD", str(txt))
            if not unicodedata.combining(c)
        )

    def normalizar_texto(txt):
        txt = remover_acentos(txt).lower().strip()
        txt = re.sub(r"[\r\n\t]+", " ", txt)
        txt = re.sub(r"[/_]+", " ", txt)
        txt = re.sub(r"[^a-z0-9\s\-]", " ", txt)
        txt = re.sub(r"\s+", " ", txt).strip()

        for k, v in MAPA_SUBSTITUICAO.items():
            txt = txt.replace(k, v)

        txt = re.sub(r"\s+", " ", txt).strip()
        return txt

    def gerar_id_texto(motivo, registro):
        base = f"{motivo or ''}|{registro or ''}"
        return hashlib.md5(base.encode("utf-8")).hexdigest()

    def criticidade_num(txt):
        mapa = {"BAIXA": 1, "MEDIA": 2, "ALTA": 3, "CRITICA": 4}
        return mapa.get(txt, 0)


    # ============================================================
    # CONSTRUIR BASE DE REFERENCIA SEMANTICA
    # ============================================================

    refs = []
    for item in TAXONOMIA:
        for ex in item["examples"]:
            refs.append({
                "macroclasse": item["macroclasse"],
                "classe": item["classe"],
                "subclasse": item["subclasse"],
                "grau_risco": item["grau_risco"],
                "criticidade": item["criticidade"],
                "texto_referencia": normalizar_texto(ex),
                "patterns": item["patterns"]
            })

    if SKLEARN_OK:
        corpus_ref = [r["texto_referencia"] for r in refs]
        vectorizer = TfidfVectorizer(analyzer="char_wb", ngram_range=(3, 5), min_df=1)
        ref_matrix = vectorizer.fit_transform(corpus_ref)
    else:
        vectorizer = None
        ref_matrix = None


    # ============================================================
    # CLASSIFICADOR HIBRIDO
    # ============================================================

    def classificar_por_regra(texto_norm):
        melhor = None
        melhor_score = -1
        sinais = []

        for item in TAXONOMIA:
            if item["classe"] == "OUTROS":
                continue

            hits = []
            for p in item["patterns"]:
                if re.search(p, texto_norm):
                    hits.append(p)

            score = len(hits)

            if score > 0:
                score_ajustado = score * 10 + item["grau_risco"] + criticidade_num(item["criticidade"])
                if score_ajustado > melhor_score:
                    melhor_score = score_ajustado
                    melhor = item
                    sinais = hits

        if melhor is None:
            return None

        confianca = min(99, 70 + len(sinais) * 8 + melhor["grau_risco"])
        return {
            "macroclasse": melhor["macroclasse"],
            "classe": melhor["classe"],
            "subclasse": melhor["subclasse"],
            "grau_risco": melhor["grau_risco"],
            "criticidade": melhor["criticidade"],
            "confianca_classificacao": float(confianca),
            "origem_classificacao": "REGRA",
            "justificativa_classificacao": f"match_regra:{', '.join(sinais[:8])}",
            "sinais_detectados": json.dumps(sinais[:20], ensure_ascii=False)
        }

    def classificar_por_similaridade(texto_norm):
        if not texto_norm:
            return None

        if SKLEARN_OK:
            vec = vectorizer.transform([texto_norm])
            sims = cosine_similarity(vec, ref_matrix)[0]
            idx = int(sims.argmax())
            score = float(sims[idx])
            ref = refs[idx]

            if score < 0.33:
                return None

            confianca = round(score * 100, 2)
            return {
                "macroclasse": ref["macroclasse"],
                "classe": ref["classe"],
                "subclasse": ref["subclasse"],
                "grau_risco": ref["grau_risco"],
                "criticidade": ref["criticidade"],
                "confianca_classificacao": confianca,
                "origem_classificacao": "SIMILARIDADE",
                "justificativa_classificacao": f"match_semantico:{ref['texto_referencia']}",
                "sinais_detectados": json.dumps([ref["texto_referencia"]], ensure_ascii=False)
            }

        melhor = None
        melhor_score = -1.0

        for ref in refs:
            score = SequenceMatcher(None, texto_norm, ref["texto_referencia"]).ratio()
            if score > melhor_score:
                melhor_score = score
                melhor = ref

        if melhor is None or melhor_score < 0.45:
            return None

        return {
            "macroclasse": melhor["macroclasse"],
            "classe": melhor["classe"],
            "subclasse": melhor["subclasse"],
            "grau_risco": melhor["grau_risco"],
            "criticidade": melhor["criticidade"],
            "confianca_classificacao": round(melhor_score * 100, 2),
            "origem_classificacao": "SIMILARIDADE",
            "justificativa_classificacao": f"match_aproximado:{melhor['texto_referencia']}",
            "sinais_detectados": json.dumps([melhor["texto_referencia"]], ensure_ascii=False)
        }

    def classificar_texto(motivo_original, registro_original):
        motivo_original = motivo_original or ""
        registro_original = registro_original or ""

        motivo_norm = normalizar_texto(motivo_original)
        registro_norm = normalizar_texto(registro_original)
        texto_norm = normalizar_texto(f"{motivo_original} | {registro_original}")

        # prioridade alta para regra
        r = classificar_por_regra(texto_norm)
        if r is not None:
            return {
                "motivo_normalizado": motivo_norm,
                "registro_normalizado": registro_norm,
                "texto_classificacao_normalizado": texto_norm,
                **r
            }

        # fallback semantico
        s = classificar_por_similaridade(texto_norm)
        if s is not None:
            return {
                "motivo_normalizado": motivo_norm,
                "registro_normalizado": registro_norm,
                "texto_classificacao_normalizado": texto_norm,
                **s
            }

        return {
            "motivo_normalizado": motivo_norm,
            "registro_normalizado": registro_norm,
            "texto_classificacao_normalizado": texto_norm,
            "macroclasse": "OUTROS",
            "classe": "OUTROS",
            "subclasse": "NAO_CLASSIFICADO",
            "grau_risco": 1,
            "criticidade": "BAIXA",
            "confianca_classificacao": 15.0,
            "origem_classificacao": "FALLBACK",
            "justificativa_classificacao": "sem_match_regra_ou_semantico",
            "sinais_detectados": json.dumps([], ensure_ascii=False)
        }


    # ============================================================
    # BASE DE TEXTOS DISTINTOS PARA CLASSIFICAR
    # ============================================================

    df_texto_base = spark.sql("""
        select distinct
            md5(concat_ws('|', coalesce(motivo, ''), coalesce(registro, ''))) as id_texto_classificacao,
            coalesce(motivo, '') as motivo_original,
            coalesce(registro, '') as registro_original
        from gold.sinp_fat_ocorrencia_livro
    """)

    tabela = "tmp_ocorrencia_livro_texto_base"

    df_texto_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_texto_base, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_ocorrencia_livro_texto_base")


    # ============================================================
    # IDENTIFICAR SOMENTE TEXTOS NOVOS
    # ============================================================

    tem_dim_anterior = spark.sql("show tables in gold like 'sinp_dim_classificacao_ocorrencia_livro'").count() > 0

    if tem_dim_anterior:
        spark.sql("refresh table gold.sinp_dim_classificacao_ocorrencia_livro")

        df_texto_novo = spark.sql("""
            select
                b.id_texto_classificacao,
                b.motivo_original,
                b.registro_original
            from gold.tmp_ocorrencia_livro_texto_base b
            left join gold.sinp_dim_classificacao_ocorrencia_livro d
                on b.id_texto_classificacao = d.id_texto_classificacao
            where d.id_texto_classificacao is null
        """)
    else:
        df_texto_novo = spark.sql("""
            select
                id_texto_classificacao,
                motivo_original,
                registro_original
            from gold.tmp_ocorrencia_livro_texto_base
        """)

    tabela = "tmp_ocorrencia_livro_texto_novo"

    df_texto_novo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_texto_novo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_ocorrencia_livro_texto_novo")


    # ============================================================
    # CLASSIFICAR EM PYTHON APENAS OS NOVOS
    # ============================================================

    schema_classificacao = T.StructType([
        T.StructField("id_texto_classificacao", T.StringType(), False),
        T.StructField("motivo_original", T.StringType(), True),
        T.StructField("registro_original", T.StringType(), True),
        T.StructField("motivo_normalizado", T.StringType(), True),
        T.StructField("registro_normalizado", T.StringType(), True),
        T.StructField("texto_classificacao_normalizado", T.StringType(), True),
        T.StructField("macroclasse", T.StringType(), True),
        T.StructField("classe", T.StringType(), True),
        T.StructField("subclasse", T.StringType(), True),
        T.StructField("grau_risco", T.IntegerType(), True),
        T.StructField("criticidade", T.StringType(), True),
        T.StructField("confianca_classificacao", T.DoubleType(), True),
        T.StructField("origem_classificacao", T.StringType(), True),
        T.StructField("versao_modelo", T.StringType(), True),
        T.StructField("justificativa_classificacao", T.StringType(), True),
        T.StructField("sinais_detectados", T.StringType(), True),
        T.StructField("dt_classificacao", T.TimestampType(), True),
    ])

    novos = df_texto_novo.collect()

    registros_classificados = []
    versao_modelo = "CLASSIF_LIVRO_V1"

    for row in novos:
        motivo_original = row["motivo_original"]
        registro_original = row["registro_original"]

        c = classificar_texto(motivo_original, registro_original)

        registros_classificados.append({
            "id_texto_classificacao": row["id_texto_classificacao"],
            "motivo_original": motivo_original,
            "registro_original": registro_original,
            "motivo_normalizado": c["motivo_normalizado"],
            "registro_normalizado": c["registro_normalizado"],
            "texto_classificacao_normalizado": c["texto_classificacao_normalizado"],
            "macroclasse": c["macroclasse"],
            "classe": c["classe"],
            "subclasse": c["subclasse"],
            "grau_risco": int(c["grau_risco"]),
            "criticidade": c["criticidade"],
            "confianca_classificacao": float(c["confianca_classificacao"]),
            "origem_classificacao": c["origem_classificacao"],
            "versao_modelo": versao_modelo,
            "justificativa_classificacao": c["justificativa_classificacao"],
            "sinais_detectados": c["sinais_detectados"],
            "dt_classificacao": datetime.now()
        })

    if len(registros_classificados) > 0:
        df_classificacao_nova = spark.createDataFrame(pd.DataFrame(registros_classificados), schema=schema_classificacao)
    else:
        df_classificacao_nova = spark.createDataFrame([], schema=schema_classificacao)

    tabela = "tmp_dim_classificacao_ocorrencia_livro_novo"

    df_classificacao_nova.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_classificacao_nova, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_dim_classificacao_ocorrencia_livro_novo")


    # ============================================================
    # DIM FINAL DE CLASSIFICACAO
    # ============================================================

    if tem_dim_anterior:
        df_dim_final = spark.sql("""
            select * from gold.sinp_dim_classificacao_ocorrencia_livro
            union all
            select * from gold.tmp_dim_classificacao_ocorrencia_livro_novo
        """)
    else:
        df_dim_final = spark.sql("""
            select * from gold.tmp_dim_classificacao_ocorrencia_livro_novo
        """)

    tabela = "tmp_dim_classificacao_ocorrencia_livro_final"

    df_dim_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_dim_final, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_dim_classificacao_ocorrencia_livro_final")


    df_dim_persistida = spark.sql("""
        select distinct
            id_texto_classificacao,
            motivo_original,
            registro_original,
            motivo_normalizado,
            registro_normalizado,
            texto_classificacao_normalizado,
            macroclasse,
            classe,
            subclasse,
            grau_risco,
            criticidade,
            confianca_classificacao,
            origem_classificacao,
            versao_modelo,
            justificativa_classificacao,
            sinais_detectados,
            dt_classificacao
        from gold.tmp_dim_classificacao_ocorrencia_livro_final
    """)

    tabela = "sinp_dim_classificacao_ocorrencia_livro"

    df_dim_persistida.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_dim_persistida, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_texto_classificacao")


    # ============================================================
    # ENRIQUECER FATO DO LIVRO
    # ============================================================

    df_fat_ocorrencia_livro_classificada = spark.sql("""
        select
            f.*,

            md5(concat_ws('|', coalesce(f.motivo, ''), coalesce(f.registro, ''))) as id_texto_classificacao,

            d.motivo_normalizado,
            d.registro_normalizado,
            d.texto_classificacao_normalizado,

            d.macroclasse as macroclasse_motivo,
            d.classe as classe_motivo,
            d.subclasse as subclasse_motivo,
            d.grau_risco as grau_risco_motivo,
            d.criticidade as criticidade_motivo,
            d.confianca_classificacao,
            d.origem_classificacao,
            d.versao_modelo,
            d.justificativa_classificacao,
            d.sinais_detectados,
            d.dt_classificacao,

            case
                when d.grau_risco >= 8 then 1
                else 0
            end as flag_motivo_critico,

            case
                when d.classe in ('APREENSAO_ILICITO', 'CONTRABANDO') then 1
                else 0
            end as flag_motivo_ilicito,

            case
                when d.classe in ('VIOLENCIA') then 1
                else 0
            end as flag_motivo_violencia,

            case
                when d.classe in ('FUGA_EVASAO') then 1
                else 0
            end as flag_motivo_fuga,

            case
                when d.classe in ('VISITA_IRREGULAR') then 1
                else 0
            end as flag_motivo_visita,

            case
                when d.classe in ('CONDUTA_FUNCIONAL') then 1
                else 0
            end as flag_motivo_servidor,

            coalesce(f.score_complexidade_basica, 0) + coalesce(d.grau_risco, 0) as score_risco_ocorrencia_livro
        from gold.sinp_fat_ocorrencia_livro f
        left join gold.sinp_dim_classificacao_ocorrencia_livro d
            on md5(concat_ws('|', coalesce(f.motivo, ''), coalesce(f.registro, ''))) = d.id_texto_classificacao
    """)

    tabela = "sinp_fat_ocorrencia_livro_classificada"

    df_fat_ocorrencia_livro_classificada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_ocorrencia_livro_classificada, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_ocorrencia")


    # ===== CELL 31 =====
    import os

    # ============================================================
    # VALIDACAO DE ENTRADA
    # ============================================================

    if spark.sql("show tables in gold like 'sinp_fat_ocorrencia_livro_classificada'").count() == 0:
        raise Exception("Tabela gold.sinp_fat_ocorrencia_livro_classificada não encontrada. Gere a classificação semântica antes desta etapa.")

    # ============================================================
    # LIMPEZA DEFENSIVA
    # ============================================================

    tabelas_drop = [
        "tmp_sinal_ocorrencia_livro_metricas",
        "tmp_sinal_ocorrencia_livro_cenarios_base",
        "tmp_sinal_ocorrencia_livro_flags",
        "tmp_sinal_ocorrencia_livro_cenarios_final",
        "tmp_rl_ocorrencia_livro_cenario",
        "sinp_fat_ocorrencia_livro_risco",
        "sinp_rl_ocorrencia_livro_cenario"
    ]

    for t in tabelas_drop:
        spark.sql(f"drop table if exists gold.{t}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{t} >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_fat_ocorrencia_livro_classificada")


    # ============================================================
    # METRICAS BASE DE RISCO
    # ============================================================

    df_sinal_ocorrencia_livro_metricas = spark.sql("""
        select
            f.*,

            coalesce(qtd_advogados_raw, 0) +
            coalesce(qtd_ass_religiosas_raw, 0) +
            coalesce(qtd_policiais_raw, 0) +
            coalesce(qtd_servidores_raw, 0) +
            coalesce(qtd_visitantes_raw, 0) as qtd_envolvidos_externos_raw,

            coalesce(qtd_internos_raw, 0) +
            coalesce(qtd_presos_distintos_livro, 0) as qtd_envolvidos_internos_total,

            coalesce(qtd_internos_raw, 0) +
            coalesce(qtd_advogados_raw, 0) +
            coalesce(qtd_ass_religiosas_raw, 0) +
            coalesce(qtd_policiais_raw, 0) +
            coalesce(qtd_servidores_raw, 0) +
            coalesce(qtd_visitantes_raw, 0) +
            coalesce(qtd_presos_distintos_livro, 0) as qtd_envolvidos_total,

            coalesce(qtd_hist_ocorrencia_livro, 0) +
            coalesce(qtd_hist_registrovinculo_livro, 0) +
            coalesce(qtd_hist_vinculacao_livro, 0) as qtd_historico_total,

            length(coalesce(motivo, '')) as tam_motivo,
            length(coalesce(registro, '')) as tam_registro,
            length(concat_ws(' ', coalesce(motivo, ''), coalesce(registro, ''))) as tam_texto_total,

            case
                when length(concat_ws(' ', coalesce(motivo, ''), coalesce(registro, ''))) >= 250 then 1
                else 0
            end as flag_texto_denso,

            case
                when coalesce(qtd_internos_raw, 0) +
                     coalesce(qtd_advogados_raw, 0) +
                     coalesce(qtd_ass_religiosas_raw, 0) +
                     coalesce(qtd_policiais_raw, 0) +
                     coalesce(qtd_servidores_raw, 0) +
                     coalesce(qtd_visitantes_raw, 0) +
                     coalesce(qtd_presos_distintos_livro, 0) >= 6 then 1
                else 0
            end as flag_muitos_envolvidos,

            case
                when coalesce(qtd_hist_ocorrencia_livro, 0) +
                     coalesce(qtd_hist_registrovinculo_livro, 0) +
                     coalesce(qtd_hist_vinculacao_livro, 0) >= 5 then 1
                else 0
            end as flag_historico_intenso

        from gold.sinp_fat_ocorrencia_livro_classificada f
    """)

    tabela = "tmp_sinal_ocorrencia_livro_metricas"

    df_sinal_ocorrencia_livro_metricas.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinal_ocorrencia_livro_metricas, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_sinal_ocorrencia_livro_metricas")


    # ============================================================
    # SCORE DOS CENARIOS
    # ============================================================

    df_sinal_ocorrencia_livro_cenarios_base = spark.sql("""
        select
            m.*,

            case
                when coalesce(flag_motivo_fuga, 0) = 1 then
                    coalesce(grau_risco_motivo, 0)
                    + case when coalesce(flag_multiplos_presos_resolvidos, 0) = 1 then 2 else 0 end
                    + case when coalesce(flag_historico_intenso, 0) = 1 then 2 else 0 end
                else 0
            end as score_livro001_fuga,

            case
                when coalesce(flag_motivo_ilicito, 0) = 1
                 and coalesce(qtd_envolvidos_externos_raw, 0) > 0 then
                    coalesce(grau_risco_motivo, 0)
                    + least(coalesce(qtd_envolvidos_externos_raw, 0), 5)
                    + case when coalesce(qtd_visitantes_raw, 0) > 0 then 2 else 0 end
                    + case when coalesce(qtd_advogados_raw, 0) > 0 then 2 else 0 end
                else 0
            end as score_livro002_ilicito_rede,

            case
                when coalesce(flag_motivo_violencia, 0) = 1 then
                    coalesce(grau_risco_motivo, 0)
                    + case when coalesce(qtd_historico_total, 0) >= 3 then 2 else 0 end
                    + case when coalesce(qtd_presos_distintos_livro, 0) > 1 then 2 else 0 end
                    + case when coalesce(qtd_envolvidos_total, 0) >= 4 then 1 else 0 end
                else 0
            end as score_livro003_violencia,

            case
                when coalesce(flag_motivo_servidor, 0) = 1
                  or coalesce(qtd_servidores_raw, 0) > 0 then
                    greatest(coalesce(grau_risco_motivo, 0), 6)
                    + case when coalesce(qtd_servidores_raw, 0) > 0 then 2 else 0 end
                    + case when coalesce(qtd_historico_total, 0) >= 2 then 1 else 0 end
                else 0
            end as score_livro004_servidor,

            case
                when coalesce(flag_motivo_visita, 0) = 1
                  or (coalesce(qtd_visitantes_raw, 0) > 0 and coalesce(flag_motivo_ilicito, 0) = 1) then
                    greatest(coalesce(grau_risco_motivo, 0), 5)
                    + case when coalesce(qtd_visitantes_raw, 0) > 0 then 2 else 0 end
                    + case when coalesce(flag_motivo_ilicito, 0) = 1 then 2 else 0 end
                else 0
            end as score_livro005_visita,

            case
                when coalesce(flag_muitos_envolvidos, 0) = 1
                  or (coalesce(qtd_presos_distintos_livro, 0) > 1 and coalesce(qtd_envolvidos_externos_raw, 0) >= 2) then
                    5
                    + least(coalesce(qtd_envolvidos_total, 0), 6)
                    + case when coalesce(qtd_presos_distintos_livro, 0) > 1 then 2 else 0 end
                    + case when coalesce(qtd_envolvidos_externos_raw, 0) >= 2 then 2 else 0 end
                else 0
            end as score_livro006_complexidade,

            case
                when coalesce(flag_historico_intenso, 0) = 1 then
                    4
                    + least(coalesce(qtd_historico_total, 0), 6)
                    + case when coalesce(score_risco_ocorrencia_livro, 0) >= 8 then 2 else 0 end
                else 0
            end as score_livro007_persistencia

        from gold.tmp_sinal_ocorrencia_livro_metricas m
    """)

    tabela = "tmp_sinal_ocorrencia_livro_cenarios_base"

    df_sinal_ocorrencia_livro_cenarios_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinal_ocorrencia_livro_cenarios_base, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_sinal_ocorrencia_livro_cenarios_base")


    # ============================================================
    # FLAGS DOS CENARIOS
    # ============================================================

    df_sinal_ocorrencia_livro_flags = spark.sql("""
        select
            b.*,

            case when coalesce(score_livro001_fuga, 0) > 0 then 1 else 0 end as flag_livro001_fuga,
            case when coalesce(score_livro002_ilicito_rede, 0) > 0 then 1 else 0 end as flag_livro002_ilicito_rede,
            case when coalesce(score_livro003_violencia, 0) > 0 then 1 else 0 end as flag_livro003_violencia,
            case when coalesce(score_livro004_servidor, 0) > 0 then 1 else 0 end as flag_livro004_servidor,
            case when coalesce(score_livro005_visita, 0) > 0 then 1 else 0 end as flag_livro005_visita,
            case when coalesce(score_livro006_complexidade, 0) > 0 then 1 else 0 end as flag_livro006_complexidade,
            case when coalesce(score_livro007_persistencia, 0) > 0 then 1 else 0 end as flag_livro007_persistencia

        from gold.tmp_sinal_ocorrencia_livro_cenarios_base b
    """)

    tabela = "tmp_sinal_ocorrencia_livro_flags"

    df_sinal_ocorrencia_livro_flags.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinal_ocorrencia_livro_flags, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_sinal_ocorrencia_livro_flags")


    # ============================================================
    # TABELA WIDE DE RISCO
    # ============================================================

    df_sinal_ocorrencia_livro_cenarios_final = spark.sql("""
        select
            f.*,

            greatest(
                coalesce(score_livro001_fuga, 0),
                coalesce(score_livro002_ilicito_rede, 0),
                coalesce(score_livro003_violencia, 0),
                coalesce(score_livro004_servidor, 0),
                coalesce(score_livro005_visita, 0),
                coalesce(score_livro006_complexidade, 0),
                coalesce(score_livro007_persistencia, 0)
            ) as score_cenario_maximo,

            (
                coalesce(score_livro001_fuga, 0) +
                coalesce(score_livro002_ilicito_rede, 0) +
                coalesce(score_livro003_violencia, 0) +
                coalesce(score_livro004_servidor, 0) +
                coalesce(score_livro005_visita, 0) +
                coalesce(score_livro006_complexidade, 0) +
                coalesce(score_livro007_persistencia, 0)
            ) as score_cenario_total,

            (
                coalesce(flag_livro001_fuga, 0) +
                coalesce(flag_livro002_ilicito_rede, 0) +
                coalesce(flag_livro003_violencia, 0) +
                coalesce(flag_livro004_servidor, 0) +
                coalesce(flag_livro005_visita, 0) +
                coalesce(flag_livro006_complexidade, 0) +
                coalesce(flag_livro007_persistencia, 0)
            ) as qtd_cenarios_disparados

        from gold.tmp_sinal_ocorrencia_livro_flags f
    """)

    tabela = "tmp_sinal_ocorrencia_livro_cenarios_final"

    df_sinal_ocorrencia_livro_cenarios_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinal_ocorrencia_livro_cenarios_final, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_sinal_ocorrencia_livro_cenarios_final")


    # ============================================================
    # FATO DE RISCO DA OCORRENCIA LIVRO
    # ============================================================

    df_fat_ocorrencia_livro_risco = spark.sql("""
        select
            id_fato_ocorrencia,
            origem_sistema,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            dt_registro,

            motivo,
            registro,
            arquivo,

            macroclasse_motivo,
            classe_motivo,
            subclasse_motivo,
            grau_risco_motivo,
            criticidade_motivo,
            confianca_classificacao,
            origem_classificacao,
            versao_modelo,

            qtd_envolvidos_externos_raw,
            qtd_envolvidos_internos_total,
            qtd_envolvidos_total,
            qtd_historico_total,
            tam_motivo,
            tam_registro,
            tam_texto_total,
            flag_texto_denso,
            flag_muitos_envolvidos,
            flag_historico_intenso,

            flag_livro001_fuga,
            score_livro001_fuga,

            flag_livro002_ilicito_rede,
            score_livro002_ilicito_rede,

            flag_livro003_violencia,
            score_livro003_violencia,

            flag_livro004_servidor,
            score_livro004_servidor,

            flag_livro005_visita,
            score_livro005_visita,

            flag_livro006_complexidade,
            score_livro006_complexidade,

            flag_livro007_persistencia,
            score_livro007_persistencia,

            qtd_cenarios_disparados,
            score_cenario_maximo,
            score_cenario_total,

            case
                when score_cenario_maximo >= 15 then 'CRITICA'
                when score_cenario_maximo >= 10 then 'ALTA'
                when score_cenario_maximo >= 6 then 'MEDIA'
                when score_cenario_maximo > 0 then 'BAIXA'
                else 'SEM_SINAL'
            end as criticidade_cenario_maxima
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
    """)

    tabela = "sinp_fat_ocorrencia_livro_risco"

    df_fat_ocorrencia_livro_risco.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_ocorrencia_livro_risco, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_ocorrencia")


    # ============================================================
    # CENARIOS DISPARADOS - FORMATO LONG
    # ============================================================

    df_rl_ocorrencia_livro_cenario = spark.sql("""
        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO001'))) as id_cenario_ocorrencia_livro,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO001' as scenario_code,
            'FUGA_OU_TENTATIVA' as scenario_name,
            'Ocorrência classificada como fuga ou tentativa de fuga.' as scenario_description,
            score_livro001_fuga as score_cenario,
            case
                when score_livro001_fuga >= 15 then 'CRITICA'
                when score_livro001_fuga >= 10 then 'ALTA'
                when score_livro001_fuga >= 6 then 'MEDIA'
                else 'BAIXA'
            end as criticidade_cenario,
            concat(
                'classe=', coalesce(classe_motivo, ''),
                '; subclasse=', coalesce(subclasse_motivo, ''),
                '; presos=', cast(coalesce(qtd_presos_distintos_livro, 0) as string),
                '; historico=', cast(coalesce(qtd_historico_total, 0) as string)
            ) as reason
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro001_fuga > 0

        union all

        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO002'))),
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO002',
            'ILICITO_COM_REDE_EXTERNA',
            'Ocorrência com indício de ilícito associado a envolvidos externos.',
            score_livro002_ilicito_rede,
            case
                when score_livro002_ilicito_rede >= 15 then 'CRITICA'
                when score_livro002_ilicito_rede >= 10 then 'ALTA'
                when score_livro002_ilicito_rede >= 6 then 'MEDIA'
                else 'BAIXA'
            end,
            concat(
                'classe=', coalesce(classe_motivo, ''),
                '; env_externos=', cast(coalesce(qtd_envolvidos_externos_raw, 0) as string),
                '; visitantes=', cast(coalesce(qtd_visitantes_raw, 0) as string),
                '; advogados=', cast(coalesce(qtd_advogados_raw, 0) as string)
            )
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro002_ilicito_rede > 0

        union all

        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO003'))),
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO003',
            'VIOLENCIA_COM_ALTA_DINAMICA',
            'Ocorrência violenta com alta dinâmica operacional.',
            score_livro003_violencia,
            case
                when score_livro003_violencia >= 15 then 'CRITICA'
                when score_livro003_violencia >= 10 then 'ALTA'
                when score_livro003_violencia >= 6 then 'MEDIA'
                else 'BAIXA'
            end,
            concat(
                'classe=', coalesce(classe_motivo, ''),
                '; presos=', cast(coalesce(qtd_presos_distintos_livro, 0) as string),
                '; historico=', cast(coalesce(qtd_historico_total, 0) as string),
                '; envolvidos=', cast(coalesce(qtd_envolvidos_total, 0) as string)
            )
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro003_violencia > 0

        union all

        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO004'))),
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO004',
            'CONDUTA_FUNCIONAL_SENSIVEL',
            'Ocorrência com participação ou sensibilidade funcional envolvendo servidor.',
            score_livro004_servidor,
            case
                when score_livro004_servidor >= 15 then 'CRITICA'
                when score_livro004_servidor >= 10 then 'ALTA'
                when score_livro004_servidor >= 6 then 'MEDIA'
                else 'BAIXA'
            end,
            concat(
                'classe=', coalesce(classe_motivo, ''),
                '; servidores=', cast(coalesce(qtd_servidores_raw, 0) as string),
                '; historico=', cast(coalesce(qtd_historico_total, 0) as string)
            )
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro004_servidor > 0

        union all

        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO005'))),
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO005',
            'VISITA_IRREGULAR_OU_SENSIVEL',
            'Ocorrência ligada a visita irregular ou sensível.',
            score_livro005_visita,
            case
                when score_livro005_visita >= 15 then 'CRITICA'
                when score_livro005_visita >= 10 then 'ALTA'
                when score_livro005_visita >= 6 then 'MEDIA'
                else 'BAIXA'
            end,
            concat(
                'classe=', coalesce(classe_motivo, ''),
                '; visitantes=', cast(coalesce(qtd_visitantes_raw, 0) as string),
                '; ilicito=', cast(coalesce(flag_motivo_ilicito, 0) as string)
            )
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro005_visita > 0

        union all

        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO006'))),
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO006',
            'OCORRENCIA_COMPLEXA_MULTIENVOLVIDOS',
            'Ocorrência com múltiplos envolvidos e elevada complexidade relacional.',
            score_livro006_complexidade,
            case
                when score_livro006_complexidade >= 15 then 'CRITICA'
                when score_livro006_complexidade >= 10 then 'ALTA'
                when score_livro006_complexidade >= 6 then 'MEDIA'
                else 'BAIXA'
            end,
            concat(
                'envolvidos_total=', cast(coalesce(qtd_envolvidos_total, 0) as string),
                '; externos=', cast(coalesce(qtd_envolvidos_externos_raw, 0) as string),
                '; presos=', cast(coalesce(qtd_presos_distintos_livro, 0) as string)
            )
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro006_complexidade > 0

        union all

        select
            concat('SCNLIVRO_', md5(concat_ws('|', id_fato_ocorrencia, 'LIVRO007'))),
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            'LIVRO007',
            'PERSISTENCIA_HISTORICA_ELEVADA',
            'Ocorrência com persistência histórica elevada e dinâmica operacional contínua.',
            score_livro007_persistencia,
            case
                when score_livro007_persistencia >= 15 then 'CRITICA'
                when score_livro007_persistencia >= 10 then 'ALTA'
                when score_livro007_persistencia >= 6 then 'MEDIA'
                else 'BAIXA'
            end,
            concat(
                'historico=', cast(coalesce(qtd_historico_total, 0) as string),
                '; score_base=', cast(coalesce(score_risco_ocorrencia_livro, 0) as string)
            )
        from gold.tmp_sinal_ocorrencia_livro_cenarios_final
        where score_livro007_persistencia > 0
    """)

    tabela = "tmp_rl_ocorrencia_livro_cenario"

    df_rl_ocorrencia_livro_cenario.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_livro_cenario, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_rl_ocorrencia_livro_cenario")


    df_rl_ocorrencia_livro_cenario_final = spark.sql("""
        select
            id_cenario_ocorrencia_livro,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_presidio_origem,
            id_equipe_origem,
            dt_evento_referencia,
            scenario_code,
            scenario_name,
            scenario_description,
            score_cenario,
            criticidade_cenario,
            reason
        from gold.tmp_rl_ocorrencia_livro_cenario
    """)

    tabela = "sinp_rl_ocorrencia_livro_cenario"

    df_rl_ocorrencia_livro_cenario_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_livro_cenario_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_cenario_ocorrencia_livro")


