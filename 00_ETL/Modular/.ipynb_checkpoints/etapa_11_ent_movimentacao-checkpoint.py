# -*- coding: utf-8 -*-
"""Entidade movimentação, veículo e relacionamentos."""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"
    """Etapa extraída do notebook original."""
    # ===== CELL 32 =====
    import os

    # ============================================================
    # LIMPEZA DEFENSIVA
    # ============================================================

    tabelas_drop = [
        "tmp_base_pessoa_preso_ponte_movimentacao",
        "tmp_base_header_movimentacao_entrada",
        "tmp_base_header_movimentacao_saida",
        "tmp_base_movimentacao_header",
        "tmp_base_pessoa_movimentacao_entrada",
        "tmp_base_pessoa_movimentacao_saida",
        "tmp_base_pessoa_movimentacao_raw",
        "tmp_base_policial_movimentacao_entrada",
        "tmp_base_policial_movimentacao_saida",
        "tmp_base_policial_movimentacao_raw",
        "tmp_base_movimentacao_infopen_ctx",
        "tmp_base_rl_pessoa_movimentacao",
        "tmp_base_rl_policial_movimentacao",
        "tmp_base_agg_pessoa_movimentacao",
        "tmp_base_agg_policial_movimentacao",
        "tmp_base_agg_infopen_movimentacao",
        "tmp_matriz_orgao_veiculo",
        "tmp_matriz_marca_modelo_veiculo",
        "tmp_matriz_marca_veiculo",
        "tmp_base_veiculo_parse",
        "tmp_base_veiculo_raw",
        "tmp_base_rl_movimentacao_veiculo",
        "sinp_ent_movimentacao",
        "sinp_fato_movimentacao",
        "sinp_ent_veiculo",
        "sinp_rl_pessoa_movimentacao",
        "sinp_rl_movimentacao_veiculo",
        "sinp_rl_policial_movimentacao"
    ]

    for t in tabelas_drop:
        spark.sql(f"drop table if exists gold.{t}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{t} >/dev/null 2>&1")

    spark.catalog.clearCache()

    spark.sql("refresh table bronze.livros_acesso_unidade_caracteristicaescolta")
    spark.sql("refresh table bronze.livros_acesso_unidade_caracteristicaescoltasaida")
    spark.sql("refresh table bronze.livros_acesso_unidade_internoescolta")
    spark.sql("refresh table bronze.livros_acesso_unidade_internoescoltasaida")
    spark.sql("refresh table bronze.livros_acesso_unidade_policialescolta")
    spark.sql("refresh table bronze.livros_acesso_unidade_policialescoltasaida")
    spark.sql("refresh table bronze.infopen_movimentacoes")
    spark.sql("refresh table bronze.infopen_tipos_movimentacao")
    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")
    spark.sql("refresh table gold.sinp_ent_pessoa")


    # ============================================================
    # PONTE PESSOA X PRESO
    # ============================================================

    df_base_pessoa_preso_ponte_movimentacao = spark.sql("""
        select distinct
            cast(id_preso as string) as id_preso_origem,
            id_pessoa as id_pessoa_presidiario
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
    """)

    tabela = "tmp_base_pessoa_preso_ponte_movimentacao"

    df_base_pessoa_preso_ponte_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_preso_ponte_movimentacao, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_preso_ponte_movimentacao")


    # ============================================================
    # HEADER MOVIMENTACAO - ESCOLTA ENTRADA
    # ============================================================

    df_base_header_movimentacao_entrada = spark.sql("""
        select
            cast(id as string) as id_evento_origem,
            'LIVROS' as origem_sistema,
            'ESCOLTA_ENTRADA' as tipo_movimentacao,
            trim(regexp_replace(coalesce(instituicao, ''), '\\\\s+', ' ')) as instituicao,
            trim(regexp_replace(coalesce(origem, ''), '\\\\s+', ' ')) as origem_local,
            cast(null as string) as destino_local,
            trim(regexp_replace(coalesce(motivo, ''), '\\\\s+', ' ')) as motivo,
            cast(null as string) as solicitacao,
            trim(regexp_replace(coalesce(autorizacao, ''), '\\\\s+', ' ')) as autorizacao,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento,
            trim(regexp_replace(coalesce(viatura, ''), '\\\\s+', ' ')) as veiculo_raw,
            cast(hr_chegada as string) as hr_inicio,
            cast(null as string) as hr_fim,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem,
            1 as flag_escolta
        from bronze.livros_acesso_unidade_caracteristicaescolta
    """)

    tabela = "tmp_base_header_movimentacao_entrada"

    df_base_header_movimentacao_entrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_header_movimentacao_entrada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_header_movimentacao_entrada")


    # ============================================================
    # HEADER MOVIMENTACAO - ESCOLTA SAIDA
    # ============================================================

    df_base_header_movimentacao_saida = spark.sql("""
        select
            cast(id as string) as id_evento_origem,
            'LIVROS' as origem_sistema,
            'ESCOLTA_SAIDA' as tipo_movimentacao,
            trim(regexp_replace(coalesce(instituicao, ''), '\\\\s+', ' ')) as instituicao,
            cast(null as string) as origem_local,
            trim(regexp_replace(coalesce(destino, ''), '\\\\s+', ' ')) as destino_local,
            trim(regexp_replace(coalesce(motivo, ''), '\\\\s+', ' ')) as motivo,
            trim(regexp_replace(coalesce(solicitacao, ''), '\\\\s+', ' ')) as solicitacao,
            trim(regexp_replace(coalesce(autorizacao, ''), '\\\\s+', ' ')) as autorizacao,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento,
            trim(regexp_replace(coalesce(viatura, ''), '\\\\s+', ' ')) as veiculo_raw,
            cast(hr_saida as string) as hr_inicio,
            cast(hr_retorno as string) as hr_fim,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem,
            1 as flag_escolta
        from bronze.livros_acesso_unidade_caracteristicaescoltasaida
    """)

    tabela = "tmp_base_header_movimentacao_saida"

    df_base_header_movimentacao_saida.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_header_movimentacao_saida, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_header_movimentacao_saida")


    # ============================================================
    # HEADER PADRONIZADO DA MOVIMENTACAO
    # ============================================================

    df_base_movimentacao_header = spark.sql("""
        select
            concat(
                'MOV_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(origem_sistema, ''),
                        coalesce(tipo_movimentacao, ''),
                        coalesce(id_evento_origem, ''),
                        coalesce(id_presidio_origem, ''),
                        coalesce(cast(dt_registro as string), '')
                    )
                )
            ) as id_movimentacao,
            id_evento_origem,
            origem_sistema,
            tipo_movimentacao,
            instituicao,
            origem_local,
            destino_local,
            motivo,
            solicitacao,
            autorizacao,
            documento,
            veiculo_raw,
            hr_inicio,
            hr_fim,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem,
            flag_escolta
        from (
            select * from gold.tmp_base_header_movimentacao_entrada
            union all
            select * from gold.tmp_base_header_movimentacao_saida
        ) x
    """)

    tabela = "tmp_base_movimentacao_header"

    df_base_movimentacao_header.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_movimentacao_header, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_movimentacao_header")


    # ============================================================
    # PESSOA/MOVIMENTACAO - ESCOLTA ENTRADA
    # ============================================================

    df_base_pessoa_movimentacao_entrada = spark.sql("""
        select
            'ESCOLTA_ENTRADA' as tipo_movimentacao,
            cast(escolta_id as string) as id_evento_origem,
            cast(id as string) as id_item_origem,
            cast(infopen as string) as id_preso_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_presidiario_raw,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem
        from bronze.livros_acesso_unidade_internoescolta
    """)

    tabela = "tmp_base_pessoa_movimentacao_entrada"

    df_base_pessoa_movimentacao_entrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_movimentacao_entrada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_movimentacao_entrada")


    # ============================================================
    # PESSOA/MOVIMENTACAO - ESCOLTA SAIDA
    # ============================================================

    df_base_pessoa_movimentacao_saida = spark.sql("""
        select
            'ESCOLTA_SAIDA' as tipo_movimentacao,
            cast(escolta_id as string) as id_evento_origem,
            cast(id as string) as id_item_origem,
            cast(infopen as string) as id_preso_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_presidiario_raw,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem
        from bronze.livros_acesso_unidade_internoescoltasaida
    """)

    tabela = "tmp_base_pessoa_movimentacao_saida"

    df_base_pessoa_movimentacao_saida.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_movimentacao_saida, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_movimentacao_saida")


    # ============================================================
    # PESSOA/MOVIMENTACAO RAW PADRONIZADA
    # ============================================================

    df_base_pessoa_movimentacao_raw = spark.sql("""
        select
            h.id_movimentacao,
            p.tipo_movimentacao,
            p.id_evento_origem,
            p.id_item_origem,
            p.id_preso_origem,
            pp.id_pessoa_presidiario,
            p.nome_presidiario_raw,
            p.dt_registro,
            p.id_equipe_origem,
            p.id_presidio_origem
        from (
            select * from gold.tmp_base_pessoa_movimentacao_entrada
            union all
            select * from gold.tmp_base_pessoa_movimentacao_saida
        ) p
        inner join gold.tmp_base_movimentacao_header h
            on p.tipo_movimentacao = h.tipo_movimentacao
           and p.id_evento_origem = h.id_evento_origem
        left join gold.tmp_base_pessoa_preso_ponte_movimentacao pp
            on p.id_preso_origem = pp.id_preso_origem
    """)

    tabela = "tmp_base_pessoa_movimentacao_raw"

    df_base_pessoa_movimentacao_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_movimentacao_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_movimentacao_raw")


    # ============================================================
    # POLICIAL/MOVIMENTACAO - ESCOLTA ENTRADA
    # ============================================================

    df_base_policial_movimentacao_entrada = spark.sql("""
        select
            'ESCOLTA_ENTRADA' as tipo_movimentacao,
            cast(escolta_id as string) as id_evento_origem,
            cast(id as string) as id_item_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_policial_raw,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento_policial_raw,
            0 as flag_condutor,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem
        from bronze.livros_acesso_unidade_policialescolta
    """)

    tabela = "tmp_base_policial_movimentacao_entrada"

    df_base_policial_movimentacao_entrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_policial_movimentacao_entrada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_policial_movimentacao_entrada")


    # ============================================================
    # POLICIAL/MOVIMENTACAO - ESCOLTA SAIDA
    # ============================================================

    df_base_policial_movimentacao_saida = spark.sql("""
        select
            'ESCOLTA_SAIDA' as tipo_movimentacao,
            cast(escolta_id as string) as id_evento_origem,
            cast(id as string) as id_item_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_policial_raw,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento_policial_raw,
            case
                when coalesce(condutor, false) = true then 1
                else 0
            end as flag_condutor,
            to_timestamp(data_registro) as dt_registro,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem
        from bronze.livros_acesso_unidade_policialescoltasaida
    """)

    tabela = "tmp_base_policial_movimentacao_saida"

    df_base_policial_movimentacao_saida.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_policial_movimentacao_saida, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_policial_movimentacao_saida")


    # ============================================================
    # POLICIAL/MOVIMENTACAO RAW PADRONIZADA
    # ============================================================

    df_base_policial_movimentacao_raw = spark.sql("""
        select
            h.id_movimentacao,
            p.tipo_movimentacao,
            p.id_evento_origem,
            p.id_item_origem,
            pp.id_pessoa_presidiario as id_pessoa,
            p.nome_policial_raw,
            p.documento_policial_raw,
            p.flag_condutor,
            p.dt_registro,
            p.id_equipe_origem,
            p.id_presidio_origem
        from (
            select * from gold.tmp_base_policial_movimentacao_entrada
            union all
            select * from gold.tmp_base_policial_movimentacao_saida
        ) p
        inner join gold.tmp_base_movimentacao_header h
            on p.tipo_movimentacao = h.tipo_movimentacao
           and p.id_evento_origem = h.id_evento_origem
        left join gold.tmp_base_pessoa_preso_ponte_movimentacao pp
            on trim(cast(p.documento_policial_raw as string)) = trim(cast(pp.id_preso_origem as string))
    """)

    tabela = "tmp_base_policial_movimentacao_raw"

    df_base_policial_movimentacao_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_policial_movimentacao_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_policial_movimentacao_raw")


    # ============================================================
    # CONTEXTO INFOPEN PARA A MOVIMENTACAO
    # ============================================================

    df_base_movimentacao_infopen_ctx = spark.sql("""
        select
            cast(m.id_movimentacao as string) as id_movimentacao_infopen_origem,
            cast(m.id_preso as string) as id_preso_origem,
            cast(m.id_tipomovimentacao as string) as id_tipomovimentacao_infopen,
            trim(regexp_replace(coalesce(t.tipomovimentacao_descricao, ''), '\\\\s+', ' ')) as ds_tipomovimentacao_infopen,
            to_timestamp(m.movimentacao_data) as dt_movimentacao_infopen,
            to_date(m.movimentacao_data) as dt_movimentacao_infopen_ref
        from bronze.infopen_movimentacoes m
        left join bronze.infopen_tipos_movimentacao t
            on m.id_tipomovimentacao = t.id_tipomovimentacao
    """)

    tabela = "tmp_base_movimentacao_infopen_ctx"

    df_base_movimentacao_infopen_ctx.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_movimentacao_infopen_ctx, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_movimentacao_infopen_ctx")


    # ============================================================
    # RL PESSOA X MOVIMENTACAO
    # ============================================================

    df_base_rl_pessoa_movimentacao = spark.sql("""
        select
            concat(
                'RLPM_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(r.id_movimentacao, ''),
                        coalesce(r.id_preso_origem, ''),
                        coalesce(r.id_item_origem, '')
                    )
                )
            ) as id_rl_pessoa_movimentacao,
            r.id_movimentacao,
            r.tipo_movimentacao,
            r.id_evento_origem,
            r.id_item_origem,
            r.id_preso_origem,
            r.id_pessoa_presidiario,
            r.nome_presidiario_raw,
            r.dt_registro,
            r.id_equipe_origem,
            r.id_presidio_origem,
            count(i.id_movimentacao_infopen_origem) as qtd_movimentacoes_infopen_relacionadas,
            concat_ws(
                ' | ',
                sort_array(collect_set(i.ds_tipomovimentacao_infopen))
            ) as txt_tipos_movimentacao_infopen_relacionadas,
            case
                when count(i.id_movimentacao_infopen_origem) > 0 then 1
                else 0
            end as flag_tem_movimentacao_infopen_mesmo_dia
        from gold.tmp_base_pessoa_movimentacao_raw r
        left join gold.tmp_base_movimentacao_infopen_ctx i
            on r.id_preso_origem = i.id_preso_origem
           and to_date(r.dt_registro) = i.dt_movimentacao_infopen_ref
        group by
            r.id_movimentacao,
            r.tipo_movimentacao,
            r.id_evento_origem,
            r.id_item_origem,
            r.id_preso_origem,
            r.id_pessoa_presidiario,
            r.nome_presidiario_raw,
            r.dt_registro,
            r.id_equipe_origem,
            r.id_presidio_origem
    """)

    tabela = "tmp_base_rl_pessoa_movimentacao"

    df_base_rl_pessoa_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_rl_pessoa_movimentacao, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_rl_pessoa_movimentacao")


    # ============================================================
    # RL POLICIAL X MOVIMENTACAO
    # ============================================================

    df_base_rl_policial_movimentacao = spark.sql("""
        select
            concat(
                'RLPOLM_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(id_movimentacao, ''),
                        coalesce(id_item_origem, ''),
                        coalesce(nome_policial_raw, ''),
                        coalesce(documento_policial_raw, '')
                    )
                )
            ) as id_rl_policial_movimentacao,
            id_movimentacao,
            tipo_movimentacao,
            id_evento_origem,
            id_item_origem,
            id_pessoa,
            nome_policial_raw,
            documento_policial_raw,
            flag_condutor,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem
        from gold.tmp_base_policial_movimentacao_raw
    """)

    tabela = "tmp_base_rl_policial_movimentacao"

    df_base_rl_policial_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_rl_policial_movimentacao, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_rl_policial_movimentacao")


    # ============================================================
    # AGREGADOS PESSOA X MOVIMENTACAO
    # ============================================================

    df_base_agg_pessoa_movimentacao = spark.sql("""
        select
            id_movimentacao,
            count(*) as qtd_presidiarios_relacionados,
            count(distinct id_preso_origem) as qtd_presidiarios_distintos,
            sum(case when id_pessoa_presidiario is not null then 1 else 0 end) as qtd_presidiarios_resolvidos_pessoa,
            sum(case when flag_tem_movimentacao_infopen_mesmo_dia = 1 then 1 else 0 end) as qtd_presidiarios_com_movimentacao_infopen_mesmo_dia,
            sum(qtd_movimentacoes_infopen_relacionadas) as qtd_movimentacoes_infopen_relacionadas,
            concat_ws(
                ' | ',
                sort_array(collect_set(id_preso_origem))
            ) as txt_ids_preso_relacionados,
            concat_ws(
                ' | ',
                sort_array(collect_set(cast(id_pessoa_presidiario as string)))
            ) as txt_ids_pessoa_relacionados,
            concat_ws(
                ' | ',
                sort_array(collect_set(nome_presidiario_raw))
            ) as txt_nomes_presidiarios_relacionados,
            concat_ws(
                ' | ',
                sort_array(collect_set(txt_tipos_movimentacao_infopen_relacionadas))
            ) as txt_tipos_movimentacao_infopen_relacionadas
        from gold.tmp_base_rl_pessoa_movimentacao
        group by id_movimentacao
    """)

    tabela = "tmp_base_agg_pessoa_movimentacao"

    df_base_agg_pessoa_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_agg_pessoa_movimentacao, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_agg_pessoa_movimentacao")


    # ============================================================
    # AGREGADOS POLICIAL X MOVIMENTACAO
    # ============================================================

    df_base_agg_policial_movimentacao = spark.sql("""
        select
            id_movimentacao,
            count(*) as qtd_policiais_relacionados,
            count(distinct coalesce(id_pessoa, documento_policial_raw)) as qtd_policiais_distintos,
            sum(coalesce(flag_condutor, 0)) as qtd_condutores,
            concat_ws(
                ' | ',
                sort_array(collect_set(nome_policial_raw))
            ) as txt_nomes_policiais_relacionados
        from gold.tmp_base_rl_policial_movimentacao
        group by id_movimentacao
    """)

    tabela = "tmp_base_agg_policial_movimentacao"

    df_base_agg_policial_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_agg_policial_movimentacao, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_agg_policial_movimentacao")


    # ============================================================
    # AGREGADO INFOPEN POR MOVIMENTACAO
    # ============================================================

    df_base_agg_infopen_movimentacao = spark.sql("""
        select
            id_movimentacao,
            case
                when coalesce(qtd_movimentacoes_infopen_relacionadas, 0) > 0 then 1
                else 0
            end as flag_tem_movimentacao_infopen_mesmo_dia
        from gold.tmp_base_agg_pessoa_movimentacao
    """)

    tabela = "tmp_base_agg_infopen_movimentacao"

    df_base_agg_infopen_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_agg_infopen_movimentacao, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_agg_infopen_movimentacao")


    # ============================================================
    # MATRIZ DE ORGAOS DE SEGURANCA PARA VEICULOS
    # Atualizacao: alterar esta lista quando novos codigos aparecerem.
    # ============================================================

    matriz_orgao_veiculo = [
        ("SEJUS", "SEJUS", 1),
        ("SESP", "SESP", 2),
        ("PC", "PC|POLICIA CIVIL", 3),
        ("PP", "PP|POLICIA PENAL", 4),
        ("DERP", "DERP", 5),
        ("GLI", "GLI", 6),
        ("SLGO", "SLGO", 7),
        ("GRE", "GRE", 8)
    ]

    df_matriz_orgao_veiculo = spark.createDataFrame(
        matriz_orgao_veiculo,
        ["ds_orgao_seguranca", "regex_orgao", "prioridade_orgao"]
    )

    tabela = "tmp_matriz_orgao_veiculo"

    df_matriz_orgao_veiculo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_matriz_orgao_veiculo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_matriz_orgao_veiculo")


    # ============================================================
    # MATRIZ DE MARCAS E MODELOS COMUNS NO BRASIL
    # Atualizacao: alterar esta lista quando novos modelos aparecerem.
    # Campos:
    #   ds_marca_veiculo
    #   ds_modelo_veiculo
    #   termo_modelo_busca
    #   prioridade_modelo
    # ============================================================

    matriz_marca_modelo_veiculo = [
        ("FIAT", "STRADA", "STRADA", 1),
        ("FIAT", "ARGO", "ARGO", 2),
        ("FIAT", "MOBI", "MOBI", 3),
        ("FIAT", "TORO", "TORO", 4),
        ("FIAT", "CRONOS", "CRONOS", 5),
        ("FIAT", "PULSE", "PULSE", 6),
        ("FIAT", "FASTBACK", "FASTBACK", 7),
        ("FIAT", "FIORINO", "FIORINO", 8),
        ("FIAT", "UNO", "UNO", 9),
        ("FIAT", "PALIO", "PALIO", 10),
        ("FIAT", "SIENA", "SIENA", 11),
        ("FIAT", "DUCATO", "DUCATO", 12),
        ("FIAT", "DOBLO", "DOBLO", 13),

        ("VOLKSWAGEN", "POLO", "POLO", 20),
        ("VOLKSWAGEN", "T-CROSS", "T CROSS", 21),
        ("VOLKSWAGEN", "T-CROSS", "TCROSS", 22),
        ("VOLKSWAGEN", "GOL", "GOL", 23),
        ("VOLKSWAGEN", "SAVEIRO", "SAVEIRO", 24),
        ("VOLKSWAGEN", "NIVUS", "NIVUS", 25),
        ("VOLKSWAGEN", "VIRTUS", "VIRTUS", 26),
        ("VOLKSWAGEN", "VOYAGE", "VOYAGE", 27),
        ("VOLKSWAGEN", "FOX", "FOX", 28),
        ("VOLKSWAGEN", "AMAROK", "AMAROK", 29),
        ("VOLKSWAGEN", "JETTA", "JETTA", 30),
        ("VOLKSWAGEN", "UP", "UP", 31),

        ("CHEVROLET", "ONIX PLUS", "ONIX PLUS", 40),
        ("CHEVROLET", "ONIX", "ONIX", 41),
        ("CHEVROLET", "TRACKER", "TRACKER", 42),
        ("CHEVROLET", "S10", "S10", 43),
        ("CHEVROLET", "SPIN", "SPIN", 44),
        ("CHEVROLET", "MONTANA", "MONTANA", 45),
        ("CHEVROLET", "CRUZE", "CRUZE", 46),
        ("CHEVROLET", "PRISMA", "PRISMA", 47),
        ("CHEVROLET", "COBALT", "COBALT", 48),
        ("CHEVROLET", "CORSA", "CORSA", 49),
        ("CHEVROLET", "CELTA", "CELTA", 50),
        ("CHEVROLET", "MERIVA", "MERIVA", 51),
        ("CHEVROLET", "ZAFIRA", "ZAFIRA", 52),

        ("HYUNDAI", "HB20", "HB20", 60),
        ("HYUNDAI", "CRETA", "CRETA", 61),
        ("HYUNDAI", "TUCSON", "TUCSON", 62),
        ("HYUNDAI", "SANTA FE", "SANTA FE", 63),
        ("HYUNDAI", "IX35", "IX35", 64),
        ("HYUNDAI", "I30", "I30", 65),

        ("TOYOTA", "COROLLA CROSS", "COROLLA CROSS", 70),
        ("TOYOTA", "COROLLA", "COROLLA", 71),
        ("TOYOTA", "HILUX", "HILUX", 72),
        ("TOYOTA", "HILUX", "HILLUX", 72),
        ("TOYOTA", "SW4", "SW4", 73),
        ("TOYOTA", "YARIS", "YARIS", 74),
        ("TOYOTA", "ETIOS", "ETIOS", 75),
        ("TOYOTA", "BANDEIRANTE", "BANDEIRANTE", 76),

        ("RENAULT", "KWID", "KWID", 80),
        ("RENAULT", "SANDERO", "SANDERO", 81),
        ("RENAULT", "LOGAN", "LOGAN", 82),
        ("RENAULT", "DUSTER", "DUSTER", 83),
        ("RENAULT", "OROCH", "OROCH", 84),
        ("RENAULT", "MASTER", "MASTER", 85),
        ("RENAULT", "CLIO", "CLIO", 86),
        ("RENAULT", "KANGOO", "KANGOO", 87),

        ("FORD", "RANGER", "RANGER", 90),
        ("FORD", "KA", "KA", 91),
        ("FORD", "ECOSPORT", "ECOSPORT", 92),
        ("FORD", "FIESTA", "FIESTA", 93),
        ("FORD", "FOCUS", "FOCUS", 94),
        ("FORD", "FUSION", "FUSION", 95),
        ("FORD", "TRANSIT", "TRANSIT", 96),
        ("FORD", "F1000", "F1000", 97),
        ("FORD", "F-1000", "F 1000", 98),

        ("HONDA", "HR-V", "HR V", 100),
        ("HONDA", "WR-V", "WR V", 101),
        ("HONDA", "CIVIC", "CIVIC", 102),
        ("HONDA", "CITY", "CITY", 103),
        ("HONDA", "FIT", "FIT", 104),
        ("HONDA", "CR-V", "CR V", 105),

        ("JEEP", "COMPASS", "COMPASS", 110),
        ("JEEP", "RENEGADE", "RENEGADE", 111),
        ("JEEP", "COMMANDER", "COMMANDER", 112),
        ("JEEP", "CHEROKEE", "CHEROKEE", 113),
        ("JEEP", "GRAND CHEROKEE", "GRAND CHEROKEE", 114),

        ("NISSAN", "KICKS", "KICKS", 120),
        ("NISSAN", "VERSA", "VERSA", 121),
        ("NISSAN", "FRONTIER", "FRONTIER", 122),
        ("NISSAN", "MARCH", "MARCH", 123),
        ("NISSAN", "SENTRA", "SENTRA", 124),
        ("NISSAN", "LIVINA", "LIVINA", 125),

        ("PEUGEOT", "208", "208", 130),
        ("PEUGEOT", "2008", "2008", 131),
        ("PEUGEOT", "207", "207", 132),
        ("PEUGEOT", "206", "206", 133),
        ("PEUGEOT", "307", "307", 134),
        ("PEUGEOT", "PARTNER", "PARTNER", 135),
        ("PEUGEOT", "EXPERT", "EXPERT", 136),
        ("PEUGEOT", "BOXER", "BOXER", 137),

        ("CITROEN", "C3 AIRCROSS", "C3 AIRCROSS", 140),
        ("CITROEN", "C4 CACTUS", "C4 CACTUS", 141),
        ("CITROEN", "BASALT", "BASALT", 142),
        ("CITROEN", "C3", "C3", 143),
        ("CITROEN", "C4", "C4", 144),
        ("CITROEN", "AIRCROSS", "AIRCROSS", 145),
        ("CITROEN", "JUMPER", "JUMPER", 146),
        ("CITROEN", "BERLINGO", "BERLINGO", 147),

        ("MITSUBISHI", "L200", "L200", 150),
        ("MITSUBISHI", "PAJERO", "PAJERO", 151),
        ("MITSUBISHI", "ASX", "ASX", 152),
        ("MITSUBISHI", "ECLIPSE CROSS", "ECLIPSE CROSS", 153),
        ("MITSUBISHI", "OUTLANDER", "OUTLANDER", 154),

        ("MERCEDES-BENZ", "SPRINTER", "SPRINTER", 160),
        ("MERCEDES-BENZ", "ATEGO", "ATEGO", 161),
        ("MERCEDES-BENZ", "ACCELO", "ACCELO", 162),
        ("MERCEDES-BENZ", "AXOR", "AXOR", 163),

        ("IVECO", "DAILY", "DAILY", 170),
        ("IVECO", "TECTOR", "TECTOR", 171),
        ("IVECO", "STRALIS", "STRALIS", 172),

        ("VOLVO", "FH", "FH", 180),
        ("VOLVO", "VM", "VM", 181),
        ("VOLVO", "FM", "FM", 182),

        ("SCANIA", "R440", "R440", 190),
        ("SCANIA", "P310", "P310", 191),
        ("SCANIA", "P360", "P360", 192),

        ("BYD", "DOLPHIN MINI", "DOLPHIN MINI", 200),
        ("BYD", "DOLPHIN", "DOLPHIN", 201),
        ("BYD", "SONG", "SONG", 202),
        ("BYD", "YUAN", "YUAN", 203),
        ("BYD", "SEAL", "SEAL", 204),

        ("GWM", "HAVAL H6", "HAVAL H6", 210),
        ("GWM", "ORA 03", "ORA 03", 211)
    ]

    df_matriz_marca_modelo_veiculo = spark.createDataFrame(
        matriz_marca_modelo_veiculo,
        ["ds_marca_veiculo", "ds_modelo_veiculo", "termo_modelo_busca", "prioridade_modelo"]
    )

    tabela = "tmp_matriz_marca_modelo_veiculo"

    df_matriz_marca_modelo_veiculo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_matriz_marca_modelo_veiculo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_matriz_marca_modelo_veiculo")


    # ============================================================
    # MATRIZ DE MARCAS
    # Atualizacao: alterar esta lista quando novas marcas aparecerem.
    # ============================================================

    matriz_marca_veiculo = [
        ("FIAT", "FIAT", 1),
        ("VOLKSWAGEN", "VOLKSWAGEN", 2),
        ("VOLKSWAGEN", "VW", 3),
        ("CHEVROLET", "CHEVROLET", 4),
        ("CHEVROLET", "GM", 5),
        ("HYUNDAI", "HYUNDAI", 6),
        ("TOYOTA", "TOYOTA", 7),
        ("RENAULT", "RENAULT", 8),
        ("FORD", "FORD", 9),
        ("HONDA", "HONDA", 10),
        ("JEEP", "JEEP", 11),
        ("NISSAN", "NISSAN", 12),
        ("PEUGEOT", "PEUGEOT", 13),
        ("CITROEN", "CITROEN", 14),
        ("MITSUBISHI", "MITSUBISHI", 15),
        ("MERCEDES-BENZ", "MERCEDES", 16),
        ("MERCEDES-BENZ", "MERCEDES BENZ", 17),
        ("IVECO", "IVECO", 18),
        ("VOLVO", "VOLVO", 19),
        ("SCANIA", "SCANIA", 20),
        ("BYD", "BYD", 21),
        ("GWM", "GWM", 22)
    ]

    df_matriz_marca_veiculo = spark.createDataFrame(
        matriz_marca_veiculo,
        ["ds_marca_veiculo", "termo_marca_busca", "prioridade_marca"]
    )

    tabela = "tmp_matriz_marca_veiculo"

    df_matriz_marca_veiculo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_matriz_marca_veiculo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_matriz_marca_veiculo")


    # ============================================================
    # PARSE VEICULO - CHAVE CANONICA PARA ENTIDADE E PONTE
    # Regra:
    #   1. Placa prevalece.
    #   2. Orgao + matricula prevalece quando nao houver placa.
    #   3. Raw normalizado fica como fallback.
    #   4. A entidade veiculo e deduplicada por chave_veiculo.
    #   5. A ponte usa o parse para apontar cada movimentacao ao id_veiculo canonico.
    # ============================================================

    regex_orgao_veiculo_limpeza = "|".join([x[1] for x in matriz_orgao_veiculo])

    df_base_veiculo_parse = spark.sql(f"""
        with origem as (
            select
                m.id_movimentacao,
                m.dt_registro,
                m.id_equipe_origem,
                m.id_presidio_origem,
                trim(regexp_replace(coalesce(m.veiculo_raw, ''), '\\\\s+', ' ')) as ds_veiculo_raw,
                translate(
                    upper(trim(regexp_replace(coalesce(m.veiculo_raw, ''), '\\\\s+', ' '))),
                    'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                    'AAAAAEEEEIIIIOOOOOUUUUC'
                ) as ds_veiculo_normalizado,
                concat(
                    ' ',
                    regexp_replace(
                        translate(
                            upper(trim(regexp_replace(coalesce(m.veiculo_raw, ''), '\\\\s+', ' '))),
                            'ÁÀÂÃÄÉÈÊËÍÌÎÏÓÒÔÕÖÚÙÛÜÇ',
                            'AAAAAEEEEIIIIOOOOOUUUUC'
                        ),
                        '[^A-Z0-9]+',
                        ' '
                    ),
                    ' '
                ) as ds_busca
            from gold.tmp_base_movimentacao_header m
            where trim(coalesce(m.veiculo_raw, '')) <> ''
        ),

        placa_extraida as (
            select
                id_movimentacao,
                dt_registro,
                id_equipe_origem,
                id_presidio_origem,
                ds_veiculo_raw,
                ds_veiculo_normalizado,
                ds_busca,
                case
                    when trim(coalesce(
                        regexp_replace(
                            regexp_extract(
                                ds_veiculo_normalizado,
                                '(^|\\\\s)([A-Z]{{3}})[ -]*([0-9]{{4}}|[0-9][A-Z0-9]{{3}})(?=$|\\\\s|[^A-Z0-9])',
                                0
                            ),
                            '[^A-Z0-9]',
                            ''
                        ),
                        ''
                    )) <> ''
                    then regexp_replace(
                        regexp_extract(
                            ds_veiculo_normalizado,
                            '(^|\\\\s)([A-Z]{{3}})[ -]*([0-9]{{4}}|[0-9][A-Z0-9]{{3}})(?=$|\\\\s|[^A-Z0-9])',
                            0
                        ),
                        '[^A-Z0-9]',
                        ''
                    )
                    else cast(null as string)
                end as ds_placa
            from origem
        ),

        org_match as (
            select *
            from (
                select
                    p.id_movimentacao,
                    p.ds_veiculo_raw,
                    o.ds_orgao_seguranca,
                    regexp_extract(
                        p.ds_veiculo_normalizado,
                        concat('(^|\\\\s)(', o.regex_orgao, ')(?:\\\\s+|[ -]+)*(?:N[º°]?|NO|NRO|NUM|NUMERO)?(?:\\\\s+|[ -]+)*([0-9]+)(?=$|\\\\s|[^A-Z0-9])'),
                        3
                    ) as nr_matricula_veiculo,
                    row_number() over (
                        partition by p.id_movimentacao, p.ds_veiculo_raw
                        order by o.prioridade_orgao asc
                    ) as rn
                from placa_extraida p
                inner join gold.tmp_matriz_orgao_veiculo o
                    on regexp_extract(
                        p.ds_veiculo_normalizado,
                        concat('(^|\\\\s)(', o.regex_orgao, ')(?:\\\\s+|[ -]+)*(?:N[º°]?|NO|NRO|NUM|NUMERO)?(?:\\\\s+|[ -]+)*([0-9]+)(?=$|\\\\s|[^A-Z0-9])'),
                        3
                    ) <> ''
            ) x
            where rn = 1
        ),

        modelo_match as (
            select *
            from (
                select
                    p.id_movimentacao,
                    p.ds_veiculo_raw,
                    mm.ds_marca_veiculo,
                    mm.ds_modelo_veiculo,
                    row_number() over (
                        partition by p.id_movimentacao, p.ds_veiculo_raw
                        order by
                            length(mm.termo_modelo_busca) desc,
                            mm.prioridade_modelo asc
                    ) as rn
                from placa_extraida p
                inner join gold.tmp_matriz_marca_modelo_veiculo mm
                    on instr(p.ds_busca, concat(' ', mm.termo_modelo_busca, ' ')) > 0
            ) x
            where rn = 1
        ),

        marca_match as (
            select *
            from (
                select
                    p.id_movimentacao,
                    p.ds_veiculo_raw,
                    ma.ds_marca_veiculo,
                    row_number() over (
                        partition by p.id_movimentacao, p.ds_veiculo_raw
                        order by
                            length(ma.termo_marca_busca) desc,
                            ma.prioridade_marca asc
                    ) as rn
                from placa_extraida p
                inner join gold.tmp_matriz_marca_veiculo ma
                    on instr(p.ds_busca, concat(' ', ma.termo_marca_busca, ' ')) > 0
            ) x
            where rn = 1
        ),

        enriquecida as (
            select
                p.id_movimentacao,
                p.dt_registro,
                p.id_equipe_origem,
                p.id_presidio_origem,
                p.ds_veiculo_raw,
                p.ds_veiculo_normalizado,
                p.ds_busca,
                p.ds_placa,
                o.ds_orgao_seguranca,
                o.nr_matricula_veiculo,
                coalesce(mo.ds_marca_veiculo, ma.ds_marca_veiculo) as ds_marca_veiculo,
                mo.ds_modelo_veiculo
            from placa_extraida p
            left join org_match o
                on p.id_movimentacao = o.id_movimentacao
               and p.ds_veiculo_raw = o.ds_veiculo_raw
            left join modelo_match mo
                on p.id_movimentacao = mo.id_movimentacao
               and p.ds_veiculo_raw = mo.ds_veiculo_raw
            left join marca_match ma
                on p.id_movimentacao = ma.id_movimentacao
               and p.ds_veiculo_raw = ma.ds_veiculo_raw
        ),

        limpa as (
            select
                id_movimentacao,
                dt_registro,
                id_equipe_origem,
                id_presidio_origem,
                ds_veiculo_raw,
                ds_veiculo_normalizado,
                ds_placa,
                ds_orgao_seguranca,
                case
                    when trim(coalesce(nr_matricula_veiculo, '')) <> '' then trim(nr_matricula_veiculo)
                    else cast(null as string)
                end as nr_matricula_veiculo,
                ds_marca_veiculo,
                ds_modelo_veiculo,
                trim(
                    regexp_replace(
                        regexp_replace(
                            regexp_replace(
                                ds_veiculo_normalizado,
                                '(^|\\\\s)([A-Z]{{3}})[ -]*([0-9]{{4}}|[0-9][A-Z0-9]{{3}})(?=$|\\\\s|[^A-Z0-9])',
                                ' '
                            ),
                            concat('(^|\\\\s)(', '{regex_orgao_veiculo_limpeza}', ')(?:\\\\s+|[ -]+)*(?:N[º°]?|NO|NRO|NUM|NUMERO)?(?:\\\\s+|[ -]+)*[0-9]+(?=$|\\\\s|[^A-Z0-9])'),
                            ' '
                        ),
                        '[^A-Z0-9]+',
                        ' '
                    )
                ) as ds_veiculo_limpo
            from enriquecida
        ),

        chaveada as (
            select
                id_movimentacao,
                dt_registro,
                id_equipe_origem,
                id_presidio_origem,
                ds_veiculo_raw,
                ds_veiculo_normalizado,

                case
                    when ds_marca_veiculo is not null and ds_modelo_veiculo is not null
                        then concat(ds_marca_veiculo, ' ', ds_modelo_veiculo)
                    when ds_marca_veiculo is not null
                        then ds_marca_veiculo
                    when ds_veiculo_limpo is not null and trim(ds_veiculo_limpo) <> ''
                        then ds_veiculo_limpo
                    else cast(null as string)
                end as ds_veiculo,

                ds_marca_veiculo,
                ds_modelo_veiculo,
                ds_placa,
                ds_orgao_seguranca,
                nr_matricula_veiculo,

                case
                    when ds_placa is not null
                        then concat('PLACA|', ds_placa)
                    when ds_orgao_seguranca is not null and nr_matricula_veiculo is not null
                        then concat('MATRICULA|', ds_orgao_seguranca, '|', nr_matricula_veiculo)
                    else concat('RAW|', ds_veiculo_normalizado)
                end as chave_veiculo
            from limpa
        )

        select
            concat('VEI_', md5(chave_veiculo)) as id_veiculo,
            chave_veiculo,
            id_movimentacao,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem,
            ds_veiculo,
            ds_marca_veiculo,
            ds_modelo_veiculo,
            ds_placa,
            ds_orgao_seguranca,
            nr_matricula_veiculo,
            ds_veiculo_raw,
            ds_veiculo_normalizado
        from chaveada
    """)

    tabela = "tmp_base_veiculo_parse"

    df_base_veiculo_parse.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_veiculo_parse, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_veiculo_parse")


    # ============================================================
    # VEICULO CANONICO - DEDUPLICADO POR PLACA / ORGAO+MATRICULA / RAW
    # ============================================================

    df_base_veiculo_raw = spark.sql("""
        select
            id_veiculo,
            chave_veiculo,
            ds_veiculo,
            ds_marca_veiculo,
            ds_modelo_veiculo,
            ds_placa,
            ds_orgao_seguranca,
            nr_matricula_veiculo,
            ds_veiculo_raw,
            ds_veiculo_normalizado
        from (
            select
                id_veiculo,
                chave_veiculo,
                ds_veiculo,
                ds_marca_veiculo,
                ds_modelo_veiculo,
                ds_placa,
                ds_orgao_seguranca,
                nr_matricula_veiculo,
                ds_veiculo_raw,
                ds_veiculo_normalizado,
                row_number() over (
                    partition by chave_veiculo
                    order by
                        case when ds_placa is not null then 0 else 1 end,
                        case when ds_orgao_seguranca is not null and nr_matricula_veiculo is not null then 0 else 1 end,
                        case when ds_marca_veiculo is not null and ds_modelo_veiculo is not null then 0 else 1 end,
                        length(coalesce(ds_veiculo, '')) desc,
                        length(coalesce(ds_veiculo_raw, '')) desc,
                        ds_veiculo_raw asc
                ) as rn
            from gold.tmp_base_veiculo_parse
        ) x
        where rn = 1
    """)

    tabela = "tmp_base_veiculo_raw"

    df_base_veiculo_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_veiculo_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_veiculo_raw")


    # ============================================================
    # RL MOVIMENTACAO X VEICULO - BASEADA NO PARSE CANONICO
    # ============================================================

    df_base_rl_movimentacao_veiculo = spark.sql("""
        select distinct
            concat(
                'RLMV_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(p.id_movimentacao, ''),
                        coalesce(v.id_veiculo, '')
                    )
                )
            ) as id_rl_movimentacao_veiculo,
            p.id_movimentacao,
            v.id_veiculo,
            p.dt_registro,
            p.id_equipe_origem,
            p.id_presidio_origem
        from gold.tmp_base_veiculo_parse p
        inner join gold.tmp_base_veiculo_raw v
            on p.chave_veiculo = v.chave_veiculo
    """)

    tabela = "tmp_base_rl_movimentacao_veiculo"

    df_base_rl_movimentacao_veiculo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_rl_movimentacao_veiculo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_rl_movimentacao_veiculo")


    # ============================================================
    # ENTIDADE VEICULO
    # ============================================================

    df_ent_veiculo = spark.sql("""
        select
            id_veiculo,
            ds_veiculo,
            ds_marca_veiculo,
            ds_modelo_veiculo,
            ds_placa,
            ds_orgao_seguranca,
            nr_matricula_veiculo,
            ds_veiculo_raw
        from gold.tmp_base_veiculo_raw
    """)

    tabela = "sinp_ent_veiculo"

    df_ent_veiculo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_ent_veiculo, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_veiculo")


    # ============================================================
    # FATO MOVIMENTACAO
    # ============================================================

    df_fato_movimentacao = spark.sql("""
        select
            h.id_movimentacao,
            h.id_evento_origem,
            h.origem_sistema,
            h.tipo_movimentacao,
            h.instituicao,
            h.origem_local,
            h.destino_local,
            h.motivo,
            h.solicitacao,
            h.autorizacao,
            h.documento,
            h.veiculo_raw,
            h.hr_inicio,
            h.hr_fim,
            h.dt_registro,
            h.id_equipe_origem,
            h.id_presidio_origem,
            h.flag_escolta,

            case when trim(coalesce(h.veiculo_raw, '')) <> '' then 1 else 0 end as flag_tem_veiculo,
            case when trim(coalesce(h.documento, '')) <> '' then 1 else 0 end as flag_tem_documento,
            case when trim(coalesce(h.autorizacao, '')) <> '' then 1 else 0 end as flag_tem_autorizacao,

            coalesce(p.qtd_presidiarios_relacionados, 0) as qtd_presidiarios_relacionados,
            coalesce(p.qtd_presidiarios_distintos, 0) as qtd_presidiarios_distintos,
            coalesce(p.qtd_presidiarios_resolvidos_pessoa, 0) as qtd_presidiarios_resolvidos_pessoa,
            coalesce(p.qtd_presidiarios_com_movimentacao_infopen_mesmo_dia, 0) as qtd_presidiarios_com_movimentacao_infopen_mesmo_dia,
            coalesce(p.qtd_movimentacoes_infopen_relacionadas, 0) as qtd_movimentacoes_infopen_relacionadas,
            p.txt_ids_preso_relacionados,
            p.txt_ids_pessoa_relacionados,
            p.txt_nomes_presidiarios_relacionados,
            p.txt_tipos_movimentacao_infopen_relacionadas,

            coalesce(pol.qtd_policiais_relacionados, 0) as qtd_policiais_relacionados,
            coalesce(pol.qtd_policiais_distintos, 0) as qtd_policiais_distintos,
            coalesce(pol.qtd_condutores, 0) as qtd_condutores,
            pol.txt_nomes_policiais_relacionados,

            coalesce(i.flag_tem_movimentacao_infopen_mesmo_dia, 0) as flag_tem_movimentacao_infopen_mesmo_dia,

            case when coalesce(p.qtd_presidiarios_distintos, 0) > 1 then 1 else 0 end as flag_movimentacao_coletiva,
            case when coalesce(pol.qtd_policiais_distintos, 0) > 1 then 1 else 0 end as flag_multiplos_policiais
        from gold.tmp_base_movimentacao_header h
        left join gold.tmp_base_agg_pessoa_movimentacao p
            on h.id_movimentacao = p.id_movimentacao
        left join gold.tmp_base_agg_policial_movimentacao pol
            on h.id_movimentacao = pol.id_movimentacao
        left join gold.tmp_base_agg_infopen_movimentacao i
            on h.id_movimentacao = i.id_movimentacao
    """)

    tabela = "sinp_fato_movimentacao"

    df_fato_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fato_movimentacao, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_movimentacao")


    # ============================================================
    # RL PESSOA X MOVIMENTACAO
    # ============================================================

    df_rl_pessoa_movimentacao = spark.sql("""
        select
            id_rl_pessoa_movimentacao,
            id_movimentacao,
            tipo_movimentacao,
            id_evento_origem,
            id_item_origem,
            id_preso_origem,
            id_pessoa_presidiario,
            nome_presidiario_raw,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem,
            qtd_movimentacoes_infopen_relacionadas,
            txt_tipos_movimentacao_infopen_relacionadas,
            flag_tem_movimentacao_infopen_mesmo_dia
        from gold.tmp_base_rl_pessoa_movimentacao
    """)

    tabela = "sinp_rl_pessoa_movimentacao"

    df_rl_pessoa_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_pessoa_movimentacao, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_pessoa_movimentacao")


    # ============================================================
    # RL MOVIMENTACAO X VEICULO
    # ============================================================

    df_rl_movimentacao_veiculo = spark.sql("""
        select
            id_rl_movimentacao_veiculo,
            id_movimentacao,
            id_veiculo,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem
        from gold.tmp_base_rl_movimentacao_veiculo
    """)

    tabela = "sinp_rl_movimentacao_veiculo"

    df_rl_movimentacao_veiculo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_movimentacao_veiculo, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_movimentacao_veiculo")


    # ============================================================
    # RL POLICIAL X MOVIMENTACAO
    # ============================================================

    df_rl_policial_movimentacao = spark.sql("""
        select
            id_rl_policial_movimentacao,
            id_movimentacao,
            tipo_movimentacao,
            id_evento_origem,
            id_item_origem,
            id_pessoa,
            nome_policial_raw,
            documento_policial_raw,
            flag_condutor,
            dt_registro,
            id_equipe_origem,
            id_presidio_origem
        from gold.tmp_base_rl_policial_movimentacao
    """)

    tabela = "sinp_rl_policial_movimentacao"

    df_rl_policial_movimentacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_policial_movimentacao, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_policial_movimentacao")


    # ============================================================
    # VALIDACOES
    # ============================================================

    spark.sql("""
    select
        count(*) as qtd_veiculos,
        count(distinct id_veiculo) as qtd_veiculos_distintos
    from gold.sinp_ent_veiculo
    """).show(truncate=False)

    spark.sql("""
    select
        id_veiculo,
        count(*) as qtd
    from gold.sinp_ent_veiculo
    group by id_veiculo
    having count(*) > 1
    order by qtd desc
    """).show(50, truncate=False)

    spark.sql("""
    select
        tipo_movimentacao,
        count(*) as qtd_movimentacoes,
        sum(qtd_presidiarios_distintos) as soma_presidiarios,
        sum(qtd_policiais_distintos) as soma_policiais,
        sum(flag_movimentacao_coletiva) as movimentacoes_coletivas,
        sum(flag_tem_veiculo) as movimentacoes_com_veiculo,
        sum(flag_tem_movimentacao_infopen_mesmo_dia) as movimentacoes_com_ctx_infopen
    from gold.sinp_fato_movimentacao
    group by tipo_movimentacao
    order by tipo_movimentacao
    """).show(truncate=False)

    spark.sql("""
    select
        count(*) as qtd_rl_pessoa_movimentacao,
        sum(case when id_pessoa_presidiario is not null then 1 else 0 end) as qtd_rl_pessoa_resolvida
    from gold.sinp_rl_pessoa_movimentacao
    """).show(truncate=False)

    spark.sql("""
    select
        count(*) as qtd_rl_movimentacao_veiculo
    from gold.sinp_rl_movimentacao_veiculo
    """).show(truncate=False)

    spark.sql("""
    select
        count(*) as qtd_rl_policial_movimentacao,
        sum(coalesce(flag_condutor, 0)) as qtd_condutores
    from gold.sinp_rl_policial_movimentacao
    """).show(truncate=False)


