# -*- coding: utf-8 -*-
"""Fato de ocorrência INFOPEN."""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"
    """Etapa extraída do notebook original."""
    # ===== CELL 25 =====
    import os

    # ============================================================
    # LIMPEZA DEFENSIVA
    # ============================================================

    tabelas_drop = [
        "tmp_base_infopen_ocorrencias",
        "tmp_base_infopen_tipos_ocorrencia",
        "tmp_base_infopen_status_ocorrencia_raw",
        "tmp_base_infopen_status_ocorrencia_rank",
        "tmp_base_infopen_status_ocorrencia_ultimo",
        "tmp_base_infopen_status_ocorrencia_agg",
        "tmp_base_infopen_observacoes_raw",
        "tmp_base_infopen_observacoes_agg",
        "tmp_base_infopen_ocorrencia_processo_raw",
        "tmp_base_infopen_ocorrencia_processo_agg",
        "tmp_base_pessoa_preso_ponte_ocorrencia",
        "tmp_base_presidiario_catalogo_ocorrencia",
        "tmp_base_infopen_ocorrencia_preso_raw",
        "tmp_base_infopen_ocorrencia_preso_agg",
        "tmp_fat_ocorrencia_infopen",
        "sinp_fat_ocorrencia_infopen",
        "sinp_rl_ocorrencia_preso_infopen",
        "sinp_rl_ocorrencia_processo_infopen",
        "sinp_rl_ocorrencia_observacao_infopen",
        "sinp_rl_ocorrencia_status_infopen"
    ]

    for t in tabelas_drop:
        spark.sql(f"drop table if exists gold.{t}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{t} >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")
    spark.sql("refresh table gold.sinp_ent_pessoa")


    # ============================================================
    # BASE INFOPEN OCORRENCIAS
    # ============================================================

    df_base_infopen_ocorrencias = spark.sql("""
        select
            cast(id_ocorrencia as string) as id_ocorrencia_origem,
            cast(id_tipoocorrencia as string) as id_tipo_ocorrencia,
            cast(id_municipio as string) as id_municipio,
            cast(id_status as string) as id_status_atual,
            cast(ocorrencia_numprontuario as string) as nr_prontuario,
            cast(ocorrencia_numero as string) as nr_ocorrencia,
            to_timestamp(ocorrencia_dtfato) as dt_fato,
            to_timestamp(ocorrencia_dtexpedicao) as dt_expedicao
        from bronze.infopen_ocorrencias
    """)

    tabela = "tmp_base_infopen_ocorrencias"

    df_base_infopen_ocorrencias.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_ocorrencias, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_ocorrencias")


    # ============================================================
    # BASE INFOPEN TIPOS OCORRENCIA
    # ============================================================

    df_base_infopen_tipos_ocorrencia = spark.sql("""
        select
            cast(id_tipoocorrencia as string) as id_tipo_ocorrencia,
            trim(regexp_replace(coalesce(tipoocorrencia_descricao, ''), '\\\\s+', ' ')) as ds_tipo_ocorrencia
        from bronze.infopen_tipos_ocorrencia
    """)

    tabela = "tmp_base_infopen_tipos_ocorrencia"

    df_base_infopen_tipos_ocorrencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_tipos_ocorrencia, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_tipos_ocorrencia")


    # ============================================================
    # RL STATUS INFOPEN RAW
    # ============================================================

    df_base_infopen_status_ocorrencia_raw = spark.sql("""
        select
            concat(
                'RLSTATUS_',
                md5(
                    concat_ws(
                        '|',
                        cast(id_ocorrencia as string),
                        cast(id_status as string),
                        cast(statusocorrencia_data as string),
                        cast(coalesce(statusocorrencia_ultima, false) as string)
                    )
                )
            ) as id_rl_ocorrencia_status,
            concat('OCR_INFOPEN_', cast(id_ocorrencia as string)) as id_fato_ocorrencia,
            cast(id_ocorrencia as string) as id_ocorrencia_origem,
            cast(id_status as string) as id_status_historico,
            to_timestamp(statusocorrencia_data) as dt_status_historico,
            case when coalesce(statusocorrencia_ultima, false) = true then 1 else 0 end as flag_status_marcado_ultimo
        from bronze.infopen_status_ocorrencias
    """)

    tabela = "tmp_base_infopen_status_ocorrencia_raw"

    df_base_infopen_status_ocorrencia_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_status_ocorrencia_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_status_ocorrencia_raw")


    # ============================================================
    # STATUS INFOPEN RANKEADO
    # ============================================================

    df_base_infopen_status_ocorrencia_rank = spark.sql("""
        select
            *,
            row_number() over (
                partition by id_ocorrencia_origem
                order by
                    case when flag_status_marcado_ultimo = 1 then 1 else 2 end,
                    dt_status_historico desc,
                    id_status_historico desc
            ) as rn
        from gold.tmp_base_infopen_status_ocorrencia_raw
    """)

    tabela = "tmp_base_infopen_status_ocorrencia_rank"

    df_base_infopen_status_ocorrencia_rank.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_status_ocorrencia_rank, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_status_ocorrencia_rank")


    # ============================================================
    # STATUS INFOPEN ULTIMO
    # ============================================================

    df_base_infopen_status_ocorrencia_ultimo = spark.sql("""
        select
            id_ocorrencia_origem,
            id_status_historico as id_status_ultimo,
            dt_status_historico as dt_status_ultimo,
            flag_status_marcado_ultimo
        from gold.tmp_base_infopen_status_ocorrencia_rank
        where rn = 1
    """)

    tabela = "tmp_base_infopen_status_ocorrencia_ultimo"

    df_base_infopen_status_ocorrencia_ultimo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_status_ocorrencia_ultimo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_status_ocorrencia_ultimo")


    # ============================================================
    # STATUS INFOPEN AGREGADO
    # ============================================================

    df_base_infopen_status_ocorrencia_agg = spark.sql("""
        select
            id_ocorrencia_origem,
            count(*) as qtd_status_historico,
            count(distinct id_status_historico) as qtd_status_distintos,
            min(dt_status_historico) as dt_primeiro_status_historico,
            max(dt_status_historico) as dt_ultimo_status_historico,
            max(flag_status_marcado_ultimo) as flag_possui_status_marcado_ultimo,
            concat_ws(
                ' | ',
                sort_array(collect_set(id_status_historico))
            ) as txt_ids_status_historico
        from gold.tmp_base_infopen_status_ocorrencia_raw
        group by id_ocorrencia_origem
    """)

    tabela = "tmp_base_infopen_status_ocorrencia_agg"

    df_base_infopen_status_ocorrencia_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_status_ocorrencia_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_status_ocorrencia_agg")


    # ============================================================
    # RL OBSERVACAO INFOPEN RAW
    # ============================================================

    df_base_infopen_observacoes_raw = spark.sql("""
        select
            concat(
                'RLOBS_',
                md5(
                    concat_ws(
                        '|',
                        cast(id_ocorrencia as string),
                        trim(regexp_replace(coalesce(ocorrenciaobservacao_descricao, ''), '\\\\s+', ' '))
                    )
                )
            ) as id_rl_ocorrencia_observacao,
            concat('OCR_INFOPEN_', cast(id_ocorrencia as string)) as id_fato_ocorrencia,
            cast(id_ocorrencia as string) as id_ocorrencia_origem,
            trim(regexp_replace(coalesce(ocorrenciaobservacao_descricao, ''), '\\\\s+', ' ')) as ds_observacao
        from bronze.infopen_ocorrencias_observacoes
    """)

    tabela = "tmp_base_infopen_observacoes_raw"

    df_base_infopen_observacoes_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_observacoes_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_observacoes_raw")


    # ============================================================
    # OBSERVACOES INFOPEN AGREGADAS
    # ============================================================

    df_base_infopen_observacoes_agg = spark.sql("""
        select
            id_ocorrencia_origem,
            count(*) as qtd_observacoes_infopen,
            concat_ws(
                ' | ',
                sort_array(collect_set(ds_observacao))
            ) as txt_observacoes_infopen
        from gold.tmp_base_infopen_observacoes_raw
        group by id_ocorrencia_origem
    """)

    tabela = "tmp_base_infopen_observacoes_agg"

    df_base_infopen_observacoes_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_observacoes_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_observacoes_agg")


    # ============================================================
    # RL OCORRENCIA X PROCESSO INFOPEN RAW
    # ============================================================

    df_base_infopen_ocorrencia_processo_raw = spark.sql("""
        select
            concat(
                'RLPROC_',
                md5(
                    concat_ws(
                        '|',
                        cast(id_ocorrencia as string),
                        cast(id_processo as string)
                    )
                )
            ) as id_rl_ocorrencia_processo,
            concat('OCR_INFOPEN_', cast(id_ocorrencia as string)) as id_fato_ocorrencia,
            cast(id_ocorrencia as string) as id_ocorrencia_origem,
            cast(id_processo as string) as id_processo_origem
        from bronze.infopen_ocorrencias_processos
    """)

    tabela = "tmp_base_infopen_ocorrencia_processo_raw"

    df_base_infopen_ocorrencia_processo_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_ocorrencia_processo_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_ocorrencia_processo_raw")


    # ============================================================
    # PROCESSOS INFOPEN AGREGADOS
    # ============================================================

    df_base_infopen_ocorrencia_processo_agg = spark.sql("""
        select
            id_ocorrencia_origem,
            count(*) as qtd_processos_infopen,
            concat_ws(
                ' | ',
                sort_array(collect_set(id_processo_origem))
            ) as txt_ids_processo_infopen
        from gold.tmp_base_infopen_ocorrencia_processo_raw
        group by id_ocorrencia_origem
    """)

    tabela = "tmp_base_infopen_ocorrencia_processo_agg"

    df_base_infopen_ocorrencia_processo_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_ocorrencia_processo_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_ocorrencia_processo_agg")


    # ============================================================
    # BASE PONTE PESSOA X PRESO
    # ============================================================

    df_base_pessoa_preso_ponte_ocorrencia = spark.sql("""
        select distinct
            cast(id_preso as string) as id_preso_origem,
            id_pessoa as id_pessoa_presidiario
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
    """)

    tabela = "tmp_base_pessoa_preso_ponte_ocorrencia"

    df_base_pessoa_preso_ponte_ocorrencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_preso_ponte_ocorrencia, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_preso_ponte_ocorrencia")


    # ============================================================
    # CATALOGO PRESIDIARIO ENRIQUECIDO
    # ============================================================

    df_base_presidiario_catalogo_ocorrencia = spark.sql("""
        select
            p.id_preso_origem,
            p.id_pessoa_presidiario,
            e.nome_pessoa as nome_presidiario,
            e.documento as documento_presidiario
        from gold.tmp_base_pessoa_preso_ponte_ocorrencia p
        left join gold.sinp_ent_pessoa e
            on p.id_pessoa_presidiario = e.id_pessoa
    """)

    tabela = "tmp_base_presidiario_catalogo_ocorrencia"

    df_base_presidiario_catalogo_ocorrencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_presidiario_catalogo_ocorrencia, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_presidiario_catalogo_ocorrencia")


    # ============================================================
    # RL OCORRENCIA X PRESO INFOPEN RAW
    # ============================================================

    df_base_infopen_ocorrencia_preso_raw = spark.sql("""
        select
            concat(
                'RLPRESO_',
                md5(
                    concat_ws(
                        '|',
                        cast(po.id_ocorrencia as string),
                        cast(po.id_preso as string)
                    )
                )
            ) as id_rl_ocorrencia_preso,
            concat('OCR_INFOPEN_', cast(po.id_ocorrencia as string)) as id_fato_ocorrencia,
            cast(po.id_ocorrencia as string) as id_ocorrencia_origem,
            cast(po.id_preso as string) as id_preso_origem,
            pc.id_pessoa_presidiario,
            pc.nome_presidiario,
            pc.documento_presidiario
        from bronze.infopen_presos_ocorrencias po
        left join gold.tmp_base_presidiario_catalogo_ocorrencia pc
            on cast(po.id_preso as string) = pc.id_preso_origem
    """)

    tabela = "tmp_base_infopen_ocorrencia_preso_raw"

    df_base_infopen_ocorrencia_preso_raw.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_ocorrencia_preso_raw, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_ocorrencia_preso_raw")


    # ============================================================
    # PRESOS INFOPEN AGREGADOS
    # ============================================================

    df_base_infopen_ocorrencia_preso_agg = spark.sql("""
        select
            id_ocorrencia_origem,
            count(*) as qtd_presos_infopen,
            sum(case when id_pessoa_presidiario is not null then 1 else 0 end) as qtd_presos_resolvidos_infopen,
            concat_ws(
                ' | ',
                sort_array(collect_set(id_preso_origem))
            ) as txt_ids_preso_infopen,
            concat_ws(
                ' | ',
                sort_array(collect_set(cast(id_pessoa_presidiario as string)))
            ) as txt_ids_pessoa_presidiario
        from gold.tmp_base_infopen_ocorrencia_preso_raw
        group by id_ocorrencia_origem
    """)

    tabela = "tmp_base_infopen_ocorrencia_preso_agg"

    df_base_infopen_ocorrencia_preso_agg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_infopen_ocorrencia_preso_agg, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_infopen_ocorrencia_preso_agg")


    # ============================================================
    # FATO OCORRENCIA INFOPEN - MAXIMA
    # ============================================================

    df_fat_ocorrencia_infopen = spark.sql("""
        select
            concat('OCR_INFOPEN_', o.id_ocorrencia_origem) as id_fato_ocorrencia,
            'INFOPEN' as origem_sistema,

            o.id_ocorrencia_origem,

            o.id_tipo_ocorrencia,
            t.ds_tipo_ocorrencia,

            o.id_municipio,

            o.id_status_atual,
            su.id_status_ultimo,
            su.dt_status_ultimo,
            su.flag_status_marcado_ultimo,

            coalesce(sa.qtd_status_historico, 0) as qtd_status_historico,
            coalesce(sa.qtd_status_distintos, 0) as qtd_status_distintos,
            sa.dt_primeiro_status_historico,
            sa.dt_ultimo_status_historico,
            coalesce(sa.flag_possui_status_marcado_ultimo, 0) as flag_possui_status_marcado_ultimo,
            sa.txt_ids_status_historico,

            case
                when o.id_status_atual is not null
                 and su.id_status_ultimo is not null
                 and o.id_status_atual <> su.id_status_ultimo then 1
                else 0
            end as flag_status_divergente,

            case when coalesce(sa.qtd_status_distintos, 0) > 1 then 1 else 0 end as flag_multiplos_status,

            o.nr_prontuario,
            o.nr_ocorrencia,

            o.dt_fato,
            o.dt_expedicao,
            coalesce(o.dt_fato, o.dt_expedicao) as dt_evento_referencia,
            datediff(current_date(), to_date(coalesce(o.dt_fato, o.dt_expedicao))) as dias_desde_evento,

            case when o.nr_prontuario is not null then 1 else 0 end as flag_tem_prontuario,
            case when o.nr_ocorrencia is not null then 1 else 0 end as flag_tem_numero_ocorrencia,
            case when o.dt_fato is not null then 1 else 0 end as flag_tem_dt_fato,
            case when o.dt_expedicao is not null then 1 else 0 end as flag_tem_dt_expedicao,

            coalesce(obs.qtd_observacoes_infopen, 0) as qtd_observacoes_infopen,
            case when coalesce(obs.qtd_observacoes_infopen, 0) > 0 then 1 else 0 end as flag_tem_observacao,
            case when coalesce(obs.qtd_observacoes_infopen, 0) > 1 then 1 else 0 end as flag_multiplas_observacoes,
            obs.txt_observacoes_infopen,

            coalesce(pr.qtd_processos_infopen, 0) as qtd_processos_infopen,
            case when coalesce(pr.qtd_processos_infopen, 0) > 0 then 1 else 0 end as flag_tem_processo,
            case when coalesce(pr.qtd_processos_infopen, 0) > 1 then 1 else 0 end as flag_multiplos_processos,
            pr.txt_ids_processo_infopen,

            coalesce(p.qtd_presos_infopen, 0) as qtd_presos_infopen,
            coalesce(p.qtd_presos_resolvidos_infopen, 0) as qtd_presos_resolvidos_infopen,
            case when coalesce(p.qtd_presos_infopen, 0) > 0 then 1 else 0 end as flag_tem_preso,
            case when coalesce(p.qtd_presos_infopen, 0) > 1 then 1 else 0 end as flag_multiplos_presos,
            p.txt_ids_preso_infopen,
            p.txt_ids_pessoa_presidiario,

            (
                coalesce(obs.qtd_observacoes_infopen, 0) +
                coalesce(pr.qtd_processos_infopen, 0) +
                coalesce(p.qtd_presos_infopen, 0) +
                coalesce(sa.qtd_status_historico, 0)
            ) as score_complexidade_basica
        from gold.tmp_base_infopen_ocorrencias o
        left join gold.tmp_base_infopen_tipos_ocorrencia t
            on o.id_tipo_ocorrencia = t.id_tipo_ocorrencia
        left join gold.tmp_base_infopen_status_ocorrencia_ultimo su
            on o.id_ocorrencia_origem = su.id_ocorrencia_origem
        left join gold.tmp_base_infopen_status_ocorrencia_agg sa
            on o.id_ocorrencia_origem = sa.id_ocorrencia_origem
        left join gold.tmp_base_infopen_observacoes_agg obs
            on o.id_ocorrencia_origem = obs.id_ocorrencia_origem
        left join gold.tmp_base_infopen_ocorrencia_processo_agg pr
            on o.id_ocorrencia_origem = pr.id_ocorrencia_origem
        left join gold.tmp_base_infopen_ocorrencia_preso_agg p
            on o.id_ocorrencia_origem = p.id_ocorrencia_origem
    """)

    tabela = "tmp_fat_ocorrencia_infopen"

    df_fat_ocorrencia_infopen.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_ocorrencia_infopen, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_fat_ocorrencia_infopen")


    # ============================================================
    # FATO OCORRENCIA INFOPEN FINAL
    # ============================================================

    df_fat_ocorrencia_infopen_final = spark.sql("""
        select
            id_fato_ocorrencia,
            origem_sistema,
            id_ocorrencia_origem,

            id_tipo_ocorrencia,
            ds_tipo_ocorrencia,
            id_municipio,

            id_status_atual,
            id_status_ultimo,
            dt_status_ultimo,
            flag_status_marcado_ultimo,
            qtd_status_historico,
            qtd_status_distintos,
            dt_primeiro_status_historico,
            dt_ultimo_status_historico,
            flag_possui_status_marcado_ultimo,
            txt_ids_status_historico,
            flag_status_divergente,
            flag_multiplos_status,

            nr_prontuario,
            nr_ocorrencia,
            dt_fato,
            dt_expedicao,
            dt_evento_referencia,
            dias_desde_evento,
            flag_tem_prontuario,
            flag_tem_numero_ocorrencia,
            flag_tem_dt_fato,
            flag_tem_dt_expedicao,

            qtd_observacoes_infopen,
            flag_tem_observacao,
            flag_multiplas_observacoes,
            txt_observacoes_infopen,

            qtd_processos_infopen,
            flag_tem_processo,
            flag_multiplos_processos,
            txt_ids_processo_infopen,

            qtd_presos_infopen,
            qtd_presos_resolvidos_infopen,
            flag_tem_preso,
            flag_multiplos_presos,
            txt_ids_preso_infopen,
            txt_ids_pessoa_presidiario,

            score_complexidade_basica
        from gold.tmp_fat_ocorrencia_infopen
    """)

    tabela = "sinp_fat_ocorrencia_infopen"

    df_fat_ocorrencia_infopen_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_ocorrencia_infopen_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_ocorrencia")


    # ============================================================
    # RL OCORRENCIA X PRESO INFOPEN FINAL
    # ============================================================

    df_rl_ocorrencia_preso_infopen_final = spark.sql("""
        select
            id_rl_ocorrencia_preso,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_preso_origem,
            id_pessoa_presidiario,
            nome_presidiario,
            documento_presidiario
        from gold.tmp_base_infopen_ocorrencia_preso_raw
    """)

    tabela = "sinp_rl_ocorrencia_preso_infopen"

    df_rl_ocorrencia_preso_infopen_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_preso_infopen_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_ocorrencia_preso")


    # ============================================================
    # RL OCORRENCIA X PROCESSO INFOPEN FINAL
    # ============================================================

    df_rl_ocorrencia_processo_infopen_final = spark.sql("""
        select
            id_rl_ocorrencia_processo,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_processo_origem
        from gold.tmp_base_infopen_ocorrencia_processo_raw
    """)

    tabela = "sinp_rl_ocorrencia_processo_infopen"

    df_rl_ocorrencia_processo_infopen_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_processo_infopen_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_ocorrencia_processo")


    # ============================================================
    # RL OCORRENCIA X OBSERVACAO INFOPEN FINAL
    # ============================================================

    df_rl_ocorrencia_observacao_infopen_final = spark.sql("""
        select
            id_rl_ocorrencia_observacao,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            ds_observacao
        from gold.tmp_base_infopen_observacoes_raw
    """)

    tabela = "sinp_rl_ocorrencia_observacao_infopen"

    df_rl_ocorrencia_observacao_infopen_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_observacao_infopen_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_ocorrencia_observacao")


    # ============================================================
    # RL OCORRENCIA X STATUS INFOPEN FINAL
    # ============================================================

    df_rl_ocorrencia_status_infopen_final = spark.sql("""
        select
            id_rl_ocorrencia_status,
            id_fato_ocorrencia,
            id_ocorrencia_origem,
            id_status_historico,
            dt_status_historico,
            flag_status_marcado_ultimo
        from gold.tmp_base_infopen_status_ocorrencia_raw
    """)

    tabela = "sinp_rl_ocorrencia_status_infopen"

    df_rl_ocorrencia_status_infopen_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_rl_ocorrencia_status_infopen_final, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rl_ocorrencia_status")


