# -*- coding: utf-8 -*-
"""Fato de visita familiar."""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"
    """Etapa extraída do notebook original."""
    # ===== CELL 23 =====
    import os

    # ============================================================
    # LIMPEZA DEFENSIVA DAS TABELAS TEMPORARIAS - FATO VISITA FAMILIAR
    # ============================================================

    spark.sql("drop table if exists gold.tmp_base_controle_familiar")
    spark.sql("drop table if exists gold.tmp_base_historicalvisitafamiliar_evento")
    spark.sql("drop table if exists gold.tmp_base_pessoa_familiar")
    spark.sql("drop table if exists gold.tmp_base_interno_livro_familiar")
    spark.sql("drop table if exists gold.tmp_base_pessoa_preso_ponte_familiar")
    spark.sql("drop table if exists gold.tmp_base_presidiario_resolvido_familiar")
    spark.sql("drop table if exists gold.tmp_evento_base_visita_familiar")
    spark.sql("drop table if exists gold.tmp_evento_enriquecido_visita_familiar")
    spark.sql("drop table if exists gold.tmp_evento_calculado_visita_familiar")
    spark.sql("drop table if exists gold.tmp_evento_rank_visita_familiar")
    spark.sql("drop table if exists gold.sinp_fat_visita_familiar")

    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_controle_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_historicalvisitafamiliar_evento >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_interno_livro_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_preso_ponte_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_presidiario_resolvido_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_base_visita_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_enriquecido_visita_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_calculado_visita_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_rank_visita_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}sinp_fat_visita_familiar >/dev/null 2>&1")

    spark.catalog.clearCache()
    #spark.sql("refresh table gold.df_pessoa_final_familiar")
    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")
    spark.sql("refresh table gold.sinp_ent_pessoa")
    spark.sql("refresh table bronze.livros_acesso_unidade_controlefamiliares")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalvisitafamiliar")
    spark.sql("refresh table bronze.livros_acesso_unidade_interno")


    # ============================================================
    # BASE CONTROLE FAMILIAR
    # ============================================================

    df_base_controle_familiar = spark.sql("""
        select
            cast(id as string) as id_evento_origem,
            cast(vinculo_id as string) as id_vinculo_origem,
            cast(vinculo_id as string) as id_interno_origem,
            cast(equipe_id as string) as id_equipe_origem,
            cast(presidio_id as string) as id_presidio_origem,
            to_timestamp(hr_entrada) as dt_hr_entrada,
            to_timestamp(hr_saida) as dt_hr_saida,
            to_timestamp(data_registro) as dt_registro,
            case
                when hr_entrada is not null then date_format(hr_entrada, 'HH:mm:ss')
                else null
            end as hr_entrada,
            case
                when hr_saida is not null then date_format(hr_saida, 'HH:mm:ss')
                else null
            end as hr_saida,
            trim(regexp_replace(coalesce(tipo, ''), '\\\\s+', ' ')) as tipo_visita
        from bronze.livros_acesso_unidade_controlefamiliares
    """)

    tabela = "tmp_base_controle_familiar"

    df_base_controle_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_controle_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_controle_familiar")


    # ============================================================
    # BASE HISTORICAL VISITA FAMILIAR EVENTO
    # history_id = id do controle
    # ============================================================

    df_base_historicalvisitafamiliar_evento = spark.sql("""
        select
            id_visitante_origem,
            id_evento_origem,
            nome_visitante,
            documento_original,
            telefone,
            history_date,
            history_type,
            history_user_id,
            chave_documento
        from (
            select
                cast(id as string) as id_visitante_origem,
                cast(history_id as string) as id_evento_origem,
                trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_visitante,
                trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento_original,
                trim(regexp_replace(coalesce(telefone, ''), '\\\\s+', ' ')) as telefone,
                to_timestamp(history_date) as history_date,
                trim(regexp_replace(coalesce(history_type, ''), '\\\\s+', ' ')) as history_type,
                cast(history_user_id as string) as history_user_id,
                case
                    when length(regexp_replace(coalesce(documento, ''), '[^0-9]', '')) = 11 then
                        concat(
                            'CPF_',
                            lpad(regexp_replace(coalesce(documento, ''), '[^0-9]', ''), 11, '0')
                        )
                    else
                        concat(
                            'DOC_',
                            upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', ''))
                        )
                end as chave_documento,
                row_number() over (
                    partition by cast(history_id as string)
                    order by
                        case when history_date is not null then 1 else 2 end,
                        history_date desc,
                        id desc
                ) as rn
            from bronze.livros_acesso_unidade_historicalvisitafamiliar
        ) x
        where rn = 1
    """)

    tabela = "tmp_base_historicalvisitafamiliar_evento"

    df_base_historicalvisitafamiliar_evento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_historicalvisitafamiliar_evento, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_historicalvisitafamiliar_evento")


    # ============================================================
    # BASE PESSOA FAMILIAR
    # ============================================================

    df_base_pessoa_familiar = spark.sql("""
        select distinct
            id_pessoa as id_pessoa_visitante,
            documento as documento_visitante,
            nome_pessoa as nome_visitante_pessoa,
            case
                when cod_documento_referencia = 19 then
                    concat(
                        'CPF_',
                        lpad(regexp_replace(coalesce(documento, ''), '[^0-9]', ''), 11, '0')
                    )
                else
                    concat(
                        'DOC_',
                        upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', ''))
                    )
            end as chave_documento
        from gold.sinp_ent_pessoa
        where coalesce(flag_visitante, 0) = 1
          and documento is not null
          and trim(documento) <> ''
    """)

    tabela = "tmp_base_pessoa_familiar"

    df_base_pessoa_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_familiar")


    # ============================================================
    # BASE INTERNO LIVRO FAMILIAR
    # controle.vinculo_id = interno.id
    # ============================================================

    df_base_interno_livro_familiar = spark.sql("""
        select
            cast(id as string) as id_interno_origem,
            cast(infopen as string) as id_preso_infopen,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_presidiario_livro,
            trim(regexp_replace(coalesce(galeria, ''), '\\\\s+', ' ')) as galeria_presidiario_livro,
            trim(regexp_replace(coalesce(cela, ''), '\\\\s+', ' ')) as cela_presidiario_livro,
            cast(status as string) as status_presidiario_livro,
            cast(presidio_id as string) as id_presidio_interno,
            cast(regime_id as string) as id_regime_interno,
            cast(grupo_visita_intima_id as string) as grupo_visita_intima_id,
            cast(grupo_visita_social_id as string) as grupo_visita_social_id,
            cast(grupo_saida_id as string) as grupo_saida_id,
            cast(situacao_id as string) as situacao_id,
            cast(n_uniforme as string) as n_uniforme
        from bronze.livros_acesso_unidade_interno
        where infopen is not null
    """)

    tabela = "tmp_base_interno_livro_familiar"

    df_base_interno_livro_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_interno_livro_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_interno_livro_familiar")


    # ============================================================
    # BASE PONTE PESSOA/PRESO
    # ============================================================

    df_base_pessoa_preso_ponte_familiar = spark.sql("""
        select distinct
            trim(cast(id_preso as string)) as id_preso_infopen,
            trim(cast(id_preso as string)) as id_preso_presidiario,
            id_pessoa as id_pessoa_presidiario
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
    """)

    tabela = "tmp_base_pessoa_preso_ponte_familiar"

    df_base_pessoa_preso_ponte_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_preso_ponte_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_preso_ponte_familiar")


    # ============================================================
    # BASE PRESIDIARIO RESOLVIDO FAMILIAR
    # MODELO IGUAL AO ADVOGADO:
    # controle -> interno -> infopen -> ponte -> ent_pessoa
    # ============================================================

    df_base_presidiario_resolvido_familiar = spark.sql("""
        select
            c.id_evento_origem,
            c.id_vinculo_origem,
            c.id_interno_origem,
            trim(i.id_preso_infopen) as id_preso_infopen,
            trim(p.id_preso_presidiario) as id_preso_presidiario,
            p.id_pessoa_presidiario,
            e.documento as documento_presidiario,
            e.nome_pessoa as nome_presidiario_pessoa,
            i.nome_presidiario_livro,
            i.galeria_presidiario_livro,
            i.cela_presidiario_livro,
            i.status_presidiario_livro,
            i.id_presidio_interno,
            i.id_regime_interno,
            i.grupo_visita_intima_id,
            i.grupo_visita_social_id,
            i.grupo_saida_id,
            i.situacao_id,
            i.n_uniforme
        from gold.tmp_base_controle_familiar c
        inner join gold.tmp_base_interno_livro_familiar i
            on trim(c.id_interno_origem) = trim(i.id_interno_origem)
        inner join gold.tmp_base_pessoa_preso_ponte_familiar p
            on trim(i.id_preso_infopen) = trim(p.id_preso_infopen)
        left join gold.sinp_ent_pessoa e
            on p.id_pessoa_presidiario = e.id_pessoa
    """)

    tabela = "tmp_base_presidiario_resolvido_familiar"

    df_base_presidiario_resolvido_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_presidiario_resolvido_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_presidiario_resolvido_familiar")


    # ============================================================
    # EVENTO BASE VISITA FAMILIAR
    # controle.id = historicalvisitafamiliar.history_id
    # ============================================================

    df_evento_base_visita_familiar = spark.sql("""
        select
            c.id_evento_origem,
            c.id_vinculo_origem,
            c.id_interno_origem,
            c.id_equipe_origem,
            c.id_presidio_origem,
            c.dt_hr_entrada,
            c.dt_hr_saida,
            c.hr_entrada,
            c.hr_saida,
            c.dt_registro,
            c.tipo_visita,

            h.id_visitante_origem,
            h.nome_visitante,
            h.documento_original,
            h.telefone,
            h.history_date,
            h.history_type,
            h.history_user_id,
            h.chave_documento

        from gold.tmp_base_controle_familiar c
        inner join gold.tmp_base_historicalvisitafamiliar_evento h
            on trim(c.id_evento_origem) = trim(h.id_evento_origem)
    """)

    tabela = "tmp_evento_base_visita_familiar"

    df_evento_base_visita_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_base_visita_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_base_visita_familiar")


    # ============================================================
    # EVENTO ENRIQUECIDO VISITA FAMILIAR
    # ============================================================

    df_evento_enriquecido_visita_familiar = spark.sql("""
        select
            e.id_evento_origem,
            e.id_vinculo_origem,
            e.id_interno_origem,
            e.id_equipe_origem,
            e.id_presidio_origem,

            e.id_visitante_origem,
            p.id_pessoa_visitante,

            pr.id_pessoa_presidiario,
            pr.id_preso_presidiario,
            pr.id_preso_infopen,

            coalesce(p.nome_visitante_pessoa, e.nome_visitante) as nome_visitante,
            coalesce(p.documento_visitante, e.documento_original) as documento_visitante,
            e.telefone,

            e.dt_hr_entrada,
            e.dt_hr_saida,
            e.hr_entrada,
            e.hr_saida,
            e.dt_registro,
            coalesce(e.dt_hr_entrada, e.dt_hr_saida, e.dt_registro, e.history_date) as dt_evento_referencia,
            e.tipo_visita,

            case
                when p.id_pessoa_visitante is not null then 'VISITANTE_RESOLVIDO_POR_DOCUMENTO'
                else 'VISITANTE_SEM_RESOLUCAO_POR_DOCUMENTO'
            end as origem_resolucao_visitante,

            case
                when pr.id_pessoa_presidiario is not null then 'CONTROLE_INTERNO_INFOPEN_PESSOA'
                when pr.id_preso_infopen is not null then 'CONTROLE_INTERNO_INFOPEN_SEM_PESSOA'
                else 'SEM_RESOLUCAO'
            end as origem_resolucao_presidiario,

            e.nome_visitante as nome_visitante_historico,
            e.documento_original as documento_visitante_historico,
            e.history_date,
            e.history_type,
            e.history_user_id,

            pr.nome_presidiario_livro,
            pr.nome_presidiario_pessoa,
            pr.documento_presidiario,
            pr.galeria_presidiario_livro,
            pr.cela_presidiario_livro,
            pr.status_presidiario_livro,
            pr.id_presidio_interno,
            pr.id_regime_interno,
            pr.grupo_visita_intima_id,
            pr.grupo_visita_social_id,
            pr.grupo_saida_id,
            pr.situacao_id,
            pr.n_uniforme

        from gold.tmp_evento_base_visita_familiar e
        left join gold.tmp_base_pessoa_familiar p
            on e.chave_documento = p.chave_documento
        left join gold.tmp_base_presidiario_resolvido_familiar pr
            on trim(e.id_evento_origem) = trim(pr.id_evento_origem)
    """)

    tabela = "tmp_evento_enriquecido_visita_familiar"

    df_evento_enriquecido_visita_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_enriquecido_visita_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_enriquecido_visita_familiar")


    # ============================================================
    # EVENTO CALCULADO VISITA FAMILIAR
    # ============================================================

    df_evento_calculado_visita_familiar = spark.sql("""
        select
            concat(
                'FVF_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(id_evento_origem, ''),
                        coalesce(id_vinculo_origem, ''),
                        coalesce(id_visitante_origem, ''),
                        coalesce(id_pessoa_visitante, ''),
                        coalesce(cast(dt_evento_referencia as string), '')
                    )
                )
            ) as id_fato_visita_familiar,

            -- CAMPOS JA EXISTENTES
            id_evento_origem,
            id_vinculo_origem,
            id_equipe_origem,
            id_presidio_origem,
            id_pessoa_visitante,
            id_pessoa_presidiario,
            id_preso_presidiario,
            nome_visitante,
            documento_visitante,
            telefone,
            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            to_date(dt_evento_referencia) as dt_evento,
            tipo_visita,
            origem_resolucao_presidiario,
            case when dt_hr_entrada is not null then 1 else 0 end as flag_tem_entrada,
            case when dt_hr_saida is not null then 1 else 0 end as flag_tem_saida,
            case
                when dt_hr_entrada is not null and dt_hr_saida is not null and dt_hr_saida >= dt_hr_entrada then 1
                else 0
            end as flag_duracao_valida,
            case
                when dt_hr_entrada is not null and dt_hr_saida is not null and dt_hr_saida >= dt_hr_entrada
                then cast((unix_timestamp(dt_hr_saida) - unix_timestamp(dt_hr_entrada)) / 60.0 as decimal(18,2))
                else cast(null as decimal(18,2))
            end as duracao_minutos,

            -- NOVOS CAMPOS
            id_visitante_origem,
            id_interno_origem,
            id_preso_infopen,
            origem_resolucao_visitante,

            nome_visitante_historico,
            documento_visitante_historico,
            history_date,
            history_type,
            history_user_id,

            nome_presidiario_livro,
            nome_presidiario_pessoa,
            documento_presidiario,
            galeria_presidiario_livro,
            cela_presidiario_livro,
            status_presidiario_livro,
            id_presidio_interno,
            id_regime_interno,
            grupo_visita_intima_id,
            grupo_visita_social_id,
            grupo_saida_id,
            situacao_id,
            n_uniforme,

            case when id_pessoa_visitante is not null then 1 else 0 end as flag_visitante_resolvido,
            case when id_pessoa_presidiario is not null then 1 else 0 end as flag_presidiario_resolvido
        from gold.tmp_evento_enriquecido_visita_familiar
    """)

    tabela = "tmp_evento_calculado_visita_familiar"

    df_evento_calculado_visita_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_calculado_visita_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_calculado_visita_familiar")


    # ============================================================
    # EVENTO RANKEADO VISITA FAMILIAR
    # ============================================================

    df_evento_rank_visita_familiar = spark.sql("""
        select
            *,
            row_number() over (
                partition by coalesce(id_evento_origem, id_vinculo_origem, id_visitante_origem)
                order by
                    case when id_pessoa_visitante is not null then 1 else 2 end,
                    case when id_pessoa_presidiario is not null then 1 else 2 end,
                    case when dt_evento_referencia is not null then 1 else 2 end,
                    dt_evento_referencia desc,
                    history_date desc,
                    id_visitante_origem desc
            ) as rn
        from gold.tmp_evento_calculado_visita_familiar
    """)

    tabela = "tmp_evento_rank_visita_familiar"

    df_evento_rank_visita_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_rank_visita_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_rank_visita_familiar")


    # ============================================================
    # FATO VISITA FAMILIAR
    # ============================================================

    df_fat_visita_familiar = spark.sql("""
        select
            -- CAMPOS JA EXISTENTES
            id_fato_visita_familiar,
            id_evento_origem,
            id_vinculo_origem,
            id_equipe_origem,
            id_presidio_origem,
            id_pessoa_visitante,
            id_pessoa_presidiario,
            id_preso_presidiario,
            nome_visitante,
            documento_visitante,
            telefone,
            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            dt_evento,
            tipo_visita,
            origem_resolucao_presidiario,
            flag_tem_entrada,
            flag_tem_saida,
            flag_duracao_valida,
            duracao_minutos,

            -- NOVOS CAMPOS
            id_visitante_origem,
            id_interno_origem,
            id_preso_infopen,
            origem_resolucao_visitante,

            nome_visitante_historico,
            documento_visitante_historico,
            history_date,
            history_type,
            history_user_id,

            nome_presidiario_livro,
            nome_presidiario_pessoa,
            documento_presidiario,
            galeria_presidiario_livro,
            cela_presidiario_livro,
            status_presidiario_livro,
            id_presidio_interno,
            id_regime_interno,
            grupo_visita_intima_id,
            grupo_visita_social_id,
            grupo_saida_id,
            situacao_id,
            n_uniforme,

            flag_visitante_resolvido,
            flag_presidiario_resolvido
        from gold.tmp_evento_rank_visita_familiar
        where rn = 1
    """)

    tabela = "sinp_fat_visita_familiar"

    df_fat_visita_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_visita_familiar, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_visita_familiar")

