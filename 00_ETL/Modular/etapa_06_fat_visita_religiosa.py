# -*- coding: utf-8 -*-
"""Fato de visita religiosa."""

from contexto import *

def executar(spark, path=path):
    """Etapa extraída do notebook original."""
    # ===== CELL 24 =====
    import os

    # ============================================================
    # DEFINE BASE DE PESSOAS
    # ============================================================

    tbl_religiosa = spark.sql("show tables in gold like 'df_pessoa_final_religiosa'").count()
    tbl_familiar = spark.sql("show tables in gold like 'df_pessoa_final_familiar'").count()

    if tbl_religiosa > 0:
        tabela_base_pessoas = "gold.df_pessoa_final_religiosa"
    elif tbl_familiar > 0:
        tabela_base_pessoas = "gold.df_pessoa_final_familiar"
    else:
        raise Exception("Nenhuma base de pessoas encontrada: nem gold.df_pessoa_final_religiosa nem gold.df_pessoa_final_familiar.")

    print(f"Base de pessoas utilizada: {tabela_base_pessoas}")

    # ============================================================
    # LIMPEZA DEFENSIVA DAS TABELAS TEMPORARIAS - FATO VISITA RELIGIOSA
    # ============================================================

    spark.sql("drop table if exists gold.tmp_base_controle_visitareligiosa")
    spark.sql("drop table if exists gold.tmp_base_visitareligiosa_evento")
    spark.sql("drop table if exists gold.tmp_base_pessoa_visitareligiosa")
    spark.sql("drop table if exists gold.tmp_base_restricao_visitareligiosa")
    spark.sql("drop table if exists gold.tmp_base_restricao_visitareligiosa_presidio")
    spark.sql("drop table if exists gold.tmp_base_restricao_visitareligiosa_global")
    spark.sql("drop table if exists gold.tmp_evento_base_visita_religiosa")
    spark.sql("drop table if exists gold.tmp_evento_enriquecido_visita_religiosa")
    spark.sql("drop table if exists gold.tmp_evento_calculado_visita_religiosa")
    spark.sql("drop table if exists gold.tmp_evento_rank_visita_religiosa")
    spark.sql("drop table if exists gold.sinp_fat_visita_religiosa")

    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_controle_visitareligiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_visitareligiosa_evento >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_visitareligiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_visitareligiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_visitareligiosa_presidio >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_visitareligiosa_global >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_base_visita_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_enriquecido_visita_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_calculado_visita_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_rank_visita_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}sinp_fat_visita_religiosa >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql(f"refresh table {tabela_base_pessoas}")


    # ============================================================
    # BASE CONTROLE VISITA RELIGIOSA
    # ============================================================

    df_base_controle_visitareligiosa = spark.sql("""
        select
            cast(id as string) as id_evento_origem,
            cast(nome_id as string) as id_visitante_religioso_origem,
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
            end as hr_saida
        from bronze.livros_acesso_unidade_controlevisitareligiosa
    """)

    tabela = "tmp_base_controle_visitareligiosa"

    df_base_controle_visitareligiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_controle_visitareligiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_controle_visitareligiosa")


    # ============================================================
    # BASE VISITA RELIGIOSA EVENTO
    # ============================================================

    df_base_visitareligiosa_evento = spark.sql("""
        select
            cast(id as string) as id_visitante_religioso_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_visitante,
            trim(regexp_replace(coalesce(instituicao, ''), '\\\\s+', ' ')) as instituicao,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento_original,
            cast(presidio_id as string) as presidio_id_cadastro,
            regexp_replace(
                regexp_replace(cast(documento as string), '\\\\.0+$', ''),
                '[^0-9]',
                ''
            ) as documento_digitos,
            case
                when length(
                    regexp_replace(
                        regexp_replace(cast(documento as string), '\\\\.0+$', ''),
                        '[^0-9]',
                        ''
                    )
                ) between 1 and 11 then
                    concat(
                        'CPF_',
                        lpad(
                            regexp_replace(
                                regexp_replace(cast(documento as string), '\\\\.0+$', ''),
                                '[^0-9]',
                                ''
                            ),
                            11,
                            '0'
                        )
                    )
                else
                    concat(
                        'DOC_',
                        upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', ''))
                    )
            end as chave_documento
        from bronze.livros_acesso_unidade_visitareligiosa
    """)

    tabela = "tmp_base_visitareligiosa_evento"

    df_base_visitareligiosa_evento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_visitareligiosa_evento, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_visitareligiosa_evento")


    # ============================================================
    # BASE PESSOA VISITA RELIGIOSA
    # ============================================================

    df_base_pessoa_visitareligiosa = spark.sql(f"""
        select
            id_pessoa as id_pessoa_visitante,
            documento as documento_visitante,
            nome_pessoa as nome_visitante_pessoa,
            case
                when regexp_replace(coalesce(documento, ''), '[^0-9]', '') <> ''
                 and length(regexp_replace(coalesce(documento, ''), '[^0-9]', '')) between 1 and 11 then
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
        from {tabela_base_pessoas}
        where coalesce(flag_visitante, 0) = 1
    """)

    tabela = "tmp_base_pessoa_visitareligiosa"

    df_base_pessoa_visitareligiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_visitareligiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_visitareligiosa")


    # ============================================================
    # BASE RESTRICAO VISITA RELIGIOSA
    # ============================================================

    df_base_restricao_visitareligiosa = spark.sql("""
        select
            cast(visitante_id as string) as id_visitante_religioso_origem,
            cast(presidio_id as string) as id_presidio_origem,
            max(case when coalesce(bloquear_todos_presidios, false) = true then 1 else 0 end) as flag_bloquear_todos_presidios,
            count(*) as qtd_restricoes
        from bronze.livros_acesso_unidade_restricaovisitareligiosa
        group by
            cast(visitante_id as string),
            cast(presidio_id as string)
    """)

    tabela = "tmp_base_restricao_visitareligiosa"

    df_base_restricao_visitareligiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_restricao_visitareligiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_visitareligiosa")


    # ============================================================
    # RESTRICAO POR VISITANTE + PRESIDIO
    # ============================================================

    df_base_restricao_visitareligiosa_presidio = spark.sql("""
        select
            trim(id_visitante_religioso_origem) as id_visitante_religioso_origem,
            trim(id_presidio_origem) as id_presidio_origem,
            max(flag_bloquear_todos_presidios) as flag_bloquear_todos_presidios,
            sum(qtd_restricoes) as qtd_restricoes_presidio
        from gold.tmp_base_restricao_visitareligiosa
        group by
            trim(id_visitante_religioso_origem),
            trim(id_presidio_origem)
    """)

    tabela = "tmp_base_restricao_visitareligiosa_presidio"

    df_base_restricao_visitareligiosa_presidio.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_restricao_visitareligiosa_presidio, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_visitareligiosa_presidio")


    # ============================================================
    # RESTRICAO GLOBAL POR VISITANTE
    # ============================================================

    df_base_restricao_visitareligiosa_global = spark.sql("""
        select
            trim(id_visitante_religioso_origem) as id_visitante_religioso_origem,
            max(flag_bloquear_todos_presidios) as flag_bloquear_todos_presidios_global,
            sum(qtd_restricoes) as qtd_restricoes_global
        from gold.tmp_base_restricao_visitareligiosa
        group by trim(id_visitante_religioso_origem)
    """)

    tabela = "tmp_base_restricao_visitareligiosa_global"

    df_base_restricao_visitareligiosa_global.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_restricao_visitareligiosa_global, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_visitareligiosa_global")


    # ============================================================
    # EVENTO BASE VISITA RELIGIOSA
    # ============================================================

    df_evento_base_visita_religiosa = spark.sql("""
        select
            c.id_evento_origem,
            c.id_visitante_religioso_origem,
            c.id_equipe_origem,
            c.id_presidio_origem,
            c.dt_hr_entrada,
            c.dt_hr_saida,
            c.hr_entrada,
            c.hr_saida,
            c.dt_registro,
            v.nome_visitante,
            v.instituicao,
            v.documento_original,
            v.presidio_id_cadastro,
            v.chave_documento
        from gold.tmp_base_controle_visitareligiosa c
        inner join gold.tmp_base_visitareligiosa_evento v
            on trim(c.id_visitante_religioso_origem) = trim(v.id_visitante_religioso_origem)
    """)

    tabela = "tmp_evento_base_visita_religiosa"

    df_evento_base_visita_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_base_visita_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_base_visita_religiosa")


    # ============================================================
    # EVENTO ENRIQUECIDO VISITA RELIGIOSA
    # ============================================================

    df_evento_enriquecido_visita_religiosa = spark.sql("""
        select
            e.id_evento_origem,
            e.id_visitante_religioso_origem,
            e.id_equipe_origem,
            e.id_presidio_origem,
            p.id_pessoa_visitante,
            coalesce(p.nome_visitante_pessoa, e.nome_visitante) as nome_visitante,
            coalesce(p.documento_visitante, e.documento_original) as documento_visitante,
            e.instituicao,
            e.dt_hr_entrada,
            e.dt_hr_saida,
            e.hr_entrada,
            e.hr_saida,
            e.dt_registro,
            coalesce(e.dt_hr_entrada, e.dt_hr_saida, e.dt_registro) as dt_evento_referencia,
            coalesce(rp.qtd_restricoes_presidio, 0) as qtd_restricoes_presidio,
            coalesce(rg.qtd_restricoes_global, 0) as qtd_restricoes_global,
            coalesce(rp.flag_bloquear_todos_presidios, 0) as flag_bloquear_todos_presidios_presidio,
            coalesce(rg.flag_bloquear_todos_presidios_global, 0) as flag_bloquear_todos_presidios_global
        from gold.tmp_evento_base_visita_religiosa e
        left join gold.tmp_base_pessoa_visitareligiosa p
            on e.chave_documento = p.chave_documento
        left join gold.tmp_base_restricao_visitareligiosa_presidio rp
            on trim(e.id_visitante_religioso_origem) = trim(rp.id_visitante_religioso_origem)
           and trim(e.id_presidio_origem) = trim(rp.id_presidio_origem)
        left join gold.tmp_base_restricao_visitareligiosa_global rg
            on trim(e.id_visitante_religioso_origem) = trim(rg.id_visitante_religioso_origem)
    """)

    tabela = "tmp_evento_enriquecido_visita_religiosa"

    df_evento_enriquecido_visita_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_enriquecido_visita_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_enriquecido_visita_religiosa")


    # ============================================================
    # EVENTO CALCULADO VISITA RELIGIOSA
    # ============================================================

    df_evento_calculado_visita_religiosa = spark.sql("""
        select
            concat(
                'FVR_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(id_evento_origem, ''),
                        coalesce(id_visitante_religioso_origem, ''),
                        coalesce(id_pessoa_visitante, ''),
                        coalesce(cast(dt_evento_referencia as string), '')
                    )
                )
            ) as id_fato_visita_religiosa,
            id_evento_origem,
            id_visitante_religioso_origem,
            id_equipe_origem,
            id_presidio_origem,
            id_pessoa_visitante,
            nome_visitante,
            documento_visitante,
            instituicao,
            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            to_date(dt_evento_referencia) as dt_evento,
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
            case
                when coalesce(qtd_restricoes_presidio, 0) > 0 then 1
                else 0
            end as flag_restricao_mesmo_presidio,
            greatest(
                coalesce(flag_bloquear_todos_presidios_presidio, 0),
                coalesce(flag_bloquear_todos_presidios_global, 0)
            ) as flag_bloquear_todos_presidios,
            case
                when coalesce(qtd_restricoes_presidio, 0) > 0
                  or coalesce(qtd_restricoes_global, 0) > 0
                  or coalesce(flag_bloquear_todos_presidios_presidio, 0) = 1
                  or coalesce(flag_bloquear_todos_presidios_global, 0) = 1
                then 1
                else 0
            end as flag_visita_com_restricao
        from gold.tmp_evento_enriquecido_visita_religiosa
    """)

    tabela = "tmp_evento_calculado_visita_religiosa"

    df_evento_calculado_visita_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_calculado_visita_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_calculado_visita_religiosa")


    # ============================================================
    # EVENTO RANKEADO VISITA RELIGIOSA
    # ============================================================

    df_evento_rank_visita_religiosa = spark.sql("""
        select
            *,
            row_number() over (
                partition by coalesce(id_evento_origem, id_visitante_religioso_origem)
                order by
                    case when id_pessoa_visitante is not null then 1 else 2 end,
                    case when dt_evento_referencia is not null then 1 else 2 end,
                    dt_evento_referencia desc,
                    id_visitante_religioso_origem desc
            ) as rn
        from gold.tmp_evento_calculado_visita_religiosa
    """)

    tabela = "tmp_evento_rank_visita_religiosa"

    df_evento_rank_visita_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_evento_rank_visita_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_rank_visita_religiosa")


    # ============================================================
    # FATO VISITA RELIGIOSA
    # ============================================================

    df_fat_visita_religiosa = spark.sql("""
        select
            id_fato_visita_religiosa,
            id_evento_origem,
            id_visitante_religioso_origem,
            id_equipe_origem,
            id_presidio_origem,
            id_pessoa_visitante,
            nome_visitante,
            documento_visitante,
            instituicao,
            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            dt_evento,
            flag_tem_entrada,
            flag_tem_saida,
            flag_duracao_valida,
            duracao_minutos,
            flag_restricao_mesmo_presidio,
            flag_bloquear_todos_presidios,
            flag_visita_com_restricao
        from gold.tmp_evento_rank_visita_religiosa
        where rn = 1
    """)

    tabela = "sinp_fat_visita_religiosa"

    df_fat_visita_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_visita_religiosa, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_visita_religiosa")


