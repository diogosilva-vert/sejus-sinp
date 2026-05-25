# -*- coding: utf-8 -*-
"""Fato de visita de advogado."""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"
    """Etapa extraída do notebook original."""
    # ===== CELL 21 =====
    # ============================================================
    # LIMPEZA DEFENSIVA DAS TABELAS TEMPORARIAS DA FATO
    # ============================================================

    spark.sql("drop table if exists gold.tmp_base_controle_advogado")
    spark.sql("drop table if exists gold.tmp_base_vinculo_advogado")
    spark.sql("drop table if exists gold.tmp_base_cadastro_advogado")
    spark.sql("drop table if exists gold.tmp_base_interno_livro")
    spark.sql("drop table if exists gold.tmp_base_pessoa_preso_ponte")
    spark.sql("drop table if exists gold.tmp_base_presidiario_resolvido")
    spark.sql("drop table if exists gold.tmp_base_restricao_advogado")
    spark.sql("drop table if exists gold.tmp_base_restricao_advogado_interno")
    spark.sql("drop table if exists gold.tmp_base_restricao_advogado_presidio")
    spark.sql("drop table if exists gold.tmp_base_restricao_advogado_global")
    spark.sql("drop table if exists gold.tmp_base_pessoa_advogado")
    spark.sql("drop table if exists gold.tmp_evento_base_visita_advogado")
    spark.sql("drop table if exists gold.tmp_evento_enriquecido_visita_advogado")
    spark.sql("drop table if exists gold.tmp_evento_calculado_visita_advogado")
    spark.sql("drop table if exists gold.tmp_evento_rank_visita_advogado")
    spark.sql("drop table if exists gold.sinp_fat_visita_advogado")

    import os
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_controle_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_vinculo_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_cadastro_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_interno_livro >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_preso_ponte >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_presidiario_resolvido >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_advogado_interno >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_advogado_presidio >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_restricao_advogado_global >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_base_visita_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_enriquecido_visita_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_calculado_visita_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_evento_rank_visita_advogado >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}sinp_fat_visita_advogado >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_ent_pessoa")
    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")
    spark.sql("refresh table bronze.livros_acesso_unidade_controleadvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalcontroleadvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_vinculavisitaadvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalvinculavisitaadvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_advogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicaladvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_restricaoadvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_historicalrestricaoadvogado")
    spark.sql("refresh table bronze.livros_acesso_unidade_interno")

    spark.sql("refresh table gold.sinp_ent_pessoa")
    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")

    # ============================================================
    # BASE CONTROLE ADVOGADO
    # ============================================================

    df_base_controle_advogado = spark.sql("""
        select
            cast(id as string) as id_evento_origem,
            cast(vinculos_id as string) as id_vinculo_origem,
            cast(presidio_id as string) as id_presidio_controle,
            cast(equipe_id as string) as id_equipe_origem,
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
            trim(regexp_replace(coalesce(autorizacao, ''), '\\\\s+', ' ')) as autorizacao,
            trim(regexp_replace(coalesce(motivo, ''), '\\\\s+', ' ')) as motivo
        from (
        select
            id,
            vinculos_id,
            presidio_id,
            equipe_id,
            hr_entrada,
            hr_saida,
            data_registro,
            autorizacao,
            motivo
        from bronze.livros_acesso_unidade_controleadvogado

        union all

        select
            id,
            vinculos_id,
            presidio_id,
            equipe_id,
            hr_entrada,
            hr_saida,
            data_registro,
            autorizacao,
            motivo
        from bronze.livros_acesso_unidade_historicalcontroleadvogado
    ) controleadvogado
    """)

    tabela = "tmp_base_controle_advogado"

    df_base_controle_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_controle_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_controle_advogado")


    # ============================================================
    # BASE VINCULO ADVOGADO
    # ============================================================

    df_base_vinculo_advogado = spark.sql("""
        select
            cast(id as string) as id_vinculo_origem,
            cast(advogado_id_id as string) as id_advogado_origem,
            cast(interno_id_id as string) as id_interno_origem,
            cast(presidio_id as string) as id_presidio_vinculo,
            coalesce(inativo, false) as inativo,
            coalesce(procuracao, false) as procuracao,
            to_timestamp(data_registro) as dt_registro_vinculo,
            trim(regexp_replace(coalesce(advogado, ''), '\\\\s+', ' ')) as nome_advogado_vinculo,
            trim(regexp_replace(coalesce(interno, ''), '\\\\s+', ' ')) as nome_interno_vinculo,
            trim(regexp_replace(coalesce(arquivo, ''), '\\\\s+', ' ')) as arquivo
        from (
        select
            id,
            advogado_id_id,
            interno_id_id,
            presidio_id,
            inativo,
            procuracao,
            data_registro,
            advogado,
            interno,
            arquivo
        from bronze.livros_acesso_unidade_vinculavisitaadvogado

        union all

        select
            id,
            advogado_id_id,
            interno_id_id,
            presidio_id,
            inativo,
            procuracao,
            data_registro,
            advogado,
            interno,
            arquivo
        from bronze.livros_acesso_unidade_historicalvinculavisitaadvogado
    ) vinculavisitaadvogado
    """)

    tabela = "tmp_base_vinculo_advogado"

    df_base_vinculo_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_vinculo_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_vinculo_advogado")


    # ============================================================
    # BASE CADASTRO ADVOGADO
    # ============================================================

    df_base_cadastro_advogado = spark.sql("""
        select
            cast(id as string) as id_advogado_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_advogado_cadastro,
            upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' '))) as estado_oab,
            upper(trim(regexp_replace(coalesce(oab, ''), '\\\\s+', ' '))) as oab_bruta,
            upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', '')) as oab_normalizada,
            concat(
                'OAB_',
                upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' '))),
                '_',
                upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', ''))
            ) as id_pessoa_advogado_chave,
            concat(
                upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', '')),
                '/',
                upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' ')))
            ) as documento_advogado_oab
        from (
        select
            id,
            nome,
            estado,
            oab
        from bronze.livros_acesso_unidade_advogado

        union all

        select
            id,
            nome,
            estado,
            oab
        from bronze.livros_acesso_unidade_historicaladvogado
    ) advogado
    """)

    tabela = "tmp_base_cadastro_advogado"

    df_base_cadastro_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_cadastro_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_cadastro_advogado")


    # ============================================================
    # BASE INTERNO LIVRO
    # ============================================================

    df_base_interno_livro = spark.sql("""
        select
            cast(id as string) as id_interno_origem,
            cast(infopen as string) as id_preso_infopen
        from bronze.livros_acesso_unidade_interno
        where infopen is not null
    """)

    tabela = "tmp_base_interno_livro"

    df_base_interno_livro.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_interno_livro, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_interno_livro")


    # ============================================================
    # BASE PESSOA PRESO PONTE
    # ============================================================

    df_base_pessoa_preso_ponte = spark.sql("""
        select distinct
            cast(id_preso as string) as id_preso_infopen,
            cast(id_preso as string) as id_preso_presidiario,
            id_pessoa as id_pessoa_presidiario
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
    """)

    tabela = "tmp_base_pessoa_preso_ponte"

    df_base_pessoa_preso_ponte.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_pessoa_preso_ponte, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_preso_ponte")


    # ============================================================
    # BASE PRESIDIARIO RESOLVIDO
    # ============================================================

    df_base_presidiario_resolvido = spark.sql("""
        select
            trim(i.id_interno_origem) as id_interno_origem,
            trim(i.id_preso_infopen) as id_preso_infopen,
            trim(p.id_preso_presidiario) as id_preso_presidiario,
            p.id_pessoa_presidiario,
            e.documento as documento_presidiario,
            e.nome_pessoa as nome_presidiario_pessoa
        from gold.tmp_base_interno_livro i
        inner join gold.tmp_base_pessoa_preso_ponte p
            on trim(i.id_preso_infopen) = trim(p.id_preso_infopen)
        left join gold.sinp_ent_pessoa e
            on p.id_pessoa_presidiario = e.id_pessoa
    """)

    tabela = "tmp_base_presidiario_resolvido"

    df_base_presidiario_resolvido.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_presidiario_resolvido, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_presidiario_resolvido")


    # ============================================================
    # BASE RESTRICAO ADVOGADO
    # ============================================================

    df_base_restricao_advogado = spark.sql("""
        select
            cast(advogado_id as string) as id_advogado_origem,
            cast(interno_id as string) as id_interno_origem,
            cast(presidio_id as string) as id_presidio_origem,
            max(case when coalesce(bloquear_todos_internos, false) = true then 1 else 0 end) as flag_bloqueia_todos_internos,
            max(case when coalesce(bloquear_todos_presidios, false) = true then 1 else 0 end) as flag_bloqueia_todos_presidios,
            count(*) as qtd_restricoes
        from (
        select
            advogado_id,
            interno_id,
            presidio_id,
            bloquear_todos_internos,
            bloquear_todos_presidios
        from bronze.livros_acesso_unidade_restricaoadvogado

        union all

        select
            advogado_id,
            interno_id,
            presidio_id,
            bloquear_todos_internos,
            bloquear_todos_presidios
        from bronze.livros_acesso_unidade_historicalrestricaoadvogado
    ) restricaoadvogado
        group by
            cast(advogado_id as string),
            cast(interno_id as string),
            cast(presidio_id as string)
    """)

    tabela = "tmp_base_restricao_advogado"

    df_base_restricao_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_restricao_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_advogado")


    # ============================================================
    # RESTRICAO POR ADVOGADO + INTERNO
    # ============================================================

    df_base_restricao_advogado_interno = spark.sql("""
        select
            trim(id_advogado_origem) as id_advogado_origem,
            trim(id_interno_origem) as id_interno_origem,
            max(flag_bloqueia_todos_internos) as flag_bloqueia_todos_internos_interno,
            max(flag_bloqueia_todos_presidios) as flag_bloqueia_todos_presidios_interno,
            sum(qtd_restricoes) as qtd_restricoes_interno
        from gold.tmp_base_restricao_advogado
        group by
            trim(id_advogado_origem),
            trim(id_interno_origem)
    """)

    tabela = "tmp_base_restricao_advogado_interno"

    df_base_restricao_advogado_interno.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_restricao_advogado_interno, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_advogado_interno")


    # ============================================================
    # RESTRICAO POR ADVOGADO + PRESIDIO
    # ============================================================

    df_base_restricao_advogado_presidio = spark.sql("""
        select
            trim(id_advogado_origem) as id_advogado_origem,
            trim(id_presidio_origem) as id_presidio_origem,
            max(flag_bloqueia_todos_internos) as flag_bloqueia_todos_internos_presidio,
            max(flag_bloqueia_todos_presidios) as flag_bloqueia_todos_presidios_presidio,
            sum(qtd_restricoes) as qtd_restricoes_presidio
        from gold.tmp_base_restricao_advogado
        group by
            trim(id_advogado_origem),
            trim(id_presidio_origem)
    """)

    tabela = "tmp_base_restricao_advogado_presidio"

    df_base_restricao_advogado_presidio.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_restricao_advogado_presidio, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_advogado_presidio")


    # ============================================================
    # RESTRICAO GLOBAL POR ADVOGADO
    # ============================================================

    df_base_restricao_advogado_global = spark.sql("""
        select
            trim(id_advogado_origem) as id_advogado_origem,
            max(flag_bloqueia_todos_internos) as flag_bloqueia_todos_internos_global,
            max(flag_bloqueia_todos_presidios) as flag_bloqueia_todos_presidios_global,
            sum(qtd_restricoes) as qtd_restricoes_global
        from gold.tmp_base_restricao_advogado
        group by trim(id_advogado_origem)
    """)

    tabela = "tmp_base_restricao_advogado_global"

    df_base_restricao_advogado_global.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_restricao_advogado_global, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_restricao_advogado_global")


    # ============================================================
    # BASE PESSOA ADVOGADO
    # ============================================================

    df_base_pessoa_advogado = spark.sql("""
        select distinct
            id_preso as id_preso_advogado,
            id_pessoa as id_pessoa_advogado,
            documento as documento_advogado,
            nome_pessoa as nome_advogado_pessoa
        from gold.sinp_ent_pessoa
        where coalesce(flag_advogado, 0) = 1
          and id_preso is not null
          and id_preso like 'OAB_%'
    """)

    tabela = "tmp_base_pessoa_advogado"

    df_base_pessoa_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_pessoa_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_advogado")


    # ============================================================
    # EVENTO BASE VISITA ADVOGADO
    # ============================================================

    df_evento_base_visita_advogado = spark.sql("""
        select
            v.id_vinculo_origem as id_vinculo_origem_raw,
            v.id_vinculo_origem as id_vinculo_origem,
            c.id_evento_origem,
            c.id_presidio_controle,
            c.id_equipe_origem,
            c.dt_hr_entrada,
            c.dt_hr_saida,
            c.dt_registro,
            c.hr_entrada,
            c.hr_saida,
            c.autorizacao,
            c.motivo,
            v.id_advogado_origem,
            v.id_interno_origem,
            v.id_presidio_vinculo,
            v.inativo,
            v.procuracao,
            v.dt_registro_vinculo,
            v.nome_advogado_vinculo,
            v.nome_interno_vinculo,
            v.arquivo,
            pr.id_preso_presidiario,
            pr.id_pessoa_presidiario,
            pr.documento_presidiario,
            pr.nome_presidiario_pessoa
        from gold.tmp_base_vinculo_advogado v
        left join gold.tmp_base_controle_advogado c
            on trim(v.id_vinculo_origem) = trim(c.id_vinculo_origem)
        left join gold.tmp_base_presidiario_resolvido pr
            on trim(v.id_interno_origem) = trim(pr.id_interno_origem)
    """)

    tabela = "tmp_evento_base_visita_advogado"

    df_evento_base_visita_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_evento_base_visita_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_base_visita_advogado")


    # ============================================================
    # EVENTO ENRIQUECIDO VISITA ADVOGADO
    # ============================================================

    df_evento_enriquecido_visita_advogado = spark.sql("""
        select
            e.id_evento_origem,
            e.id_vinculo_origem,
            e.id_advogado_origem,
            e.id_interno_origem,
            e.id_preso_presidiario,
            e.id_presidio_controle,
            e.id_presidio_vinculo,
            e.id_equipe_origem,
            cad.id_pessoa_advogado_chave,
            pa.id_pessoa_advogado,
            e.id_pessoa_presidiario,
            cad.documento_advogado_oab,
            coalesce(pa.documento_advogado, cad.documento_advogado_oab) as documento_advogado,
            coalesce(pa.nome_advogado_pessoa, cad.nome_advogado_cadastro, e.nome_advogado_vinculo) as nome_advogado,
            e.documento_presidiario,
            coalesce(e.nome_presidiario_pessoa, e.nome_interno_vinculo) as nome_presidiario,
            e.dt_hr_entrada,
            e.dt_hr_saida,
            e.hr_entrada,
            e.hr_saida,
            e.dt_registro,
            coalesce(e.dt_hr_entrada, e.dt_hr_saida, e.dt_registro, e.dt_registro_vinculo) as dt_evento_referencia,
            e.autorizacao,
            e.motivo,
            e.inativo,
            e.procuracao,
            e.dt_registro_vinculo,
            e.arquivo,
            case
                when e.id_pessoa_presidiario is not null then 'INTERNO_INFOOPEN_PONTE'
                else 'NAO_RESOLVIDO'
            end as origem_resolucao_presidiario,
            coalesce(ri.qtd_restricoes_interno, 0) as qtd_restricoes_interno,
            coalesce(rp.qtd_restricoes_presidio, 0) as qtd_restricoes_presidio,
            coalesce(rg.qtd_restricoes_global, 0) as qtd_restricoes_global,
            coalesce(ri.flag_bloqueia_todos_internos_interno, 0) as flag_bloqueia_todos_internos_interno,
            coalesce(ri.flag_bloqueia_todos_presidios_interno, 0) as flag_bloqueia_todos_presidios_interno,
            coalesce(rp.flag_bloqueia_todos_internos_presidio, 0) as flag_bloqueia_todos_internos_presidio,
            coalesce(rp.flag_bloqueia_todos_presidios_presidio, 0) as flag_bloqueia_todos_presidios_presidio,
            coalesce(rg.flag_bloqueia_todos_internos_global, 0) as flag_bloqueia_todos_internos_global,
            coalesce(rg.flag_bloqueia_todos_presidios_global, 0) as flag_bloqueia_todos_presidios_global
        from gold.tmp_evento_base_visita_advogado e
        left join gold.tmp_base_cadastro_advogado cad
            on trim(e.id_advogado_origem) = trim(cad.id_advogado_origem)
        left join gold.tmp_base_pessoa_advogado pa
            on cad.id_pessoa_advogado_chave = pa.id_preso_advogado
        left join gold.tmp_base_restricao_advogado_interno ri
            on trim(e.id_advogado_origem) = trim(ri.id_advogado_origem)
           and trim(e.id_interno_origem) = trim(ri.id_interno_origem)
        left join gold.tmp_base_restricao_advogado_presidio rp
            on trim(e.id_advogado_origem) = trim(rp.id_advogado_origem)
           and trim(coalesce(e.id_presidio_controle, e.id_presidio_vinculo)) = trim(rp.id_presidio_origem)
        left join gold.tmp_base_restricao_advogado_global rg
            on trim(e.id_advogado_origem) = trim(rg.id_advogado_origem)
    """)

    tabela = "tmp_evento_enriquecido_visita_advogado"

    df_evento_enriquecido_visita_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_evento_enriquecido_visita_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_enriquecido_visita_advogado")


    # ============================================================
    # EVENTO CALCULADO VISITA ADVOGADO
    # ============================================================

    df_evento_calculado_visita_advogado = spark.sql("""
        select
            concat(
                'FVA_',
                md5(
                    concat_ws(
                        '|',
                        coalesce(id_evento_origem, ''),
                        coalesce(id_vinculo_origem, ''),
                        coalesce(id_advogado_origem, ''),
                        coalesce(id_interno_origem, ''),
                        coalesce(id_pessoa_presidiario, '')
                    )
                )
            ) as id_fato_visita_advogado,
            id_evento_origem,
            id_vinculo_origem,
            id_advogado_origem,
            id_interno_origem,
            id_preso_presidiario,
            coalesce(id_presidio_controle, id_presidio_vinculo) as id_presidio_origem,
            id_equipe_origem,
            id_pessoa_advogado,
            id_pessoa_presidiario,
            documento_advogado,
            nome_advogado,
            documento_presidiario,
            nome_presidiario,
            origem_resolucao_presidiario,
            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            to_date(dt_evento_referencia) as dt_evento,
            autorizacao,
            motivo,
            coalesce(inativo, false) as inativo,
            coalesce(procuracao, false) as procuracao,
            dt_registro_vinculo,
            arquivo,
            case when dt_hr_entrada is not null then 1 else 0 end as flag_tem_entrada,
            case when dt_hr_saida is not null then 1 else 0 end as flag_tem_saida,
            case
                when dt_hr_entrada is not null and dt_hr_saida is not null and dt_hr_saida >= dt_hr_entrada then 1
                else 0
            end as flag_duracao_valida,
            case
                when coalesce(inativo, false) = false then 1
                else 0
            end as flag_vinculo_ativo,
            case
                when coalesce(procuracao, false) = true then 1
                else 0
            end as flag_procuracao,
            case
                when coalesce(autorizacao, '') <> '' then 1
                else 0
            end as flag_autorizacao_preenchida,
            case
                when coalesce(motivo, '') <> '' then 1
                else 0
            end as flag_motivo_preenchido,
            case
                when coalesce(qtd_restricoes_interno, 0) > 0 then 1
                else 0
            end as flag_restricao_mesmo_interno,
            case
                when coalesce(qtd_restricoes_presidio, 0) > 0 then 1
                else 0
            end as flag_restricao_mesmo_presidio,
            greatest(
                coalesce(flag_bloqueia_todos_internos_interno, 0),
                coalesce(flag_bloqueia_todos_internos_presidio, 0),
                coalesce(flag_bloqueia_todos_internos_global, 0)
            ) as flag_bloqueia_todos_internos,
            greatest(
                coalesce(flag_bloqueia_todos_presidios_interno, 0),
                coalesce(flag_bloqueia_todos_presidios_presidio, 0),
                coalesce(flag_bloqueia_todos_presidios_global, 0)
            ) as flag_bloqueia_todos_presidios,
            case
                when coalesce(qtd_restricoes_interno, 0) > 0
                  or coalesce(qtd_restricoes_presidio, 0) > 0
                  or coalesce(qtd_restricoes_global, 0) > 0
                  or coalesce(flag_bloqueia_todos_internos_interno, 0) = 1
                  or coalesce(flag_bloqueia_todos_presidios_interno, 0) = 1
                  or coalesce(flag_bloqueia_todos_internos_presidio, 0) = 1
                  or coalesce(flag_bloqueia_todos_presidios_presidio, 0) = 1
                  or coalesce(flag_bloqueia_todos_internos_global, 0) = 1
                  or coalesce(flag_bloqueia_todos_presidios_global, 0) = 1
                then 1
                else 0
            end as flag_visita_com_restricao,
            case
                when dt_hr_entrada is not null and dt_hr_saida is not null and dt_hr_saida >= dt_hr_entrada
                then cast((unix_timestamp(dt_hr_saida) - unix_timestamp(dt_hr_entrada)) / 60.0 as decimal(18,2))
                else cast(null as decimal(18,2))
            end as duracao_minutos
        from gold.tmp_evento_enriquecido_visita_advogado
    """)

    tabela = "tmp_evento_calculado_visita_advogado"

    df_evento_calculado_visita_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_evento_calculado_visita_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_calculado_visita_advogado")


    # ============================================================
    # EVENTO RANKEADO VISITA ADVOGADO
    # ============================================================

    df_evento_rank_visita_advogado = spark.sql("""
        select
            *,
            row_number() over (
                partition by coalesce(id_evento_origem, id_vinculo_origem)
                order by
                    case when id_pessoa_advogado is not null then 1 else 2 end,
                    case when id_pessoa_presidiario is not null then 1 else 2 end,
                    case when id_preso_presidiario is not null then 1 else 2 end,
                    case when dt_evento_referencia is not null then 1 else 2 end,
                    dt_evento_referencia desc,
                    id_vinculo_origem desc
            ) as rn
        from gold.tmp_evento_calculado_visita_advogado
    """)

    tabela = "tmp_evento_rank_visita_advogado"

    df_evento_rank_visita_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_evento_rank_visita_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_evento_rank_visita_advogado")


    # ============================================================
    # FATO VISITA ADVOGADO
    # ============================================================

    df_fat_visita_advogado = spark.sql("""
        select
            id_fato_visita_advogado,
            id_evento_origem,
            id_vinculo_origem,
            id_advogado_origem,
            id_interno_origem,
            id_preso_presidiario,
            id_presidio_origem,
            id_equipe_origem,
            id_pessoa_advogado,
            id_pessoa_presidiario,
            documento_advogado,
            nome_advogado,
            documento_presidiario,
            nome_presidiario,
            origem_resolucao_presidiario,
            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            dt_evento,
            autorizacao,
            motivo,
            inativo,
            procuracao,
            dt_registro_vinculo,
            arquivo,
            flag_tem_entrada,
            flag_tem_saida,
            flag_duracao_valida,
            flag_vinculo_ativo,
            flag_procuracao,
            flag_autorizacao_preenchida,
            flag_motivo_preenchido,
            flag_restricao_mesmo_interno,
            flag_restricao_mesmo_presidio,
            flag_bloqueia_todos_internos,
            flag_bloqueia_todos_presidios,
            flag_visita_com_restricao,
            duracao_minutos
        from gold.tmp_evento_rank_visita_advogado
        where rn = 1
    """)

    tabela = "sinp_fat_visita_advogado"

    df_fat_visita_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_fat_visita_advogado, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_fato_visita_advogado")
