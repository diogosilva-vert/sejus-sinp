# -*- coding: utf-8 -*-
"""Fato de visita religiosa.

Melhorias aplicadas:
- usa gold.sinp_ent_pessoa como base oficial de pessoas;
- usa bronze.livros_controle_visita_religiosa como controle principal;
- preserva eventos sem nome_id/cadastro de visitante;
- resolve visitante por documento normalizado;
- mantém id_preso_visitado/id_pessoa_visitado nulos com motivo explícito, pois a origem não possui preso/interno visitado;
- adiciona campos de qualidade, documento, cadastro, instituição, religião inferida e restrição.
"""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

    import os

    # ============================================================
    # REFRESH DAS ORIGENS
    # ============================================================

    spark.sql("refresh table gold.sinp_ent_pessoa")
    spark.sql("refresh table bronze.livros_controle_visita_religiosa")
    spark.sql("refresh table bronze.livros_acesso_unidade_visitareligiosa")
    spark.sql("refresh table bronze.livros_acesso_unidade_restricaovisitareligiosa")

    spark.catalog.clearCache()


    # ============================================================
    # LIMPEZA DEFENSIVA DAS TABELAS TEMPORARIAS - FATO VISITA RELIGIOSA
    # ============================================================

    temporarias = [
        "tmp_base_controle_visitareligiosa",
        "tmp_base_visitareligiosa_evento",
        "tmp_base_pessoa_visitareligiosa",
        "tmp_base_restricao_visitareligiosa",
        "tmp_base_restricao_visitareligiosa_presidio",
        "tmp_base_restricao_visitareligiosa_global",
        "tmp_evento_base_visita_religiosa",
        "tmp_evento_enriquecido_visita_religiosa",
        "tmp_evento_calculado_visita_religiosa",
        "tmp_evento_rank_visita_religiosa",
    ]

    for tabela_tmp in temporarias:
        spark.sql(f"drop table if exists gold.{tabela_tmp}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela_tmp} >/dev/null 2>&1")

    spark.sql("drop table if exists gold.sinp_fat_visita_religiosa")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}sinp_fat_visita_religiosa >/dev/null 2>&1")

    spark.catalog.clearCache()


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
            end as hr_saida,

            case when nome_id is null then 1 else 0 end as flag_controle_sem_visitante,
            case when equipe_id is null then 1 else 0 end as flag_controle_sem_equipe,
            case when presidio_id is null then 1 else 0 end as flag_controle_sem_presidio

        from bronze.livros_controle_visita_religiosa
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
    # BASE CADASTRO VISITANTE RELIGIOSO
    # ============================================================

    df_base_visitareligiosa_evento = spark.sql("""
        select
            id_visitante_religioso_origem,
            nome_visitante,
            instituicao,
            instituicao_normalizada,
            categoria_instituicao,
            religiao_inferida_instituicao,
            documento_original,
            documento_digitos,
            cpf_normalizado,
            documento_normalizado,
            tipo_documento_visitante,
            chave_documento,
            presidio_id_cadastro,
            flag_documento_informado,
            flag_documento_cpf_possivel,
            flag_documento_zerado,
            flag_nome_visitante_informado,
            flag_instituicao_informada
        from (
            select
                cast(id as string) as id_visitante_religioso_origem,

                trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_visitante,
                trim(regexp_replace(coalesce(instituicao, ''), '\\\\s+', ' ')) as instituicao,

                upper(
                    regexp_replace(
                        trim(regexp_replace(coalesce(instituicao, ''), '\\\\s+', ' ')),
                        '\\\\s+',
                        ' '
                    )
                ) as instituicao_normalizada,

                case
                    when lower(coalesce(instituicao, '')) rlike 'batista' then 'BATISTA'
                    when lower(coalesce(instituicao, '')) rlike 'cat[oó]lic' then 'CATOLICA'
                    when lower(coalesce(instituicao, '')) rlike 'assembleia|assembl[ée]ia' then 'ASSEMBLEIA_DE_DEUS'
                    when lower(coalesce(instituicao, '')) rlike 'universal' then 'UNIVERSAL'
                    when lower(coalesce(instituicao, '')) rlike 'adventista' then 'ADVENTISTA'
                    when lower(coalesce(instituicao, '')) rlike 'evang[eé]lic|evangelic' then 'EVANGELICA'
                    when lower(coalesce(instituicao, '')) rlike 'esp[ií]rita|kardec' then 'ESPIRITA'
                    when lower(coalesce(instituicao, '')) rlike 'testemunha.*jeov|jeov[aá]' then 'TESTEMUNHA_DE_JEOVA'
                    when lower(coalesce(instituicao, '')) rlike 'umbanda|candombl|matriz africana' then 'MATRIZ_AFRICANA'
                    when trim(coalesce(instituicao, '')) = '' then 'NAO_INFORMADA'
                    else 'OUTRA'
                end as categoria_instituicao,

                case
                    when lower(coalesce(instituicao, '')) rlike 'batista|assembleia|assembl[ée]ia|universal|adventista|evang[eé]lic|evangelic' then 'Evangélico (a)'
                    when lower(coalesce(instituicao, '')) rlike 'cat[oó]lic' then 'Católico (a)'
                    when lower(coalesce(instituicao, '')) rlike 'esp[ií]rita|kardec' then 'Espírita'
                    when lower(coalesce(instituicao, '')) rlike 'umbanda|candombl|matriz africana' then 'MATRIZ AFRICANA'
                    when lower(coalesce(instituicao, '')) rlike 'jeov[aá]' then 'JUDAISMO'
                    else cast(null as string)
                end as religiao_inferida_instituicao,

                trim(regexp_replace(coalesce(cast(documento as string), ''), '\\\\s+', ' ')) as documento_original,

                regexp_replace(
                    regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''),
                    '[^0-9]',
                    ''
                ) as documento_digitos,

                lpad(
                    regexp_replace(
                        regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''),
                        '[^0-9]',
                        ''
                    ),
                    11,
                    '0'
                ) as cpf_normalizado,

                upper(regexp_replace(coalesce(cast(documento as string), ''), '[^0-9A-Za-z]', '')) as documento_normalizado,

                case
                    when regexp_replace(regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''), '[^0-9]', '') <> ''
                     and length(regexp_replace(regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''), '[^0-9]', '')) between 1 and 11
                        then 'CPF'
                    when upper(regexp_replace(coalesce(cast(documento as string), ''), '[^0-9A-Za-z]', '')) <> ''
                        then 'DOCUMENTO'
                    else 'SEM_DOCUMENTO'
                end as tipo_documento_visitante,

                case
                    when regexp_replace(regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''), '[^0-9]', '') <> ''
                     and length(regexp_replace(regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''), '[^0-9]', '')) between 1 and 11
                        then concat(
                            'CPF_',
                            lpad(
                                regexp_replace(
                                    regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''),
                                    '[^0-9]',
                                    ''
                                ),
                                11,
                                '0'
                            )
                        )
                    else concat(
                        'DOC_',
                        upper(regexp_replace(coalesce(cast(documento as string), ''), '[^0-9A-Za-z]', ''))
                    )
                end as chave_documento,

                cast(presidio_id as string) as presidio_id_cadastro,

                case
                    when trim(coalesce(cast(documento as string), '')) <> '' then 1
                    else 0
                end as flag_documento_informado,

                case
                    when regexp_replace(regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''), '[^0-9]', '') <> ''
                     and length(regexp_replace(regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''), '[^0-9]', '')) between 1 and 11
                        then 1
                    else 0
                end as flag_documento_cpf_possivel,

                case
                    when lpad(
                        regexp_replace(
                            regexp_replace(coalesce(cast(documento as string), ''), '\\\\.0+$', ''),
                            '[^0-9]',
                            ''
                        ),
                        11,
                        '0'
                    ) = '00000000000' then 1
                    else 0
                end as flag_documento_zerado,

                case when trim(coalesce(nome, '')) <> '' then 1 else 0 end as flag_nome_visitante_informado,
                case when trim(coalesce(instituicao, '')) <> '' then 1 else 0 end as flag_instituicao_informada,

                row_number() over (
                    partition by cast(id as string)
                    order by
                        case when trim(coalesce(nome, '')) <> '' then 1 else 2 end,
                        case when trim(coalesce(instituicao, '')) <> '' then 1 else 2 end,
                        case when trim(coalesce(cast(documento as string), '')) <> '' then 1 else 2 end,
                        cast(presidio_id as string) desc
                ) as rn

            from bronze.livros_acesso_unidade_visitareligiosa
        ) x
        where rn = 1
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

    df_base_pessoa_visitareligiosa = spark.sql("""
        select distinct
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

        from gold.sinp_ent_pessoa
        where coalesce(flag_visitante, 0) = 1
          and documento is not null
          and trim(documento) <> ''
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
            max(flag_bloquear_todos_presidios) as flag_bloquear_todos_presidios_presidio,
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
    # LEFT JOIN PARA PRESERVAR CONTROLES SEM CADASTRO/NOME_ID
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

            c.flag_controle_sem_visitante,
            c.flag_controle_sem_equipe,
            c.flag_controle_sem_presidio,

            v.nome_visitante,
            v.instituicao,
            v.instituicao_normalizada,
            v.categoria_instituicao,
            v.religiao_inferida_instituicao,
            v.documento_original,
            v.documento_digitos,
            v.cpf_normalizado,
            v.documento_normalizado,
            v.tipo_documento_visitante,
            v.chave_documento,
            v.presidio_id_cadastro,
            v.flag_documento_informado,
            v.flag_documento_cpf_possivel,
            v.flag_documento_zerado,
            v.flag_nome_visitante_informado,
            v.flag_instituicao_informada,

            case
                when v.id_visitante_religioso_origem is null then 1
                else 0
            end as flag_visitante_sem_cadastro,

            case
                when c.id_presidio_origem is not null
                 and v.presidio_id_cadastro is not null
                 and trim(c.id_presidio_origem) <> trim(v.presidio_id_cadastro) then 1
                else 0
            end as flag_presidio_evento_diferente_cadastro

        from gold.tmp_base_controle_visitareligiosa c

        left join gold.tmp_base_visitareligiosa_evento v
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

            cast(null as string) as id_preso_visitado,
            cast(null as string) as id_pessoa_visitado,

            case
                when trim(coalesce(p.nome_visitante_pessoa, '')) <> ''
                    then trim(p.nome_visitante_pessoa)
                when trim(coalesce(e.nome_visitante, '')) <> ''
                    then trim(e.nome_visitante)
                else cast(null as string)
            end as nome_visitante,

            case
                when trim(coalesce(p.documento_visitante, '')) <> ''
                    then trim(p.documento_visitante)
                when trim(coalesce(e.documento_original, '')) <> ''
                    then trim(e.documento_original)
                else cast(null as string)
            end as documento_visitante,

            e.documento_original as documento_visitante_cadastro,
            e.documento_digitos,
            e.cpf_normalizado,
            e.documento_normalizado,
            e.tipo_documento_visitante,
            e.chave_documento,

            e.instituicao,
            e.instituicao_normalizada,
            e.categoria_instituicao,
            e.religiao_inferida_instituicao,
            e.presidio_id_cadastro,

            e.dt_hr_entrada,
            e.dt_hr_saida,
            e.hr_entrada,
            e.hr_saida,
            e.dt_registro,
            coalesce(e.dt_hr_entrada, e.dt_hr_saida, e.dt_registro) as dt_evento_referencia,

            case
                when e.id_visitante_religioso_origem is null then 'SEM_ID_VISITANTE_NO_CONTROLE'
                when e.flag_visitante_sem_cadastro = 1 then 'VISITANTE_SEM_CADASTRO'
                when p.id_pessoa_visitante is not null then 'VISITANTE_RESOLVIDO_POR_DOCUMENTO'
                when trim(coalesce(e.chave_documento, '')) <> ''
                 and e.chave_documento not in ('DOC_', 'CPF_00000000000') then 'VISITANTE_COM_DOCUMENTO_NAO_RESOLVIDO'
                else 'VISITANTE_SEM_DOCUMENTO_VALIDO'
            end as origem_resolucao_visitante,

            'SEM_VINCULO_COM_PRESO_NA_ORIGEM' as origem_resolucao_visitado,

            e.flag_controle_sem_visitante,
            e.flag_controle_sem_equipe,
            e.flag_controle_sem_presidio,
            e.flag_visitante_sem_cadastro,
            e.flag_presidio_evento_diferente_cadastro,
            coalesce(e.flag_documento_informado, 0) as flag_documento_informado,
            coalesce(e.flag_documento_cpf_possivel, 0) as flag_documento_cpf_possivel,
            coalesce(e.flag_documento_zerado, 0) as flag_documento_zerado,
            coalesce(e.flag_nome_visitante_informado, 0) as flag_nome_visitante_informado,
            coalesce(e.flag_instituicao_informada, 0) as flag_instituicao_informada,

            coalesce(rp.qtd_restricoes_presidio, 0) as qtd_restricoes_presidio,
            coalesce(rg.qtd_restricoes_global, 0) as qtd_restricoes_global,
            coalesce(rp.flag_bloquear_todos_presidios_presidio, 0) as flag_bloquear_todos_presidios_presidio,
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
                        coalesce(id_presidio_origem, ''),
                        coalesce(cast(dt_evento_referencia as string), '')
                    )
                )
            ) as id_fato_visita_religiosa,

            id_evento_origem,
            id_visitante_religioso_origem,
            id_equipe_origem,
            id_presidio_origem,

            id_pessoa_visitante,
            id_preso_visitado,
            id_pessoa_visitado,

            nome_visitante,
            documento_visitante,
            documento_visitante_cadastro,
            documento_digitos,
            cpf_normalizado,
            documento_normalizado,
            tipo_documento_visitante,
            chave_documento,

            instituicao,
            instituicao_normalizada,
            categoria_instituicao,
            religiao_inferida_instituicao,
            presidio_id_cadastro,

            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            to_date(dt_evento_referencia) as dt_evento,
            year(to_date(dt_evento_referencia)) as ano_evento,
            month(to_date(dt_evento_referencia)) as mes_evento,
            dayofweek(to_date(dt_evento_referencia)) as dia_semana_evento,

            origem_resolucao_visitante,
            origem_resolucao_visitado,

            case when id_pessoa_visitante is not null then 1 else 0 end as flag_visitante_resolvido,

            flag_controle_sem_visitante,
            flag_controle_sem_equipe,
            flag_controle_sem_presidio,
            flag_visitante_sem_cadastro,
            flag_presidio_evento_diferente_cadastro,
            flag_documento_informado,
            flag_documento_cpf_possivel,
            flag_documento_zerado,
            flag_nome_visitante_informado,
            flag_instituicao_informada,

            case when dt_hr_entrada is not null then 1 else 0 end as flag_tem_entrada,
            case when dt_hr_saida is not null then 1 else 0 end as flag_tem_saida,

            case
                when dt_hr_entrada is not null and dt_hr_saida is null then 1
                else 0
            end as flag_visita_em_aberto,

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
            end as flag_visita_com_restricao,

            qtd_restricoes_presidio,
            qtd_restricoes_global

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
                partition by id_evento_origem
                order by
                    case when id_pessoa_visitante is not null then 1 else 2 end,
                    case when nome_visitante is not null then 1 else 2 end,
                    case when documento_visitante is not null then 1 else 2 end,
                    case when instituicao is not null then 1 else 2 end,
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
            id_preso_visitado,
            id_pessoa_visitado,

            nome_visitante,
            documento_visitante,
            documento_visitante_cadastro,
            documento_digitos,
            cpf_normalizado,
            documento_normalizado,
            tipo_documento_visitante,
            chave_documento,

            instituicao,
            instituicao_normalizada,
            categoria_instituicao,
            religiao_inferida_instituicao,
            presidio_id_cadastro,

            dt_hr_entrada,
            dt_hr_saida,
            hr_entrada,
            hr_saida,
            dt_registro,
            dt_evento_referencia,
            dt_evento,
            ano_evento,
            mes_evento,
            dia_semana_evento,

            origem_resolucao_visitante,
            origem_resolucao_visitado,

            flag_visitante_resolvido,
            flag_controle_sem_visitante,
            flag_controle_sem_equipe,
            flag_controle_sem_presidio,
            flag_visitante_sem_cadastro,
            flag_presidio_evento_diferente_cadastro,
            flag_documento_informado,
            flag_documento_cpf_possivel,
            flag_documento_zerado,
            flag_nome_visitante_informado,
            flag_instituicao_informada,
            flag_tem_entrada,
            flag_tem_saida,
            flag_visita_em_aberto,
            flag_duracao_valida,
            duracao_minutos,

            flag_restricao_mesmo_presidio,
            flag_bloquear_todos_presidios,
            flag_visita_com_restricao,
            qtd_restricoes_presidio,
            qtd_restricoes_global

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
