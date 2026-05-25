# -*- coding: utf-8 -*-
"""
Entidade pessoa e bases auxiliares de advogado, familiar, religiosa, SIARHES e CAVI.

Refatoração:
- script puro
- sem def
- sem helper novo
- sem orquestração paralela
- sem tmp_pg
- sem WITH gigante
- materialização física em gold.tmp_*
- hash final por UPDATE
- envio ao Postgres somente no final
"""

from contexto import *

def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"
    """Etapa extraída do notebook original."""


    # ============================================================
    # 00 - REFRESH DAS ORIGENS
    # ============================================================

    tabelas = [
        "bronze.infopen_preso_documentos",
        "bronze.infopen_documentos_tipos",
        "bronze.infopen_presos",
        "bronze.infopen_preso_filiacao",
        "bronze.infopen_preso_cor_pele_etnia",
        "bronze.infopen_cor_pele_etnia",
        "bronze.infopen_preso_alcunha",
        "bronze.infopen_social_estado_civil",
        "bronze.infopen_estado_civil",
        "bronze.infopen_social_escolaridade",
        "bronze.infopen_escolaridade",
        "bronze.infopen_grau_instrucao",
        "bronze.infopen_presos_profissao",
        "bronze.infopen_profissao",
        "bronze.infopen_social_religiao",
        "bronze.infopen_religiao",
        "bronze.infopen_social_quantidadefilho",
        "bronze.infopen_preso_quantidadefilho",
        "bronze.infopen_preso_naturalidade",
        "bronze.infopen_geral_municipios",
        "bronze.infopen_preso_nacionalidade_estrangeiro",
        "bronze.infopen_necessidade_especial",
        "bronze.infopen_ficha_social",
        "bronze.infopen_preso_prontuario_social_corr",
        "bronze.siarhes_servidores",
        "bronze.obsejus_cc_requerimentoscavi",
        "bronze.livros_acesso_unidade_advogado",
        "bronze.livros_acesso_unidade_visitafamiliar",
        "bronze.livros_acesso_unidade_visitareligiosa",
    ]

    for tabela in tabelas:
        spark.sql(f"REFRESH TABLE {tabela}")

    spark.catalog.clearCache()


    # ============================================================
    # 00.1 - LIMPEZA INICIAL DAS TEMPORÁRIAS
    # ============================================================

    temporarias = [
        "tmp_sinp_pessoa_docs_base",
        "tmp_sinp_pessoa_classificacao_doc",
        "tmp_sinp_pessoa_filiacao",
        "tmp_sinp_pessoa_etnia",
        "tmp_sinp_pessoa_vulgo",
        "tmp_sinp_pessoa_estado_civil",
        "tmp_sinp_pessoa_ficha_social",
        "tmp_sinp_pessoa_escolaridade",
        "tmp_sinp_pessoa_profissao",
        "tmp_sinp_pessoa_religiao",
        "tmp_sinp_pessoa_qtd_filhos",
        "tmp_sinp_pessoa_naturalidade",
        "tmp_sinp_pessoa_nacionalidade_estr",
        "tmp_sinp_pessoa_prontuario_social",
        "tmp_sinp_pessoa_base_documento",
        "tmp_sinp_pessoa_melhor_preso",
        "tmp_sinp_pessoa_dedup",
        "tmp_sinp_pessoa_base_final",
        "tmp_sinp_pessoa_00_presidiario",
        "tmp_sinp_pessoa_outras_aberta",
        "tmp_sinp_pnt_pessoa_preso_aberta",
        "tmp_sinp_siarhes_filtrada",
        "tmp_sinp_pessoa_01_base_cpf",
        "tmp_sinp_pessoa_01_siarhes",
        "tmp_sinp_cavi_filtrada",
        "tmp_sinp_pessoa_02_base_cpf",
        "tmp_sinp_pessoa_02_cavi",
        "tmp_sinp_advogado_filtrada",
        "tmp_sinp_pessoa_03_base_adv",
        "tmp_sinp_pessoa_03_advogado",
        "tmp_sinp_familiar_filtrada",
        "tmp_sinp_pessoa_04_base_doc",
        "tmp_sinp_pessoa_04_familiar",
        "tmp_sinp_religiosa_filtrada",
        "tmp_sinp_pessoa_05_base_doc",
        "tmp_sinp_pessoa_05_religiosa",
    ]

    for tabela in temporarias:
        spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    spark.catalog.clearCache()


    # ============================================================
    # 01 - DOCUMENTOS BASE
    # ============================================================

    tabela = "tmp_sinp_pessoa_docs_base"

    df_tmp_sinp_pessoa_docs_base = spark.sql(r"""
        select
            cast(d.id_preso as string) as id_preso,
            d.id_documentotipo,
            regexp_replace(coalesce(d.presodocumento_numero, ''), '[^0-9]', '') as id_documento_limpo
        from bronze.infopen_preso_documentos d
        where d.presodocumento_numero is not null
          and regexp_replace(coalesce(d.presodocumento_numero, ''), '[^0-9]', '') <> ''
          and cast(regexp_replace(coalesce(d.presodocumento_numero, ''), '[^0-9]', '') as bigint) >= 1000
    """)

    df_tmp_sinp_pessoa_docs_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_docs_base, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 02 - CLASSIFICAÇÃO DOCUMENTAL
    # ============================================================

    tabela = "tmp_sinp_pessoa_classificacao_doc"

    df_tmp_sinp_pessoa_classificacao_doc = spark.sql(r"""
        select
            id_preso,
            max(case when id_documentotipo in (18,19) then 1 else 0 end) as tem_doc_nacional,
            max(case when id_documentotipo = 26 then 1 else 0 end) as tem_passaporte
        from gold.tmp_sinp_pessoa_docs_base
        group by id_preso
    """)

    df_tmp_sinp_pessoa_classificacao_doc.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_classificacao_doc, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 03 - FILIAÇÃO
    # ============================================================

    tabela = "tmp_sinp_pessoa_filiacao"

    df_tmp_sinp_pessoa_filiacao = spark.sql(r"""
        select
            cast(id_preso as string) as id_preso,

            max(
                case
                    when upper(regexp_replace(coalesce(presofiliacao_mae, ''), '\\s+', ' ')) rlike ' OU '
                        then trim(
                            regexp_extract(
                                regexp_replace(coalesce(presofiliacao_mae, ''), '\\s+', ' '),
                                '^(.*?)\\s+(?i:OU)\\s+.*$',
                                1
                            )
                        )
                    else trim(regexp_replace(coalesce(presofiliacao_mae, ''), '\\s+', ' '))
                end
            ) as nome_mae,

            max(
                case
                    when upper(regexp_replace(coalesce(presofiliacao_pai, ''), '\\s+', ' ')) rlike ' OU '
                        then trim(
                            regexp_extract(
                                regexp_replace(coalesce(presofiliacao_pai, ''), '\\s+', ' '),
                                '^(.*?)\\s+(?i:OU)\\s+.*$',
                                1
                            )
                        )
                    else trim(regexp_replace(coalesce(presofiliacao_pai, ''), '\\s+', ' '))
                end
            ) as nome_pai

        from bronze.infopen_preso_filiacao
        group by cast(id_preso as string)
    """)

    df_tmp_sinp_pessoa_filiacao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_filiacao, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 04 - ETNIA
    # ============================================================

    tabela = "tmp_sinp_pessoa_etnia"

    df_tmp_sinp_pessoa_etnia = spark.sql(r"""
        select
            cast(cpe.id_preso as string) as id_preso,
            concat_ws(', ', collect_set(cor.corpeleetnia_descricao)) as etnia
        from bronze.infopen_preso_cor_pele_etnia cpe
        inner join bronze.infopen_cor_pele_etnia cor
            on cor.id_corpeleetnia = cpe.id_corpeleetnia
        group by cast(cpe.id_preso as string)
    """)

    df_tmp_sinp_pessoa_etnia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_etnia, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 04.1 - VULGO
    # ============================================================

    tabela = "tmp_sinp_pessoa_vulgo"

    df_tmp_sinp_pessoa_vulgo = spark.sql(r"""
        select
            cast(id_preso as string) as id_preso,
            concat_ws(
                ', ',
                sort_array(
                    collect_set(
                        trim(regexp_replace(coalesce(presoalcunha_descricao, ''), '\\s+', ' '))
                    )
                )
            ) as vulgo
        from bronze.infopen_preso_alcunha
        where trim(regexp_replace(coalesce(presoalcunha_descricao, ''), '\\s+', ' ')) <> ''
        group by cast(id_preso as string)
    """)

    df_tmp_sinp_pessoa_vulgo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_vulgo, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 05 - ESTADO CIVIL SOCIAL
    # ============================================================

    tabela = "tmp_sinp_pessoa_estado_civil"

    df_tmp_sinp_pessoa_estado_civil = spark.sql(r"""
        select
            id_preso,
            estado_civil_social
        from (
            select
                cast(s.id_preso as string) as id_preso,
                trim(regexp_replace(coalesce(ec.estadocivil_descricao, ''), '\\s+', ' ')) as estado_civil_social,
                row_number() over (
                    partition by cast(s.id_preso as string)
                    order by
                        case when s.social_estadocivil_data is not null then 1 else 2 end,
                        s.social_estadocivil_data desc,
                        s.id_socialestadocivil desc
                ) as rn
            from bronze.infopen_social_estado_civil s
            left join bronze.infopen_estado_civil ec
                on ec.id_estadocivil = s.id_estadocivil
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_estado_civil.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_estado_civil, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 06 - FICHA SOCIAL
    # ============================================================

    tabela = "tmp_sinp_pessoa_ficha_social"

    df_tmp_sinp_pessoa_ficha_social = spark.sql(r"""
        select
            id_preso,
            estado_civil_ficha,
            escolaridade_ficha,
            profissao_ficha,
            religiao_ficha,
            necessidade_especial_ficha
        from (
            select
                cast(fs.id_preso as string) as id_preso,
                trim(regexp_replace(coalesce(ec.estadocivil_descricao, ''), '\\s+', ' ')) as estado_civil_ficha,
                trim(regexp_replace(coalesce(gi.grauinstrucao_descricao, ''), '\\s+', ' ')) as escolaridade_ficha,
                trim(regexp_replace(coalesce(pr.profissao_descricao, ''), '\\s+', ' ')) as profissao_ficha,
                trim(regexp_replace(coalesce(rg.religiao_descricao, ''), '\\s+', ' ')) as religiao_ficha,
                trim(regexp_replace(coalesce(ne.necessidadeespecial_descricao, ''), '\\s+', ' ')) as necessidade_especial_ficha,
                row_number() over (
                    partition by cast(fs.id_preso as string)
                    order by fs.id_fichasocial desc
                ) as rn
            from bronze.infopen_ficha_social fs
            left join bronze.infopen_estado_civil ec
                on ec.id_estadocivil = fs.id_estadocivil
            left join bronze.infopen_grau_instrucao gi
                on gi.id_grauinstrucao = fs.id_grauinstrucao
            left join bronze.infopen_profissao pr
                on pr.id_profissao = fs.id_profissao
            left join bronze.infopen_religiao rg
                on rg.id_religiao = fs.id_religiao
            left join bronze.infopen_necessidade_especial ne
                on ne.id_necessidadeespecial = fs.id_necessidadeespecial
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_ficha_social.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_ficha_social, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 07 - ESCOLARIDADE SOCIAL
    # ============================================================

    tabela = "tmp_sinp_pessoa_escolaridade"

    df_tmp_sinp_pessoa_escolaridade = spark.sql(r"""
        select
            id_preso,
            escolaridade_social
        from (
            select
                cast(s.id_preso as string) as id_preso,
                trim(regexp_replace(coalesce(e.escolaridade_descricao, ''), '\\s+', ' ')) as escolaridade_social,
                row_number() over (
                    partition by cast(s.id_preso as string)
                    order by
                        case when s.social_escolaridade_data is not null then 1 else 2 end,
                        s.social_escolaridade_data desc,
                        s.id_socialescolaridade desc
                ) as rn
            from bronze.infopen_social_escolaridade s
            left join bronze.infopen_escolaridade e
                on e.id_escolaridade = s.id_escolaridade
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_escolaridade.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_escolaridade, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 08 - PROFISSÃO
    # ============================================================

    tabela = "tmp_sinp_pessoa_profissao"

    df_tmp_sinp_pessoa_profissao = spark.sql(r"""
        select
            id_preso,
            profissao_social
        from (
            select
                cast(pf.id_preso as string) as id_preso,
                trim(regexp_replace(coalesce(pf.descricao_profissao, p.profissao_descricao, ''), '\\s+', ' ')) as profissao_social,
                row_number() over (
                    partition by cast(pf.id_preso as string)
                    order by
                        case when pf.dt_profissao is not null then 1 else 2 end,
                        pf.dt_profissao desc,
                        pf.id_presoprofissao desc
                ) as rn
            from bronze.infopen_presos_profissao pf
            left join bronze.infopen_profissao p
                on p.id_profissao = pf.id_profissao
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_profissao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_profissao, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 09 - RELIGIÃO SOCIAL
    # ============================================================

    tabela = "tmp_sinp_pessoa_religiao"

    df_tmp_sinp_pessoa_religiao = spark.sql(r"""
        select
            id_preso,
            religiao_social
        from (
            select
                cast(s.id_preso as string) as id_preso,
                trim(regexp_replace(coalesce(r.religiao_descricao, ''), '\\s+', ' ')) as religiao_social,
                row_number() over (
                    partition by cast(s.id_preso as string)
                    order by
                        case when s.social_religiao_data is not null then 1 else 2 end,
                        s.social_religiao_data desc,
                        s.id_socialreligiao desc
                ) as rn
            from bronze.infopen_social_religiao s
            left join bronze.infopen_religiao r
                on r.id_religiao = s.id_religiao
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_religiao.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_religiao, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 10 - QUANTIDADE DE FILHOS
    # ============================================================

    tabela = "tmp_sinp_pessoa_qtd_filhos"

    df_tmp_sinp_pessoa_qtd_filhos = spark.sql(r"""
        select
            id_preso,
            quantidade_filhos
        from (
            select
                cast(s.id_preso as string) as id_preso,
                trim(regexp_replace(coalesce(q.presoquantidadefilho_descricao, ''), '\\s+', ' ')) as quantidade_filhos,
                row_number() over (
                    partition by cast(s.id_preso as string)
                    order by
                        case when s.social_quantidadefilho_data is not null then 1 else 2 end,
                        s.social_quantidadefilho_data desc,
                        s.id_socialquantidadefilho desc
                ) as rn
            from bronze.infopen_social_quantidadefilho s
            left join bronze.infopen_preso_quantidadefilho q
                on q.id_presoquantidadefilho = s.id_presoquantidadefilho
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_qtd_filhos.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_qtd_filhos, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 11 - NATURALIDADE
    # ============================================================

    tabela = "tmp_sinp_pessoa_naturalidade"

    df_tmp_sinp_pessoa_naturalidade = spark.sql(r"""
        select
            cast(n.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(m.municipio_nome, ''), '\\s+', ' ')) as naturalidade_municipio,
            trim(regexp_replace(coalesce(m.municipio_siglauf, ''), '\\s+', ' ')) as naturalidade_uf
        from bronze.infopen_preso_naturalidade n
        left join bronze.infopen_geral_municipios m
            on m.id_municipio = n.id_municipio
    """)

    df_tmp_sinp_pessoa_naturalidade.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_naturalidade, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 12 - NACIONALIDADE ESTRANGEIRO
    # ============================================================

    tabela = "tmp_sinp_pessoa_nacionalidade_estr"

    df_tmp_sinp_pessoa_nacionalidade_estr = spark.sql(r"""
        select
            cast(id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(presonacionalidadeestrangeiro_paisorigem, ''), '\\s+', ' ')) as pais_origem_estrangeiro,
            trim(regexp_replace(coalesce(presonacionalidadeestrangeiro_cidade, ''), '\\s+', ' ')) as cidade_origem_estrangeiro
        from bronze.infopen_preso_nacionalidade_estrangeiro
    """)

    df_tmp_sinp_pessoa_nacionalidade_estr.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_nacionalidade_estr, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 13 - PRONTUÁRIO SOCIAL
    # ============================================================

    tabela = "tmp_sinp_pessoa_prontuario_social"

    df_tmp_sinp_pessoa_prontuario_social = spark.sql(r"""
        select
            id_preso,
            flag_tem_filhos_prontuario,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal
        from (
            select
                cast(id_preso as string) as id_preso,

                case
                    when lower(trim(cast(st_tem_filhos as string))) in ('1', 'true', 't', 'sim', 's') then 1
                    when lower(trim(cast(st_tem_filhos as string))) in ('0', 'false', 'f', 'nao', 'não', 'n') then 0
                    else null
                end as flag_tem_filhos_prontuario,

                case
                    when lower(trim(cast(st_sabe_ler as string))) in ('1', 'true', 't', 'sim', 's') then 1
                    when lower(trim(cast(st_sabe_ler as string))) in ('0', 'false', 'f', 'nao', 'não', 'n') then 0
                    else null
                end as flag_sabe_ler,

                case
                    when lower(trim(cast(st_sabe_escrever as string))) in ('1', 'true', 't', 'sim', 's') then 1
                    when lower(trim(cast(st_sabe_escrever as string))) in ('0', 'false', 'f', 'nao', 'não', 'n') then 0
                    else null
                end as flag_sabe_escrever,

                case
                    when lower(trim(cast(st_recebe_visita as string))) in ('1', 'true', 't', 'sim', 's') then 1
                    when lower(trim(cast(st_recebe_visita as string))) in ('0', 'false', 'f', 'nao', 'não', 'n') then 0
                    else null
                end as flag_recebe_visita,

                case
                    when lower(trim(cast(st_relac_conjugal as string))) in ('1', 'true', 't', 'sim', 's') then 1
                    when lower(trim(cast(st_relac_conjugal as string))) in ('0', 'false', 'f', 'nao', 'não', 'n') then 0
                    else null
                end as flag_relacao_conjugal,

                row_number() over (
                    partition by cast(id_preso as string)
                    order by
                        case when dt_cadastro_ficha is not null then 1 else 2 end,
                        to_date(dt_cadastro_ficha) desc,
                        id_prontuario_social desc
                ) as rn

            from bronze.infopen_preso_prontuario_social_corr
        ) x
        where rn = 1
    """)

    df_tmp_sinp_pessoa_prontuario_social.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_prontuario_social, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 14 - BASE DOCUMENTO CONSOLIDADA
    # ============================================================

    tabela = "tmp_sinp_pessoa_base_documento"

    df_tmp_sinp_pessoa_base_documento = spark.sql(r"""
        select
            d.id_preso,
            d.id_documentotipo,
            dt.documentotipo_descricao,
            d.id_documento_limpo,
            c.tem_doc_nacional,
            c.tem_passaporte,

            case
                when upper(regexp_replace(coalesce(p.preso_nome, ''), '\\s+', ' ')) rlike ' OU '
                    then trim(regexp_extract(regexp_replace(coalesce(p.preso_nome, ''), '\\s+', ' '), '^(.*?)\\s+(?i:OU)\\s+.*$', 1))
                else trim(regexp_replace(coalesce(p.preso_nome, ''), '\\s+', ' '))
            end as nome_pessoa,

            p.preso_sexo as sexo_pessoa,
            p.preso_datanascimento as data_nascimento_pessoa,
            p.preso_dataultimaprisao as data_ultima_prisao,

            f.nome_mae,
            f.nome_pai,
            e.etnia,
            v.vulgo,

            coalesce(ec.estado_civil_social, fs.estado_civil_ficha) as estado_civil,
            coalesce(es.escolaridade_social, fs.escolaridade_ficha) as escolaridade,
            coalesce(pr.profissao_social, fs.profissao_ficha) as profissao,
            coalesce(rg.religiao_social, fs.religiao_ficha) as religiao,

            case
                when ps.flag_tem_filhos_prontuario is not null then ps.flag_tem_filhos_prontuario
                when upper(coalesce(qf.quantidade_filhos, '')) in ('', 'NAO INFORMADO', 'NÃO INFORMADO') then 0
                when upper(coalesce(qf.quantidade_filhos, '')) rlike '^(0|ZERO|NENHUM|NENHUMA|SEM FILHOS)$' then 0
                else 1
            end as flag_tem_filhos,

            qf.quantidade_filhos,
            nat.naturalidade_municipio,
            nat.naturalidade_uf,
            estr.pais_origem_estrangeiro,
            estr.cidade_origem_estrangeiro,

            fs.necessidade_especial_ficha as necessidade_especial,

            ps.flag_sabe_ler,
            ps.flag_sabe_escrever,
            ps.flag_recebe_visita,
            ps.flag_relacao_conjugal,

            row_number() over (
                partition by d.id_preso
                order by
                    case
                        when d.id_documentotipo = 19 then 1
                        when d.id_documentotipo = 18 then 2
                        when d.id_documentotipo = 26 then 3
                        else 4
                    end,
                    length(d.id_documento_limpo) desc,
                    d.id_documento_limpo desc
            ) as rn_doc

        from gold.tmp_sinp_pessoa_docs_base d

        inner join bronze.infopen_presos p
            on cast(p.id_preso as string) = d.id_preso

        inner join gold.tmp_sinp_pessoa_classificacao_doc c
            on c.id_preso = d.id_preso

        left join bronze.infopen_documentos_tipos dt
            on dt.id_documentotipo = d.id_documentotipo

        left join gold.tmp_sinp_pessoa_filiacao f
            on f.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_etnia e
            on e.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_vulgo v
            on v.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_estado_civil ec
            on ec.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_ficha_social fs
            on fs.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_escolaridade es
            on es.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_profissao pr
            on pr.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_religiao rg
            on rg.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_qtd_filhos qf
            on qf.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_naturalidade nat
            on nat.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_nacionalidade_estr estr
            on estr.id_preso = d.id_preso

        left join gold.tmp_sinp_pessoa_prontuario_social ps
            on ps.id_preso = d.id_preso
    """)

    df_tmp_sinp_pessoa_base_documento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_base_documento, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 15 - MELHOR DOCUMENTO POR PRESO
    # ============================================================

    tabela = "tmp_sinp_pessoa_melhor_preso"

    df_tmp_sinp_pessoa_melhor_preso = spark.sql(r"""
        select
            id_preso,
            id_documentotipo,
            documentotipo_descricao,
            id_documento_limpo,
            tem_doc_nacional,
            tem_passaporte,
            nome_pessoa,
            sexo_pessoa,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal,

            case
                when tem_doc_nacional = 1 then concat('NAC_', id_documento_limpo)
                when tem_doc_nacional = 0 and tem_passaporte = 1 and id_documentotipo = 26 then concat('EST_', id_documento_limpo)
                else concat('NAC_', id_documento_limpo)
            end as id_pessoa

        from gold.tmp_sinp_pessoa_base_documento
        where rn_doc = 1
    """)

    df_tmp_sinp_pessoa_melhor_preso.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_melhor_preso, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 16 - DEDUP POR ID_PESSOA
    # ============================================================

    tabela = "tmp_sinp_pessoa_dedup"

    df_tmp_sinp_pessoa_dedup = spark.sql(r"""
        select
            *,
            row_number() over (
                partition by id_pessoa
                order by
                    case when data_ultima_prisao is not null then 1 else 2 end,
                    to_date(data_ultima_prisao) desc,
                    case when data_nascimento_pessoa is not null then 1 else 2 end,
                    to_date(data_nascimento_pessoa) desc,
                    id_preso desc
            ) as rn_pessoa,

            count(*) over (
                partition by id_pessoa
            ) as qtd_mesmo_id_pessoa,

            first_value(id_preso) over (
                partition by id_pessoa
                order by
                    case when data_ultima_prisao is not null then 1 else 2 end,
                    to_date(data_ultima_prisao) desc,
                    case when data_nascimento_pessoa is not null then 1 else 2 end,
                    to_date(data_nascimento_pessoa) desc,
                    id_preso desc
            ) as id_preso_original

        from gold.tmp_sinp_pessoa_melhor_preso
    """)

    df_tmp_sinp_pessoa_dedup.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_dedup, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 17 - BASE FINAL ABERTA
    # ============================================================

    tabela = "tmp_sinp_pessoa_base_final"

    df_tmp_sinp_pessoa_base_final = spark.sql(r"""
        select
            cast(id_preso as string) as id_preso,
            cast(id_preso_original as string) as id_preso_original,
            id_pessoa,

            case
                when tem_doc_nacional = 1 then 'NACIONAL'
                when tem_doc_nacional = 0 and tem_passaporte = 1 then 'ESTRANGEIRO'
                else 'NACIONAL'
            end as origem,

            id_documentotipo as cod_documento_referencia,
            documentotipo_descricao as desc_documento_referencia,

            case
                when id_documentotipo = 19 then concat(
                    substr(lpad(id_documento_limpo, 11, '0'), 1, 3), '.',
                    substr(lpad(id_documento_limpo, 11, '0'), 4, 3), '.',
                    substr(lpad(id_documento_limpo, 11, '0'), 7, 3), '-',
                    substr(lpad(id_documento_limpo, 11, '0'), 10, 2)
                )
                else id_documento_limpo
            end as documento,

            nome_pessoa,
            sexo_pessoa,

            1 as flag_presidiario,
            0 as flag_advogado,
            0 as flag_servidor,
            0 as flag_visitante,
            0 as flag_ocorrencia_10d,
            0 as flag_ocorrencia_30d,
            0 as flag_ocorrencia_60d,

            to_date(data_nascimento_pessoa) as data_nascimento_pessoa,
            to_date(data_ultima_prisao) as data_ultima_prisao,

            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal,
            rn_pessoa,
            qtd_mesmo_id_pessoa

        from gold.tmp_sinp_pessoa_dedup
    """)

    df_tmp_sinp_pessoa_base_final.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_base_final, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 18 - PESSOA PRESIDIÁRIO
    # ============================================================

    tabela = "tmp_sinp_pessoa_00_presidiario"

    df_tmp_sinp_pessoa_00_presidiario = spark.sql(r"""
        select
            id_preso,
            id_pessoa,
            origem,
            cod_documento_referencia,
            desc_documento_referencia,
            documento,
            nome_pessoa,
            sexo_pessoa,
            flag_presidiario,
            flag_advogado,
            flag_servidor,
            flag_visitante,
            flag_ocorrencia_10d,
            flag_ocorrencia_30d,
            flag_ocorrencia_60d,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_base_final
        where rn_pessoa = 1
    """)

    df_tmp_sinp_pessoa_00_presidiario.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_00_presidiario, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 19 - OUTRAS PESSOAS DA DEDUP
    # ============================================================

    tabela = "tmp_sinp_pessoa_outras_aberta"

    df_tmp_sinp_pessoa_outras_aberta = spark.sql(r"""
        select
            id_preso,
            id_preso_original,
            id_pessoa,
            origem,
            cod_documento_referencia,
            desc_documento_referencia,
            documento,
            nome_pessoa,
            sexo_pessoa,
            flag_presidiario,
            flag_advogado,
            flag_servidor,
            flag_visitante,
            flag_ocorrencia_10d,
            flag_ocorrencia_30d,
            flag_ocorrencia_60d,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal,
            rn_pessoa,
            qtd_mesmo_id_pessoa,
            'EXCLUIDO_NA_DEDUPLICACAO_POR_ID_PESSOA' as motivo_exclusao
        from gold.tmp_sinp_pessoa_base_final
        where rn_pessoa > 1
    """)

    df_tmp_sinp_pessoa_outras_aberta.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_outras_aberta, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 20 - PONTE PESSOA PRESO ABERTA
    # ============================================================

    tabela = "tmp_sinp_pnt_pessoa_preso_aberta"

    df_tmp_sinp_pnt_pessoa_preso_aberta = spark.sql(r"""
        select distinct
            cast(id_preso as string) as id_preso,
            cast(id_pessoa as string) as id_pessoa,
            cast(nome_pessoa as string) as nome_pessoa
        from gold.tmp_sinp_pessoa_base_final
        where id_preso is not null
          and id_pessoa is not null
    """)

    df_tmp_sinp_pnt_pessoa_preso_aberta.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pnt_pessoa_preso_aberta, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 21 - SIARHES FILTRADA
    # ============================================================

    tabela = "tmp_sinp_siarhes_filtrada"

    df_tmp_sinp_siarhes_filtrada = spark.sql(r"""
        select
            cpf_normalizado,
            concat(substr(cpf_normalizado, 1, 3), '.', substr(cpf_normalizado, 4, 3), '.', substr(cpf_normalizado, 7, 3), '-', substr(cpf_normalizado, 10, 2)) as cpf_formatado,
            nome_pessoa,
            rg,
            dt_extracao,
            numero_funcional,
            cargo,
            categoria,
            subcategoria,
            situacao,
            tipo_vinculo,
            subempresa
        from (
            select
                lpad(regexp_replace(regexp_replace(cast(cpf as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0') as cpf_normalizado,
                trim(regexp_replace(coalesce(nome_servidor, ''), '\\s+', ' ')) as nome_pessoa,
                trim(regexp_replace(coalesce(cast(rg as string), ''), '\\s+', ' ')) as rg,
                dt_extracao,
                trim(regexp_extract(numero_funcional, '^([^ ]+)', 1)) as numero_funcional,
                cargo,
                categoria,
                subcategoria,
                situacao,
                tipo_vinculo,
                subempresa,
                row_number() over (
                    partition by lpad(regexp_replace(regexp_replace(cast(cpf as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0')
                    order by
                        case when dt_extracao is not null then 1 else 2 end,
                        dt_extracao desc,
                        case when coalesce(nome_servidor, '') <> '' then 1 else 2 end,
                        case when coalesce(cast(rg as string), '') <> '' then 1 else 2 end,
                        cast(numero_funcional as string) desc
                ) as rn
            from bronze.siarhes_servidores
        ) x
        where rn = 1
          and cpf_normalizado is not null
          and cpf_normalizado <> ''
          and cpf_normalizado <> '00000000000'
          and length(cpf_normalizado) = 11
    """)

    df_tmp_sinp_siarhes_filtrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_siarhes_filtrada, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 22 - PESSOA BASE CPF PARA SIARHES
    # ============================================================

    tabela = "tmp_sinp_pessoa_01_base_cpf"

    df_tmp_sinp_pessoa_01_base_cpf = spark.sql(r"""
        select
            p.*,
            case
                when p.cod_documento_referencia = 19
                    then lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0')
                else null
            end as cpf_normalizado
        from gold.tmp_sinp_pessoa_00_presidiario p
    """)

    df_tmp_sinp_pessoa_01_base_cpf.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_01_base_cpf, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 23 - PESSOA APÓS SIARHES
    # ============================================================

    tabela = "tmp_sinp_pessoa_01_siarhes"

    df_tmp_sinp_pessoa_01_siarhes = spark.sql(r"""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_01_base_cpf p
        left join gold.tmp_sinp_siarhes_filtrada s
            on p.cpf_normalizado = s.cpf_normalizado
        where s.cpf_normalizado is null

        union all

        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            1 as flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_01_base_cpf p
        inner join gold.tmp_sinp_siarhes_filtrada s
            on p.cpf_normalizado = s.cpf_normalizado

        union all

        select
            cast(s.numero_funcional as string) as id_preso,
            concat('CPF_', s.cpf_normalizado) as id_pessoa,
            'SERVIDOR' as origem,
            19 as cod_documento_referencia,
            'CPF' as desc_documento_referencia,
            s.cpf_formatado as documento,
            s.nome_pessoa as nome_pessoa,
            cast(null as string) as sexo_pessoa,
            0 as flag_presidiario,
            0 as flag_advogado,
            1 as flag_servidor,
            0 as flag_visitante,
            0 as flag_ocorrencia_10d,
            0 as flag_ocorrencia_30d,
            0 as flag_ocorrencia_60d,
            cast(null as date) as data_nascimento_pessoa,
            cast(null as date) as data_ultima_prisao,
            cast(null as string) as nome_mae,
            cast(null as string) as nome_pai,
            cast(null as string) as etnia,
            cast(null as string) as vulgo,
            cast(null as string) as estado_civil,
            cast(null as string) as escolaridade,
            cast(null as string) as profissao,
            cast(null as string) as religiao,
            cast(null as int) as flag_tem_filhos,
            cast(null as string) as quantidade_filhos,
            cast(null as string) as naturalidade_municipio,
            cast(null as string) as naturalidade_uf,
            cast(null as string) as pais_origem_estrangeiro,
            cast(null as string) as cidade_origem_estrangeiro,
            cast(null as string) as necessidade_especial,
            cast(null as int) as flag_sabe_ler,
            cast(null as int) as flag_sabe_escrever,
            cast(null as int) as flag_recebe_visita,
            cast(null as int) as flag_relacao_conjugal
        from gold.tmp_sinp_siarhes_filtrada s
        left join gold.tmp_sinp_pessoa_01_base_cpf p
            on p.cpf_normalizado = s.cpf_normalizado
        where p.cpf_normalizado is null
    """)

    df_tmp_sinp_pessoa_01_siarhes.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_01_siarhes, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 24 - CAVI FILTRADA
    # ============================================================

    tabela = "tmp_sinp_cavi_filtrada"

    df_tmp_sinp_cavi_filtrada = spark.sql(r"""
        select
            cpf_normalizado,
            concat(substr(cpf_normalizado, 1, 3), '.', substr(cpf_normalizado, 4, 3), '.', substr(cpf_normalizado, 7, 3), '-', substr(cpf_normalizado, 10, 2)) as cpf_formatado,
            nome_pessoa,
            rg,
            sexo_pessoa,
            orgaoemissor,
            estadoemissor,
            profissao,
            telcelular,
            telresidencial,
            emailinteressado,
            itemnum,
            numeroonbase,
            statussolicitacao,
            resultanalise,
            tipovisita,
            vinculodetento,
            grauparentesco,
            unidadeprisional
        from (
            select
                cast(itemnum as string) as itemnum,
                trim(regexp_replace(coalesce(cast(numeroonbase as string), ''), '\\s+', ' ')) as numeroonbase,
                trim(regexp_replace(coalesce(statussolicitacao, ''), '\\s+', ' ')) as statussolicitacao,
                trim(regexp_replace(coalesce(resultanalise, ''), '\\s+', ' ')) as resultanalise,
                trim(regexp_replace(coalesce(cast(telcelular as string), ''), '\\s+', ' ')) as telcelular,
                trim(regexp_replace(coalesce(nomeinteressado, ''), '\\s+', ' ')) as nome_pessoa,
                lower(trim(regexp_replace(coalesce(emailinteressado, ''), '\\s+', ' '))) as emailinteressado,
                lpad(regexp_replace(regexp_replace(cast(cpfinteressado as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0') as cpf_normalizado,
                trim(regexp_replace(coalesce(cast(telresidencial as string), ''), '\\s+', ' ')) as telresidencial,
                trim(regexp_replace(regexp_replace(coalesce(cast(rginteressado as string), ''), '\\.0+$', ''), '\\s+', ' ')) as rg,
                trim(regexp_replace(coalesce(orgaoemissor, ''), '\\s+', ' ')) as orgaoemissor,
                trim(regexp_replace(coalesce(estadoemissor, ''), '\\s+', ' ')) as estadoemissor,
                trim(regexp_replace(coalesce(sexo, ''), '\\s+', ' ')) as sexo_pessoa,
                trim(regexp_replace(coalesce(profissao, ''), '\\s+', ' ')) as profissao,
                tipovisita,
                vinculodetento,
                grauparentesco,
                unidadeprisional,

                row_number() over (
                    partition by lpad(regexp_replace(regexp_replace(cast(cpfinteressado as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0')
                    order by
                        case when itemnum is not null and itemnum <> '' then 1 else 2 end,
                        cast(itemnum as bigint) desc,
                        case when coalesce(nomeinteressado, '') <> '' then 1 else 2 end,
                        case when coalesce(cast(rginteressado as string), '') <> '' then 1 else 2 end,
                        case when coalesce(emailinteressado, '') <> '' then 1 else 2 end
                ) as rn

            from bronze.obsejus_cc_requerimentoscavi
        ) x
        where rn = 1
          and cpf_normalizado is not null
          and cpf_normalizado <> ''
          and cpf_normalizado <> '00000000000'
          and length(cpf_normalizado) = 11
    """)

    df_tmp_sinp_cavi_filtrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_cavi_filtrada, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 25 - PESSOA BASE CPF PARA CAVI
    # ============================================================

    tabela = "tmp_sinp_pessoa_02_base_cpf"

    df_tmp_sinp_pessoa_02_base_cpf = spark.sql(r"""
        select
            p.*,
            case
                when p.cod_documento_referencia = 19
                    then lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0')
                else null
            end as cpf_normalizado
        from gold.tmp_sinp_pessoa_01_siarhes p
    """)

    df_tmp_sinp_pessoa_02_base_cpf.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_02_base_cpf, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 26 - PESSOA APÓS CAVI
    # ============================================================

    tabela = "tmp_sinp_pessoa_02_cavi"

    df_tmp_sinp_pessoa_02_cavi = spark.sql(r"""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_02_base_cpf p
        left join gold.tmp_sinp_cavi_filtrada c
            on p.cpf_normalizado = c.cpf_normalizado
        where c.cpf_normalizado is null

        union all

        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            case when coalesce(p.sexo_pessoa, '') <> '' then p.sexo_pessoa else c.sexo_pessoa end as sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            1 as flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_02_base_cpf p
        inner join gold.tmp_sinp_cavi_filtrada c
            on p.cpf_normalizado = c.cpf_normalizado

        union all

        select
            cast(null as string) as id_preso,
            concat('CPF_', c.cpf_normalizado) as id_pessoa,
            'VISITANTE' as origem,
            19 as cod_documento_referencia,
            'CPF' as desc_documento_referencia,
            c.cpf_formatado as documento,
            c.nome_pessoa as nome_pessoa,
            c.sexo_pessoa as sexo_pessoa,
            0 as flag_presidiario,
            0 as flag_advogado,
            0 as flag_servidor,
            1 as flag_visitante,
            0 as flag_ocorrencia_10d,
            0 as flag_ocorrencia_30d,
            0 as flag_ocorrencia_60d,
            cast(null as date) as data_nascimento_pessoa,
            cast(null as date) as data_ultima_prisao,
            cast(null as string) as nome_mae,
            cast(null as string) as nome_pai,
            cast(null as string) as etnia,
            cast(null as string) as vulgo,
            cast(null as string) as estado_civil,
            cast(null as string) as escolaridade,
            c.profissao as profissao,
            cast(null as string) as religiao,
            cast(null as int) as flag_tem_filhos,
            cast(null as string) as quantidade_filhos,
            cast(null as string) as naturalidade_municipio,
            cast(null as string) as naturalidade_uf,
            cast(null as string) as pais_origem_estrangeiro,
            cast(null as string) as cidade_origem_estrangeiro,
            cast(null as string) as necessidade_especial,
            cast(null as int) as flag_sabe_ler,
            cast(null as int) as flag_sabe_escrever,
            cast(null as int) as flag_recebe_visita,
            cast(null as int) as flag_relacao_conjugal
        from gold.tmp_sinp_cavi_filtrada c
        left join gold.tmp_sinp_pessoa_02_base_cpf p
            on p.cpf_normalizado = c.cpf_normalizado
        where p.cpf_normalizado is null
    """)

    df_tmp_sinp_pessoa_02_cavi.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_02_cavi, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 27 - ADVOGADO FILTRADO
    # ============================================================

    tabela = "tmp_sinp_advogado_filtrada"

    df_tmp_sinp_advogado_filtrada = spark.sql(r"""
        select
            id_advogado_origem,
            id_preso_advogado,
            id_pessoa,
            nome_pessoa,
            estado_oab,
            oab_bruta,
            oab_normalizada,
            documento_oab
        from (
            select
                cast(id as string) as id_advogado_origem,
                trim(regexp_replace(coalesce(nome, ''), '\\s+', ' ')) as nome_pessoa,
                upper(trim(regexp_replace(coalesce(estado, ''), '\\s+', ' '))) as estado_oab,
                upper(trim(regexp_replace(coalesce(oab, ''), '\\s+', ' '))) as oab_bruta,
                upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', '')) as oab_normalizada,

                concat(
                    'OAB_',
                    upper(trim(regexp_replace(coalesce(estado, ''), '\\s+', ' '))),
                    '_',
                    upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', ''))
                ) as id_preso_advogado,

                concat(
                    'OAB_',
                    upper(trim(regexp_replace(coalesce(estado, ''), '\\s+', ' '))),
                    '_',
                    upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', ''))
                ) as id_pessoa,

                concat(
                    upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', '')),
                    '/',
                    upper(trim(regexp_replace(coalesce(estado, ''), '\\s+', ' ')))
                ) as documento_oab,

                row_number() over (
                    partition by
                        upper(trim(regexp_replace(coalesce(estado, ''), '\\s+', ' '))),
                        upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', ''))
                    order by
                        case when coalesce(nome, '') <> '' then 1 else 2 end,
                        cast(id as string) desc
                ) as rn

            from bronze.livros_acesso_unidade_advogado
        ) x
        where rn = 1
          and estado_oab is not null
          and estado_oab <> ''
          and oab_normalizada is not null
          and oab_normalizada <> ''
          and id_preso_advogado is not null
          and id_preso_advogado <> 'OAB__'
    """)

    df_tmp_sinp_advogado_filtrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_advogado_filtrada, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 28 - PESSOA BASE ADVOGADO
    # ============================================================

    tabela = "tmp_sinp_pessoa_03_base_adv"

    df_tmp_sinp_pessoa_03_base_adv = spark.sql(r"""
        select
            p.*,

            case
                when p.id_preso like 'OAB_%' then p.id_preso
                when p.id_pessoa like 'OAB_%' then p.id_pessoa
                else null
            end as id_preso_advogado

        from gold.tmp_sinp_pessoa_02_cavi p
    """)

    df_tmp_sinp_pessoa_03_base_adv.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_03_base_adv, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 29 - PESSOA APÓS ADVOGADO
    # ============================================================

    tabela = "tmp_sinp_pessoa_03_advogado"

    df_tmp_sinp_pessoa_03_advogado = spark.sql(r"""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_03_base_adv p
        left join gold.tmp_sinp_advogado_filtrada a
            on p.id_preso_advogado = a.id_preso_advogado
        where a.id_preso_advogado is null

        union all

        select
            coalesce(p.id_preso, a.id_preso_advogado) as id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            case when coalesce(p.desc_documento_referencia, '') <> '' then p.desc_documento_referencia else 'OAB' end as desc_documento_referencia,
            case when coalesce(p.documento, '') <> '' then p.documento else a.documento_oab end as documento,
            case when coalesce(p.nome_pessoa, '') <> '' then p.nome_pessoa else a.nome_pessoa end as nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            1 as flag_advogado,
            p.flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_03_base_adv p
        inner join gold.tmp_sinp_advogado_filtrada a
            on p.id_preso_advogado = a.id_preso_advogado

        union all

        select
            a.id_preso_advogado as id_preso,
            a.id_pessoa as id_pessoa,
            'ADVOGADO' as origem,
            cast(null as int) as cod_documento_referencia,
            'OAB' as desc_documento_referencia,
            a.documento_oab as documento,
            a.nome_pessoa as nome_pessoa,
            cast(null as string) as sexo_pessoa,
            0 as flag_presidiario,
            1 as flag_advogado,
            0 as flag_servidor,
            0 as flag_visitante,
            0 as flag_ocorrencia_10d,
            0 as flag_ocorrencia_30d,
            0 as flag_ocorrencia_60d,
            cast(null as date) as data_nascimento_pessoa,
            cast(null as date) as data_ultima_prisao,
            cast(null as string) as nome_mae,
            cast(null as string) as nome_pai,
            cast(null as string) as etnia,
            cast(null as string) as vulgo,
            cast(null as string) as estado_civil,
            cast(null as string) as escolaridade,
            cast(null as string) as profissao,
            cast(null as string) as religiao,
            cast(null as int) as flag_tem_filhos,
            cast(null as string) as quantidade_filhos,
            cast(null as string) as naturalidade_municipio,
            cast(null as string) as naturalidade_uf,
            cast(null as string) as pais_origem_estrangeiro,
            cast(null as string) as cidade_origem_estrangeiro,
            cast(null as string) as necessidade_especial,
            cast(null as int) as flag_sabe_ler,
            cast(null as int) as flag_sabe_escrever,
            cast(null as int) as flag_recebe_visita,
            cast(null as int) as flag_relacao_conjugal
        from gold.tmp_sinp_advogado_filtrada a
        left join gold.tmp_sinp_pessoa_03_base_adv p
            on p.id_preso_advogado = a.id_preso_advogado
        where p.id_preso_advogado is null
    """)

    df_tmp_sinp_pessoa_03_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_03_advogado, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 30 - FAMILIAR FILTRADO
    # ============================================================

    tabela = "tmp_sinp_familiar_filtrada"

    df_tmp_sinp_familiar_filtrada = spark.sql(r"""
        select
            id_vinculo_origem,
            nome_pessoa,
            documento_original,
            telefone,
            cpf_normalizado,
            documento_normalizado,
            chave_documento,
            case
                when chave_documento like 'CPF_%'
                    then concat(substr(cpf_normalizado, 1, 3), '.', substr(cpf_normalizado, 4, 3), '.', substr(cpf_normalizado, 7, 3), '-', substr(cpf_normalizado, 10, 2))
                else documento_original
            end as documento_formatado,
            case when chave_documento like 'CPF_%' then 19 else cast(null as int) end as cod_documento_referencia,
            case when chave_documento like 'CPF_%' then 'CPF' else 'DOCUMENTO' end as desc_documento_referencia
        from (
            select
                *,
                row_number() over (
                    partition by chave_documento
                    order by
                        case when coalesce(nome_pessoa, '') <> '' then 1 else 2 end,
                        case when coalesce(telefone, '') <> '' then 1 else 2 end,
                        id_vinculo_origem desc
                ) as rn
            from (
                select
                    cast(id as string) as id_vinculo_origem,
                    trim(regexp_replace(coalesce(nome, ''), '\\s+', ' ')) as nome_pessoa,
                    trim(regexp_replace(coalesce(documento, ''), '\\s+', ' ')) as documento_original,
                    trim(regexp_replace(coalesce(telefone, ''), '\\s+', ' ')) as telefone,
                    lpad(regexp_replace(regexp_replace(cast(documento as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0') as cpf_normalizado,
                    upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', '')) as documento_normalizado,
                    case
                        when length(regexp_replace(coalesce(documento, ''), '[^0-9]', '')) = 11
                            then concat('CPF_', lpad(regexp_replace(coalesce(documento, ''), '[^0-9]', ''), 11, '0'))
                        else concat('DOC_', upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', '')))
                    end as chave_documento
                from bronze.livros_acesso_unidade_visitafamiliar
            ) b
        ) x
        where rn = 1
          and chave_documento is not null
          and chave_documento <> 'DOC_'
          and chave_documento <> 'CPF_00000000000'
    """)

    df_tmp_sinp_familiar_filtrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_familiar_filtrada, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 31 - PESSOA BASE DOC FAMILIAR
    # ============================================================

    tabela = "tmp_sinp_pessoa_04_base_doc"

    df_tmp_sinp_pessoa_04_base_doc = spark.sql(r"""
        select
            p.*,
            case
                when p.cod_documento_referencia = 19
                    then concat('CPF_', lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0'))
                else concat('DOC_', upper(regexp_replace(coalesce(p.documento, ''), '[^0-9A-Za-z]', '')))
            end as chave_documento
        from gold.tmp_sinp_pessoa_03_advogado p
    """)

    df_tmp_sinp_pessoa_04_base_doc.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_04_base_doc, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 32 - PESSOA APÓS FAMILIAR
    # ============================================================

    tabela = "tmp_sinp_pessoa_04_familiar"

    df_tmp_sinp_pessoa_04_familiar = spark.sql(r"""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_04_base_doc p
        left join gold.tmp_sinp_familiar_filtrada f
            on p.chave_documento = f.chave_documento
        where f.chave_documento is null

        union all

        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            case when p.cod_documento_referencia is not null then p.cod_documento_referencia else f.cod_documento_referencia end as cod_documento_referencia,
            case when coalesce(p.desc_documento_referencia, '') <> '' then p.desc_documento_referencia else f.desc_documento_referencia end as desc_documento_referencia,
            case when coalesce(p.documento, '') <> '' then p.documento else f.documento_formatado end as documento,
            case when coalesce(p.nome_pessoa, '') <> '' then p.nome_pessoa else f.nome_pessoa end as nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            1 as flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_04_base_doc p
        inner join gold.tmp_sinp_familiar_filtrada f
            on p.chave_documento = f.chave_documento

        union all

        select
            cast(null as string) as id_preso,
            f.chave_documento as id_pessoa,
            'VISITA_FAMILIAR' as origem,
            f.cod_documento_referencia,
            f.desc_documento_referencia,
            f.documento_formatado as documento,
            f.nome_pessoa,
            cast(null as string) as sexo_pessoa,
            0 as flag_presidiario,
            0 as flag_advogado,
            0 as flag_servidor,
            1 as flag_visitante,
            0 as flag_ocorrencia_10d,
            0 as flag_ocorrencia_30d,
            0 as flag_ocorrencia_60d,
            cast(null as date) as data_nascimento_pessoa,
            cast(null as date) as data_ultima_prisao,
            cast(null as string) as nome_mae,
            cast(null as string) as nome_pai,
            cast(null as string) as etnia,
            cast(null as string) as vulgo,
            cast(null as string) as estado_civil,
            cast(null as string) as escolaridade,
            cast(null as string) as profissao,
            cast(null as string) as religiao,
            cast(null as int) as flag_tem_filhos,
            cast(null as string) as quantidade_filhos,
            cast(null as string) as naturalidade_municipio,
            cast(null as string) as naturalidade_uf,
            cast(null as string) as pais_origem_estrangeiro,
            cast(null as string) as cidade_origem_estrangeiro,
            cast(null as string) as necessidade_especial,
            cast(null as int) as flag_sabe_ler,
            cast(null as int) as flag_sabe_escrever,
            cast(null as int) as flag_recebe_visita,
            cast(null as int) as flag_relacao_conjugal
        from gold.tmp_sinp_familiar_filtrada f
        left join gold.tmp_sinp_pessoa_04_base_doc p
            on p.chave_documento = f.chave_documento
        where p.chave_documento is null
    """)

    df_tmp_sinp_pessoa_04_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_04_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 33 - RELIGIOSA FILTRADA
    # ============================================================

    tabela = "tmp_sinp_religiosa_filtrada"

    df_tmp_sinp_religiosa_filtrada = spark.sql(r"""
        select
            id_visitante_religioso_origem,
            nome_pessoa,
            instituicao,
            documento_original,
            documento_digitos,
            cpf_normalizado,
            documento_normalizado,
            chave_documento,
            presidio_id_origem,

            case
                when length(documento_digitos) between 1 and 11
                    then concat(substr(cpf_normalizado, 1, 3), '.', substr(cpf_normalizado, 4, 3), '.', substr(cpf_normalizado, 7, 3), '-', substr(cpf_normalizado, 10, 2))
                else documento_original
            end as documento_formatado,

            case when length(documento_digitos) between 1 and 11 then 19 else cast(null as int) end as cod_documento_referencia,
            case when length(documento_digitos) between 1 and 11 then 'CPF' else 'DOCUMENTO' end as desc_documento_referencia

        from (
            select
                *,
                row_number() over (
                    partition by chave_documento
                    order by
                        case when coalesce(nome_pessoa, '') <> '' then 1 else 2 end,
                        case when coalesce(instituicao, '') <> '' then 1 else 2 end,
                        case when coalesce(presidio_id_origem, '') <> '' then 1 else 2 end,
                        id_visitante_religioso_origem desc
                ) as rn
            from (
                select
                    cast(id as string) as id_visitante_religioso_origem,
                    trim(regexp_replace(coalesce(nome, ''), '\\s+', ' ')) as nome_pessoa,
                    trim(regexp_replace(coalesce(instituicao, ''), '\\s+', ' ')) as instituicao,
                    trim(regexp_replace(coalesce(documento, ''), '\\s+', ' ')) as documento_original,
                    cast(presidio_id as string) as presidio_id_origem,

                    regexp_replace(regexp_replace(cast(documento as string), '\\.0+$', ''), '[^0-9]', '') as documento_digitos,
                    lpad(regexp_replace(regexp_replace(cast(documento as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0') as cpf_normalizado,
                    upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', '')) as documento_normalizado,

                    case
                        when length(regexp_replace(regexp_replace(cast(documento as string), '\\.0+$', ''), '[^0-9]', '')) between 1 and 11
                            then concat('CPF_', lpad(regexp_replace(regexp_replace(cast(documento as string), '\\.0+$', ''), '[^0-9]', ''), 11, '0'))
                        else concat('DOC_', upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', '')))
                    end as chave_documento

                from bronze.livros_acesso_unidade_visitareligiosa
            ) b
        ) x
        where rn = 1
          and chave_documento is not null
          and chave_documento <> 'DOC_'
          and chave_documento <> 'CPF_00000000000'
    """)

    df_tmp_sinp_religiosa_filtrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_religiosa_filtrada, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 34 - PESSOA BASE DOC RELIGIOSA
    # ============================================================

    tabela = "tmp_sinp_pessoa_05_base_doc"

    df_tmp_sinp_pessoa_05_base_doc = spark.sql(r"""
        select
            p.*,
            regexp_replace(coalesce(p.documento, ''), '[^0-9]', '') as documento_digitos,
            case
                when length(regexp_replace(coalesce(p.documento, ''), '[^0-9]', '')) between 1 and 11
                    then concat('CPF_', lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0'))
                else concat('DOC_', upper(regexp_replace(coalesce(p.documento, ''), '[^0-9A-Za-z]', '')))
            end as chave_documento
        from gold.tmp_sinp_pessoa_04_familiar p
    """)

    df_tmp_sinp_pessoa_05_base_doc.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_05_base_doc, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 35 - PESSOA APÓS RELIGIOSA
    # ============================================================

    tabela = "tmp_sinp_pessoa_05_religiosa"

    df_tmp_sinp_pessoa_05_religiosa = spark.sql(r"""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            p.flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_05_base_doc p
        left join gold.tmp_sinp_religiosa_filtrada r
            on p.chave_documento = r.chave_documento
        where r.chave_documento is null

        union all

        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            case when p.cod_documento_referencia is not null then p.cod_documento_referencia else r.cod_documento_referencia end as cod_documento_referencia,
            case when coalesce(p.desc_documento_referencia, '') <> '' then p.desc_documento_referencia else r.desc_documento_referencia end as desc_documento_referencia,
            case when coalesce(p.documento, '') <> '' then p.documento else r.documento_formatado end as documento,
            case when coalesce(p.nome_pessoa, '') <> '' then p.nome_pessoa else r.nome_pessoa end as nome_pessoa,
            p.sexo_pessoa,
            p.flag_presidiario,
            p.flag_advogado,
            p.flag_servidor,
            1 as flag_visitante,
            p.flag_ocorrencia_10d,
            p.flag_ocorrencia_30d,
            p.flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia,
            p.vulgo,
            p.estado_civil,
            p.escolaridade,
            p.profissao,
            p.religiao,
            p.flag_tem_filhos,
            p.quantidade_filhos,
            p.naturalidade_municipio,
            p.naturalidade_uf,
            p.pais_origem_estrangeiro,
            p.cidade_origem_estrangeiro,
            p.necessidade_especial,
            p.flag_sabe_ler,
            p.flag_sabe_escrever,
            p.flag_recebe_visita,
            p.flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_05_base_doc p
        inner join gold.tmp_sinp_religiosa_filtrada r
            on p.chave_documento = r.chave_documento

        union all

        select
            cast(null as string) as id_preso,
            r.chave_documento as id_pessoa,
            'VISITA_RELIGIOSA' as origem,
            r.cod_documento_referencia,
            r.desc_documento_referencia,
            r.documento_formatado as documento,
            r.nome_pessoa,
            cast(null as string) as sexo_pessoa,
            0 as flag_presidiario,
            0 as flag_advogado,
            0 as flag_servidor,
            1 as flag_visitante,
            0 as flag_ocorrencia_10d,
            0 as flag_ocorrencia_30d,
            0 as flag_ocorrencia_60d,
            cast(null as date) as data_nascimento_pessoa,
            cast(null as date) as data_ultima_prisao,
            cast(null as string) as nome_mae,
            cast(null as string) as nome_pai,
            cast(null as string) as etnia,
            cast(null as string) as vulgo,
            cast(null as string) as estado_civil,
            cast(null as string) as escolaridade,
            cast(null as string) as profissao,
            cast(null as string) as religiao,
            cast(null as int) as flag_tem_filhos,
            cast(null as string) as quantidade_filhos,
            cast(null as string) as naturalidade_municipio,
            cast(null as string) as naturalidade_uf,
            cast(null as string) as pais_origem_estrangeiro,
            cast(null as string) as cidade_origem_estrangeiro,
            cast(null as string) as necessidade_especial,
            cast(null as int) as flag_sabe_ler,
            cast(null as int) as flag_sabe_escrever,
            cast(null as int) as flag_recebe_visita,
            cast(null as int) as flag_relacao_conjugal
        from gold.tmp_sinp_religiosa_filtrada r
        left join gold.tmp_sinp_pessoa_05_base_doc p
            on p.chave_documento = r.chave_documento
        where p.chave_documento is null
    """)

    df_tmp_sinp_pessoa_05_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_pessoa_05_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 36 - PUBLICAÇÃO FINAL SEM HASH
    # ============================================================

    tabela = "sinp_ent_pessoa"

    df_sinp_ent_pessoa = spark.sql(r"""
        select
            id_preso,
            id_pessoa,
            origem,
            cod_documento_referencia,
            desc_documento_referencia,
            documento,
            nome_pessoa,
            sexo_pessoa,
            flag_presidiario,
            flag_advogado,
            flag_servidor,
            flag_visitante,
            flag_ocorrencia_10d,
            flag_ocorrencia_30d,
            flag_ocorrencia_60d,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_05_religiosa
    """)

    df_sinp_ent_pessoa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_ent_pessoa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    tabela = "sinp_ent_pessoa_outras"

    df_sinp_ent_pessoa_outras = spark.sql(r"""
        select
            id_preso,
            id_preso_original,
            id_pessoa,
            origem,
            cod_documento_referencia,
            desc_documento_referencia,
            documento,
            nome_pessoa,
            sexo_pessoa,
            flag_presidiario,
            flag_advogado,
            flag_servidor,
            flag_visitante,
            flag_ocorrencia_10d,
            flag_ocorrencia_30d,
            flag_ocorrencia_60d,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal,
            rn_pessoa,
            qtd_mesmo_id_pessoa,
            motivo_exclusao
        from gold.tmp_sinp_pessoa_outras_aberta
    """)

    df_sinp_ent_pessoa_outras.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_ent_pessoa_outras, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    tabela = "sinp_pnt_pessoa_preso"

    df_sinp_pnt_pessoa_preso = spark.sql(r"""
        select
            id_preso,
            id_pessoa,
            nome_pessoa
        from gold.tmp_sinp_pnt_pessoa_preso_aberta
    """)

    df_sinp_pnt_pessoa_preso.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_pnt_pessoa_preso, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    # ============================================================
    # 37 - CORREÇÃO DO HASH FINAL
    # UPDATE NÃO SUPORTADO NO SPARK/HIVE DESSE AMBIENTE
    # REGRAVAÇÃO FINAL DAS 3 TABELAS COM HASH APLICADO
    # ============================================================

    tabela = "sinp_ent_pessoa"

    df_sinp_ent_pessoa = spark.sql(r"""
        select
            id_preso,

            case
                when id_pessoa is null then cast(null as string)
                else upper(substr(md5(trim(cast(id_pessoa as string))), 1, 30))
            end as id_pessoa,

            origem,
            cod_documento_referencia,
            desc_documento_referencia,
            documento,
            nome_pessoa,
            sexo_pessoa,
            flag_presidiario,
            flag_advogado,
            flag_servidor,
            flag_visitante,
            flag_ocorrencia_10d,
            flag_ocorrencia_30d,
            flag_ocorrencia_60d,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal
        from gold.tmp_sinp_pessoa_05_religiosa
    """)

    df_sinp_ent_pessoa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_ent_pessoa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_ent_pessoa")


    tabela = "sinp_ent_pessoa_outras"

    df_sinp_ent_pessoa_outras = spark.sql(r"""
        select
            id_preso,
            id_preso_original,

            case
                when id_pessoa is null then cast(null as string)
                else upper(substr(md5(trim(cast(id_pessoa as string))), 1, 30))
            end as id_pessoa,

            origem,
            cod_documento_referencia,
            desc_documento_referencia,
            documento,
            nome_pessoa,
            sexo_pessoa,
            flag_presidiario,
            flag_advogado,
            flag_servidor,
            flag_visitante,
            flag_ocorrencia_10d,
            flag_ocorrencia_30d,
            flag_ocorrencia_60d,
            data_nascimento_pessoa,
            data_ultima_prisao,
            nome_mae,
            nome_pai,
            etnia,
            vulgo,
            estado_civil,
            escolaridade,
            profissao,
            religiao,
            flag_tem_filhos,
            quantidade_filhos,
            naturalidade_municipio,
            naturalidade_uf,
            pais_origem_estrangeiro,
            cidade_origem_estrangeiro,
            necessidade_especial,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal,
            rn_pessoa,
            qtd_mesmo_id_pessoa,
            motivo_exclusao
        from gold.tmp_sinp_pessoa_outras_aberta
    """)

    df_sinp_ent_pessoa_outras.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_ent_pessoa_outras, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_ent_pessoa_outras")


    tabela = "sinp_pnt_pessoa_preso"

    df_sinp_pnt_pessoa_preso = spark.sql(r"""
        select
            id_preso,

            case
                when id_pessoa is null then cast(null as string)
                else upper(substr(md5(trim(cast(id_pessoa as string))), 1, 30))
            end as id_pessoa,

            nome_pessoa
        from gold.tmp_sinp_pnt_pessoa_preso_aberta
    """)

    df_sinp_pnt_pessoa_preso.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_pnt_pessoa_preso, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_pnt_pessoa_preso")


    # ============================================================
    # 38 - ENVIO FINAL PARA POSTGRES
    # ============================================================

    enviar_gold_para_postgres("gold.sinp_ent_pessoa", "id_pessoa")
    enviar_gold_para_postgres("gold.sinp_ent_pessoa_outras", "id_preso")
    enviar_gold_para_postgres("gold.sinp_pnt_pessoa_preso", "id_preso")


    # ============================================================
    # 39 - LIMPEZA FINAL DAS TEMPORÁRIAS
    # ============================================================

    temporarias = [
        "tmp_sinp_pessoa_docs_base",
        "tmp_sinp_pessoa_classificacao_doc",
        "tmp_sinp_pessoa_filiacao",
        "tmp_sinp_pessoa_etnia",
        "tmp_sinp_pessoa_vulgo",
        "tmp_sinp_pessoa_estado_civil",
        "tmp_sinp_pessoa_ficha_social",
        "tmp_sinp_pessoa_escolaridade",
        "tmp_sinp_pessoa_profissao",
        "tmp_sinp_pessoa_religiao",
        "tmp_sinp_pessoa_qtd_filhos",
        "tmp_sinp_pessoa_naturalidade",
        "tmp_sinp_pessoa_nacionalidade_estr",
        "tmp_sinp_pessoa_prontuario_social",
        "tmp_sinp_pessoa_base_documento",
        "tmp_sinp_pessoa_melhor_preso",
        "tmp_sinp_pessoa_dedup",
        "tmp_sinp_pessoa_base_final",
        "tmp_sinp_pessoa_00_presidiario",
        "tmp_sinp_pessoa_outras_aberta",
        "tmp_sinp_pnt_pessoa_preso_aberta",
        "tmp_sinp_siarhes_filtrada",
        "tmp_sinp_pessoa_01_base_cpf",
        "tmp_sinp_pessoa_01_siarhes",
        "tmp_sinp_cavi_filtrada",
        "tmp_sinp_pessoa_02_base_cpf",
        "tmp_sinp_pessoa_02_cavi",
        "tmp_sinp_advogado_filtrada",
        "tmp_sinp_pessoa_03_base_adv",
        "tmp_sinp_pessoa_03_advogado",
        "tmp_sinp_familiar_filtrada",
        "tmp_sinp_pessoa_04_base_doc",
        "tmp_sinp_pessoa_04_familiar",
        "tmp_sinp_religiosa_filtrada",
        "tmp_sinp_pessoa_05_base_doc",
        "tmp_sinp_pessoa_05_religiosa",
    ]

    for tabela in temporarias:
        spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    spark.catalog.clearCache()