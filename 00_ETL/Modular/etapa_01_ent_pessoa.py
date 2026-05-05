# -*- coding: utf-8 -*-
"""Entidade pessoa e bases auxiliares de advogado, familiar, religiosa, SIARHES e CAVI."""

from contexto import *

def executar(spark, path=path):
    """Etapa extraída do notebook original."""
    # ===== CELL 3 =====
    spark.sql("REFRESH TABLE bronze.infopen_preso_documentos")
    spark.sql("REFRESH TABLE bronze.infopen_documentos_tipos")
    spark.sql("REFRESH TABLE bronze.infopen_presos")
    spark.sql("REFRESH TABLE bronze.infopen_preso_filiacao")
    spark.sql("REFRESH TABLE bronze.infopen_preso_cor_pele_etnia")
    spark.sql("REFRESH TABLE bronze.infopen_cor_pele_etnia")

    spark.sql("REFRESH TABLE bronze.infopen_social_estado_civil")
    spark.sql("REFRESH TABLE bronze.infopen_estado_civil")
    spark.sql("REFRESH TABLE bronze.infopen_social_escolaridade")
    spark.sql("REFRESH TABLE bronze.infopen_escolaridade")
    spark.sql("REFRESH TABLE bronze.infopen_grau_instrucao")
    spark.sql("REFRESH TABLE bronze.infopen_presos_profissao")
    spark.sql("REFRESH TABLE bronze.infopen_profissao")
    spark.sql("REFRESH TABLE bronze.infopen_social_religiao")
    spark.sql("REFRESH TABLE bronze.infopen_religiao")
    spark.sql("REFRESH TABLE bronze.infopen_social_quantidadefilho")
    spark.sql("REFRESH TABLE bronze.infopen_preso_quantidadefilho")
    spark.sql("REFRESH TABLE bronze.infopen_preso_naturalidade")
    spark.sql("REFRESH TABLE bronze.infopen_geral_municipios")
    spark.sql("REFRESH TABLE bronze.infopen_preso_nacionalidade_estrangeiro")
    spark.sql("REFRESH TABLE bronze.infopen_necessidade_especial")
    spark.sql("REFRESH TABLE bronze.infopen_ficha_social")
    spark.sql("REFRESH TABLE bronze.infopen_preso_prontuario_social_corr")

    spark.catalog.clearCache()

    base_sql = """
    with docs_base as (
        select
            cast(d.id_preso as string) as id_preso,
            d.id_documentotipo,
            regexp_replace(coalesce(d.presodocumento_numero, ''), '[^0-9]', '') as id_documento_limpo
        from bronze.infopen_preso_documentos d
        where d.presodocumento_numero is not null
          and regexp_replace(coalesce(d.presodocumento_numero, ''), '[^0-9]', '') <> ''
          and cast(regexp_replace(coalesce(d.presodocumento_numero, ''), '[^0-9]', '') as bigint) >= 1000
    ),

    classificacao as (
        select
            id_preso,
            max(case when id_documentotipo in (18,19) then 1 else 0 end) as tem_doc_nacional,
            max(case when id_documentotipo = 26 then 1 else 0 end) as tem_passaporte
        from docs_base
        group by id_preso
    ),

    filiacao as (
        select
            cast(id_preso as string) as id_preso,
            max(
                case
                    when upper(regexp_replace(coalesce(presofiliacao_mae, ''), '\\\\s+', ' ')) rlike ' OU '
                        then trim(
                            regexp_extract(
                                regexp_replace(coalesce(presofiliacao_mae, ''), '\\\\s+', ' '),
                                '^(.*?)\\\\s+(?i:OU)\\\\s+.*$',
                                1
                            )
                        )
                    else trim(regexp_replace(coalesce(presofiliacao_mae, ''), '\\\\s+', ' '))
                end
            ) as nome_mae,
            max(
                case
                    when upper(regexp_replace(coalesce(presofiliacao_pai, ''), '\\\\s+', ' ')) rlike ' OU '
                        then trim(
                            regexp_extract(
                                regexp_replace(coalesce(presofiliacao_pai, ''), '\\\\s+', ' '),
                                '^(.*?)\\\\s+(?i:OU)\\\\s+.*$',
                                1
                            )
                        )
                    else trim(regexp_replace(coalesce(presofiliacao_pai, ''), '\\\\s+', ' '))
                end
            ) as nome_pai
        from bronze.infopen_preso_filiacao
        group by cast(id_preso as string)
    ),

    etnia_base as (
        select
            cast(cpe.id_preso as string) as id_preso,
            concat_ws(', ', collect_set(cor.corpeleetnia_descricao)) as etnia
        from bronze.infopen_preso_cor_pele_etnia cpe
        inner join bronze.infopen_cor_pele_etnia cor
            on cor.id_corpeleetnia = cpe.id_corpeleetnia
        group by cast(cpe.id_preso as string)
    ),

    estado_civil_social_rank as (
        select
            cast(s.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(ec.estadocivil_descricao, ''), '\\\\s+', ' ')) as estado_civil_social,
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
    ),

    estado_civil_social_base as (
        select
            id_preso,
            estado_civil_social
        from estado_civil_social_rank
        where rn = 1
    ),

    ficha_social_rank as (
        select
            cast(fs.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(ec.estadocivil_descricao, ''), '\\\\s+', ' ')) as estado_civil_ficha,
            trim(regexp_replace(coalesce(gi.grauinstrucao_descricao, ''), '\\\\s+', ' ')) as escolaridade_ficha,
            trim(regexp_replace(coalesce(pr.profissao_descricao, ''), '\\\\s+', ' ')) as profissao_ficha,
            trim(regexp_replace(coalesce(rg.religiao_descricao, ''), '\\\\s+', ' ')) as religiao_ficha,
            trim(regexp_replace(coalesce(ne.necessidadeespecial_descricao, ''), '\\\\s+', ' ')) as necessidade_especial_ficha,
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
    ),

    ficha_social_base as (
        select
            id_preso,
            estado_civil_ficha,
            escolaridade_ficha,
            profissao_ficha,
            religiao_ficha,
            necessidade_especial_ficha
        from ficha_social_rank
        where rn = 1
    ),

    escolaridade_social_rank as (
        select
            cast(s.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(e.escolaridade_descricao, ''), '\\\\s+', ' ')) as escolaridade_social,
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
    ),

    escolaridade_social_base as (
        select
            id_preso,
            escolaridade_social
        from escolaridade_social_rank
        where rn = 1
    ),

    profissao_rank as (
        select
            cast(pf.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(pf.descricao_profissao, p.profissao_descricao, ''), '\\\\s+', ' ')) as profissao_social,
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
    ),

    profissao_base as (
        select
            id_preso,
            profissao_social
        from profissao_rank
        where rn = 1
    ),

    religiao_social_rank as (
        select
            cast(s.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(r.religiao_descricao, ''), '\\\\s+', ' ')) as religiao_social,
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
    ),

    religiao_social_base as (
        select
            id_preso,
            religiao_social
        from religiao_social_rank
        where rn = 1
    ),

    quantidade_filho_rank as (
        select
            cast(s.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(q.presoquantidadefilho_descricao, ''), '\\\\s+', ' ')) as quantidade_filhos,
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
    ),

    quantidade_filho_base as (
        select
            id_preso,
            quantidade_filhos
        from quantidade_filho_rank
        where rn = 1
    ),

    naturalidade_base as (
        select
            cast(n.id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(m.municipio_nome, ''), '\\\\s+', ' ')) as naturalidade_municipio,
            trim(regexp_replace(coalesce(m.municipio_siglauf, ''), '\\\\s+', ' ')) as naturalidade_uf
        from bronze.infopen_preso_naturalidade n
        left join bronze.infopen_geral_municipios m
            on m.id_municipio = n.id_municipio
    ),

    nacionalidade_estrangeiro_base as (
        select
            cast(id_preso as string) as id_preso,
            trim(regexp_replace(coalesce(presonacionalidadeestrangeiro_paisorigem, ''), '\\\\s+', ' ')) as pais_origem_estrangeiro,
            trim(regexp_replace(coalesce(presonacionalidadeestrangeiro_cidade, ''), '\\\\s+', ' ')) as cidade_origem_estrangeiro
        from bronze.infopen_preso_nacionalidade_estrangeiro
    ),

    prontuario_social_rank as (
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
    ),

    prontuario_social_base as (
        select
            id_preso,
            flag_tem_filhos_prontuario,
            flag_sabe_ler,
            flag_sabe_escrever,
            flag_recebe_visita,
            flag_relacao_conjugal
        from prontuario_social_rank
        where rn = 1
    ),

    base_documento as (
        select
            d.id_preso,
            d.id_documentotipo,
            dt.documentotipo_descricao,
            d.id_documento_limpo,
            c.tem_doc_nacional,
            c.tem_passaporte,
            case
                when upper(regexp_replace(coalesce(p.preso_nome, ''), '\\\\s+', ' ')) rlike ' OU '
                    then trim(
                        regexp_extract(
                            regexp_replace(coalesce(p.preso_nome, ''), '\\\\s+', ' '),
                            '^(.*?)\\\\s+(?i:OU)\\\\s+.*$',
                            1
                        )
                    )
                else trim(regexp_replace(coalesce(p.preso_nome, ''), '\\\\s+', ' '))
            end as nome_pessoa,
            p.preso_sexo as sexo_pessoa,
            p.preso_datanascimento as data_nascimento_pessoa,
            p.preso_dataultimaprisao as data_ultima_prisao,
            f.nome_mae,
            f.nome_pai,
            e.etnia,

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
                order by case
                    when d.id_documentotipo = 19 then 1
                    when d.id_documentotipo = 18 then 2
                    when d.id_documentotipo = 26 then 3
                    else 4
                end,
                length(d.id_documento_limpo) desc,
                d.id_documento_limpo desc
            ) as rn_doc
        from docs_base d
        inner join bronze.infopen_presos p
            on cast(p.id_preso as string) = d.id_preso
        inner join classificacao c
            on c.id_preso = d.id_preso
        left join bronze.infopen_documentos_tipos dt
            on dt.id_documentotipo = d.id_documentotipo
        left join filiacao f
            on f.id_preso = d.id_preso
        left join etnia_base e
            on e.id_preso = d.id_preso
        left join estado_civil_social_base ec
            on ec.id_preso = d.id_preso
        left join ficha_social_base fs
            on fs.id_preso = d.id_preso
        left join escolaridade_social_base es
            on es.id_preso = d.id_preso
        left join profissao_base pr
            on pr.id_preso = d.id_preso
        left join religiao_social_base rg
            on rg.id_preso = d.id_preso
        left join quantidade_filho_base qf
            on qf.id_preso = d.id_preso
        left join naturalidade_base nat
            on nat.id_preso = d.id_preso
        left join nacionalidade_estrangeiro_base estr
            on estr.id_preso = d.id_preso
        left join prontuario_social_base ps
            on ps.id_preso = d.id_preso
    ),

    melhor_por_preso as (
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
        from base_documento
        where rn_doc = 1
    ),

    dedup_id_pessoa as (
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
        from melhor_por_preso
    ),

    base_final as (
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
        from dedup_id_pessoa
    )

    select * from base_final
    """

    df_base_pessoa = spark.sql(base_sql)
    df_base_pessoa.createOrReplaceTempView("vw_base_pessoa_dedup")

    df_pres = spark.sql("""
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

    from vw_base_pessoa_dedup
    where rn_pessoa = 1
    """)

    df_pres_outras = spark.sql("""
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
    from vw_base_pessoa_dedup
    where rn_pessoa > 1
    """)

    df_ponte = spark.sql("""
    select distinct id_preso, id_pessoa, nome_pessoa
    from vw_base_pessoa_dedup
    """)

    tabela = "sinp_ent_pes_p1"
    df_pres.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pres, "gold", tabela, f"{path}{tabela}")


    # ===== CELL 4 =====
    # ============================================================
    # BASE SIARHES
    # ============================================================

    df_base_siarhes = spark.sql("""
        select
            lpad(
                regexp_replace(
                    regexp_replace(cast(cpf as string), '\\\\.0+$', ''),
                    '[^0-9]',
                    ''
                ),
                11,
                '0'
            ) as cpf_normalizado,
            trim(regexp_replace(coalesce(nome_servidor, ''), '\\\\s+', ' ')) as nome_pessoa,
            trim(regexp_replace(coalesce(cast(rg as string), ''), '\\\\s+', ' ')) as rg,
            dt_extracao,
            numero_funcional,
            cargo,
            categoria,
            subcategoria,
            situacao,
            tipo_vinculo,
            subempresa,
            row_number() over (
                partition by lpad(
                    regexp_replace(
                        regexp_replace(cast(cpf as string), '\\\\.0+$', ''),
                        '[^0-9]',
                        ''
                    ),
                    11,
                    '0'
                )
                order by
                    case when dt_extracao is not null then 1 else 2 end,
                    dt_extracao desc,
                    case when coalesce(nome_servidor, '') <> '' then 1 else 2 end,
                    case when coalesce(cast(rg as string), '') <> '' then 1 else 2 end,
                    cast(numero_funcional as string) desc
            ) as rn
        from bronze.siarhes_servidores
    """)

    tabela = "tmp_base_siarhes"

    df_base_siarhes.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_siarhes, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE SIARHES FILTRADA
    # ============================================================

    df_filtrada_siarhes = spark.sql("""
        select
            cpf_normalizado,
            concat(
                substr(cpf_normalizado, 1, 3), '.',
                substr(cpf_normalizado, 4, 3), '.',
                substr(cpf_normalizado, 7, 3), '-',
                substr(cpf_normalizado, 10, 2)
            ) as cpf_formatado,
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
        from gold.tmp_base_siarhes
        where rn = 1
          and cpf_normalizado is not null
          and cpf_normalizado <> ''
          and cpf_normalizado <> '00000000000'
          and length(cpf_normalizado) = 11
    """)

    tabela = "tmp_filtrada_siarhes"

    df_filtrada_siarhes.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_filtrada_siarhes, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE PESSOA ATUAL PARA CRUZAMENTO
    # ============================================================

    df_base_pessoa_atual = spark.sql("""
        select
            p.*,
            case
                when p.cod_documento_referencia = 19 then
                    lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0')
                else null
            end as cpf_normalizado
        from gold.sinp_ent_pes_p1 p
    """)

    tabela = "tmp_base_pessoa_atual"

    df_base_pessoa_atual.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_pessoa_atual, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # PESSOAS ATUALIZADAS COM FLAG_SERVIDOR = 1
    # ============================================================

    df_pessoa_atualizada_servidor = spark.sql("""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            p.sexo_pessoa,
            coalesce(p.flag_presidiario, 0) as flag_presidiario,
            coalesce(p.flag_advogado, 0) as flag_advogado,
            1 as flag_servidor,
            coalesce(p.flag_visitante, 0) as flag_visitante,
            coalesce(p.flag_ocorrencia_10d, 0) as flag_ocorrencia_10d,
            coalesce(p.flag_ocorrencia_30d, 0) as flag_ocorrencia_30d,
            coalesce(p.flag_ocorrencia_60d, 0) as flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia
        from gold.tmp_base_pessoa_atual p
        inner join gold.tmp_filtrada_siarhes s
            on p.cpf_normalizado = s.cpf_normalizado
    """)

    tabela = "tmp_pessoa_atualizada_servidor"

    df_pessoa_atualizada_servidor.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_atualizada_servidor, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # PESSOAS MANTIDAS
    # ============================================================

    df_pessoa_mantida = spark.sql("""
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
            p.etnia
        from gold.tmp_base_pessoa_atual p
        left join gold.tmp_filtrada_siarhes s
            on p.cpf_normalizado = s.cpf_normalizado
        where s.cpf_normalizado is null
    """)

    tabela = "tmp_pessoa_mantida"

    df_pessoa_mantida.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_mantida, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # NOVAS PESSOAS VINDAS DO SIARHES
    # ============================================================

    df_pessoa_nova_servidor = spark.sql("""
        select
            cast(null as string) as id_preso,
            concat('CPF_', s.cpf_normalizado) as id_pessoa,
            'SERVIDOR' as origem,
            19 as cod_documento_referencia,
            'CPF' as desc_documento_referencia,
            s.cpf_formatado as documento,
            s.nome_pessoa,
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
            cast(null as string) as etnia
        from gold.tmp_filtrada_siarhes s
        left join gold.tmp_base_pessoa_atual p
            on p.cpf_normalizado = s.cpf_normalizado
        where p.cpf_normalizado is null
    """)

    tabela = "tmp_pessoa_nova_servidor"

    df_pessoa_nova_servidor.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_nova_servidor, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # SAIDA FINAL ATE O UNION DOS MANTIDOS
    # ============================================================

    df_pessoa_shiares = spark.sql("""
        select * from gold.tmp_pessoa_mantida
        union all
        select * from gold.tmp_pessoa_atualizada_servidor
        union all
        select * from gold.tmp_pessoa_nova_servidor
    """)

    tabela = "sinp_ent_pessoa_shiares"

    df_pessoa_shiares.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pessoa_shiares, "gold", tabela, f"{path}{tabela}")


    # ===== CELL 5 =====
    # ============================================================
    # BASE CAVI NORMALIZADA
    # ============================================================

    df_base_cavi = spark.sql("""
        select
            cast(itemnum as string) as itemnum,
            trim(regexp_replace(coalesce(cast(numeroonbase as string), ''), '\\\\s+', ' ')) as numeroonbase,
            trim(regexp_replace(coalesce(statussolicitacao, ''), '\\\\s+', ' ')) as statussolicitacao,
            trim(regexp_replace(coalesce(resultanalise, ''), '\\\\s+', ' ')) as resultanalise,

            trim(regexp_replace(coalesce(cast(telcelular as string), ''), '\\\\s+', ' ')) as telcelular,
            trim(regexp_replace(coalesce(nomeinteressado, ''), '\\\\s+', ' ')) as nome_pessoa,
            lower(trim(regexp_replace(coalesce(emailinteressado, ''), '\\\\s+', ' '))) as emailinteressado,

            lpad(
                regexp_replace(
                    regexp_replace(cast(cpfinteressado as string), '\\\\.0+$', ''),
                    '[^0-9]',
                    ''
                ),
                11,
                '0'
            ) as cpf_normalizado,

            trim(regexp_replace(coalesce(cast(telresidencial as string), ''), '\\\\s+', ' ')) as telresidencial,

            trim(
                regexp_replace(
                    regexp_replace(coalesce(cast(rginteressado as string), ''), '\\\\.0+$', ''),
                    '\\\\s+',
                    ' '
                )
            ) as rg,

            trim(regexp_replace(coalesce(orgaoemissor, ''), '\\\\s+', ' ')) as orgaoemissor,
            trim(regexp_replace(coalesce(estadoemissor, ''), '\\\\s+', ' ')) as estadoemissor,
            trim(regexp_replace(coalesce(sexo, ''), '\\\\s+', ' ')) as sexo_pessoa,
            trim(regexp_replace(coalesce(profissao, ''), '\\\\s+', ' ')) as profissao,

            lpad(
                regexp_replace(
                    regexp_replace(cast(cpfdentento as string), '\\\\.0+$', ''),
                    '[^0-9]',
                    ''
                ),
                11,
                '0'
            ) as cpf_detento_normalizado,

            trim(regexp_replace(coalesce(nomedetento, ''), '\\\\s+', ' ')) as nome_detento,

            trim(
                regexp_replace(
                    regexp_replace(coalesce(cast(rgdetento as string), ''), '\\\\.0+$', ''),
                    '\\\\s+',
                    ' '
                )
            ) as rg_detento,

            trim(regexp_replace(coalesce(nomemae, ''), '\\\\s+', ' ')) as nome_mae_detento,
            trim(regexp_replace(coalesce(tipovisita, ''), '\\\\s+', ' ')) as tipovisita,
            trim(regexp_replace(coalesce(vinculodetento, ''), '\\\\s+', ' ')) as vinculodetento,
            trim(regexp_replace(coalesce(grauparentesco, ''), '\\\\s+', ' ')) as grauparentesco,
            trim(regexp_replace(coalesce(ctprocesso, ''), '\\\\s+', ' ')) as ctprocesso,
            trim(regexp_replace(coalesce(ctfilaatual, ''), '\\\\s+', ' ')) as ctfilaatual,
            trim(regexp_replace(coalesce(ctfilaorigem, ''), '\\\\s+', ' ')) as ctfilaorigem,
            trim(regexp_replace(coalesce(setorresponsavel, ''), '\\\\s+', ' ')) as setorresponsavel,
            trim(regexp_replace(coalesce(unidadeprisional, ''), '\\\\s+', ' ')) as unidadeprisional
        from bronze.obsejus_cc_requerimentoscavi
    """)

    tabela = "tmp_base_cavi"

    df_base_cavi.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_cavi, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE CAVI RANKEADA
    # ============================================================

    df_base_cavi_rank = spark.sql("""
        select
            *,
            row_number() over (
                partition by cpf_normalizado
                order by
                    case when itemnum is not null and itemnum <> '' then 1 else 2 end,
                    cast(itemnum as bigint) desc,
                    case when coalesce(nome_pessoa, '') <> '' then 1 else 2 end,
                    case when coalesce(rg, '') <> '' then 1 else 2 end,
                    case when coalesce(emailinteressado, '') <> '' then 1 else 2 end
            ) as rn
        from gold.tmp_base_cavi
    """)

    tabela = "tmp_base_cavi_rank"

    df_base_cavi_rank.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_cavi_rank, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE CAVI FILTRADA
    # ============================================================

    df_filtrada_cavi = spark.sql("""
        select
            cpf_normalizado,
            concat(
                substr(cpf_normalizado, 1, 3), '.',
                substr(cpf_normalizado, 4, 3), '.',
                substr(cpf_normalizado, 7, 3), '-',
                substr(cpf_normalizado, 10, 2)
            ) as cpf_formatado,
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
        from gold.tmp_base_cavi_rank
        where rn = 1
          and cpf_normalizado is not null
          and cpf_normalizado <> ''
          and cpf_normalizado <> '00000000000'
          and length(cpf_normalizado) = 11
    """)

    tabela = "tmp_filtrada_cavi"

    df_filtrada_cavi.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_filtrada_cavi, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE PESSOA ATUAL PARA CRUZAMENTO
    # ============================================================

    df_base_pessoa_atual = spark.sql("""
        select
            p.*,
            case
                when p.cod_documento_referencia = 19 then
                    lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0')
                else null
            end as cpf_normalizado
        from gold.sinp_ent_pessoa_shiares p
    """)

    tabela = "tmp_base_pessoa_atual_cavi"

    df_base_pessoa_atual.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_pessoa_atual, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # PESSOAS ATUALIZADAS COM FLAG_VISITANTE = 1
    # ============================================================

    df_pessoa_atualizada_visitante = spark.sql("""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            p.desc_documento_referencia,
            p.documento,
            p.nome_pessoa,
            case
                when coalesce(p.sexo_pessoa, '') <> '' then p.sexo_pessoa
                else c.sexo_pessoa
            end as sexo_pessoa,
            coalesce(p.flag_presidiario, 0) as flag_presidiario,
            coalesce(p.flag_advogado, 0) as flag_advogado,
            coalesce(p.flag_servidor, 0) as flag_servidor,
            1 as flag_visitante,
            coalesce(p.flag_ocorrencia_10d, 0) as flag_ocorrencia_10d,
            coalesce(p.flag_ocorrencia_30d, 0) as flag_ocorrencia_30d,
            coalesce(p.flag_ocorrencia_60d, 0) as flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia
        from gold.tmp_base_pessoa_atual_cavi p
        inner join gold.tmp_filtrada_cavi c
            on p.cpf_normalizado = c.cpf_normalizado
    """)

    tabela = "tmp_pessoa_atualizada_visitante"

    df_pessoa_atualizada_visitante.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_atualizada_visitante, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # PESSOAS MANTIDAS
    # ============================================================

    df_pessoa_mantida_visitante = spark.sql("""
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
            p.etnia
        from gold.tmp_base_pessoa_atual_cavi p
        left join gold.tmp_filtrada_cavi c
            on p.cpf_normalizado = c.cpf_normalizado
        where c.cpf_normalizado is null
    """)

    tabela = "tmp_pessoa_mantida_visitante"

    df_pessoa_mantida_visitante.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_mantida_visitante, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # NOVAS PESSOAS VINDAS DO CAVI
    # ============================================================

    df_pessoa_nova_visitante = spark.sql("""
        select
            cast(null as string) as id_preso,
            concat('CPF_', c.cpf_normalizado) as id_pessoa,
            'VISITANTE' as origem,
            19 as cod_documento_referencia,
            'CPF' as desc_documento_referencia,
            c.cpf_formatado as documento,
            c.nome_pessoa,
            c.sexo_pessoa,
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
            cast(null as string) as etnia
        from gold.tmp_filtrada_cavi c
        left join gold.tmp_base_pessoa_atual_cavi p
            on p.cpf_normalizado = c.cpf_normalizado
        where p.cpf_normalizado is null
    """)

    tabela = "tmp_pessoa_nova_visitante"

    df_pessoa_nova_visitante.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_nova_visitante, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # SAIDA FINAL DAS PESSOAS
    # ============================================================

    df_pessoa_final_visitante = spark.sql("""
        select * from gold.tmp_pessoa_mantida_visitante
        union all
        select * from gold.tmp_pessoa_atualizada_visitante
        union all
        select * from gold.tmp_pessoa_nova_visitante
    """)


    tabela = "df_pessoa_nova_visitante"

    df_pessoa_final_visitante.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_final_visitante, "gold", tabela, f"{path}{tabela}")


    # ===== CELL 6 =====
    # ============================================================
    # BASE ADVOGADO
    # ============================================================

    df_base_advogado = spark.sql("""
        select
            cast(id as string) as id_advogado_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_pessoa,
            upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' '))) as estado_oab,
            upper(trim(regexp_replace(coalesce(oab, ''), '\\\\s+', ' '))) as oab_bruta,
            upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', '')) as oab_normalizada,
            concat(
                'OAB_',
                upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' '))),
                '_',
                upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', ''))
            ) as id_pessoa,
            concat(
                upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', '')),
                '/',
                upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' ')))
            ) as documento_oab,
            row_number() over (
                partition by
                    upper(trim(regexp_replace(coalesce(estado, ''), '\\\\s+', ' '))),
                    upper(regexp_replace(coalesce(oab, ''), '[^0-9A-Za-z]', ''))
                order by
                    case when coalesce(nome, '') <> '' then 1 else 2 end,
                    cast(id as string) desc
            ) as rn
        from bronze.livros_acesso_unidade_advogado
    """)

    tabela = "tmp_base_advogado"

    df_base_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_advogado, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE ADVOGADO FILTRADA
    # ============================================================

    df_filtrada_advogado = spark.sql("""
        select
            id_advogado_origem,
            id_pessoa,
            nome_pessoa,
            estado_oab,
            oab_bruta,
            oab_normalizada,
            documento_oab
        from gold.tmp_base_advogado
        where rn = 1
          and estado_oab is not null
          and estado_oab <> ''
          and oab_normalizada is not null
          and oab_normalizada <> ''
    """)

    tabela = "tmp_filtrada_advogado"

    df_filtrada_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_filtrada_advogado, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # BASE PESSOA ATUAL PARA CRUZAMENTO
    # ============================================================

    df_base_pessoa_atual_advogado = spark.sql("""
        select
            p.*,
            case
                when p.id_pessoa like 'OAB_%' then p.id_pessoa
                else null
            end as id_pessoa_advogado
        from gold.df_pessoa_nova_visitante p
    """)

    tabela = "tmp_base_pessoa_atual_advogado"

    df_base_pessoa_atual_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_pessoa_atual_advogado, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # PESSOAS ATUALIZADAS COM FLAG_ADVOGADO = 1
    # ============================================================

    df_pessoa_atualizada_advogado = spark.sql("""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            p.cod_documento_referencia,
            case
                when coalesce(p.desc_documento_referencia, '') <> '' then p.desc_documento_referencia
                else 'OAB'
            end as desc_documento_referencia,
            case
                when coalesce(p.documento, '') <> '' then p.documento
                else a.documento_oab
            end as documento,
            case
                when coalesce(p.nome_pessoa, '') <> '' then p.nome_pessoa
                else a.nome_pessoa
            end as nome_pessoa,
            p.sexo_pessoa,
            coalesce(p.flag_presidiario, 0) as flag_presidiario,
            1 as flag_advogado,
            coalesce(p.flag_servidor, 0) as flag_servidor,
            coalesce(p.flag_visitante, 0) as flag_visitante,
            coalesce(p.flag_ocorrencia_10d, 0) as flag_ocorrencia_10d,
            coalesce(p.flag_ocorrencia_30d, 0) as flag_ocorrencia_30d,
            coalesce(p.flag_ocorrencia_60d, 0) as flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia
        from gold.tmp_base_pessoa_atual_advogado p
        inner join gold.tmp_filtrada_advogado a
            on p.id_pessoa_advogado = a.id_pessoa
    """)

    tabela = "tmp_pessoa_atualizada_advogado"

    df_pessoa_atualizada_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_atualizada_advogado, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # PESSOAS MANTIDAS
    # ============================================================

    df_pessoa_mantida_advogado = spark.sql("""
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
            p.etnia
        from gold.tmp_base_pessoa_atual_advogado p
        left join gold.tmp_filtrada_advogado a
            on p.id_pessoa_advogado = a.id_pessoa
        where a.id_pessoa is null
    """)

    tabela = "tmp_pessoa_mantida_advogado"

    df_pessoa_mantida_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_mantida_advogado, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # NOVAS PESSOAS VINDAS DO CADASTRO DE ADVOGADO
    # ============================================================

    df_pessoa_nova_advogado = spark.sql("""
        select
            cast(null as string) as id_preso,
            a.id_pessoa,
            'ADVOGADO' as origem,
            cast(null as int) as cod_documento_referencia,
            'OAB' as desc_documento_referencia,
            a.documento_oab as documento,
            a.nome_pessoa,
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
            cast(null as string) as etnia
        from gold.tmp_filtrada_advogado a
        left join gold.tmp_base_pessoa_atual_advogado p
            on p.id_pessoa_advogado = a.id_pessoa
        where p.id_pessoa_advogado is null
    """)

    tabela = "tmp_pessoa_nova_advogado"

    df_pessoa_nova_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_nova_advogado, "gold", tabela, f"{path}{tabela}")


    # ============================================================
    # SAIDA FINAL DAS PESSOAS
    # ============================================================

    df_pessoa_final_advogado = spark.sql("""
        select * from gold.tmp_pessoa_mantida_advogado
        union all
        select * from gold.tmp_pessoa_atualizada_advogado
        union all
        select * from gold.tmp_pessoa_nova_advogado
    """)


    tabela = "df_pessoa_final_advogado"

    df_pessoa_final_advogado.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_final_advogado, "gold", tabela, f"{path}{tabela}")


    # ===== CELL 7 =====
    import os

    # ============================================================
    # LIMPEZA DEFENSIVA DAS TABELAS TEMPORARIAS - PESSOAS FAMILIARES
    # ============================================================

    spark.sql("drop table if exists gold.tmp_base_visitafamiliar")
    spark.sql("drop table if exists gold.tmp_base_visitafamiliar_rank")
    spark.sql("drop table if exists gold.tmp_filtrada_visitafamiliar")
    spark.sql("drop table if exists gold.tmp_base_pessoa_atual_familiar")
    spark.sql("drop table if exists gold.tmp_pessoa_atualizada_familiar")
    spark.sql("drop table if exists gold.tmp_pessoa_mantida_familiar")
    spark.sql("drop table if exists gold.tmp_pessoa_nova_familiar")
    spark.sql("drop table if exists gold.df_pessoa_final_familiar")

    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_visitafamiliar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_visitafamiliar_rank >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_filtrada_visitafamiliar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_atual_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_pessoa_atualizada_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_pessoa_mantida_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_pessoa_nova_familiar >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}df_pessoa_final_familiar >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.df_pessoa_final_advogado")


    # ============================================================
    # BASE VISITA FAMILIAR
    # ============================================================

    df_base_visitafamiliar = spark.sql("""
        select
            cast(id as string) as id_vinculo_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_pessoa,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento_original,
            trim(regexp_replace(coalesce(telefone, ''), '\\\\s+', ' ')) as telefone,
            lpad(
                regexp_replace(
                    regexp_replace(cast(documento as string), '\\\\.0+$', ''),
                    '[^0-9]',
                    ''
                ),
                11,
                '0'
            ) as cpf_normalizado,
            upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', '')) as documento_normalizado,
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
            end as chave_documento
        from bronze.livros_acesso_unidade_visitafamiliar
    """)

    tabela = "tmp_base_visitafamiliar"

    df_base_visitafamiliar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_visitafamiliar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_visitafamiliar")


    # ============================================================
    # BASE VISITA FAMILIAR RANKEADA
    # ============================================================

    df_base_visitafamiliar_rank = spark.sql("""
        select
            *,
            row_number() over (
                partition by chave_documento
                order by
                    case when coalesce(nome_pessoa, '') <> '' then 1 else 2 end,
                    case when coalesce(telefone, '') <> '' then 1 else 2 end,
                    id_vinculo_origem desc
            ) as rn
        from gold.tmp_base_visitafamiliar
    """)

    tabela = "tmp_base_visitafamiliar_rank"

    df_base_visitafamiliar_rank.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_visitafamiliar_rank, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_visitafamiliar_rank")


    # ============================================================
    # BASE FAMILIAR FILTRADA
    # ============================================================

    df_filtrada_visitafamiliar = spark.sql("""
        select
            id_vinculo_origem,
            nome_pessoa,
            documento_original,
            telefone,
            cpf_normalizado,
            documento_normalizado,
            chave_documento,
            case
                when chave_documento like 'CPF_%' then concat(
                    substr(cpf_normalizado, 1, 3), '.',
                    substr(cpf_normalizado, 4, 3), '.',
                    substr(cpf_normalizado, 7, 3), '-',
                    substr(cpf_normalizado, 10, 2)
                )
                else documento_original
            end as documento_formatado,
            case
                when chave_documento like 'CPF_%' then 19
                else cast(null as int)
            end as cod_documento_referencia,
            case
                when chave_documento like 'CPF_%' then 'CPF'
                else 'DOCUMENTO'
            end as desc_documento_referencia
        from gold.tmp_base_visitafamiliar_rank
        where rn = 1
          and chave_documento is not null
          and chave_documento <> 'DOC_'
          and chave_documento <> 'CPF_00000000000'
    """)

    tabela = "tmp_filtrada_visitafamiliar"

    df_filtrada_visitafamiliar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_filtrada_visitafamiliar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_filtrada_visitafamiliar")


    # ============================================================
    # BASE PESSOA ATUAL PARA CRUZAMENTO
    # ============================================================

    df_base_pessoa_atual_familiar = spark.sql("""
        select
            p.*,
            case
                when p.cod_documento_referencia = 19 then
                    concat(
                        'CPF_',
                        lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0')
                    )
                else
                    concat(
                        'DOC_',
                        upper(regexp_replace(coalesce(p.documento, ''), '[^0-9A-Za-z]', ''))
                    )
            end as chave_documento
        from gold.df_pessoa_final_advogado p
    """)

    tabela = "tmp_base_pessoa_atual_familiar"

    df_base_pessoa_atual_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_pessoa_atual_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_atual_familiar")


    # ============================================================
    # PESSOAS ATUALIZADAS COM FLAG_VISITANTE = 1
    # ============================================================

    df_pessoa_atualizada_familiar = spark.sql("""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            case
                when p.cod_documento_referencia is not null then p.cod_documento_referencia
                else f.cod_documento_referencia
            end as cod_documento_referencia,
            case
                when coalesce(p.desc_documento_referencia, '') <> '' then p.desc_documento_referencia
                else f.desc_documento_referencia
            end as desc_documento_referencia,
            case
                when coalesce(p.documento, '') <> '' then p.documento
                else f.documento_formatado
            end as documento,
            case
                when coalesce(p.nome_pessoa, '') <> '' then p.nome_pessoa
                else f.nome_pessoa
            end as nome_pessoa,
            p.sexo_pessoa,
            coalesce(p.flag_presidiario, 0) as flag_presidiario,
            coalesce(p.flag_advogado, 0) as flag_advogado,
            coalesce(p.flag_servidor, 0) as flag_servidor,
            1 as flag_visitante,
            coalesce(p.flag_ocorrencia_10d, 0) as flag_ocorrencia_10d,
            coalesce(p.flag_ocorrencia_30d, 0) as flag_ocorrencia_30d,
            coalesce(p.flag_ocorrencia_60d, 0) as flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia
        from gold.tmp_base_pessoa_atual_familiar p
        inner join gold.tmp_filtrada_visitafamiliar f
            on p.chave_documento = f.chave_documento
    """)

    tabela = "tmp_pessoa_atualizada_familiar"

    df_pessoa_atualizada_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_atualizada_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_pessoa_atualizada_familiar")


    # ============================================================
    # PESSOAS MANTIDAS
    # ============================================================

    df_pessoa_mantida_familiar = spark.sql("""
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
            p.etnia
        from gold.tmp_base_pessoa_atual_familiar p
        left join gold.tmp_filtrada_visitafamiliar f
            on p.chave_documento = f.chave_documento
        where f.chave_documento is null
    """)

    tabela = "tmp_pessoa_mantida_familiar"

    df_pessoa_mantida_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_mantida_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_pessoa_mantida_familiar")


    # ============================================================
    # NOVAS PESSOAS VINDAS DA VISITA FAMILIAR
    # ============================================================

    df_pessoa_nova_familiar = spark.sql("""
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
            cast(null as string) as etnia
        from gold.tmp_filtrada_visitafamiliar f
        left join gold.tmp_base_pessoa_atual_familiar p
            on p.chave_documento = f.chave_documento
        where p.chave_documento is null
    """)

    tabela = "tmp_pessoa_nova_familiar"

    df_pessoa_nova_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_nova_familiar, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_pessoa_nova_familiar")


    # ============================================================
    # SAIDA FINAL DAS PESSOAS
    # ============================================================

    df_pessoa_final_familiar = spark.sql("""
        select * from gold.tmp_pessoa_mantida_familiar
        union all
        select * from gold.tmp_pessoa_atualizada_familiar
        union all
        select * from gold.tmp_pessoa_nova_familiar
    """)

    tabela = "df_pessoa_final_familiar"

    df_pessoa_final_familiar.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pessoa_final_familiar, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_pessoa")


    # ============================================================
    # VALIDACAO RAPIDA
    # ============================================================

    spark.sql("""
    select
        count(*) as total_pessoas,
        sum(case when coalesce(flag_visitante, 0) = 1 then 1 else 0 end) as total_visitantes
    from gold.df_pessoa_final_familiar
    """).show(truncate=False)


    # ===== CELL 8 =====
    import os

    # ============================================================
    # LIMPEZA DEFENSIVA DAS TABELAS TEMPORARIAS - PESSOAS VISITA RELIGIOSA
    # ============================================================

    spark.sql("drop table if exists gold.tmp_base_visitareligiosa")
    spark.sql("drop table if exists gold.tmp_base_visitareligiosa_rank")
    spark.sql("drop table if exists gold.tmp_filtrada_visitareligiosa")
    spark.sql("drop table if exists gold.tmp_base_pessoa_atual_religiosa")
    spark.sql("drop table if exists gold.tmp_pessoa_atualizada_religiosa")
    spark.sql("drop table if exists gold.tmp_pessoa_mantida_religiosa")
    spark.sql("drop table if exists gold.tmp_pessoa_nova_religiosa")
    spark.sql("drop table if exists gold.df_pessoa_final_religiosa")

    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_visitareligiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_visitareligiosa_rank >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_filtrada_visitareligiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_base_pessoa_atual_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_pessoa_atualizada_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_pessoa_mantida_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}tmp_pessoa_nova_religiosa >/dev/null 2>&1")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}df_pessoa_final_religiosa >/dev/null 2>&1")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.df_pessoa_final_familiar")


    # ============================================================
    # BASE VISITA RELIGIOSA
    # ============================================================

    df_base_visitareligiosa = spark.sql("""
        select
            cast(id as string) as id_visitante_religioso_origem,
            trim(regexp_replace(coalesce(nome, ''), '\\\\s+', ' ')) as nome_pessoa,
            trim(regexp_replace(coalesce(instituicao, ''), '\\\\s+', ' ')) as instituicao,
            trim(regexp_replace(coalesce(documento, ''), '\\\\s+', ' ')) as documento_original,
            cast(presidio_id as string) as presidio_id_origem,

            regexp_replace(
                regexp_replace(cast(documento as string), '\\\\.0+$', ''),
                '[^0-9]',
                ''
            ) as documento_digitos,

            lpad(
                regexp_replace(
                    regexp_replace(cast(documento as string), '\\\\.0+$', ''),
                    '[^0-9]',
                    ''
                ),
                11,
                '0'
            ) as cpf_normalizado,

            upper(regexp_replace(coalesce(documento, ''), '[^0-9A-Za-z]', '')) as documento_normalizado,

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

    tabela = "tmp_base_visitareligiosa"

    df_base_visitareligiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_visitareligiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_visitareligiosa")


    # ============================================================
    # BASE VISITA RELIGIOSA RANKEADA
    # ============================================================

    df_base_visitareligiosa_rank = spark.sql("""
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
        from gold.tmp_base_visitareligiosa
    """)

    tabela = "tmp_base_visitareligiosa_rank"

    df_base_visitareligiosa_rank.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_visitareligiosa_rank, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_visitareligiosa_rank")


    # ============================================================
    # BASE RELIGIOSA FILTRADA
    # ============================================================

    df_filtrada_visitareligiosa = spark.sql("""
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
                when length(documento_digitos) between 1 and 11 then concat(
                    substr(cpf_normalizado, 1, 3), '.',
                    substr(cpf_normalizado, 4, 3), '.',
                    substr(cpf_normalizado, 7, 3), '-',
                    substr(cpf_normalizado, 10, 2)
                )
                else documento_original
            end as documento_formatado,

            case
                when length(documento_digitos) between 1 and 11 then 19
                else cast(null as int)
            end as cod_documento_referencia,

            case
                when length(documento_digitos) between 1 and 11 then 'CPF'
                else 'DOCUMENTO'
            end as desc_documento_referencia
        from gold.tmp_base_visitareligiosa_rank
        where rn = 1
          and chave_documento is not null
          and chave_documento <> 'DOC_'
          and chave_documento <> 'CPF_00000000000'
    """)

    tabela = "tmp_filtrada_visitareligiosa"

    df_filtrada_visitareligiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_filtrada_visitareligiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_filtrada_visitareligiosa")


    # ============================================================
    # BASE PESSOA ATUAL PARA CRUZAMENTO
    # ============================================================

    df_base_pessoa_atual_religiosa = spark.sql("""
        select
            p.*,
            regexp_replace(coalesce(p.documento, ''), '[^0-9]', '') as documento_digitos,
            case
                when length(regexp_replace(coalesce(p.documento, ''), '[^0-9]', '')) between 1 and 11 then
                    concat(
                        'CPF_',
                        lpad(regexp_replace(coalesce(p.documento, ''), '[^0-9]', ''), 11, '0')
                    )
                else
                    concat(
                        'DOC_',
                        upper(regexp_replace(coalesce(p.documento, ''), '[^0-9A-Za-z]', ''))
                    )
            end as chave_documento
        from gold.df_pessoa_final_familiar p
    """)

    tabela = "tmp_base_pessoa_atual_religiosa"

    df_base_pessoa_atual_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_base_pessoa_atual_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_base_pessoa_atual_religiosa")


    # ============================================================
    # PESSOAS ATUALIZADAS COM FLAG_VISITANTE = 1
    # ============================================================

    df_pessoa_atualizada_religiosa = spark.sql("""
        select
            p.id_preso,
            p.id_pessoa,
            p.origem,
            case
                when p.cod_documento_referencia is not null then p.cod_documento_referencia
                else r.cod_documento_referencia
            end as cod_documento_referencia,
            case
                when coalesce(p.desc_documento_referencia, '') <> '' then p.desc_documento_referencia
                else r.desc_documento_referencia
            end as desc_documento_referencia,
            case
                when coalesce(p.documento, '') <> '' then p.documento
                else r.documento_formatado
            end as documento,
            case
                when coalesce(p.nome_pessoa, '') <> '' then p.nome_pessoa
                else r.nome_pessoa
            end as nome_pessoa,
            p.sexo_pessoa,
            coalesce(p.flag_presidiario, 0) as flag_presidiario,
            coalesce(p.flag_advogado, 0) as flag_advogado,
            coalesce(p.flag_servidor, 0) as flag_servidor,
            1 as flag_visitante,
            coalesce(p.flag_ocorrencia_10d, 0) as flag_ocorrencia_10d,
            coalesce(p.flag_ocorrencia_30d, 0) as flag_ocorrencia_30d,
            coalesce(p.flag_ocorrencia_60d, 0) as flag_ocorrencia_60d,
            p.data_nascimento_pessoa,
            p.data_ultima_prisao,
            p.nome_mae,
            p.nome_pai,
            p.etnia
        from gold.tmp_base_pessoa_atual_religiosa p
        inner join gold.tmp_filtrada_visitareligiosa r
            on p.chave_documento = r.chave_documento
    """)

    tabela = "tmp_pessoa_atualizada_religiosa"

    df_pessoa_atualizada_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pessoa_atualizada_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_pessoa_atualizada_religiosa")


    # ============================================================
    # PESSOAS MANTIDAS
    # ============================================================

    df_pessoa_mantida_religiosa = spark.sql("""
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
            p.etnia
        from gold.tmp_base_pessoa_atual_religiosa p
        left join gold.tmp_filtrada_visitareligiosa r
            on p.chave_documento = r.chave_documento
        where r.chave_documento is null
    """)

    tabela = "tmp_pessoa_mantida_religiosa"

    df_pessoa_mantida_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pessoa_mantida_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_pessoa_mantida_religiosa")


    # ============================================================
    # NOVAS PESSOAS VINDAS DA VISITA RELIGIOSA
    # ============================================================

    df_pessoa_nova_religiosa = spark.sql("""
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
            cast(null as string) as etnia
        from gold.tmp_filtrada_visitareligiosa r
        left join gold.tmp_base_pessoa_atual_religiosa p
            on p.chave_documento = r.chave_documento
        where p.chave_documento is null
    """)

    tabela = "tmp_pessoa_nova_religiosa"

    df_pessoa_nova_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pessoa_nova_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.tmp_pessoa_nova_religiosa")


    # ============================================================
    # SAIDA FINAL DAS PESSOAS
    # ============================================================

    df_pessoa_final_religiosa = spark.sql("""
        select * from gold.tmp_pessoa_mantida_religiosa
        union all
        select * from gold.tmp_pessoa_atualizada_religiosa
        union all
        select * from gold.tmp_pessoa_nova_religiosa
    """)

    tabela = "df_pessoa_final_religiosa"

    #df_pessoa_final_religiosa.write \
    #    .mode("overwrite") \
    #    .option("maxRecordsPerFile", 1_000_000) \
    #    .option("compression", "snappy") \
    #    .parquet(f"{path}{tabela}")
    #write_impala_table_partioned(df_pessoa_final_religiosa, "gold", tabela, f"{path}{tabela}")


    # ===== CELL 9 =====
    tabela = "sinp_ent_pessoa"

    df_pessoa_final_religiosa.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_pessoa_final_religiosa, "gold", tabela, f"{path}{tabela}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")


    df_pessoa_final_religiosa_pg = df_pessoa_final_religiosa.withColumn(
        "id_pessoa",
        F.when(
            F.col("id_pessoa").isNull(),
            F.lit(None)
        ).otherwise(
            F.upper(F.substring(F.sha2(F.col("id_pessoa").cast("string"), 256), 1, 30))
        )
    )

    tabela_pg = "tmp_pg_sinp_ent_pessoa"

    spark.sql(f"drop table if exists gold.{tabela_pg}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela_pg} >/dev/null 2>&1")

    df_pessoa_final_religiosa_pg.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela_pg}")

    write_impala_table_partioned(df_pessoa_final_religiosa_pg, "gold", tabela_pg, f"{path}{tabela_pg}")

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela_pg}")

    enviar_gold_para_postgres(f"gold.{tabela_pg}", "id_pessoa")


    tabela = "sinp_ent_pessoa_outras"
    df_pres_outras.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pres_outras, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_preso")


    tabela = "sinp_pnt_pessoa_preso"
    df_ponte.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_pres_outras, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_preso")

    spark.sql("drop table if exists gold.tmp_base_siarhes")
    spark.sql("drop table if exists gold.tmp_filtrada_siarhes")
    spark.sql("drop table if exists gold.tmp_base_pessoa_atual")
    spark.sql("drop table if exists gold.tmp_pessoa_atualizada_servidor")
    spark.sql("drop table if exists gold.tmp_pessoa_mantida")
    spark.sql("drop table if exists gold.tmp_pessoa_nova_servidor")
    spark.sql("drop table if exists gold.tmp_pessoa_final")
    spark.sql("drop table if exists gold.sinp_ent_pes_p1")
    spark.sql("drop table if exists gold.tmp_base_cavi")
    spark.sql("drop table if exists gold.tmp_base_cavi_rank")
    spark.sql("drop table if exists gold.tmp_filtrada_cavi")
    spark.sql("drop table if exists gold.tmp_base_pessoa_atual_cavi")
    spark.sql("drop table if exists gold.tmp_pessoa_atualizada_visitante")
    spark.sql("drop table if exists gold.tmp_pessoa_mantida_visitante")
    spark.sql("drop table if exists gold.tmp_pessoa_nova_visitante")
    spark.sql("drop table if exists gold.tmp_pessoa_final_visitante")


