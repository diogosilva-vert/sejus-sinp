# -*- coding: utf-8 -*-
"""
Etapa 05 - Fato de encarceramento.

Gera:
- gold.sinp_fat_encarceramento
- gold.sinp_fat_encarceramento_evento
- gold.sinp_fat_encarceramento_inconsistencia

Regra crítica:
- id_pessoa deve vir exclusivamente de gold.sinp_pnt_pessoa_preso por id_preso.
- Não usar id_pessoa das tabelas gold.sinp_ent_mov_*.
"""

import os

from contexto import *
from pyspark.sql import functions as F


def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

    # ============================================================
    # 00 - REFRESH DAS ORIGENS
    # ============================================================

    spark.sql("REFRESH TABLE gold.sinp_ent_mov_entrada")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_saida")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_interna")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_externa")
    spark.sql("REFRESH TABLE gold.sinp_ent_mov_saidinha")
    spark.sql("REFRESH TABLE gold.sinp_pnt_pessoa_preso")
    spark.catalog.clearCache()

    # ============================================================
    # 00.1 - REFERÊNCIA OFICIAL PESSOA/PRESO
    # ============================================================

    tabela = "tmp_ref_pessoa_preso_encarceramento"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_ref_pessoa_preso_encarceramento = spark.sql("""
        select
            cast(id_preso as string) as id_preso,
            max(cast(id_pessoa as string)) as id_pessoa,
            max(cast(nome_pessoa as string)) as nome_pessoa
        from gold.sinp_pnt_pessoa_preso
        where id_preso is not null
          and id_pessoa is not null
        group by cast(id_preso as string)
    """)

    df_ref_pessoa_preso_encarceramento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_ref_pessoa_preso_encarceramento,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 01 - BASE UNIFICADA DE EVENTOS MACRO: ENTRADA / SAÍDA
    # ============================================================

    df_eventos_macro = spark.sql("""
        select
            cast(m.id_preso as string) as id_preso,
            r.id_pessoa,
            coalesce(r.nome_pessoa, m.nome_pessoa) as nome_pessoa,
            cast(m.id_movimentacao as string) as id_movimentacao,
            cast(m.id_tipomovimentacao as string) as id_tipomovimentacao,
            m.ds_tipo_mov,
            cast(m.movimentacao_data as timestamp) as dt_evento,
            cast(m.id_estabelecimentosecurity as string) as id_estabelecimento,
            m.observacao,
            m.categoria_movimentacao,
            cast(null as string) as subcategoria_evento,
            cast(m.ids_alvara as string) as ids_alvara,
            cast(m.qtd_alvaras as bigint) as qtd_alvaras,
            cast(m.ids_artigo as string) as ids_artigo,
            cast(m.ds_tipificacao_penal as string) as ds_tipificacao_penal,
            cast(m.ds_tipificacao_penal_principal as string) as ds_tipificacao_penal_principal,
            cast(m.qtd_tipificacoes_penais as bigint) as qtd_tipificacoes_penais,
            cast(m.ids_estabelecimento_externo as string) as ids_estabelecimento_externo,
            cast(m.ds_estabelecimento_externo as string) as ds_estabelecimento_externo,
            cast(m.ids_estabelecimento_security as string) as ids_estabelecimento_security,
            cast(m.ids_estabelecimento_anterior as string) as ids_estabelecimento_anterior,
            cast(m.ids_tipo_obito as string) as ids_tipo_obito,
            cast(m.ds_tipo_obito as string) as ds_tipo_obito,
            cast(m.ids_tipo_saida_temporaria as string) as ids_tipo_saida_temporaria,
            cast(m.ds_tipo_saida_temporaria as string) as ds_tipo_saida_temporaria,
            cast(m.dt_retorno_saida_temporaria as timestamp) as dt_retorno_saida_temporaria,
            1 as prioridade_categoria
        from gold.sinp_ent_mov_entrada m
        left join gold.tmp_ref_pessoa_preso_encarceramento r
            on cast(m.id_preso as string) = r.id_preso
        where m.id_preso is not null
          and m.movimentacao_data is not null

        union all

        select
            cast(m.id_preso as string) as id_preso,
            r.id_pessoa,
            coalesce(r.nome_pessoa, m.nome_pessoa) as nome_pessoa,
            cast(m.id_movimentacao as string) as id_movimentacao,
            cast(m.id_tipomovimentacao as string) as id_tipomovimentacao,
            m.ds_tipo_mov,
            cast(m.movimentacao_data as timestamp) as dt_evento,
            cast(m.id_estabelecimentosecurity as string) as id_estabelecimento,
            m.observacao,
            m.categoria_movimentacao,
            cast(null as string) as subcategoria_evento,
            cast(m.ids_alvara as string) as ids_alvara,
            cast(m.qtd_alvaras as bigint) as qtd_alvaras,
            cast(m.ids_artigo as string) as ids_artigo,
            cast(m.ds_tipificacao_penal as string) as ds_tipificacao_penal,
            cast(m.ds_tipificacao_penal_principal as string) as ds_tipificacao_penal_principal,
            cast(m.qtd_tipificacoes_penais as bigint) as qtd_tipificacoes_penais,
            cast(m.ids_estabelecimento_externo as string) as ids_estabelecimento_externo,
            cast(m.ds_estabelecimento_externo as string) as ds_estabelecimento_externo,
            cast(m.ids_estabelecimento_security as string) as ids_estabelecimento_security,
            cast(m.ids_estabelecimento_anterior as string) as ids_estabelecimento_anterior,
            cast(m.ids_tipo_obito as string) as ids_tipo_obito,
            cast(m.ds_tipo_obito as string) as ds_tipo_obito,
            cast(m.ids_tipo_saida_temporaria as string) as ids_tipo_saida_temporaria,
            cast(m.ds_tipo_saida_temporaria as string) as ds_tipo_saida_temporaria,
            cast(m.dt_retorno_saida_temporaria as timestamp) as dt_retorno_saida_temporaria,
            2 as prioridade_categoria
        from gold.sinp_ent_mov_saida m
        left join gold.tmp_ref_pessoa_preso_encarceramento r
            on cast(m.id_preso as string) = r.id_preso
        where m.id_preso is not null
          and m.movimentacao_data is not null
    """)

    tabela = "tmp_eventos_encarceramento_macro"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_eventos_macro.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_eventos_macro,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 02 - BASE UNIFICADA DE EVENTOS MENORES
    # ============================================================

    df_eventos_menores = spark.sql("""
        select
            cast(m.id_preso as string) as id_preso,
            r.id_pessoa,
            coalesce(r.nome_pessoa, m.nome_pessoa) as nome_pessoa,
            cast(m.id_movimentacao as string) as id_movimentacao,
            cast(m.id_tipomovimentacao as string) as id_tipomovimentacao,
            m.ds_tipo_mov,
            cast(m.movimentacao_data as timestamp) as dt_evento,
            cast(m.id_estabelecimentosecurity as string) as id_estabelecimento,
            m.observacao,
            m.categoria_movimentacao,
            cast(m.subcategoria_saidinha as string) as subcategoria_evento,
            cast(m.ids_alvara as string) as ids_alvara,
            cast(m.qtd_alvaras as bigint) as qtd_alvaras,
            cast(m.ids_artigo as string) as ids_artigo,
            cast(m.ds_tipificacao_penal as string) as ds_tipificacao_penal,
            cast(m.ds_tipificacao_penal_principal as string) as ds_tipificacao_penal_principal,
            cast(m.qtd_tipificacoes_penais as bigint) as qtd_tipificacoes_penais,
            cast(m.ids_estabelecimento_externo as string) as ids_estabelecimento_externo,
            cast(m.ds_estabelecimento_externo as string) as ds_estabelecimento_externo,
            cast(m.ids_estabelecimento_security as string) as ids_estabelecimento_security,
            cast(m.ids_estabelecimento_anterior as string) as ids_estabelecimento_anterior,
            cast(m.ids_tipo_obito as string) as ids_tipo_obito,
            cast(m.ds_tipo_obito as string) as ds_tipo_obito,
            cast(m.ids_tipo_saida_temporaria as string) as ids_tipo_saida_temporaria,
            cast(m.ds_tipo_saida_temporaria as string) as ds_tipo_saida_temporaria,
            cast(m.dt_retorno_saida_temporaria as timestamp) as dt_retorno_saida_temporaria,
            case
                when m.categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then 3
                when m.categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then 4
                when m.categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then 5
                else 9
            end as prioridade_categoria
        from (
            select * from gold.sinp_ent_mov_interna
            union all
            select * from gold.sinp_ent_mov_externa
            union all
            select * from gold.sinp_ent_mov_saidinha
        ) m
        left join gold.tmp_ref_pessoa_preso_encarceramento r
            on cast(m.id_preso as string) = r.id_preso
        where m.id_preso is not null
          and m.movimentacao_data is not null
    """)

    tabela = "tmp_eventos_encarceramento_menores"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_eventos_menores.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_eventos_menores,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 03 - BASE GERAL DE EVENTOS DE ENCARCERAMENTO
    # ============================================================

    df_eventos_base = spark.sql("""
        select
            md5(
                concat_ws(
                    '||',
                    coalesce(cast(id_preso as string), ''),
                    coalesce(cast(id_movimentacao as string), ''),
                    coalesce(cast(categoria_movimentacao as string), ''),
                    coalesce(cast(dt_evento as string), ''),
                    coalesce(cast(prioridade_categoria as string), '')
                )
            ) as id_evento_ref,
            x.*,
            case
                when categoria_movimentacao = 'ENTRADA' then 1
                when categoria_movimentacao = 'SAIDA' then 2
                when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then 3
                when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then 4
                when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then 5
                else 9
            end as ord_tp_evento
        from (
            select * from gold.tmp_eventos_encarceramento_macro
            union all
            select * from gold.tmp_eventos_encarceramento_menores
        ) x
    """)

    tabela = "tmp_eventos_encarceramento_base"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_eventos_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_eventos_base,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 04 - ENTRADAS ORDENADAS
    # ============================================================

    df_entradas = spark.sql("""
        select
            e.*,
            row_number() over (
                partition by e.id_preso
                order by
                    e.dt_evento,
                    e.ord_tp_evento,
                    e.prioridade_categoria,
                    coalesce(e.id_movimentacao, '')
            ) as nr_periodo_encarceramento,
            lead(e.dt_evento) over (
                partition by e.id_preso
                order by
                    e.dt_evento,
                    e.ord_tp_evento,
                    e.prioridade_categoria,
                    coalesce(e.id_movimentacao, '')
            ) as dt_proxima_entrada,
            lead(e.id_movimentacao) over (
                partition by e.id_preso
                order by
                    e.dt_evento,
                    e.ord_tp_evento,
                    e.prioridade_categoria,
                    coalesce(e.id_movimentacao, '')
            ) as id_movimentacao_proxima_entrada
        from gold.tmp_eventos_encarceramento_base e
        where e.categoria_movimentacao = 'ENTRADA'
    """)

    tabela = "tmp_enc_entradas_ordenadas"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_entradas.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_entradas,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 05 - PRIMEIRA SAÍDA DO PERÍODO
    # ============================================================

    df_saida_primeira_periodo = spark.sql("""
        select
            id_preso,
            nr_periodo_encarceramento,
            id_evento_ref_saida,
            id_movimentacao_saida,
            id_tipomovimentacao_saida,
            ds_tipo_mov_saida,
            dt_saida,
            id_estabelecimento_saida,
            observacao_saida
        from (
            select
                e.id_preso,
                e.nr_periodo_encarceramento,
                s.id_evento_ref as id_evento_ref_saida,
                s.id_movimentacao as id_movimentacao_saida,
                s.id_tipomovimentacao as id_tipomovimentacao_saida,
                s.ds_tipo_mov as ds_tipo_mov_saida,
                s.dt_evento as dt_saida,
                s.id_estabelecimento as id_estabelecimento_saida,
                s.observacao as observacao_saida,
                row_number() over (
                    partition by e.id_preso, e.nr_periodo_encarceramento
                    order by
                        s.dt_evento,
                        s.ord_tp_evento,
                        s.prioridade_categoria,
                        coalesce(s.id_movimentacao, '')
                ) as rn_saida
            from gold.tmp_enc_entradas_ordenadas e
            inner join gold.tmp_eventos_encarceramento_base s
                on e.id_preso = s.id_preso
               and s.categoria_movimentacao = 'SAIDA'
               and s.dt_evento >= e.dt_evento
               and (
                    e.dt_proxima_entrada is null
                    or s.dt_evento < e.dt_proxima_entrada
               )
        ) x
        where rn_saida = 1
    """)

    tabela = "tmp_enc_saida_primeira_periodo"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_saida_primeira_periodo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_saida_primeira_periodo,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 06 - PERÍODOS BASE DE ENCARCERAMENTO
    # ============================================================

    df_periodos_base = spark.sql("""
        select
            md5(
                concat_ws(
                    '||',
                    coalesce(cast(e.id_preso as string), ''),
                    coalesce(cast(e.nr_periodo_encarceramento as string), ''),
                    coalesce(cast(e.id_movimentacao as string), ''),
                    coalesce(cast(e.dt_evento as string), '')
                )
            ) as id_encarceramento,

            e.id_preso,
            e.id_pessoa,
            e.nome_pessoa,
            cast(e.nr_periodo_encarceramento as int) as nr_periodo_encarceramento,

            e.id_movimentacao as id_movimentacao_entrada,
            e.id_tipomovimentacao as id_tipomovimentacao_entrada,
            e.ds_tipo_mov as ds_tipo_mov_entrada,
            e.dt_evento as dt_entrada,
            e.id_estabelecimento as id_estabelecimento_entrada,
            e.observacao as observacao_entrada,
            e.ids_artigo as ids_artigo_entrada,
            e.ds_tipificacao_penal as ds_tipificacao_penal_entrada,
            e.ds_tipificacao_penal_principal as ds_tipificacao_penal_principal_entrada,

            s.id_movimentacao_saida,
            s.id_tipomovimentacao_saida,
            s.ds_tipo_mov_saida,

            case
                when s.dt_saida is not null then s.dt_saida
                when e.dt_proxima_entrada is not null then cast(from_unixtime(unix_timestamp(e.dt_proxima_entrada) - 1) as timestamp)
                else cast(null as timestamp)
            end as dt_saida,

            s.id_estabelecimento_saida,
            s.observacao_saida,

            case
                when s.dt_saida is not null then 'SAIDA'
                when e.dt_proxima_entrada is not null then 'AJUSTE_NOVA_ENTRADA'
                else cast(null as string)
            end as tp_fechamento,

            e.dt_proxima_entrada,
            e.id_movimentacao_proxima_entrada
        from gold.tmp_enc_entradas_ordenadas e
        left join gold.tmp_enc_saida_primeira_periodo s
            on e.id_preso = s.id_preso
           and e.nr_periodo_encarceramento = s.nr_periodo_encarceramento
    """)

    tabela = "tmp_enc_periodos_base"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_periodos_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_periodos_base,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 07 - EVENTOS MENORES ASSOCIADOS AOS PERÍODOS
    # ============================================================

    df_eventos_periodo_base = spark.sql("""
        select
            md5(
                concat_ws(
                    '||',
                    coalesce(cast(p.id_encarceramento as string), ''),
                    coalesce(cast(e.id_movimentacao as string), ''),
                    coalesce(cast(e.categoria_movimentacao as string), '')
                )
            ) as id_encarceramento_evento,

            e.id_evento_ref,
            p.id_encarceramento,
            p.id_preso,
            p.id_pessoa,
            p.nome_pessoa,
            p.nr_periodo_encarceramento,

            e.categoria_movimentacao,
            e.subcategoria_evento,
            e.id_movimentacao,
            e.id_tipomovimentacao,
            e.ds_tipo_mov,
            e.dt_evento,
            e.id_estabelecimento,
            e.observacao,
            e.ids_alvara,
            e.qtd_alvaras,
            e.ids_artigo,
            e.ds_tipificacao_penal,
            e.ds_tipificacao_penal_principal,
            e.qtd_tipificacoes_penais,
            e.ids_estabelecimento_externo,
            e.ds_estabelecimento_externo,
            e.ids_estabelecimento_security,
            e.ids_estabelecimento_anterior,
            e.ids_tipo_obito,
            e.ds_tipo_obito,
            e.ids_tipo_saida_temporaria,
            e.ds_tipo_saida_temporaria,
            e.dt_retorno_saida_temporaria
        from gold.tmp_eventos_encarceramento_base e
        inner join gold.tmp_enc_periodos_base p
            on e.id_preso = p.id_preso
           and e.dt_evento >= p.dt_entrada
           and (
                p.dt_saida is null
                or e.dt_evento < p.dt_saida
           )
        where e.categoria_movimentacao not in ('ENTRADA', 'SAIDA')
    """)

    tabela = "tmp_enc_eventos_periodo_base"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_eventos_periodo_base.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_eventos_periodo_base,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 08 - AGREGADOS DO PERÍODO
    # ============================================================

    df_agregados_periodo = spark.sql("""
        select
            id_encarceramento,

            cast(sum(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then 1 else 0 end) as int) as qtd_mov_internas,
            cast(sum(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then 1 else 0 end) as int) as qtd_mov_externas,
            cast(sum(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then 1 else 0 end) as int) as qtd_mov_saidinha,

            cast(sum(case when subcategoria_evento = 'SAIDA_SAIDINHA' then 1 else 0 end) as int) as qtd_saida_saidinha,
            cast(sum(case when subcategoria_evento = 'RETORNO_SAIDINHA' then 1 else 0 end) as int) as qtd_retorno_saidinha,

            cast(sum(case when qtd_alvaras is not null then qtd_alvaras else 0 end) as int) as qtd_alvaras_periodo,

            min(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then dt_evento else null end) as dt_primeira_mov_interna,
            max(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then dt_evento else null end) as dt_ultima_mov_interna,

            min(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then dt_evento else null end) as dt_primeira_mov_externa,
            max(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then dt_evento else null end) as dt_ultima_mov_externa,

            min(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then dt_evento else null end) as dt_primeira_saidinha,
            max(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then dt_evento else null end) as dt_ultima_saidinha,

            case
                when sum(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' and ds_tipo_mov is not null then 1 else 0 end) > 0
                then concat_ws(',', sort_array(collect_set(case when categoria_movimentacao = 'MOVIMENTACOES_INTERNAS' then ds_tipo_mov else null end)))
                else cast(null as string)
            end as ds_eventos_internos,

            case
                when sum(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' and ds_tipo_mov is not null then 1 else 0 end) > 0
                then concat_ws(',', sort_array(collect_set(case when categoria_movimentacao = 'MOVIMENTACOES_EXTERNAS' then ds_tipo_mov else null end)))
                else cast(null as string)
            end as ds_eventos_externos,

            case
                when sum(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' and ds_tipo_mov is not null then 1 else 0 end) > 0
                then concat_ws(',', sort_array(collect_set(case when categoria_movimentacao = 'MOVIMENTACOES_SAIDINHA' then ds_tipo_mov else null end)))
                else cast(null as string)
            end as ds_eventos_saidinha
        from gold.tmp_enc_eventos_periodo_base
        group by id_encarceramento
    """)

    tabela = "tmp_enc_agregados_periodo"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_agregados_periodo.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_agregados_periodo,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 09 - FATO FINAL DE ENCARCERAMENTO
    # ============================================================

    df_fat_encarceramento = spark.sql("""
        select
            p.id_encarceramento,
            p.id_preso,
            p.id_pessoa,
            p.nome_pessoa,
            p.nr_periodo_encarceramento,

            p.id_movimentacao_entrada,
            p.id_tipomovimentacao_entrada,
            p.ds_tipo_mov_entrada,
            p.dt_entrada,
            p.id_estabelecimento_entrada,
            p.observacao_entrada,
            p.ids_artigo_entrada,
            p.ds_tipificacao_penal_entrada,
            p.ds_tipificacao_penal_principal_entrada,

            p.id_movimentacao_saida,
            p.id_tipomovimentacao_saida,
            p.ds_tipo_mov_saida,
            p.dt_saida,
            p.id_estabelecimento_saida,
            p.observacao_saida,
            p.tp_fechamento,

            case
                when p.dt_saida is not null then 'FECHADO'
                else 'ABERTO'
            end as st_encarceramento,

            cast(datediff(coalesce(to_date(p.dt_saida), current_date()), to_date(p.dt_entrada)) as int) as qt_dias_encarceramento,

            coalesce(a.qtd_mov_internas, 0) as qtd_mov_internas,
            coalesce(a.qtd_mov_externas, 0) as qtd_mov_externas,
            coalesce(a.qtd_mov_saidinha, 0) as qtd_mov_saidinha,
            coalesce(a.qtd_saida_saidinha, 0) as qtd_saida_saidinha,
            coalesce(a.qtd_retorno_saidinha, 0) as qtd_retorno_saidinha,
            coalesce(a.qtd_alvaras_periodo, 0) as qtd_alvaras_periodo,

            a.dt_primeira_mov_interna,
            a.dt_ultima_mov_interna,
            a.dt_primeira_mov_externa,
            a.dt_ultima_mov_externa,
            a.dt_primeira_saidinha,
            a.dt_ultima_saidinha,

            a.ds_eventos_internos,
            a.ds_eventos_externos,
            a.ds_eventos_saidinha,

            cast(
                coalesce(a.qtd_mov_internas, 0) * 1.0
                + coalesce(a.qtd_mov_externas, 0) * 2.0
                + coalesce(a.qtd_mov_saidinha, 0) * 1.5
                + coalesce(a.qtd_alvaras_periodo, 0) * 2.0
                as double
            ) as score_comportamental,

            case
                when (
                    coalesce(a.qtd_mov_internas, 0) * 1.0
                    + coalesce(a.qtd_mov_externas, 0) * 2.0
                    + coalesce(a.qtd_mov_saidinha, 0) * 1.5
                    + coalesce(a.qtd_alvaras_periodo, 0) * 2.0
                ) >= 20 then 'ALTA_DINAMICA'
                when (
                    coalesce(a.qtd_mov_internas, 0) * 1.0
                    + coalesce(a.qtd_mov_externas, 0) * 2.0
                    + coalesce(a.qtd_mov_saidinha, 0) * 1.5
                    + coalesce(a.qtd_alvaras_periodo, 0) * 2.0
                ) >= 8 then 'MEDIA_DINAMICA'
                else 'BAIXA_DINAMICA'
            end as perfil_comportamental,

            case
                when p.id_pessoa is null then 'S'
                else 'N'
            end as fl_inconsistente
        from gold.tmp_enc_periodos_base p
        left join gold.tmp_enc_agregados_periodo a
            on p.id_encarceramento = a.id_encarceramento
    """)

    df_fat_encarceramento = df_fat_encarceramento.dropDuplicates(["id_encarceramento"])

    tabela = "sinp_fat_encarceramento"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_fat_encarceramento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_fat_encarceramento,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres(
        f"gold.{tabela}",
        "id_encarceramento"
    )

    # ============================================================
    # 10 - FATO FINAL DE EVENTOS DO ENCARCERAMENTO
    # ============================================================

    df_fat_encarceramento_evento = (
        spark.table("gold.tmp_enc_eventos_periodo_base")
        .select(
            "id_encarceramento_evento",
            "id_encarceramento",
            "id_preso",
            "id_pessoa",
            "nome_pessoa",
            "nr_periodo_encarceramento",
            "categoria_movimentacao",
            "subcategoria_evento",
            "id_movimentacao",
            "id_tipomovimentacao",
            "ds_tipo_mov",
            "dt_evento",
            "id_estabelecimento",
            "observacao",
            "ids_alvara",
            "qtd_alvaras",
            "ids_artigo",
            "ds_tipificacao_penal",
            "ds_tipificacao_penal_principal",
            "qtd_tipificacoes_penais",
            "ids_estabelecimento_externo",
            "ds_estabelecimento_externo",
            "ids_estabelecimento_security",
            "ids_estabelecimento_anterior",
            "ids_tipo_obito",
            "ds_tipo_obito",
            "ids_tipo_saida_temporaria",
            "ds_tipo_saida_temporaria",
            "dt_retorno_saida_temporaria"
        )
        .dropDuplicates(["id_encarceramento_evento"])
    )

    tabela = "sinp_fat_encarceramento_evento"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_fat_encarceramento_evento.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_fat_encarceramento_evento,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres(
        f"gold.{tabela}",
        "id_encarceramento_evento"
    )

    # ============================================================
    # 11 - INCONSISTÊNCIA: ENTRADA SEM SAÍDA ANTERIOR
    # ============================================================

    df_incons_entrada_sem_saida = spark.sql("""
        select
            md5(
                concat_ws(
                    '||',
                    coalesce(cast(n.id_preso as string), ''),
                    coalesce(cast(n.id_movimentacao as string), ''),
                    'ENTRADA_SEM_SAIDA_ANTERIOR'
                )
            ) as id_encarceramento_inconsistencia,
            n.id_preso,
            n.id_pessoa,
            n.nome_pessoa,
            cast(p.nr_periodo_encarceramento as int) as nr_periodo_encarceramento,
            n.id_movimentacao as id_movimentacao_ref,
            n.categoria_movimentacao as categoria_movimentacao_ref,
            n.ds_tipo_mov as ds_tipo_mov_ref,
            n.dt_evento as dt_evento_ref,
            'ENTRADA_SEM_SAIDA_ANTERIOR' as tp_inconsistencia,
            'Periodo anterior fechado artificialmente em nova entrada - 1 segundo' as detalhe_inconsistencia
        from gold.tmp_enc_periodos_base p
        inner join gold.tmp_enc_entradas_ordenadas n
            on p.id_preso = n.id_preso
           and n.nr_periodo_encarceramento = p.nr_periodo_encarceramento + 1
        where p.tp_fechamento = 'AJUSTE_NOVA_ENTRADA'
    """)

    tabela = "tmp_enc_incons_entrada_sem_saida"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_incons_entrada_sem_saida.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_incons_entrada_sem_saida,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 12 - INCONSISTÊNCIA: SAÍDA SEM ENTRADA
    # ============================================================

    df_incons_saida_sem_entrada = spark.sql("""
        select
            md5(
                concat_ws(
                    '||',
                    coalesce(cast(s.id_preso as string), ''),
                    coalesce(cast(s.id_movimentacao as string), ''),
                    'SAIDA_SEM_ENTRADA'
                )
            ) as id_encarceramento_inconsistencia,
            s.id_preso,
            s.id_pessoa,
            s.nome_pessoa,
            cast(null as int) as nr_periodo_encarceramento,
            s.id_movimentacao as id_movimentacao_ref,
            s.categoria_movimentacao as categoria_movimentacao_ref,
            s.ds_tipo_mov as ds_tipo_mov_ref,
            s.dt_evento as dt_evento_ref,
            'SAIDA_SEM_ENTRADA' as tp_inconsistencia,
            'Saida encontrada sem periodo aberto' as detalhe_inconsistencia
        from gold.tmp_eventos_encarceramento_base s
        left join gold.tmp_enc_saida_primeira_periodo sp
            on s.id_evento_ref = sp.id_evento_ref_saida
        where s.categoria_movimentacao = 'SAIDA'
          and sp.id_evento_ref_saida is null
    """)

    tabela = "tmp_enc_incons_saida_sem_entrada"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_incons_saida_sem_entrada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_incons_saida_sem_entrada,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 13 - INCONSISTÊNCIA: EVENTO MENOR FORA DE PERÍODO
    # ============================================================

    df_incons_evento_menor = spark.sql("""
        select
            md5(
                concat_ws(
                    '||',
                    coalesce(cast(e.id_preso as string), ''),
                    coalesce(cast(e.id_movimentacao as string), ''),
                    'EVENTO_MENOR_FORA_PERIODO'
                )
            ) as id_encarceramento_inconsistencia,
            e.id_preso,
            e.id_pessoa,
            e.nome_pessoa,
            cast(null as int) as nr_periodo_encarceramento,
            e.id_movimentacao as id_movimentacao_ref,
            e.categoria_movimentacao as categoria_movimentacao_ref,
            e.ds_tipo_mov as ds_tipo_mov_ref,
            e.dt_evento as dt_evento_ref,
            'EVENTO_MENOR_FORA_PERIODO' as tp_inconsistencia,
            'Evento menor sem encarceramento aberto para agrupamento' as detalhe_inconsistencia
        from gold.tmp_eventos_encarceramento_base e
        left join gold.tmp_enc_eventos_periodo_base ep
            on e.id_evento_ref = ep.id_evento_ref
        where e.categoria_movimentacao not in ('ENTRADA', 'SAIDA')
          and ep.id_evento_ref is null
    """)

    tabela = "tmp_enc_incons_evento_menor"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_incons_evento_menor.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_incons_evento_menor,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    # ============================================================
    # 14 - FATO FINAL DE INCONSISTÊNCIAS
    # ============================================================

    df_fat_encarceramento_inconsistencia = spark.sql("""
        select * from gold.tmp_enc_incons_entrada_sem_saida
        union all
        select * from gold.tmp_enc_incons_saida_sem_entrada
        union all
        select * from gold.tmp_enc_incons_evento_menor
    """)

    df_fat_encarceramento_inconsistencia = (
        df_fat_encarceramento_inconsistencia
        .dropDuplicates(["id_encarceramento_inconsistencia"])
    )

    tabela = "sinp_fat_encarceramento_inconsistencia"

    spark.sql(f"drop table if exists gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path}{tabela} >/dev/null 2>&1")

    df_fat_encarceramento_inconsistencia.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(
        df_fat_encarceramento_inconsistencia,
        "gold",
        tabela,
        f"{path}{tabela}"
    )

    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")

    enviar_gold_para_postgres(
        f"gold.{tabela}",
        "id_encarceramento_inconsistencia"
    )

    spark.sql(f"""
        select
            '{tabela}' as tabela,
            count(*) as total_registros,
            count(distinct id_encarceramento_inconsistencia) as total_ids
        from gold.{tabela}
    """).show(truncate=False)