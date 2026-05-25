# -*- coding: utf-8 -*-
"""
Etapa 11 - Processos judiciais vinculados ao preso.

Padronização de execução:
- script puro para execução por etapa;
- execução encapsulada em executar(spark, path=None);
- refresh das origens no início;
- limpeza inicial das temporárias gold.tmp_*;
- materialização física controlada em gold.tmp_* entre blocos críticos;
- entidade processo com uma linha por id_processo;
- data_processo calculada pela menor data de prisão vinculada ao processo;
- relação preso/pessoa-processo com uma linha por id_pessoa + id_preso + id_processo;
- inclusão das descrições de tipo de crime, situação jurídica, situação do réu e situação atual do preso;
- separação do campo artigo_nome em código, descrição, parágrafo, inciso e lei;
- publicação final em gold.sinp_ent_processos e gold.sinp_rl_preso_processo;
- envio ao Postgres somente no final;
- limpeza final das temporárias.
"""

from contexto import *
from pyspark.sql import functions as F
import os


def executar(spark, path=None):
    if path is None:
        path = "/data_lake/gold/intlpris/"

    # ============================================================
    # 00 - REFRESH DAS ORIGENS
    # ============================================================

    tabelas_origem = [
        "bronze.infopen_vw_presos_processos",
        "bronze.infopen_tipos_crime",
        "bronze.infopen_situacoes",
        "bronze.infopen_situacoes_presos",
        "bronze.infopen_presos",
        "gold.sinp_pnt_pessoa_preso",
    ]

    for tabela_origem in tabelas_origem:
        spark.sql(f"REFRESH TABLE {tabela_origem}")

    spark.catalog.clearCache()

    # ============================================================
    # 00.1 - LIMPEZA INICIAL DAS TEMPORÁRIAS
    # ============================================================

    temporarias = [
        "tmp_sinp_proc_base_normalizada",
        "tmp_sinp_proc_entidade_agrupada",
        "tmp_sinp_proc_relacao_agrupada",
    ]

    for tabela in temporarias:
        spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    spark.catalog.clearCache()

    # ============================================================
    # 01 - BASE NORMALIZADA DE PROCESSOS
    # ============================================================

    tabela = "tmp_sinp_proc_base_normalizada"

    df_tmp_sinp_proc_base_normalizada = spark.sql(r"""
        select
            cast(v.id_processo as string) as id_processo,
            cast(v.id_preso as string) as id_preso,
            cast(pp.id_pessoa as string) as id_pessoa,

            case
                when v.processo_numero is null then cast(null as string)
                when trim(cast(v.processo_numero as string)) = '' then cast(null as string)
                else trim(cast(v.processo_numero as string))
            end as processo_numero,

            case
                when v.processo_numeroantigo is null then cast(null as string)
                when trim(cast(v.processo_numeroantigo as string)) = '' then cast(null as string)
                else trim(cast(v.processo_numeroantigo as string))
            end as processo_numero_antigo,

            case
                when v.id_vara is null then cast(null as string)
                when trim(cast(v.id_vara as string)) = '' then cast(null as string)
                else trim(cast(v.id_vara as string))
            end as id_vara,

            case
                when v.vara_nome is null then cast(null as string)
                when trim(regexp_replace(cast(v.vara_nome as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(v.vara_nome as string), '\\s+', ' '))
            end as vara_nome,

            case
                when v.id_artigo is null then cast(null as string)
                when trim(cast(v.id_artigo as string)) = '' then cast(null as string)
                else trim(cast(v.id_artigo as string))
            end as id_artigo,

            case
                when v.artigo_nome is null then cast(null as string)
                when trim(regexp_replace(cast(v.artigo_nome as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(v.artigo_nome as string), '\\s+', ' '))
            end as artigo_nome,

            case
                when v.id_tipocrime is null then cast(null as string)
                when trim(cast(v.id_tipocrime as string)) = '' then cast(null as string)
                else trim(cast(v.id_tipocrime as string))
            end as id_tipocrime,

            case
                when tc.tipocrime_descricao is null then cast(null as string)
                when trim(regexp_replace(cast(tc.tipocrime_descricao as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(tc.tipocrime_descricao as string), '\\s+', ' '))
            end as tipocrime_descricao,

            case
                when p.id_situacaopreso is null then cast(null as string)
                when trim(cast(p.id_situacaopreso as string)) = '' then cast(null as string)
                else trim(cast(p.id_situacaopreso as string))
            end as id_situacaopreso,

            case
                when sp.situacaopreso_descricao is null then cast(null as string)
                when trim(regexp_replace(cast(sp.situacaopreso_descricao as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(sp.situacaopreso_descricao as string), '\\s+', ' '))
            end as situacaopreso_descricao,

            case
                when v.presoprocesso_situacaojuridica is null then cast(null as string)
                when trim(cast(v.presoprocesso_situacaojuridica as string)) = '' then cast(null as string)
                else trim(cast(v.presoprocesso_situacaojuridica as string))
            end as id_situacao_juridica,

            case
                when sj.situacao_descricao is null then cast(null as string)
                when trim(regexp_replace(cast(sj.situacao_descricao as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(sj.situacao_descricao as string), '\\s+', ' '))
            end as situacao_juridica_descricao,

            case
                when v.presoprocesso_situacaoreu is null then cast(null as string)
                when trim(cast(v.presoprocesso_situacaoreu as string)) = '' then cast(null as string)
                else trim(cast(v.presoprocesso_situacaoreu as string))
            end as id_situacao_reu,

            case
                when sr.situacao_descricao is null then cast(null as string)
                when trim(regexp_replace(cast(sr.situacao_descricao as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(sr.situacao_descricao as string), '\\s+', ' '))
            end as situacao_reu_descricao,

            to_timestamp(v.presoprocesso_dataprisao) as presoprocesso_dataprisao,

            case
                when v.preso_matricula is null then cast(null as string)
                when trim(cast(v.preso_matricula as string)) = '' then cast(null as string)
                else trim(cast(v.preso_matricula as string))
            end as preso_matricula,

            case
                when v.preso_nome is null then cast(null as string)
                when trim(regexp_replace(cast(v.preso_nome as string), '\\s+', ' ')) = '' then cast(null as string)
                else trim(regexp_replace(cast(v.preso_nome as string), '\\s+', ' '))
            end as preso_nome,

            case
                when v.id_estabelecimentosecuritylocalizacaoatual is null then cast(null as string)
                when trim(cast(v.id_estabelecimentosecuritylocalizacaoatual as string)) = '' then cast(null as string)
                else trim(cast(v.id_estabelecimentosecuritylocalizacaoatual as string))
            end as id_estabelecimento_atual,

            cast(v.preso_utilizaremrelatoriosjuridicos as string) as preso_utilizaremrelatoriosjuridicos,

            case
                when v.id_regime is null then cast(null as string)
                when trim(cast(v.id_regime as string)) = '' then cast(null as string)
                else trim(cast(v.id_regime as string))
            end as id_regime,

            cast(v.presoprocesso_qtddiaspreso as int) as presoprocesso_qtddiaspreso,
            cast(v.presoprocesso_qtdtotalprocessos as int) as presoprocesso_qtdtotalprocessos,

            case
                when pp.id_pessoa is not null then 'S'
                else 'N'
            end as fl_id_pessoa_encontrado

        from bronze.infopen_vw_presos_processos v

        left join gold.sinp_pnt_pessoa_preso pp
            on cast(pp.id_preso as string) = cast(v.id_preso as string)

        left join bronze.infopen_presos p
            on cast(p.id_preso as string) = cast(v.id_preso as string)

        left join bronze.infopen_situacoes_presos sp
            on cast(sp.id_situacaopreso as string) = cast(p.id_situacaopreso as string)

        left join bronze.infopen_situacoes sj
            on cast(sj.id_situacao as string) = cast(v.presoprocesso_situacaojuridica as string)

        left join bronze.infopen_situacoes sr
            on cast(sr.id_situacao as string) = cast(v.presoprocesso_situacaoreu as string)

        left join bronze.infopen_tipos_crime tc
            on cast(tc.id_tipocrime as string) = cast(v.id_tipocrime as string)

        where v.id_processo is not null
    """)

    art = F.col("artigo_nome")

    codigo_por_artigo = F.regexp_extract(
        art,
        r"(?i)\bart(?:igo)?\.?\s*([0-9]{1,4}[a-z]?)",
        1
    )

    codigo_inicio = F.regexp_extract(
        art,
        r"(?i)^\s*([0-9]{1,4}[a-z]?)(?:\s*[-–—:]|\s+)",
        1
    )

    artigo_paragrafo = F.regexp_extract(
        art,
        r"(?i)(§\s*(?:[0-9]+[ºo]?|único|unico)|par[aá]grafo\s+(?:[úu]nico|unico|[0-9]+[ºo]?))",
        1
    )

    artigo_inciso = F.regexp_extract(
        art,
        r"(?i)\b((?:inciso|inc\.)\s*(?:[IVXLCDM]+|[0-9]+))",
        1
    )

    artigo_lei = F.regexp_extract(
        art,
        r"(?i)\b((?:lei|decreto[- ]lei)\s*(?:n[ºo°.]?\s*)?[0-9.]+(?:/[0-9]{2,4})?|c[óo]digo\s+penal|codigo\s+penal|c[óo]digo\s+de\s+processo\s+penal|codigo\s+de\s+processo\s+penal|estatuto\s+do\s+desarmamento|lei\s+de\s+drogas)",
        1
    )

    artigo_descricao = art
    artigo_descricao = F.regexp_replace(
        artigo_descricao,
        r"(?i)\bart(?:igo)?\.?\s*[0-9]{1,4}[a-z]?\s*[-–—:]?\s*",
        ""
    )
    artigo_descricao = F.regexp_replace(
        artigo_descricao,
        r"(?i)^\s*[0-9]{1,4}[a-z]?\s*[-–—:]\s*",
        ""
    )
    artigo_descricao = F.regexp_replace(
        artigo_descricao,
        r"(?i)(§\s*(?:[0-9]+[ºo]?|único|unico)|par[aá]grafo\s+(?:[úu]nico|unico|[0-9]+[ºo]?))",
        ""
    )
    artigo_descricao = F.regexp_replace(
        artigo_descricao,
        r"(?i)\b((?:inciso|inc\.)\s*(?:[IVXLCDM]+|[0-9]+))",
        ""
    )
    artigo_descricao = F.regexp_replace(
        artigo_descricao,
        r"(?i)\b((?:lei|decreto[- ]lei)\s*(?:n[ºo°.]?\s*)?[0-9.]+(?:/[0-9]{2,4})?|c[óo]digo\s+penal|codigo\s+penal|c[óo]digo\s+de\s+processo\s+penal|codigo\s+de\s+processo\s+penal|estatuto\s+do\s+desarmamento|lei\s+de\s+drogas)",
        ""
    )
    artigo_descricao = F.regexp_replace(artigo_descricao, r"\s+", " ")
    artigo_descricao = F.regexp_replace(artigo_descricao, r"^[\s,.;:/\-–—]+|[\s,.;:/\-–—]+$", "")

    df_tmp_sinp_proc_base_normalizada = (
        df_tmp_sinp_proc_base_normalizada
        .withColumn(
            "artigo_codigo",
            F.when(F.length(F.trim(codigo_por_artigo)) > 0, F.upper(F.trim(codigo_por_artigo)))
             .when(F.length(F.trim(codigo_inicio)) > 0, F.upper(F.trim(codigo_inicio)))
             .otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "artigo_paragrafo",
            F.when(F.length(F.trim(artigo_paragrafo)) > 0, F.trim(artigo_paragrafo))
             .otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "artigo_inciso",
            F.when(F.length(F.trim(artigo_inciso)) > 0, F.upper(F.trim(artigo_inciso)))
             .otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "artigo_lei",
            F.when(F.length(F.trim(artigo_lei)) > 0, F.trim(artigo_lei))
             .otherwise(F.lit(None).cast("string"))
        )
        .withColumn(
            "artigo_descricao",
            F.when(F.length(F.trim(artigo_descricao)) > 0, F.trim(artigo_descricao))
             .otherwise(F.lit(None).cast("string"))
        )
    )

    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_tmp_sinp_proc_base_normalizada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_proc_base_normalizada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_tmp_sinp_proc_base_normalizada = spark.table(f"gold.{tabela}")
    df_tmp_sinp_proc_base_normalizada.createOrReplaceTempView("vw_sinp_proc_base_normalizada")

    print(f"[OK] gold.{tabela} criada.")

    # ============================================================
    # 02 - ENTIDADE PROCESSO AGRUPADA
    # ============================================================

    tabela = "tmp_sinp_proc_entidade_agrupada"

    df_tmp_sinp_proc_entidade_agrupada = spark.sql(r"""
        select
            id_processo,

            min(processo_numero) as processo_numero,
            min(processo_numero_antigo) as processo_numero_antigo,
            min(to_date(presoprocesso_dataprisao)) as data_processo,

            min(id_vara) as id_vara,
            min(vara_nome) as vara_nome,

            concat_ws(', ', sort_array(collect_set(id_artigo))) as id_artigo,
            concat_ws(' | ', sort_array(collect_set(artigo_nome))) as artigo_nome,
            concat_ws(', ', sort_array(collect_set(artigo_codigo))) as artigo_codigo,
            concat_ws(' | ', sort_array(collect_set(artigo_descricao))) as artigo_descricao,
            concat_ws(' | ', sort_array(collect_set(artigo_paragrafo))) as artigo_paragrafo,
            concat_ws(' | ', sort_array(collect_set(artigo_inciso))) as artigo_inciso,
            concat_ws(' | ', sort_array(collect_set(artigo_lei))) as artigo_lei,

            concat_ws(', ', sort_array(collect_set(id_tipocrime))) as id_tipocrime,
            concat_ws(' | ', sort_array(collect_set(tipocrime_descricao))) as tipocrime_descricao,

            count(distinct id_preso) as qtd_presos_processo,
            count(distinct id_pessoa) as qtd_pessoas_processo

        from gold.tmp_sinp_proc_base_normalizada
        where id_processo is not null
        group by id_processo
    """)

    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_tmp_sinp_proc_entidade_agrupada.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_proc_entidade_agrupada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_tmp_sinp_proc_entidade_agrupada = spark.table(f"gold.{tabela}")
    df_tmp_sinp_proc_entidade_agrupada.createOrReplaceTempView("vw_sinp_proc_entidade_agrupada")

    print(f"[OK] gold.{tabela} criada.")

    # ============================================================
    # 03 - RELAÇÃO PESSOA/PRESO x PROCESSO AGRUPADA
    # ============================================================

    tabela = "tmp_sinp_proc_relacao_agrupada"

    df_tmp_sinp_proc_relacao_agrupada = spark.sql(r"""
        select
            upper(substr(md5(concat_ws('|',
                coalesce(id_pessoa, '[NULL]'),
                coalesce(id_preso, '[NULL]'),
                coalesce(id_processo, '[NULL]')
            )), 1, 30)) as id_pessoa_processo,

            id_pessoa,
            id_preso,
            id_processo,

            min(preso_matricula) as preso_matricula,
            min(preso_nome) as preso_nome,

            min(id_situacaopreso) as id_situacaopreso,
            min(situacaopreso_descricao) as situacaopreso_descricao,

            min(id_situacao_juridica) as id_situacao_juridica,
            min(situacao_juridica_descricao) as situacao_juridica_descricao,

            min(id_situacao_reu) as id_situacao_reu,
            min(situacao_reu_descricao) as situacao_reu_descricao,

            min(presoprocesso_dataprisao) as presoprocesso_dataprisao,

            min(id_estabelecimento_atual) as id_estabelecimento_atual,
            min(preso_utilizaremrelatoriosjuridicos) as preso_utilizaremrelatoriosjuridicos,

            concat_ws(', ', sort_array(collect_set(id_artigo))) as id_artigo,
            concat_ws(' | ', sort_array(collect_set(artigo_nome))) as artigo_nome,
            concat_ws(', ', sort_array(collect_set(artigo_codigo))) as artigo_codigo,
            concat_ws(' | ', sort_array(collect_set(artigo_descricao))) as artigo_descricao,
            concat_ws(' | ', sort_array(collect_set(artigo_paragrafo))) as artigo_paragrafo,
            concat_ws(' | ', sort_array(collect_set(artigo_inciso))) as artigo_inciso,
            concat_ws(' | ', sort_array(collect_set(artigo_lei))) as artigo_lei,

            concat_ws(', ', sort_array(collect_set(id_tipocrime))) as id_tipocrime,
            concat_ws(' | ', sort_array(collect_set(tipocrime_descricao))) as tipocrime_descricao,

            min(id_regime) as id_regime,
            max(presoprocesso_qtddiaspreso) as presoprocesso_qtddiaspreso,
            max(presoprocesso_qtdtotalprocessos) as presoprocesso_qtdtotalprocessos,

            max(fl_id_pessoa_encontrado) as fl_id_pessoa_encontrado

        from gold.tmp_sinp_proc_base_normalizada
        where id_processo is not null
          and id_preso is not null
        group by
            id_pessoa,
            id_preso,
            id_processo
    """)

    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_tmp_sinp_proc_relacao_agrupada.repartition(800, F.col("id_pessoa_processo")).write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 2000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_tmp_sinp_proc_relacao_agrupada, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql(f"refresh table gold.{tabela}")
    df_tmp_sinp_proc_relacao_agrupada = spark.table(f"gold.{tabela}")
    df_tmp_sinp_proc_relacao_agrupada.createOrReplaceTempView("vw_sinp_proc_relacao_agrupada")

    print(f"[OK] gold.{tabela} criada.")

    # ============================================================
    # 04 - PUBLICAÇÃO FINAL: ENTIDADE PROCESSO
    # ============================================================

    tabela = "sinp_ent_processos"

    df_sinp_ent_processos = spark.sql(r"""
        select
            id_processo,
            processo_numero,
            processo_numero_antigo,
            data_processo,
            id_vara,
            vara_nome,
            id_artigo,
            artigo_nome,
            artigo_codigo,
            artigo_descricao,
            artigo_paragrafo,
            artigo_inciso,
            artigo_lei,
            id_tipocrime,
            tipocrime_descricao,
            qtd_presos_processo,
            qtd_pessoas_processo
        from gold.tmp_sinp_proc_entidade_agrupada
    """)

    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_sinp_ent_processos.write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_ent_processos, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_ent_processos")

    print("[OK] gold.sinp_ent_processos criada.")

    # ============================================================
    # 05 - PUBLICAÇÃO FINAL: RELAÇÃO PESSOA/PRESO x PROCESSO
    # ============================================================

    tabela = "sinp_rl_preso_processo"

    df_sinp_rl_preso_processo = spark.sql(r"""
        select
            id_pessoa_processo,
            id_pessoa,
            id_preso,
            id_processo,
            preso_matricula,
            preso_nome,
            id_situacaopreso,
            situacaopreso_descricao,
            id_situacao_juridica,
            situacao_juridica_descricao,
            id_situacao_reu,
            situacao_reu_descricao,
            presoprocesso_dataprisao,
            id_estabelecimento_atual,
            preso_utilizaremrelatoriosjuridicos,
            id_artigo,
            artigo_nome,
            artigo_codigo,
            artigo_descricao,
            artigo_paragrafo,
            artigo_inciso,
            artigo_lei,
            id_tipocrime,
            tipocrime_descricao,
            id_regime,
            presoprocesso_qtddiaspreso,
            presoprocesso_qtdtotalprocessos,
            fl_id_pessoa_encontrado
        from gold.tmp_sinp_proc_relacao_agrupada
    """)

    spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
    os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    df_sinp_rl_preso_processo.repartition(800, F.col("id_pessoa_processo")).write \
        .mode("overwrite") \
        .option("maxRecordsPerFile", 2000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")

    write_impala_table_partioned(df_sinp_rl_preso_processo, "gold", tabela, f"{path}{tabela}")
    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_rl_preso_processo")

    print("[OK] gold.sinp_rl_preso_processo criada.")

    # ============================================================
    # 06 - ENVIO FINAL PARA POSTGRES
    # ============================================================

    envios_postgres = [
        ("gold.sinp_rl_preso_processo", "id_pessoa_processo"),
        ("gold.sinp_ent_processos", "id_processo"),
    ]

    erros_postgres = []

    for origem_pg, pk_pg in envios_postgres:
        print(f"[POSTGRES][INICIO] {origem_pg} | pk={pk_pg}", flush=True)

        try:
            enviar_gold_para_postgres(origem_pg, pk_pg)
            print(f"[POSTGRES][FIM] {origem_pg}", flush=True)
        except Exception as e:
            msg_erro = f"{origem_pg} | pk={pk_pg} | erro={str(e)}"
            erros_postgres.append(msg_erro)
            print(f"[POSTGRES][ERRO] {msg_erro}", flush=True)

    if len(erros_postgres) > 0:
        raise Exception("Falha no envio ao Postgres: " + " || ".join(erros_postgres))

    # ============================================================
    # 07 - LIMPEZA FINAL DAS TEMPORÁRIAS
    # ============================================================

    for tabela in temporarias:
        spark.sql(f"DROP TABLE IF EXISTS gold.{tabela}")
        os.system(f"hdfs dfs -rm -r -skipTrash {path.rstrip('/')}/{tabela} >/dev/null 2>&1")

    spark.catalog.clearCache()