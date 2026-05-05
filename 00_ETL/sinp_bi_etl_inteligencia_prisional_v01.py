# -*- coding: utf-8 -*-
"""
SINP - ETL ANALITICO PARA BI DE INTELIGENCIA PRISIONAL - V0.1
Autor: D-Solucoes / Projeto Inteligencia Prisional

Objetivo:
    Construir marts analiticos para BI da Inteligencia da Policia Civil,
    evitando uma unica tabela final excessivamente larga e permitindo analise por:
        - preso
        - ocorrencia
        - visita / visitante
        - cela / galeria / unidade
        - saidinha / alvara / encarceramento
        - rede de vinculos
        - alertas de prioridade investigativa
        - mapa territorial

Premissas:
    - Fontes no PostgreSQL schema sinp.
    - Execucao em PySpark.
    - Persistencia em parquet/gold, Impala e Postgres quando helpers existirem:
        write_impala_table_partioned(...)
        enviar_gold_para_postgres(origem, pk)

Observacao:
    - A senha abaixo foi mantida conforme ambiente de desenvolvimento/fake ja utilizado.
"""

from functools import reduce
from typing import Dict, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql import types as T

# =============================================================================
# 1. PARAMETROS
# =============================================================================

PG_HOST = "10.242.38.126"
PG_PORT = "5432"
PG_DATABASE = "sinp_db"
PG_SCHEMA = "sinp"
PG_USER = "usr_sinp"
PG_PASSWORD = "u9oLzKOato#nksFZ"

JDBC_URL = f"jdbc:postgresql://{PG_HOST}:{PG_PORT}/{PG_DATABASE}"
JDBC_PROPS = {
    "user": PG_USER,
    "password": PG_PASSWORD,
    "driver": "org.postgresql.Driver",
    "fetchsize": "50000",
}

HDFS_GOLD_BASE = "hdfs:///data_lake/gold/sinp/bi_inteligencia_prisional/"
IMPALA_SCHEMA = "gold"
POSTGRES_TARGET_SCHEMA = "sinp"

DT_LIM_INF = F.to_date(F.lit("1900-01-01"))
DT_LIM_SUP = F.add_months(F.current_date(), 24)

# Tabelas origem usadas pelo ETL.
SRC_TABLES = {
    "pessoa_preso": "sinp_pnt_pessoa_preso",
    "ocor_infopen": "sinp_fat_ocorrencia_infopen",
    "ocor_livro": "sinp_fat_ocorrencia_livro_clas",
    "ocor_livro_risco": "sinp_fat_ocorrencia_livro_risco",
    "rl_ocor_preso_infopen": "sinp_rl_ocorrencia_preso_infopen",
    "rl_ocor_preso_livro": "sinp_rl_ocorrencia_preso_livro",
    "visita_advogado": "sinp_fat_visita_advogado",
    "visita_familiar": "sinp_fat_visita_familiar",
    "encarceramento": "sinp_fat_encarceramento",
    "encarceramento_evento": "sinp_fat_encarceramento_evento",
    "alvaras": "sinp_ent_alvaras",
    "rl_preso_cela": "sinp_rl_preso_cela",
    "rel_preso_cela": "sinp_rel_preso_cela",
    "ent_cela": "sinp_ent_cela",
    "ent_galeria": "sinp_ent_galeria",
    "ent_estabelecimento": "sinp_ent_estabelecimento",
}


# =============================================================================
# 2. UTILITARIOS
# =============================================================================

def read_pg_table(table_name: str) -> DataFrame:
    return (
        spark.read.format("jdbc")
        .option("url", JDBC_URL)
        .option("dbtable", f"{PG_SCHEMA}.{table_name}")
        .options(**JDBC_PROPS)
        .load()
    )


def has_col(df: DataFrame, col_name: str) -> bool:
    return col_name.lower() in {c.lower() for c in df.columns}


def real_col_name(df: DataFrame, col_name: str) -> Optional[str]:
    lower_map = {c.lower(): c for c in df.columns}
    return lower_map.get(col_name.lower())


def c(df: DataFrame, col_name: str, dtype: str = "string"):
    real = real_col_name(df, col_name)
    if real:
        return F.col(real)
    return F.lit(None).cast(dtype)


def s(df: DataFrame, col_name: str):
    return c(df, col_name, "string").cast("string")


def n(df: DataFrame, col_name: str):
    return c(df, col_name, "double").cast("double")


def flag_sn(expr_col):
    return F.when(F.coalesce(expr_col.cast("int"), F.lit(0)) > 0, F.lit("S")).otherwise(F.lit("N"))


def norm_txt(col):
    return F.upper(F.trim(col.cast("string")))


def date_safe(col):
    """
    Converte valor para data de forma defensiva:
    - date/timestamp/string reconhecido pelo Spark;
    - numerico SAS date;
    - numerico SAS datetime;
    - numerico/string YYYYMMDD.
    Descarta datas fora de 1900 ate hoje + 2 anos.
    """
    col_str = F.trim(col.cast("string"))
    col_num = col.cast("double")

    dt_direct = F.to_date(col)
    dt_yymmdd = F.to_date(col_str, "yyyyMMdd")
    dt_dash = F.to_date(col_str, "yyyy-MM-dd")
    dt_br = F.to_date(col_str, "dd/MM/yyyy")

    # SAS date: dias desde 1960-01-01
    dt_sas_date = F.expr("date_add(to_date('1960-01-01'), cast(__sas_date_num__ as int))")
    # Sera substituido via withColumn auxiliar quando necessario.

    return F.coalesce(dt_direct, dt_yymmdd, dt_dash, dt_br)


def add_date_safe(df: DataFrame, src_col: str, tgt_col: str) -> DataFrame:
    real = real_col_name(df, src_col)
    if not real:
        return df.withColumn(tgt_col, F.lit(None).cast("date"))

    col = F.col(real)
    col_num = col.cast("double")
    col_str = F.trim(col.cast("string"))

    dt_direct = F.to_date(col)
    dt_yymmdd = F.to_date(col_str, "yyyyMMdd")
    dt_dash = F.to_date(col_str, "yyyy-MM-dd")
    dt_br = F.to_date(col_str, "dd/MM/yyyy")
    dt_sas_date = F.date_add(F.to_date(F.lit("1960-01-01")), col_num.cast("int"))
    dt_sas_datetime = F.date_add(F.to_date(F.lit("1960-01-01")), F.floor(col_num / F.lit(86400)).cast("int"))

    cand = F.coalesce(
        F.when((dt_direct >= DT_LIM_INF) & (dt_direct <= DT_LIM_SUP), dt_direct),
        F.when((dt_yymmdd >= DT_LIM_INF) & (dt_yymmdd <= DT_LIM_SUP), dt_yymmdd),
        F.when((dt_dash >= DT_LIM_INF) & (dt_dash <= DT_LIM_SUP), dt_dash),
        F.when((dt_br >= DT_LIM_INF) & (dt_br <= DT_LIM_SUP), dt_br),
        F.when((dt_sas_date >= DT_LIM_INF) & (dt_sas_date <= DT_LIM_SUP), dt_sas_date),
        F.when((dt_sas_datetime >= DT_LIM_INF) & (dt_sas_datetime <= DT_LIM_SUP), dt_sas_datetime),
    )
    return df.withColumn(tgt_col, cand.cast("date"))


def first_existing(df: DataFrame, candidates: Sequence[str], dtype: str = "string"):
    cols = [c(df, x, dtype) for x in candidates if has_col(df, x)]
    if not cols:
        return F.lit(None).cast(dtype)
    return F.coalesce(*cols)


def explode_tokens(df: DataFrame, id_cols: List[str], list_col: str, out_col: str) -> DataFrame:
    if not has_col(df, list_col):
        schema_cols = [c(df, x).alias(x) for x in id_cols]
        return df.select(*schema_cols, F.lit(None).cast("string").alias(out_col)).where("1 = 0")

    cleaned = F.regexp_replace(s(df, list_col), r'[\[\]\{\}\"]', '')
    arr = F.split(cleaned, r'[,;|\s]+')
    return (
        df.select(*[c(df, x).alias(x) for x in id_cols], F.explode(arr).alias(out_col))
        .withColumn(out_col, F.trim(F.col(out_col)))
        .where(F.col(out_col).isNotNull() & (F.col(out_col) != ""))
    )


def persist(df: DataFrame, table: str, pk: str = "") -> None:
    """
    Persiste em parquet e aciona helpers de Impala/Postgres quando existirem.
    """
    path = f"{HDFS_GOLD_BASE}{table}"

    (
        df.write.mode("overwrite")
        .option("compression", "snappy")
        .option("maxRecordsPerFile", 1_000_000)
        .parquet(path)
    )

    # Releitura para garantir fonte parquet canonica.
    df_parquet = spark.read.parquet(path)
    df_parquet.createOrReplaceTempView(table)

    if "write_impala_table_partioned" in globals():
        try:
            write_impala_table_partioned(df_parquet, table, path)
        except TypeError:
            try:
                write_impala_table_partioned(tabela=table, path=path)
            except TypeError:
                write_impala_table_partioned(table)

    if "enviar_gold_para_postgres" in globals():
        try:
            enviar_gold_para_postgres(table, pk)
        except TypeError:
            enviar_gold_para_postgres(origem=table, pk=pk)


def non_empty_or_fail(df: DataFrame, name: str) -> None:
    qtd = df.limit(1).count()
    if qtd == 0:
        raise RuntimeError(f"Dataset obrigatorio vazio: {name}")


# =============================================================================
# 3. CARGA DAS FONTES
# =============================================================================

src: Dict[str, DataFrame] = {alias: read_pg_table(tbl) for alias, tbl in SRC_TABLES.items()}

pessoa_preso = src["pessoa_preso"]
ocor_infopen_src = src["ocor_infopen"]
ocor_livro_src = src["ocor_livro"]
ocor_livro_risco_src = src["ocor_livro_risco"]
rl_ocor_preso_infopen_src = src["rl_ocor_preso_infopen"]
rl_ocor_preso_livro_src = src["rl_ocor_preso_livro"]
visita_advogado_src = src["visita_advogado"]
visita_familiar_src = src["visita_familiar"]
encarceramento_src = src["encarceramento"]
encarceramento_evento_src = src["encarceramento_evento"]
alvaras_src = src["alvaras"]
rl_preso_cela_src = src["rl_preso_cela"]
rel_preso_cela_src = src["rel_preso_cela"]
ent_cela_src = src["ent_cela"]
ent_galeria_src = src["ent_galeria"]
ent_estab_src = src["ent_estabelecimento"]


# =============================================================================
# 4. DATA DE REFERENCIA GLOBAL
# =============================================================================

def ref_dates_from(df: DataFrame, src_col: str) -> DataFrame:
    tmp = add_date_safe(df, src_col, "dt_ref_cand")
    return tmp.select("dt_ref_cand").where(F.col("dt_ref_cand").isNotNull())

ref_base = (
    ref_dates_from(ocor_infopen_src, "dt_evento_referencia")
    .unionByName(ref_dates_from(ocor_livro_src, "dt_evento_referencia"), allowMissingColumns=True)
    .unionByName(ref_dates_from(ocor_livro_risco_src, "dt_evento_referencia"), allowMissingColumns=True)
)

dt_ref = ref_base.agg(F.max("dt_ref_cand").alias("dt_ref")).collect()[0]["dt_ref"]
if dt_ref is None:
    dt_ref = spark.sql("select current_date() as dt").collect()[0]["dt"]

DT_REF_LIT = F.lit(str(dt_ref)).cast("date")


# =============================================================================
# 5. DIMENSAO PRESO / PESSOA
# =============================================================================

base_pessoa_pre = pessoa_preso.where(c(pessoa_preso, "id_pessoa").isNotNull())

base_pessoa_flag = base_pessoa_pre.withColumn("_flag_presidiario_txt", norm_txt(s(base_pessoa_pre, "flag_presidiario")))
base_pessoa_filtrada = base_pessoa_flag.where(
    F.col("_flag_presidiario_txt").isin("1", "S", "SIM", "Y", "YES", "TRUE", "T")
)

# Se o flag vier inutilizavel, usa a base completa com id_pessoa nao nulo.
if base_pessoa_filtrada.limit(1).count() == 0:
    base_pessoa_filtrada = base_pessoa_flag.withColumn("fl_filtro_presidiario_fallback", F.lit("S"))
else:
    base_pessoa_filtrada = base_pessoa_filtrada.withColumn("fl_filtro_presidiario_fallback", F.lit("N"))

base_pessoa_filtrada = add_date_safe(base_pessoa_filtrada, "data_nascimento_pessoa", "dt_nascimento_pessoa")
base_pessoa_filtrada = add_date_safe(base_pessoa_filtrada, "data_ultima_prisao", "dt_ult_prisao_pessoa")

w_pessoa = Window.partitionBy(s(base_pessoa_filtrada, "id_pessoa")).orderBy(
    F.col("dt_ult_prisao_pessoa").desc_nulls_last(),
    s(base_pessoa_filtrada, "id_preso").desc_nulls_last(),
)

pessoa_base = (
    base_pessoa_filtrada
    .withColumn("rn", F.row_number().over(w_pessoa))
    .where("rn = 1")
    .select(
        s(base_pessoa_filtrada, "id_pessoa").alias("id_pessoa"),
        s(base_pessoa_filtrada, "id_preso").alias("id_preso"),
        s(base_pessoa_filtrada, "id_preso_original").alias("id_preso_original"),
        s(base_pessoa_filtrada, "origem").alias("origem_pessoa"),
        s(base_pessoa_filtrada, "documento").alias("documento"),
        s(base_pessoa_filtrada, "nome_pessoa").alias("nome_pessoa"),
        s(base_pessoa_filtrada, "sexo_pessoa").alias("sexo_pessoa"),
        s(base_pessoa_filtrada, "etnia").alias("etnia_pessoa"),
        F.col("dt_nascimento_pessoa"),
        F.col("dt_ult_prisao_pessoa"),
        s(base_pessoa_filtrada, "estado_civil").alias("estado_civil"),
        s(base_pessoa_filtrada, "escolaridade").alias("escolaridade"),
        s(base_pessoa_filtrada, "profissao").alias("profissao"),
        s(base_pessoa_filtrada, "religiao").alias("religiao"),
        s(base_pessoa_filtrada, "naturalidade_municipio").alias("naturalidade_municipio"),
        s(base_pessoa_filtrada, "naturalidade_uf").alias("naturalidade_uf"),
        s(base_pessoa_filtrada, "flag_recebe_visita").alias("flag_recebe_visita_origem"),
        s(base_pessoa_filtrada, "flag_sabe_ler").alias("flag_sabe_ler"),
        s(base_pessoa_filtrada, "flag_sabe_escrever").alias("flag_sabe_escrever"),
        F.col("fl_filtro_presidiario_fallback"),
    )
)

non_empty_or_fail(pessoa_base, "pessoa_base")

# Fallback de atributos por alvaras.
alv_attr = alvaras_src.where(c(alvaras_src, "id_pessoa").isNotNull())
alv_attr = add_date_safe(alv_attr, "data_nascimento_pessoa", "dt_nascimento_alvara")
alv_attr = add_date_safe(alv_attr, "dt_referencia_alvara_ref", "dt_ref_alv_1")
alv_attr = add_date_safe(alv_attr, "dt_referencia_alvara", "dt_ref_alv_2")
alv_attr = add_date_safe(alv_attr, "dt_cumprimento_alvara_ref", "dt_ref_alv_3")
alv_attr = add_date_safe(alv_attr, "cumprimento_data", "dt_ref_alv_4")
alv_attr = alv_attr.withColumn("dt_attr_ref", F.coalesce("dt_ref_alv_1", "dt_ref_alv_2", "dt_ref_alv_3", "dt_ref_alv_4"))

w_alv_attr = Window.partitionBy(s(alv_attr, "id_pessoa")).orderBy(F.col("dt_attr_ref").desc_nulls_last())
alv_attr_top = (
    alv_attr.withColumn("rn", F.row_number().over(w_alv_attr))
    .where("rn = 1")
    .select(
        s(alv_attr, "id_pessoa").alias("id_pessoa"),
        s(alv_attr, "etnia").alias("etnia_alvara"),
        s(alv_attr, "sexo_pessoa").alias("sexo_alvara"),
        F.col("dt_nascimento_alvara"),
    )
)

# Encarceramento: primeira internacao, saida, status.
enc = encarceramento_src.where(c(encarceramento_src, "id_pessoa").isNotNull())
enc = add_date_safe(enc, "dt_entrada", "dt_entrada_d")
enc = add_date_safe(enc, "dt_saida", "dt_saida_d")
enc = add_date_safe(enc, "dt_ultima_saidinha", "dt_ultima_saidinha_d")

st_enc_txt = norm_txt(s(enc, "st_encarceramento"))
enc = enc.withColumn(
    "fl_periodo_aberto_num",
    F.when(F.col("dt_saida_d").isNull() | st_enc_txt.isin("ATIVO", "ABERTO", "EM ABERTO"), 1).otherwise(0),
).withColumn(
    "fl_saida_periodo_num",
    F.when(F.col("dt_saida_d").isNotNull() | st_enc_txt.isin("INATIVO", "ENCERRADO", "FECHADO", "BAIXADO", "SAIDA", "SAÍDA"), 1).otherwise(0),
)

enc_resumo = (
    enc.groupBy(s(enc, "id_pessoa").alias("id_pessoa"))
    .agg(
        F.count(F.lit(1)).alias("qtd_periodos_encarceramento"),
        F.min("dt_entrada_d").alias("dt_primeira_internacao"),
        F.max("dt_entrada_d").alias("dt_ult_prisao"),
        F.max("dt_saida_d").alias("dt_ultima_saida"),
        F.max("dt_ultima_saidinha_d").alias("dt_ult_saidinha_enc"),
        F.max("fl_periodo_aberto_num").alias("fl_encarceramento_aberto_num"),
        F.max("fl_saida_periodo_num").alias("fl_ja_houve_saida_num"),
        F.sum(F.coalesce(n(enc, "qtd_mov_saidinha"), F.lit(0))).alias("qtd_mov_saidinha"),
        F.sum(F.coalesce(n(enc, "qtd_saida_saidinha"), F.lit(0))).alias("qtd_saida_saidinha"),
        F.sum(F.coalesce(n(enc, "qtd_retorno_saidinha"), F.lit(0))).alias("qtd_retorno_saidinha"),
        F.sum(F.coalesce(n(enc, "qtd_alvaras_periodo"), F.lit(0))).alias("qtd_alvaras_periodo"),
    )
)

# Artigo atual. Spark aceita nome longo; tambem tenta truncado se a origem vier adaptada.
artigo_principal_col = first_existing(
    enc,
    ["ds_tipificacao_penal_principal_entrada", "ds_tipificacao_penal_principal_e"],
    "string",
)

w_artigo = Window.partitionBy(s(enc, "id_pessoa")).orderBy(
    F.col("fl_periodo_aberto_num").desc(),
    F.col("dt_entrada_d").desc_nulls_last(),
    s(enc, "id_encarceramento").desc_nulls_last(),
)
artigo_atual = (
    enc.withColumn("rn", F.row_number().over(w_artigo))
    .where("rn = 1")
    .select(
        s(enc, "id_pessoa").alias("id_pessoa"),
        s(enc, "ids_artigo_entrada").alias("ids_artigo_atual"),
        artigo_principal_col.alias("artigo_atual"),
        s(enc, "ds_tipificacao_penal_entrada").alias("artigos_atuais"),
    )
)

# Dim pessoa final sem status_cela ainda.
dim_preso_base = (
    pessoa_base.alias("p")
    .join(alv_attr_top.alias("a"), "id_pessoa", "left")
    .join(enc_resumo.alias("e"), "id_pessoa", "left")
    .withColumn("sexo", F.coalesce(F.col("p.sexo_pessoa"), F.col("a.sexo_alvara"), F.lit("NAO INFORMADO")))
    .withColumn("etnia", F.coalesce(F.col("p.etnia_pessoa"), F.col("a.etnia_alvara"), F.lit("NAO INFORMADA")))
    .withColumn("dt_nascimento", F.coalesce(F.col("p.dt_nascimento_pessoa"), F.col("a.dt_nascimento_alvara")))
    .withColumn("idade_ref", F.floor(F.months_between(DT_REF_LIT, F.col("dt_nascimento")) / F.lit(12)))
    .withColumn(
        "faixa_etaria",
        F.when(F.col("idade_ref").isNull(), "NAO INFORMADA")
        .when(F.col("idade_ref") < 18, "MENOR DE 18")
        .when(F.col("idade_ref") <= 24, "18 A 24")
        .when(F.col("idade_ref") <= 29, "25 A 29")
        .when(F.col("idade_ref") <= 34, "30 A 34")
        .when(F.col("idade_ref") <= 39, "35 A 39")
        .when(F.col("idade_ref") <= 49, "40 A 49")
        .when(F.col("idade_ref") <= 59, "50 A 59")
        .otherwise("60+"),
    )
    .withColumn("fl_ja_houve_saida", flag_sn(F.coalesce(F.col("fl_ja_houve_saida_num"), F.lit(0))))
    .withColumn("dt_ref", DT_REF_LIT)
)


# =============================================================================
# 6. PONTE DE OCORRENCIAS PARA PRESO
# =============================================================================

lookup_preso = (
    pessoa_base.select("id_pessoa", F.col("id_preso").alias("token_preso"), "nome_pessoa", "documento")
    .where(F.col("token_preso").isNotNull())
    .unionByName(
        pessoa_base.select("id_pessoa", F.col("id_preso_original").alias("token_preso"), "nome_pessoa", "documento")
        .where(F.col("token_preso").isNotNull()),
        allowMissingColumns=True,
    )
    .dropDuplicates(["token_preso"])
)

bridge_livro_exist = (
    rl_ocor_preso_livro_src.where(c(rl_ocor_preso_livro_src, "id_fato_ocorrencia").isNotNull() & c(rl_ocor_preso_livro_src, "id_pessoa_presidiario").isNotNull())
    .select(
        s(rl_ocor_preso_livro_src, "id_fato_ocorrencia").alias("id_fato_ocorrencia"),
        s(rl_ocor_preso_livro_src, "id_ocorrencia_origem").alias("id_ocorrencia_origem"),
        s(rl_ocor_preso_livro_src, "id_pessoa_presidiario").alias("id_pessoa"),
        s(rl_ocor_preso_livro_src, "id_preso_origem").alias("id_preso_origem"),
        F.lit("PONTE_EXISTENTE").alias("origem_resolucao"),
        F.lit(1).alias("prioridade_resolucao"),
    )
)

bridge_livro_parse_pessoa = explode_tokens(
    ocor_livro_src,
    ["id_fato_ocorrencia", "id_ocorrencia_origem"],
    "txt_ids_pessoa_presidiario_livro",
    "id_pessoa",
).select(
    "id_fato_ocorrencia",
    "id_ocorrencia_origem",
    "id_pessoa",
    F.lit(None).cast("string").alias("id_preso_origem"),
    F.lit("TXT_IDS_PESSOA_PRESIDIARIO_LIVRO").alias("origem_resolucao"),
    F.lit(2).alias("prioridade_resolucao"),
)

bridge_livro_parse_preso_raw = explode_tokens(
    ocor_livro_src,
    ["id_fato_ocorrencia", "id_ocorrencia_origem"],
    "txt_ids_preso_infopen_livro",
    "token_preso",
)
bridge_livro_parse_preso = (
    bridge_livro_parse_preso_raw.alias("b")
    .join(lookup_preso.alias("l"), "token_preso", "inner")
    .select(
        F.col("b.id_fato_ocorrencia"),
        F.col("b.id_ocorrencia_origem"),
        F.col("l.id_pessoa"),
        F.col("b.token_preso").alias("id_preso_origem"),
        F.lit("TXT_IDS_PRESO_INFOPEN_LIVRO").alias("origem_resolucao"),
        F.lit(3).alias("prioridade_resolucao"),
    )
)

bridge_livro = (
    bridge_livro_exist.unionByName(bridge_livro_parse_pessoa, allowMissingColumns=True)
    .unionByName(bridge_livro_parse_preso, allowMissingColumns=True)
)
w_bridge_livro = Window.partitionBy("id_fato_ocorrencia", "id_pessoa").orderBy("prioridade_resolucao")
bridge_livro = bridge_livro.withColumn("rn", F.row_number().over(w_bridge_livro)).where("rn = 1").drop("rn")

bridge_infopen_exist = (
    rl_ocor_preso_infopen_src.where(c(rl_ocor_preso_infopen_src, "id_fato_ocorrencia").isNotNull() & c(rl_ocor_preso_infopen_src, "id_pessoa_presidiario").isNotNull())
    .select(
        s(rl_ocor_preso_infopen_src, "id_fato_ocorrencia").alias("id_fato_ocorrencia"),
        s(rl_ocor_preso_infopen_src, "id_ocorrencia_origem").alias("id_ocorrencia_origem"),
        s(rl_ocor_preso_infopen_src, "id_pessoa_presidiario").alias("id_pessoa"),
        s(rl_ocor_preso_infopen_src, "id_preso_origem").alias("id_preso_origem"),
        F.lit("PONTE_EXISTENTE").alias("origem_resolucao"),
        F.lit(1).alias("prioridade_resolucao"),
    )
)

bridge_infopen_parse_pessoa = explode_tokens(
    ocor_infopen_src,
    ["id_fato_ocorrencia", "id_ocorrencia_origem"],
    "txt_ids_pessoa_presidiario",
    "id_pessoa",
).select(
    "id_fato_ocorrencia",
    "id_ocorrencia_origem",
    "id_pessoa",
    F.lit(None).cast("string").alias("id_preso_origem"),
    F.lit("TXT_IDS_PESSOA_PRESIDIARIO_INFOPEN").alias("origem_resolucao"),
    F.lit(2).alias("prioridade_resolucao"),
)

bridge_infopen = bridge_infopen_exist.unionByName(bridge_infopen_parse_pessoa, allowMissingColumns=True)
w_bridge_infopen = Window.partitionBy("id_fato_ocorrencia", "id_pessoa").orderBy("prioridade_resolucao")
bridge_infopen = bridge_infopen.withColumn("rn", F.row_number().over(w_bridge_infopen)).where("rn = 1").drop("rn")


# =============================================================================
# 7. FATO OCORRENCIA-PRESO E RESUMOS
# =============================================================================

ocor_infopen = add_date_safe(ocor_infopen_src, "dt_evento_referencia", "dt_ocor")
tipo_infopen = norm_txt(s(ocor_infopen, "ds_tipo_ocorrencia"))
ocor_infopen_fato = (
    ocor_infopen.where(c(ocor_infopen, "id_fato_ocorrencia").isNotNull() & F.col("dt_ocor").isNotNull())
    .select(
        s(ocor_infopen, "id_fato_ocorrencia").alias("id_fato_ocorrencia"),
        s(ocor_infopen, "id_ocorrencia_origem").alias("id_ocorrencia_origem"),
        F.col("dt_ocor"),
        s(ocor_infopen, "ds_tipo_ocorrencia").alias("tipo_ocor"),
        F.coalesce(n(ocor_infopen, "qtd_presos_resolvidos_infopen"), n(ocor_infopen, "qtd_presos_infopen"), F.lit(1)).alias("qtd_env"),
        F.when(
            tipo_infopen.rlike("FUGA|MORTE|HOMIC|ARMA|DROGA|REBEL|MOTIM|AGRESS|LES"), 1
        ).otherwise(0).alias("fl_grave_num"),
        F.lit(None).cast("double").alias("grau_risco"),
        F.lit(None).cast("string").alias("criticidade"),
        F.lit(None).cast("double").alias("score_risco"),
        F.lit("INFOPEN").alias("origem_ocor"),
    )
)

ocor_livro = add_date_safe(ocor_livro_src, "dt_evento_referencia", "dt_ocor")
risco_livro = ocor_livro_risco_src.select(
    s(ocor_livro_risco_src, "id_fato_ocorrencia").alias("id_fato_ocorrencia"),
    n(ocor_livro_risco_src, "qtd_envolvidos_total").alias("qtd_envolvidos_total"),
    n(ocor_livro_risco_src, "flag_livro001_fuga").alias("flag_livro001_fuga"),
    n(ocor_livro_risco_src, "flag_livro002_ilicito_rede").alias("flag_livro002_ilicito_rede"),
    n(ocor_livro_risco_src, "flag_livro003_violencia").alias("flag_livro003_violencia"),
    n(ocor_livro_risco_src, "flag_livro004_servidor").alias("flag_livro004_servidor"),
    n(ocor_livro_risco_src, "score_cenario_total").alias("score_cenario_total"),
    s(ocor_livro_risco_src, "criticidade_cenario_maxima").alias("criticidade_cenario_maxima"),
)

ocor_livro_base = (
    ocor_livro.where(c(ocor_livro, "id_fato_ocorrencia").isNotNull() & F.col("dt_ocor").isNotNull())
    .select(
        s(ocor_livro, "id_fato_ocorrencia").alias("id_fato_ocorrencia"),
        s(ocor_livro, "id_ocorrencia_origem").alias("id_ocorrencia_origem"),
        F.col("dt_ocor"),
        F.coalesce(
            s(ocor_livro, "subclasse_motivo"),
            s(ocor_livro, "classe_motivo"),
            s(ocor_livro, "macroclasse_motivo"),
            s(ocor_livro, "motivo"),
        ).alias("tipo_ocor"),
        n(ocor_livro, "qtd_presos_resolvidos_livro").alias("qtd_presos_resolvidos_livro"),
        n(ocor_livro, "qtd_presos_distintos_livro").alias("qtd_presos_distintos_livro"),
        n(ocor_livro, "qtd_internos_raw").alias("qtd_internos_raw"),
        n(ocor_livro, "flag_motivo_critico").alias("flag_motivo_critico"),
        n(ocor_livro, "flag_motivo_ilicito").alias("flag_motivo_ilicito"),
        n(ocor_livro, "flag_motivo_violencia").alias("flag_motivo_violencia"),
        n(ocor_livro, "flag_motivo_fuga").alias("flag_motivo_fuga"),
        n(ocor_livro, "flag_motivo_servidor").alias("flag_motivo_servidor"),
        n(ocor_livro, "grau_risco_motivo").alias("grau_risco_motivo"),
        s(ocor_livro, "criticidade_motivo").alias("criticidade_motivo"),
        n(ocor_livro, "score_risco_ocorrencia_livro").alias("score_risco_ocorrencia_livro"),
    )
)

ocor_livro_fato = (
    ocor_livro_base.alias("o")
    .join(risco_livro.alias("r"), "id_fato_ocorrencia", "left")
    .withColumn("qtd_env", F.coalesce("qtd_envolvidos_total", "qtd_presos_distintos_livro", "qtd_presos_resolvidos_livro", "qtd_internos_raw", F.lit(1)))
    .withColumn(
        "fl_grave_num",
        F.when(
            (F.coalesce(F.col("flag_motivo_critico"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_motivo_ilicito"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_motivo_violencia"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_motivo_fuga"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_motivo_servidor"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_livro001_fuga"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_livro002_ilicito_rede"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_livro003_violencia"), F.lit(0)) == 1)
            | (F.coalesce(F.col("flag_livro004_servidor"), F.lit(0)) == 1)
            | (F.coalesce(F.col("grau_risco_motivo"), F.lit(0)) >= 4)
            | norm_txt(F.coalesce(F.col("criticidade_motivo"), F.lit(""))).isin("ALTA", "CRITICA", "CRÍTICA")
            | norm_txt(F.coalesce(F.col("criticidade_cenario_maxima"), F.lit(""))).isin("ALTA", "CRITICA", "CRÍTICA"),
            1,
        ).otherwise(0),
    )
    .select(
        "id_fato_ocorrencia",
        "id_ocorrencia_origem",
        "dt_ocor",
        "tipo_ocor",
        "qtd_env",
        "fl_grave_num",
        F.col("grau_risco_motivo").alias("grau_risco"),
        F.coalesce("criticidade_cenario_maxima", "criticidade_motivo").alias("criticidade"),
        F.coalesce("score_cenario_total", "score_risco_ocorrencia_livro").alias("score_risco"),
        F.lit("LIVRO").alias("origem_ocor"),
    )
)

fat_ocorrencia_preso = (
    bridge_infopen.alias("b")
    .join(ocor_infopen_fato.alias("o"), "id_fato_ocorrencia", "inner")
    .select(
        F.col("b.id_pessoa"),
        F.col("b.id_preso_origem"),
        F.col("o.id_fato_ocorrencia"),
        F.col("o.id_ocorrencia_origem"),
        F.col("o.dt_ocor"),
        F.col("o.tipo_ocor"),
        F.col("o.qtd_env"),
        F.col("o.fl_grave_num"),
        F.col("o.grau_risco"),
        F.col("o.criticidade"),
        F.col("o.score_risco"),
        F.col("o.origem_ocor"),
    )
    .unionByName(
        bridge_livro.alias("b")
        .join(ocor_livro_fato.alias("o"), "id_fato_ocorrencia", "inner")
        .select(
            F.col("b.id_pessoa"),
            F.col("b.id_preso_origem"),
            F.col("o.id_fato_ocorrencia"),
            F.col("o.id_ocorrencia_origem"),
            F.col("o.dt_ocor"),
            F.col("o.tipo_ocor"),
            F.col("o.qtd_env"),
            F.col("o.fl_grave_num"),
            F.col("o.grau_risco"),
            F.col("o.criticidade"),
            F.col("o.score_risco"),
            F.col("o.origem_ocor"),
        ),
        allowMissingColumns=True,
    )
    .where(F.col("id_pessoa").isNotNull() & F.col("dt_ocor").isNotNull())
    .withColumn("dt_ref", DT_REF_LIT)
    .withColumn("dif_ref", F.datediff(DT_REF_LIT, F.col("dt_ocor")))
    .withColumn("fl_ocor_3d_num", F.when((F.col("dif_ref") >= 0) & (F.col("dif_ref") <= 3), 1).otherwise(0))
    .withColumn("fl_ocor_7d_num", F.when((F.col("dif_ref") >= 0) & (F.col("dif_ref") <= 7), 1).otherwise(0))
    .withColumn("fl_ocor_10d_num", F.when((F.col("dif_ref") >= 0) & (F.col("dif_ref") <= 10), 1).otherwise(0))
    .withColumn("fl_ocor_30d_num", F.when((F.col("dif_ref") >= 0) & (F.col("dif_ref") <= 30), 1).otherwise(0))
    .withColumn("fl_ocor_60d_num", F.when((F.col("dif_ref") >= 0) & (F.col("dif_ref") <= 60), 1).otherwise(0))
    .withColumn("fl_ocor_90d_num", F.when((F.col("dif_ref") >= 0) & (F.col("dif_ref") <= 90), 1).otherwise(0))
    .withColumn("fl_ocor_acima_90d_num", F.when(F.col("dif_ref") > 90, 1).otherwise(0))
    .withColumn("fl_ocor_indep_dias_num", F.lit(1))
)

w_ult_ocor = Window.partitionBy("id_pessoa").orderBy(F.col("dt_ocor").desc_nulls_last(), F.col("id_fato_ocorrencia").desc_nulls_last())
ultima_ocor = (
    fat_ocorrencia_preso.withColumn("rn", F.row_number().over(w_ult_ocor))
    .where("rn = 1")
    .select(
        "id_pessoa",
        F.col("id_fato_ocorrencia").alias("ult_ocor_id"),
        F.col("dt_ocor").alias("ult_ocor_dt"),
        F.col("origem_ocor").alias("ult_ocor_origem"),
        F.col("tipo_ocor").alias("ult_ocor_tipo"),
        F.col("fl_grave_num").alias("ult_ocor_grave_num"),
        F.col("qtd_env").alias("ult_ocor_qtd_env"),
        F.when(F.coalesce(F.col("qtd_env"), F.lit(0)) > 1, 1).otherwise(0).alias("ult_ocor_multi_num"),
        F.col("criticidade").alias("ult_ocor_critic"),
        F.col("score_risco").alias("ult_ocor_score"),
    )
)

ocorr_resumo = (
    fat_ocorrencia_preso.groupBy("id_pessoa")
    .agg(
        F.count(F.lit(1)).alias("qtd_ocor"),
        F.sum(F.when(F.col("origem_ocor") == "INFOPEN", 1).otherwise(0)).alias("qtd_ocor_infopen"),
        F.sum(F.when(F.col("origem_ocor") == "LIVRO", 1).otherwise(0)).alias("qtd_ocor_livro"),
        F.sum(F.coalesce(F.col("fl_grave_num"), F.lit(0))).alias("qtd_ocor_grave"),
        F.max("dt_ocor").alias("dt_ult_ocor"),
        F.max(F.when(F.col("fl_grave_num") == 1, F.col("dt_ocor"))).alias("dt_ult_ocor_grave"),
        F.max("fl_ocor_3d_num").alias("fl_ocor_3d_num"),
        F.max("fl_ocor_7d_num").alias("fl_ocor_7d_num"),
        F.max("fl_ocor_10d_num").alias("fl_ocor_10d_num"),
        F.max("fl_ocor_30d_num").alias("fl_ocor_30d_num"),
        F.max("fl_ocor_60d_num").alias("fl_ocor_60d_num"),
        F.max("fl_ocor_90d_num").alias("fl_ocor_90d_num"),
        F.max("fl_ocor_acima_90d_num").alias("fl_ocor_acima_90d_num"),
        F.max("fl_ocor_indep_dias_num").alias("fl_ocor_indep_dias_num"),
        F.sum(F.when(F.col("dif_ref") <= 365, 1).otherwise(0)).alias("qtd_ocor_365d"),
        F.sum(F.when((F.col("dif_ref") <= 365) & (F.col("fl_grave_num") == 1), 1).otherwise(0)).alias("qtd_ocor_grave_365d"),
    )
    .withColumn("qtd_ocor_indep_dias", F.col("qtd_ocor"))
)


# =============================================================================
# 8. VISITAS E VISITANTES
# =============================================================================

adv = visita_advogado_src.where(c(visita_advogado_src, "id_pessoa_presidiario").isNotNull())
for src_dt, tgt_dt in [
    ("dt_evento", "dt_evento_d"),
    ("dt_evento_referencia", "dt_evento_ref_d"),
    ("dt_hr_entrada", "dt_hr_entrada_d"),
    ("dt_registro", "dt_registro_d"),
]:
    adv = add_date_safe(adv, src_dt, tgt_dt)

vis_adv = adv.select(
    s(adv, "id_fato_visita_advogado").alias("id_visita"),
    s(adv, "id_pessoa_presidiario").alias("id_pessoa"),
    s(adv, "id_pessoa_advogado").alias("id_visitante"),
    s(adv, "nome_advogado").alias("nome_visitante"),
    s(adv, "documento_advogado").alias("doc_visitante"),
    F.lit("ADVOGADO").alias("tp_visita"),
    F.coalesce("dt_evento_d", "dt_evento_ref_d", "dt_hr_entrada_d", "dt_registro_d").alias("dt_visita"),
).where(F.col("dt_visita").isNotNull())

fam = visita_familiar_src.where(c(visita_familiar_src, "id_pessoa_presidiario").isNotNull())
for src_dt, tgt_dt in [
    ("dt_evento", "dt_evento_d"),
    ("dt_evento_referencia", "dt_evento_ref_d"),
    ("dt_hr_entrada", "dt_hr_entrada_d"),
    ("dt_registro", "dt_registro_d"),
]:
    fam = add_date_safe(fam, src_dt, tgt_dt)

vis_fam = fam.select(
    s(fam, "id_fato_visita_familiar").alias("id_visita"),
    s(fam, "id_pessoa_presidiario").alias("id_pessoa"),
    s(fam, "id_pessoa_visitante").alias("id_visitante"),
    s(fam, "nome_visitante").alias("nome_visitante"),
    s(fam, "documento_visitante").alias("doc_visitante"),
    F.lit("FAMILIAR").alias("tp_visita"),
    F.coalesce("dt_evento_d", "dt_evento_ref_d", "dt_hr_entrada_d", "dt_registro_d").alias("dt_visita"),
).where(F.col("dt_visita").isNotNull())

fat_visita_preso = vis_adv.unionByName(vis_fam, allowMissingColumns=True).withColumn("dt_ref", DT_REF_LIT)

visita_resumo = (
    fat_visita_preso.groupBy("id_pessoa")
    .agg(
        F.count(F.lit(1)).alias("qtd_visitas"),
        F.sum(F.when(F.col("tp_visita") == "ADVOGADO", 1).otherwise(0)).alias("qtd_vis_adv"),
        F.sum(F.when(F.col("tp_visita") == "FAMILIAR", 1).otherwise(0)).alias("qtd_vis_fam"),
        F.max("dt_visita").alias("dt_ult_visita"),
    )
)

w_ult_vis_tipo = Window.partitionBy("id_pessoa", "tp_visita").orderBy(F.col("dt_visita").desc_nulls_last(), F.col("id_visita").desc_nulls_last())
ult_vis_tipo = fat_visita_preso.withColumn("rn", F.row_number().over(w_ult_vis_tipo)).where("rn = 1")
ult_adv = ult_vis_tipo.where("tp_visita = 'ADVOGADO'").select("id_pessoa", F.col("dt_visita").alias("dt_ult_vis_adv"), F.col("nome_visitante").alias("ult_adv_nome"), F.col("doc_visitante").alias("ult_adv_doc"))
ult_fam = ult_vis_tipo.where("tp_visita = 'FAMILIAR'").select("id_pessoa", F.col("dt_visita").alias("dt_ult_vis_fam"), F.col("nome_visitante").alias("ult_fam_nome"), F.col("doc_visitante").alias("ult_fam_doc"))
ult_visita_pivot = ult_adv.join(ult_fam, "id_pessoa", "full")

visitas_freq_base = fat_visita_preso.withColumn("chave_visitante", F.coalesce("id_visitante", "doc_visitante", "nome_visitante"))
visitas_freq_count = (
    visitas_freq_base.where(F.col("chave_visitante").isNotNull())
    .groupBy("id_pessoa", "tp_visita", "chave_visitante")
    .agg(
        F.count(F.lit(1)).alias("qtd_visitas_visitante"),
        F.max("dt_visita").alias("dt_ult_visita_visitante"),
        F.max("nome_visitante").alias("nome_visitante_keep"),
        F.max("doc_visitante").alias("doc_visitante_keep"),
    )
)
w_freq = Window.partitionBy("id_pessoa", "tp_visita").orderBy(F.col("qtd_visitas_visitante").desc(), F.col("dt_ult_visita_visitante").desc_nulls_last())
visitas_freq_top = visitas_freq_count.withColumn("rn", F.row_number().over(w_freq)).where("rn = 1")
freq_adv = visitas_freq_top.where("tp_visita = 'ADVOGADO'").select("id_pessoa", F.col("nome_visitante_keep").alias("adv_freq_nome"), F.col("doc_visitante_keep").alias("adv_freq_doc"), F.col("qtd_visitas_visitante").alias("qtd_vis_adv_freq"))
freq_fam = visitas_freq_top.where("tp_visita = 'FAMILIAR'").select("id_pessoa", F.col("nome_visitante_keep").alias("fam_freq_nome"), F.col("doc_visitante_keep").alias("fam_freq_doc"), F.col("qtd_visitas_visitante").alias("qtd_vis_fam_freq"))
visitante_freq_pivot = freq_adv.join(freq_fam, "id_pessoa", "full")

# Visita anterior a ultima ocorrencia.
timeline_vis_ocor = (
    fat_visita_preso.select("id_pessoa", F.col("dt_visita").alias("dt_evento"), "tp_visita").withColumn("tp_reg", F.lit("VISITA")).withColumn("ordem_evento", F.lit(1))
    .unionByName(
        ultima_ocor.select("id_pessoa", F.col("ult_ocor_dt").alias("dt_evento")).withColumn("tp_visita", F.lit(None).cast("string")).withColumn("tp_reg", F.lit("OCOR")).withColumn("ordem_evento", F.lit(2)),
        allowMissingColumns=True,
    )
)
w_timeline = Window.partitionBy("id_pessoa").orderBy("dt_evento", "ordem_evento").rowsBetween(Window.unboundedPreceding, -1)
visita_antes_ult_ocor = (
    timeline_vis_ocor.withColumn("dt_vis_ant_ocor", F.max(F.when(F.col("tp_reg") == "VISITA", F.col("dt_evento"))).over(w_timeline))
    .withColumn("tp_vis_ant_ocor", F.last(F.when(F.col("tp_reg") == "VISITA", F.col("tp_visita")), ignorenulls=True).over(w_timeline))
    .where("tp_reg = 'OCOR'")
    .select(
        "id_pessoa",
        F.when(F.col("dt_vis_ant_ocor").isNotNull(), 1).otherwise(0).alias("fl_ocor_apos_visita_num"),
        "dt_vis_ant_ocor",
        "tp_vis_ant_ocor",
        F.datediff("dt_evento", "dt_vis_ant_ocor").alias("dias_vis_ocor"),
    )
)


# =============================================================================
# 9. ALVARAS E SAIDINHAS
# =============================================================================

alv = alvaras_src.where(c(alvaras_src, "id_pessoa").isNotNull())
alv = add_date_safe(alv, "dt_cumprimento_alvara", "dt_cump_1")
alv = add_date_safe(alv, "cumprimento_data", "dt_cump_2")
beneficio_txt = norm_txt(F.concat_ws(" ", s(alv, "txt_beneficio"), s(alv, "beneficio_nome"), s(alv, "beneficio_descricao")))
alv = (
    alv.withColumn("dt_cumprimento_d", F.coalesce("dt_cump_1", "dt_cump_2"))
    .withColumn("alv_cumprido_num", F.when((F.coalesce(n(alv, "flag_alvara_cumprido"), F.lit(0)) == 1) | F.col("dt_cumprimento_d").isNotNull(), 1).otherwise(0))
    .withColumn("alv_solto_num", F.when((F.col("alv_cumprido_num") == 1) & ((F.coalesce(n(alv, "flag_beneficio_liberatorio"), F.lit(0)) == 1) | beneficio_txt.rlike("SOLTURA|LIBERDADE")), 1).otherwise(0))
)
alvara_resumo = (
    alv.groupBy(s(alv, "id_pessoa").alias("id_pessoa"))
    .agg(
        F.count(F.lit(1)).alias("qtd_alvaras"),
        F.sum("alv_cumprido_num").alias("qtd_alv_cumprido"),
        F.sum("alv_solto_num").alias("qtd_alv_solto"),
        F.max(F.when(F.col("alv_solto_num") == 1, F.col("dt_cumprimento_d"))).alias("dt_ult_alv_solto"),
    )
)

said = encarceramento_evento_src.where(c(encarceramento_evento_src, "id_pessoa").isNotNull())
said = add_date_safe(said, "dt_evento", "dt_saidinha")
txt_mov = norm_txt(F.concat_ws(" ", s(said, "categoria_movimentacao"), s(said, "subcategoria_evento"), s(said, "ds_tipo_mov"), s(said, "observacao"), s(said, "ids_tipo_saida_temporaria"), s(said, "ds_tipo_saida_temporaria")))
fat_saidinha = (
    said.where(F.col("dt_saidinha").isNotNull())
    .withColumn("txt_mov", txt_mov)
    .where(
        F.col("txt_mov").rlike("SAIDINHA|TEMPORARIA|TEMPORÁRIA")
        | s(said, "ids_tipo_saida_temporaria").isNotNull()
        | c(said, "dt_retorno_saida_temporaria").isNotNull()
    )
    .select(
        s(said, "id_pessoa").alias("id_pessoa"),
        s(said, "id_preso").alias("id_preso"),
        F.col("dt_saidinha"),
        F.lit(1).alias("one"),
    )
)
saidinha_resumo = fat_saidinha.groupBy("id_pessoa").agg(F.count(F.lit(1)).alias("qtd_saidinhas_evento"), F.max("dt_saidinha").alias("dt_ult_saidinha_evento"))

# Saidinha antes/depois da ultima ocorrencia.
timeline_said_ocor = (
    fat_saidinha.select("id_pessoa", F.col("dt_saidinha").alias("dt_evento")).withColumn("tp_reg", F.lit("SAID")).withColumn("ordem_evento", F.lit(1))
    .unionByName(ultima_ocor.select("id_pessoa", F.col("ult_ocor_dt").alias("dt_evento")).withColumn("tp_reg", F.lit("OCOR")).withColumn("ordem_evento", F.lit(2)), allowMissingColumns=True)
)
w_said_antes = Window.partitionBy("id_pessoa").orderBy("dt_evento", "ordem_evento").rowsBetween(Window.unboundedPreceding, -1)
saidinha_antes_ocor = (
    timeline_said_ocor.withColumn("dt_said_antes_ocor", F.max(F.when(F.col("tp_reg") == "SAID", F.col("dt_evento"))).over(w_said_antes))
    .where("tp_reg = 'OCOR'")
    .select(
        "id_pessoa",
        F.when(F.col("dt_said_antes_ocor").isNotNull(), 1).otherwise(0).alias("fl_ocor_apos_said_num"),
        "dt_said_antes_ocor",
        F.datediff("dt_evento", "dt_said_antes_ocor").alias("dias_said_ocor"),
    )
)
w_said_depois = Window.partitionBy("id_pessoa").orderBy(F.col("dt_evento").desc(), F.col("ordem_evento").desc()).rowsBetween(Window.unboundedPreceding, -1)
saidinha_depois_ocor = (
    timeline_said_ocor.withColumn("dt_said_apos_ocor", F.max(F.when(F.col("tp_reg") == "SAID", F.col("dt_evento"))).over(w_said_depois))
    .where("tp_reg = 'OCOR'")
    .select(
        "id_pessoa",
        F.when(F.col("dt_said_apos_ocor").isNotNull(), 1).otherwise(0).alias("fl_ocor_antes_said_num"),
        "dt_said_apos_ocor",
        F.datediff("dt_said_apos_ocor", "dt_evento").alias("dias_ocor_said"),
    )
)


# =============================================================================
# 10. CELA, GALERIA, UNIDADE E GEO
# =============================================================================

cela_rl = rl_preso_cela_src.where(c(rl_preso_cela_src, "id_pessoa").isNotNull() & c(rl_preso_cela_src, "id_cela").isNotNull())
cela_rl = add_date_safe(cela_rl, "dt_entrada_uso_cela", "dt_entrada_cela")
cela_rl = add_date_safe(cela_rl, "dt_saida_uso_cela", "dt_saida_cela")
cela_rl = add_date_safe(cela_rl, "dt_registro", "dt_registro_cela")
cela_rl = cela_rl.select(
    s(cela_rl, "id_pessoa").alias("id_pessoa"),
    s(cela_rl, "id_preso").alias("id_preso_cela"),
    s(cela_rl, "id_cela").alias("id_cela"),
    s(cela_rl, "nome_cela").alias("nome_cela"),
    "dt_entrada_cela",
    "dt_saida_cela",
    "dt_registro_cela",
    s(cela_rl, "situacao").alias("situacao_cela"),
    F.lit("sinp_rl_preso_cela").alias("origem_cela"),
)

cela_rel = rel_preso_cela_src.where(c(rel_preso_cela_src, "id_pessoa").isNotNull() & c(rel_preso_cela_src, "id_cela").isNotNull())
cela_rel = add_date_safe(cela_rel, "dt_entrada_uso_cela", "dt_entrada_cela")
cela_rel = add_date_safe(cela_rel, "dt_evento_referencia", "dt_registro_cela")
cela_rel = cela_rel.select(
    s(cela_rel, "id_pessoa").alias("id_pessoa"),
    s(cela_rel, "id_preso").alias("id_preso_cela"),
    s(cela_rel, "id_cela").alias("id_cela"),
    s(cela_rel, "numero_cela").alias("nome_cela"),
    "dt_entrada_cela",
    F.lit(None).cast("date").alias("dt_saida_cela"),
    "dt_registro_cela",
    s(cela_rel, "situacao").alias("situacao_cela"),
    F.lit("sinp_rel_preso_cela").alias("origem_cela"),
)

cela_eventos = (
    cela_rl.unionByName(cela_rel, allowMissingColumns=True)
    .where(F.col("dt_entrada_cela").isNotNull())
    .withColumn(
        "fl_cela_vigente_num",
        F.when((F.col("dt_entrada_cela") <= DT_REF_LIT) & (F.col("dt_saida_cela").isNull() | (F.col("dt_saida_cela") >= DT_REF_LIT)), 1).otherwise(0),
    )
)
w_cela_atual = Window.partitionBy("id_pessoa").orderBy(F.col("fl_cela_vigente_num").desc(), F.col("dt_entrada_cela").desc_nulls_last(), F.col("dt_registro_cela").desc_nulls_last())
cela_atual_base = cela_eventos.withColumn("rn", F.row_number().over(w_cela_atual)).where("rn = 1").drop("rn")

lat_cela = first_existing(ent_cela_src, ["nr_latitude", "latitude", "lat"], "double")
lon_cela = first_existing(ent_cela_src, ["nr_longitude", "longitude", "lon", "lng"], "double")
ent_cela_dim = ent_cela_src.select(
    s(ent_cela_src, "id_cela").alias("id_cela"),
    s(ent_cela_src, "id_galeria").alias("id_galeria"),
    s(ent_cela_src, "id_estabelecimento_origem").alias("id_presidio_cela"),
    s(ent_cela_src, "descricao_galeria").alias("nome_galeria_cela"),
    s(ent_cela_src, "estabelecimento_nome").alias("estabelecimento_nome_cela"),
    F.coalesce(n(ent_cela_src, "qt_capacidade_adaptada"), n(ent_cela_src, "qt_capacidade_projetada")).alias("lim_cela"),
    lat_cela.alias("nr_latitude_cela"),
    lon_cela.alias("nr_longitude_cela"),
)

ent_galeria_dim = ent_galeria_src.select(
    s(ent_galeria_src, "id_galeria").alias("id_galeria"),
    s(ent_galeria_src, "id_estabelecimento").alias("id_presidio_galeria"),
    F.coalesce(s(ent_galeria_src, "nome_galeria"), s(ent_galeria_src, "descricao_galeria")).alias("nome_galeria_galeria"),
)

lat_estab = first_existing(ent_estab_src, ["nr_latitude", "latitude", "lat"], "double")
lon_estab = first_existing(ent_estab_src, ["nr_longitude", "longitude", "lon", "lng"], "double")
ent_estab_dim = ent_estab_src.select(
    s(ent_estab_src, "id_estabelecimento").alias("id_presidio"),
    s(ent_estab_src, "estabelecimento_nome").alias("presidio_nome"),
    s(ent_estab_src, "estabelecimento_sigla").alias("pres_sigla"),
    s(ent_estab_src, "uf").alias("uf"),
    s(ent_estab_src, "municipio").alias("municipio"),
    lat_estab.alias("nr_latitude_estab"),
    lon_estab.alias("nr_longitude_estab"),
)

cela_atual = (
    cela_atual_base.alias("ca")
    .join(ent_cela_dim.alias("cd"), "id_cela", "left")
    .join(ent_galeria_dim.alias("gd"), "id_galeria", "left")
    .withColumn("id_presidio", F.coalesce("id_presidio_galeria", "id_presidio_cela"))
    .withColumn("nome_galeria", F.coalesce("nome_galeria_galeria", "nome_galeria_cela"))
    .join(ent_estab_dim.alias("ed"), "id_presidio", "left")
    .withColumn("presidio_nome", F.coalesce("presidio_nome", "estabelecimento_nome_cela"))
    .withColumn("nr_latitude", F.coalesce("nr_latitude_cela", "nr_latitude_estab"))
    .withColumn("nr_longitude", F.coalesce("nr_longitude_cela", "nr_longitude_estab"))
    .withColumn("geo_origem", F.when(F.col("nr_latitude_cela").isNotNull() & F.col("nr_longitude_cela").isNotNull(), "CELA").when(F.col("nr_latitude_estab").isNotNull() & F.col("nr_longitude_estab").isNotNull(), "ESTABELECIMENTO").otherwise("NAO INFORMADO"))
    .select(
        "id_pessoa", "id_preso_cela", "id_cela", "nome_cela", "id_galeria", "nome_galeria", "id_presidio", "presidio_nome", "pres_sigla", "uf", "municipio",
        "lim_cela", "fl_cela_vigente_num", "origem_cela", "dt_entrada_cela", "dt_saida_cela", "nr_latitude", "nr_longitude", "geo_origem"
    )
)

ocupacao_cela = cela_atual.where(F.col("id_cela").isNotNull()).groupBy("id_cela").agg(F.countDistinct("id_pessoa").alias("ocup_cela"))

cela_artigo_base = cela_atual.select("id_pessoa", "id_cela").join(artigo_atual.select("id_pessoa", "artigo_atual"), "id_pessoa", "left").where(F.col("id_cela").isNotNull() & F.col("artigo_atual").isNotNull())
artigos_cela = cela_artigo_base.groupBy("id_cela").agg(
    F.concat_ws(" | ", F.sort_array(F.collect_set("artigo_atual"))).alias("artigos_cela"),
    F.countDistinct("artigo_atual").alias("qtd_artigos_cela"),
)


# =============================================================================
# 11. MART PRESO MONITORAMENTO
# =============================================================================

mart_preso_monitor = (
    dim_preso_base.alias("p")
    .join(ocorr_resumo.alias("or"), "id_pessoa", "left")
    .join(ultima_ocor.alias("uo"), "id_pessoa", "left")
    .join(visita_resumo.alias("vr"), "id_pessoa", "left")
    .join(ult_visita_pivot.alias("uv"), "id_pessoa", "left")
    .join(visitante_freq_pivot.alias("vf"), "id_pessoa", "left")
    .join(visita_antes_ult_ocor.alias("va"), "id_pessoa", "left")
    .join(enc_resumo.alias("er"), "id_pessoa", "left")
    .join(alvara_resumo.alias("ar"), "id_pessoa", "left")
    .join(saidinha_resumo.alias("sr"), "id_pessoa", "left")
    .join(saidinha_antes_ocor.alias("sao"), "id_pessoa", "left")
    .join(saidinha_depois_ocor.alias("sdo"), "id_pessoa", "left")
    .join(cela_atual.alias("ca"), "id_pessoa", "left")
    .join(artigo_atual.alias("aa"), "id_pessoa", "left")
    .join(ocupacao_cela.alias("oc"), "id_cela", "left")
    .join(artigos_cela.alias("ac"), "id_cela", "left")
    .withColumn("qtd_ocor", F.coalesce("qtd_ocor", F.lit(0)))
    .withColumn("qtd_ocor_indep_dias", F.coalesce("qtd_ocor_indep_dias", F.col("qtd_ocor")))
    .withColumn("qtd_ocor_infopen", F.coalesce("qtd_ocor_infopen", F.lit(0)))
    .withColumn("qtd_ocor_livro", F.coalesce("qtd_ocor_livro", F.lit(0)))
    .withColumn("qtd_ocor_grave", F.coalesce("qtd_ocor_grave", F.lit(0)))
    .withColumn("qtd_visitas", F.coalesce("qtd_visitas", F.lit(0)))
    .withColumn("qtd_vis_adv", F.coalesce("qtd_vis_adv", F.lit(0)))
    .withColumn("qtd_vis_fam", F.coalesce("qtd_vis_fam", F.lit(0)))
    .withColumn("qtd_periodos_encarceramento", F.coalesce(F.col("er.qtd_periodos_encarceramento"), F.col("p.qtd_periodos_encarceramento"), F.lit(0)))
    .withColumn("qtd_apreensoes", F.col("qtd_periodos_encarceramento"))
    .withColumn("qtd_alvaras", F.coalesce("qtd_alvaras", F.lit(0)))
    .withColumn("qtd_alv_cumprido", F.coalesce("qtd_alv_cumprido", F.lit(0)))
    .withColumn("qtd_alv_solto", F.coalesce("qtd_alv_solto", F.lit(0)))
    .withColumn("qtd_saidinhas", F.coalesce("qtd_saidinhas_evento", "qtd_saida_saidinha", F.lit(0)))
    .withColumn("ocup_cela", F.coalesce("ocup_cela", F.lit(0)))
    .withColumn("pct_ocup_cela", F.when(F.col("lim_cela") > 0, F.round((F.col("ocup_cela") / F.col("lim_cela")) * 100, 2)))
    .withColumn("fl_ocor_grave", flag_sn(F.col("qtd_ocor_grave")))
    .withColumn("fl_ocor_indep_dias", flag_sn(F.coalesce("fl_ocor_indep_dias_num", F.lit(0))))
    .withColumn("fl_ocor_3d", flag_sn(F.coalesce("fl_ocor_3d_num", F.lit(0))))
    .withColumn("fl_ocor_7d", flag_sn(F.coalesce("fl_ocor_7d_num", F.lit(0))))
    .withColumn("fl_ocor_10d", flag_sn(F.coalesce("fl_ocor_10d_num", F.lit(0))))
    .withColumn("fl_ocor_30d", flag_sn(F.coalesce("fl_ocor_30d_num", F.lit(0))))
    .withColumn("fl_ocor_60d", flag_sn(F.coalesce("fl_ocor_60d_num", F.lit(0))))
    .withColumn("fl_ocor_90d", flag_sn(F.coalesce("fl_ocor_90d_num", F.lit(0))))
    .withColumn("fl_ocor_acima_90d", flag_sn(F.coalesce("fl_ocor_acima_90d_num", F.lit(0))))
    .withColumn("ult_ocor_grave", flag_sn(F.coalesce("ult_ocor_grave_num", F.lit(0))))
    .withColumn("fl_ult_ocor_multi", flag_sn(F.coalesce("ult_ocor_multi_num", F.lit(0))))
    .withColumn("fl_recebeu_visita", flag_sn(F.col("qtd_visitas")))
    .withColumn("fl_ocor_apos_visita", flag_sn(F.coalesce("fl_ocor_apos_visita_num", F.lit(0))))
    .withColumn("fl_ocor_apos_said", flag_sn(F.coalesce("fl_ocor_apos_said_num", F.lit(0))))
    .withColumn("fl_ocor_antes_said", flag_sn(F.coalesce("fl_ocor_antes_said_num", F.lit(0))))
    .withColumn("fl_cela_vigente", flag_sn(F.coalesce("fl_cela_vigente_num", F.lit(0))))
    .withColumn("fl_cela_lotada", F.when((F.col("lim_cela") > 0) & (F.col("ocup_cela") > F.col("lim_cela")), "S").otherwise("N"))
    .withColumn("dt_ult_saidinha", F.coalesce("dt_ult_saidinha_evento", "dt_ult_saidinha_enc"))
    .withColumn("fl_ja_houve_saida", flag_sn(F.coalesce("fl_ja_houve_saida_num", F.lit(0))))
    .withColumn("status_prisional", F.when((F.coalesce("fl_encarceramento_aberto_num", F.lit(0)) == 1) | (F.coalesce("fl_cela_vigente_num", F.lit(0)) == 1), "ATIVO").otherwise("INATIVO"))
    .withColumn("dias_desde_ultima_ocor", F.datediff(DT_REF_LIT, F.col("dt_ult_ocor")))
    .withColumn("dias_no_ciclo_atual", F.datediff(DT_REF_LIT, F.col("dt_ult_prisao")))
    .withColumn("score_prioridade_investigativa", F.lit(0))
)

# Score simples e explicavel para ranking inicial.
mart_preso_monitor = (
    mart_preso_monitor
    .withColumn(
        "score_prioridade_investigativa",
        F.coalesce(F.col("qtd_ocor_grave"), F.lit(0)) * 20
        + F.coalesce(F.col("qtd_ocor_365d"), F.lit(0)) * 5
        + F.coalesce(F.col("qtd_ocor_grave_365d"), F.lit(0)) * 15
        + F.when(F.col("fl_ocor_30d") == "S", 20).otherwise(0)
        + F.when(F.col("fl_ocor_apos_visita") == "S", 10).otherwise(0)
        + F.when(F.col("fl_ocor_apos_said") == "S", 10).otherwise(0)
        + F.when(F.col("fl_cela_lotada") == "S", 5).otherwise(0)
        + F.when(F.col("status_prisional") == "ATIVO", 10).otherwise(0),
    )
    .withColumn(
        "classe_prioridade_investigativa",
        F.when(F.col("score_prioridade_investigativa") >= 80, "CRITICA")
        .when(F.col("score_prioridade_investigativa") >= 50, "ALTA")
        .when(F.col("score_prioridade_investigativa") >= 25, "MEDIA")
        .when(F.col("score_prioridade_investigativa") > 0, "BAIXA")
        .otherwise("SEM INDICIO"),
    )
    .withColumn("id_fato_monitor", F.md5(F.concat_ws("|", F.col("id_pessoa"), F.col("dt_ref").cast("string"))))
    .withColumn("dt_carga", F.current_timestamp())
)

non_empty_or_fail(mart_preso_monitor, "mart_preso_monitor")


# =============================================================================
# 12. MART CELA, UNIDADE, VISITANTE, REDE, ALERTAS E MAPA
# =============================================================================

mart_cela_risco = (
    mart_preso_monitor.where(F.col("id_cela").isNotNull())
    .groupBy("id_cela", "nome_cela", "id_galeria", "nome_galeria", "id_presidio", "presidio_nome", "pres_sigla", "uf", "municipio", "nr_latitude", "nr_longitude", "geo_origem")
    .agg(
        F.countDistinct("id_pessoa").alias("qtd_presos_cela"),
        F.max("lim_cela").alias("lim_cela"),
        F.max("ocup_cela").alias("ocup_cela"),
        F.max("pct_ocup_cela").alias("pct_ocup_cela"),
        F.sum(F.when(F.col("qtd_ocor") > 0, 1).otherwise(0)).alias("qtd_presos_com_ocor"),
        F.sum(F.when(F.col("qtd_ocor_grave") > 0, 1).otherwise(0)).alias("qtd_presos_com_ocor_grave"),
        F.sum(F.when(F.col("fl_ocor_30d") == "S", 1).otherwise(0)).alias("qtd_presos_ocor_30d"),
        F.sum(F.when(F.col("classe_prioridade_investigativa").isin("CRITICA", "ALTA"), 1).otherwise(0)).alias("qtd_presos_prioridade_alta"),
        F.avg("score_prioridade_investigativa").alias("score_medio_presos_cela"),
        F.max("score_prioridade_investigativa").alias("score_max_presos_cela"),
    )
    .withColumn("score_risco_cela", F.round(F.col("score_medio_presos_cela") + F.col("qtd_presos_prioridade_alta") * 5 + F.when(F.col("ocup_cela") > F.col("lim_cela"), 10).otherwise(0), 2))
    .withColumn("fl_cela_sensivel", F.when((F.col("score_risco_cela") >= 50) | (F.col("qtd_presos_prioridade_alta") >= 3), "S").otherwise("N"))
    .withColumn("dt_ref", DT_REF_LIT)
    .withColumn("dt_carga", F.current_timestamp())
)

mart_unidade_risco = (
    mart_preso_monitor.where(F.col("id_presidio").isNotNull())
    .groupBy("id_presidio", "presidio_nome", "pres_sigla", "uf", "municipio", "nr_latitude", "nr_longitude", "geo_origem")
    .agg(
        F.countDistinct("id_pessoa").alias("qtd_presos_unidade"),
        F.sum(F.when(F.col("status_prisional") == "ATIVO", 1).otherwise(0)).alias("qtd_presos_ativos"),
        F.sum(F.when(F.col("qtd_ocor") > 0, 1).otherwise(0)).alias("qtd_presos_com_ocor"),
        F.sum(F.when(F.col("qtd_ocor_grave") > 0, 1).otherwise(0)).alias("qtd_presos_com_ocor_grave"),
        F.sum(F.when(F.col("fl_ocor_30d") == "S", 1).otherwise(0)).alias("qtd_presos_ocor_30d"),
        F.sum(F.when(F.col("classe_prioridade_investigativa").isin("CRITICA", "ALTA"), 1).otherwise(0)).alias("qtd_presos_prioridade_alta"),
        F.avg("score_prioridade_investigativa").alias("score_medio_unidade"),
        F.max("score_prioridade_investigativa").alias("score_max_unidade"),
    )
    .withColumn("score_risco_unidade", F.round(F.col("score_medio_unidade") + F.col("qtd_presos_prioridade_alta") * 2, 2))
    .withColumn("dt_ref", DT_REF_LIT)
    .withColumn("dt_carga", F.current_timestamp())
)

# Visitante como objeto de rede/inteligencia.
visita_com_contexto = (
    fat_visita_preso.withColumn("chave_visitante", F.coalesce("id_visitante", "doc_visitante", "nome_visitante"))
    .join(mart_preso_monitor.select("id_pessoa", "id_presidio", "presidio_nome", "id_cela", "qtd_ocor", "qtd_ocor_grave", "classe_prioridade_investigativa"), "id_pessoa", "left")
)
mart_visitante_risco = (
    visita_com_contexto.where(F.col("chave_visitante").isNotNull())
    .groupBy("chave_visitante", "tp_visita")
    .agg(
        F.max("nome_visitante").alias("nome_visitante"),
        F.max("doc_visitante").alias("doc_visitante"),
        F.count(F.lit(1)).alias("qtd_visitas"),
        F.countDistinct("id_pessoa").alias("qtd_presos_visitados"),
        F.countDistinct("id_presidio").alias("qtd_unidades_visitadas"),
        F.max("dt_visita").alias("dt_ult_visita"),
        F.sum(F.when(F.col("qtd_ocor_grave") > 0, 1).otherwise(0)).alias("qtd_presos_visitados_ocor_grave"),
        F.sum(F.when(F.col("classe_prioridade_investigativa").isin("CRITICA", "ALTA"), 1).otherwise(0)).alias("qtd_presos_visitados_prioridade_alta"),
    )
    .withColumn("score_risco_visitante", F.col("qtd_presos_visitados") * 5 + F.col("qtd_unidades_visitadas") * 10 + F.col("qtd_presos_visitados_ocor_grave") * 15 + F.col("qtd_presos_visitados_prioridade_alta") * 20)
    .withColumn("fl_visitante_multiplos_presos", F.when(F.col("qtd_presos_visitados") > 1, "S").otherwise("N"))
    .withColumn("fl_visitante_multiplas_unidades", F.when(F.col("qtd_unidades_visitadas") > 1, "S").otherwise("N"))
    .withColumn("classe_risco_visitante", F.when(F.col("score_risco_visitante") >= 80, "CRITICA").when(F.col("score_risco_visitante") >= 50, "ALTA").when(F.col("score_risco_visitante") >= 25, "MEDIA").otherwise("BAIXA"))
    .withColumn("dt_ref", DT_REF_LIT)
    .withColumn("dt_carga", F.current_timestamp())
)

# Rede preso-preso: co-cela atual, co-ocorrencia e visitante compartilhado.
def ordered_pair(df: DataFrame, left_col: str, right_col: str) -> DataFrame:
    return df.where(F.col(left_col) < F.col(right_col))

edges_cela = (
    cela_atual.alias("a")
    .join(cela_atual.alias("b"), "id_cela", "inner")
    .select(F.col("a.id_pessoa").alias("id_pessoa_1"), F.col("b.id_pessoa").alias("id_pessoa_2"), F.lit("MESMA_CELA_ATUAL").alias("tipo_vinculo"), F.col("id_cela").alias("id_contexto"))
)
edges_cela = ordered_pair(edges_cela, "id_pessoa_1", "id_pessoa_2")

edges_ocor = (
    fat_ocorrencia_preso.alias("a")
    .join(fat_ocorrencia_preso.alias("b"), "id_fato_ocorrencia", "inner")
    .select(F.col("a.id_pessoa").alias("id_pessoa_1"), F.col("b.id_pessoa").alias("id_pessoa_2"), F.lit("MESMA_OCORRENCIA").alias("tipo_vinculo"), F.col("id_fato_ocorrencia").alias("id_contexto"))
)
edges_ocor = ordered_pair(edges_ocor, "id_pessoa_1", "id_pessoa_2")

vis_key = fat_visita_preso.withColumn("chave_visitante", F.coalesce("id_visitante", "doc_visitante", "nome_visitante")).where(F.col("chave_visitante").isNotNull())
edges_visitante = (
    vis_key.alias("a")
    .join(vis_key.alias("b"), "chave_visitante", "inner")
    .select(F.col("a.id_pessoa").alias("id_pessoa_1"), F.col("b.id_pessoa").alias("id_pessoa_2"), F.concat(F.lit("VISITANTE_COMUM_"), F.col("a.tp_visita")).alias("tipo_vinculo"), F.col("chave_visitante").alias("id_contexto"))
)
edges_visitante = ordered_pair(edges_visitante, "id_pessoa_1", "id_pessoa_2")

rl_preso_preso = (
    edges_cela.unionByName(edges_ocor, allowMissingColumns=True).unionByName(edges_visitante, allowMissingColumns=True)
    .groupBy("id_pessoa_1", "id_pessoa_2", "tipo_vinculo", "id_contexto")
    .agg(F.count(F.lit(1)).alias("qtd_evidencias"))
    .withColumn("dt_ref", DT_REF_LIT)
    .withColumn("dt_carga", F.current_timestamp())
)

rl_preso_visitante = (
    vis_key.groupBy("id_pessoa", "chave_visitante", "tp_visita")
    .agg(
        F.max("nome_visitante").alias("nome_visitante"),
        F.max("doc_visitante").alias("doc_visitante"),
        F.count(F.lit(1)).alias("qtd_visitas"),
        F.max("dt_visita").alias("dt_ult_visita"),
    )
    .withColumn("dt_ref", DT_REF_LIT)
    .withColumn("dt_carga", F.current_timestamp())
)

# Alertas atomicos para uso em painel e trilha de justificativa.
alertas = []
base_alerta = mart_preso_monitor.select(
    "id_pessoa", "nome_pessoa", "id_presidio", "presidio_nome", "id_cela", "nome_cela", "status_prisional", "classe_prioridade_investigativa", "score_prioridade_investigativa", "dt_ref"
)

alertas.append(base_alerta.where((F.col("status_prisional") == "ATIVO") & (F.col("fl_ocor_30d") == "S") & (F.col("qtd_ocor_grave") > 0)).withColumn("tipo_alerta", F.lit("PRESO_ATIVO_OCOR_GRAVE_RECENTE")).withColumn("nivel_alerta", F.lit("VERMELHO")))
alertas.append(base_alerta.where((F.col("status_prisional") == "INATIVO") & (F.col("fl_ja_houve_saida") == "S") & (F.col("qtd_ocor_grave") > 0)).withColumn("tipo_alerta", F.lit("PRESO_INATIVO_SAIDA_HISTORICO_GRAVE")).withColumn("nivel_alerta", F.lit("LARANJA")))
alertas.append(base_alerta.where(F.col("fl_ocor_apos_visita") == "S").withColumn("tipo_alerta", F.lit("OCORRENCIA_APOS_VISITA")).withColumn("nivel_alerta", F.lit("LARANJA")))
alertas.append(base_alerta.where(F.col("fl_ocor_apos_said") == "S").withColumn("tipo_alerta", F.lit("OCORRENCIA_APOS_SAIDINHA")).withColumn("nivel_alerta", F.lit("LARANJA")))
alertas.append(base_alerta.where(F.col("fl_cela_lotada") == "S").withColumn("tipo_alerta", F.lit("CELA_LOTADA_COM_PRESO_MONITORADO")).withColumn("nivel_alerta", F.lit("AMARELO")))

mart_alerta = reduce(lambda a, b: a.unionByName(b, allowMissingColumns=True), alertas).withColumn("id_alerta", F.md5(F.concat_ws("|", "id_pessoa", "tipo_alerta", F.col("dt_ref").cast("string")))).withColumn("dt_carga", F.current_timestamp())

mart_mapa = (
    mart_unidade_risco.select(
        F.col("id_presidio").alias("id_objeto"),
        F.lit("UNIDADE_PRISIONAL").alias("tipo_objeto"),
        F.col("presidio_nome").alias("nome_objeto"),
        "uf", "municipio", "nr_latitude", "nr_longitude", "geo_origem", "score_risco_unidade", "qtd_presos_ativos", "qtd_presos_prioridade_alta", "dt_ref"
    )
    .where(F.col("nr_latitude").isNotNull() & F.col("nr_longitude").isNotNull())
    .withColumn("dt_carga", F.current_timestamp())
)


# =============================================================================
# 13. PERSISTENCIA
# =============================================================================

outputs: List[Tuple[str, DataFrame, str]] = [
    ("sinp_bi_dim_preso", dim_preso_base.join(cela_atual.select("id_pessoa", "fl_cela_vigente_num"), "id_pessoa", "left").withColumn("status_prisional", F.when((F.coalesce("fl_encarceramento_aberto_num", F.lit(0)) == 1) | (F.coalesce("fl_cela_vigente_num", F.lit(0)) == 1), "ATIVO").otherwise("INATIVO")), "id_pessoa"),
    ("sinp_bi_fat_ocorrencia_preso", fat_ocorrencia_preso, ""),
    ("sinp_bi_fat_visita_preso", fat_visita_preso, ""),
    ("sinp_bi_fat_saidinha", fat_saidinha, ""),
    ("sinp_bi_mart_preso_monitor", mart_preso_monitor, "id_fato_monitor"),
    ("sinp_bi_mart_cela_risco", mart_cela_risco, "id_cela"),
    ("sinp_bi_mart_unidade_risco", mart_unidade_risco, "id_presidio"),
    ("sinp_bi_mart_visitante_risco", mart_visitante_risco, ""),
    ("sinp_bi_rl_preso_preso", rl_preso_preso, ""),
    ("sinp_bi_rl_preso_visitante", rl_preso_visitante, ""),
    ("sinp_bi_mart_alerta", mart_alerta, "id_alerta"),
    ("sinp_bi_mart_mapa", mart_mapa, ""),
]

for table_name, df_out, pk in outputs:
    non_empty_or_fail(df_out, table_name)
    persist(df_out, table_name, pk)

print("ETL SINP BI Inteligencia Prisional finalizado com sucesso.")
print(f"Data de referencia: {dt_ref}")
print("Tabelas geradas:")
for table_name, _, _ in outputs:
    print(f" - {table_name}")
