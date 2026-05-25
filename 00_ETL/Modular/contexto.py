# -*- coding: utf-8 -*-

path = "/data_lake/gold/intlpris/"

import os
import json
import sys
import requests
import pandas as pd

from datetime import datetime, timedelta
from impala.dbapi import connect
from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T, Row
from pyspark.sql.window import Window

sf = F

# ============================================================
# JDBC / POSTGRES
# ============================================================

POSTGRES_JAR = "/opt/cloudera/parcels/SPARK3-3.3.2.3.3.7190.0-91-1.p0.45265883/lib/spark3/jars/postgresql-42.7.3.jar"

if not os.path.exists(POSTGRES_JAR):
    raise FileNotFoundError(f"Driver JDBC PostgreSQL não encontrado: {POSTGRES_JAR}")

spark = (
    SparkSession.builder
    .appName("Inteligência Prisional")

    # ============================================================
    # JDBC / POSTGRES
    # ============================================================
    .config("spark.jars", POSTGRES_JAR)
    .config("spark.driver.extraClassPath", POSTGRES_JAR)
    .config("spark.executor.extraClassPath", POSTGRES_JAR)

    # ============================================================
    # YARN / EXECUTORES
    # ============================================================
    .config("spark.dynamicAllocation.enabled", "true")
    .config("spark.dynamicAllocation.initialExecutors", "2")
    .config("spark.dynamicAllocation.minExecutors", "2")
    .config("spark.dynamicAllocation.maxExecutors", "8")
    .config("spark.executor.instances", "2")

    .config("spark.executor.memory", "8g")
    .config("spark.executor.cores", "2")
    .config("spark.executor.memoryOverhead", "2g")

    .config("spark.driver.memory", "8g")
    .config("spark.driver.cores", "2")
    .config("spark.driver.maxResultSize", "2g")

    # ============================================================
    # ESTABILIDADE / TIMEOUT
    # ============================================================
    .config("spark.network.timeout", "800s")
    .config("spark.executor.heartbeatInterval", "60s")
    .config("spark.sql.broadcastTimeout", "1200")

    # ============================================================
    # SERIALIZAÇÃO
    # ============================================================
    .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
    .config("spark.kryoserializer.buffer", "64m")
    .config("spark.kryoserializer.buffer.max", "1024m")

    # ============================================================
    # SQL / AQE
    # ============================================================
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.adaptive.skewJoin.enabled", "true")
    .config("spark.sql.adaptive.localShuffleReader.enabled", "true")
    .config("spark.sql.adaptive.advisoryPartitionSizeInBytes", "128m")
    .config("spark.sql.shuffle.partitions", "256")
    .config("spark.default.parallelism", "256")

    # ============================================================
    # JOIN / BROADCAST
    # ============================================================
    .config("spark.sql.autoBroadcastJoinThreshold", "64m")
    .config("spark.sql.join.preferSortMergeJoin", "false")

    # ============================================================
    # PARQUET / FILE SCAN
    # ============================================================
    .config("spark.sql.parquet.compression.codec", "snappy")
    .config("spark.sql.files.maxPartitionBytes", "128m")
    .config("spark.sql.files.openCostInBytes", "16m")
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic")

    # ============================================================
    # COMPATIBILIDADE DATETIME/PARQUET
    # ============================================================
    .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY")
    .config("spark.sql.parquet.int96RebaseModeInRead", "LEGACY")
    .config("spark.sql.parquet.datetimeRebaseModeInRead", "LEGACY")

    # ============================================================
    # HIVE
    # ============================================================
    .enableHiveSupport()
    .getOrCreate()
)

try:
    spark.sparkContext.setLogLevel("WARN")
except Exception:
    pass


def get_dtype_df(dataframe):
    string_cols = [col for col, dtype in dataframe.dtypes if dtype == "string"]
    result = []

    for col_name in string_cols:
        tamanho = dataframe.select(
            F.max(F.length(F.encode(F.col(col_name), "UTF-8"))).alias("max_len")
        ).collect()[0]["max_len"]
        result.append((col_name, tamanho if tamanho is not None else 0))

    return pd.DataFrame(result, columns=["coluna", "max_len_bytes"])


def _mapear_tipo_hive(campo):
    tipo = campo.dataType.simpleString().lower()

    if tipo.startswith("string"):
        return "string"
    elif tipo.startswith("int"):
        return "int"
    elif tipo.startswith("bigint") or tipo.startswith("long"):
        return "bigint"
    elif tipo.startswith("double"):
        return "double"
    elif tipo.startswith("float"):
        return "float"
    elif tipo.startswith("boolean"):
        return "boolean"
    elif tipo.startswith("timestamp"):
        return "timestamp"
    elif tipo.startswith("date"):
        return "date"
    elif tipo.startswith("smallint"):
        return "smallint"
    elif tipo.startswith("decimal"):
        return tipo
    else:
        return "string"


def _mapear_tipo_postgres(campo):
    tipo = campo.dataType.simpleString().lower()

    if tipo.startswith("string"):
        return "varchar"
    elif tipo.startswith("int"):
        return "integer"
    elif tipo.startswith("bigint") or tipo.startswith("long"):
        return "bigint"
    elif tipo.startswith("double"):
        return "double precision"
    elif tipo.startswith("float"):
        return "real"
    elif tipo.startswith("boolean"):
        return "boolean"
    elif tipo.startswith("timestamp"):
        return "timestamp"
    elif tipo.startswith("date"):
        return "date"
    elif tipo.startswith("smallint"):
        return "smallint"
    elif tipo.startswith("decimal"):
        return tipo.replace("decimal", "numeric")
    else:
        return "text"


def write_impala_table_partioned(df, schema, tabela, local):
    spark.sql(f"create database if not exists {schema}")
    spark.sql(f"drop table if exists {schema}.{tabela}")

    colunas = []
    for campo in df.schema.fields:
        colunas.append(f"`{campo.name}` {_mapear_tipo_hive(campo)}")

    ddl = f"""
        create external table if not exists {schema}.{tabela} (
            {", ".join(colunas)}
        )
        stored as parquet
        location '{local}'
    """

    spark.sql(ddl)
    spark.catalog.refreshTable(f"{schema}.{tabela}")


def enviar_gold_para_postgres(nome_tabela_origem, pk_postgres):
    url = "jdbc:postgresql://10.242.38.126:5432/sinp_db"
    usuario = "usr_sinp"
    senha = "u9oLzKOato#nksFZ"
    driver = "org.postgresql.Driver"

    partes = nome_tabela_origem.split(".")
    if len(partes) != 2:
        raise ValueError("Informe a tabela no formato schema.tabela. Ex: gold.sinp_ent_pessoa")

    schema_destino = "sinp"
    tabela_destino = partes[1]

    spark.catalog.clearCache()
    spark.sql(f"REFRESH TABLE {nome_tabela_origem}")

    df = spark.table(nome_tabela_origem)
    colunas_df = df.columns

    tem_pk = pk_postgres is not None and str(pk_postgres).strip() != ""
    pk_postgres = str(pk_postgres).strip() if pk_postgres is not None else ""

    if tem_pk and pk_postgres not in colunas_df:
        raise ValueError(
            f"A PK '{pk_postgres}' não existe na tabela de origem. Colunas disponíveis: {colunas_df}"
        )

    ddl_colunas = []
    tipos_df = {}

    for campo in df.schema.fields:
        nome_coluna = campo.name
        tipo_pg = _mapear_tipo_postgres(campo)
        tipos_df[nome_coluna] = tipo_pg
        ddl_colunas.append(f'"{nome_coluna}" {tipo_pg}')

    ddl_create_schema = f'create schema if not exists "{schema_destino}"'

    ddl_create_table = f"""
        create table if not exists "{schema_destino}"."{tabela_destino}" (
            {", ".join(ddl_colunas)}
        )
    """

    jvm = spark._sc._gateway.jvm
    jvm.java.lang.Class.forName(driver)

    def abrir_conexao():
        return jvm.java.sql.DriverManager.getConnection(url, usuario, senha)

    def query_scalar(stmt, sql):
        rs = stmt.executeQuery(sql)
        try:
            if rs.next():
                return rs.getObject(1)
            return None
        finally:
            rs.close()

    # ============================================================
    # DDL / PREPARAÇÃO DA TABELA DESTINO
    # ============================================================

    conn = abrir_conexao()
    stmt = conn.createStatement()

    try:
        stmt.execute(ddl_create_schema)
        stmt.execute(ddl_create_table)

        existentes = set()

        rs = stmt.executeQuery(f"""
            select column_name
            from information_schema.columns
            where table_schema = '{schema_destino}'
              and table_name = '{tabela_destino}'
        """)

        try:
            while rs.next():
                existentes.add(rs.getString(1))
        finally:
            rs.close()

        for col in colunas_df:
            if col not in existentes:
                stmt.execute(
                    f'alter table "{schema_destino}"."{tabela_destino}" '
                    f'add column "{col}" {tipos_df[col]}'
                )

        nome_pk = query_scalar(stmt, f"""
            select c.conname
            from pg_constraint c
            join pg_class t on t.oid = c.conrelid
            join pg_namespace n on n.oid = t.relnamespace
            where n.nspname = '{schema_destino}'
              and t.relname = '{tabela_destino}'
              and c.contype = 'p'
            limit 1
        """)

        if nome_pk:
            stmt.execute(
                f'alter table "{schema_destino}"."{tabela_destino}" '
                f'drop constraint "{nome_pk}"'
            )

        try:
            stmt.execute(f'truncate table "{schema_destino}"."{tabela_destino}"')
        except Exception:
            stmt.execute(f'delete from "{schema_destino}"."{tabela_destino}"')

    finally:
        stmt.close()
        conn.close()

    # ============================================================
    # ESCRITA DISTRIBUÍDA VIA JDBC
    # ============================================================

    qtd_linhas = df.count()

    if qtd_linhas == 0:
        print(f"[POSTGRES] {schema_destino}.{tabela_destino} | linhas=0", flush=True)
        return

    if qtd_linhas < 100_000:
        num_partitions = 2
    elif qtd_linhas < 1_000_000:
        num_partitions = 4
    else:
        num_partitions = 8

    df_postgres = df.repartition(num_partitions)

    (
        df_postgres.write
        .format("jdbc")
        .mode("append")
        .option("url", url)
        .option("dbtable", f'"{schema_destino}"."{tabela_destino}"')
        .option("user", usuario)
        .option("password", senha)
        .option("driver", driver)
        .option("batchsize", "10000")
        .option("isolationLevel", "READ_COMMITTED")
        .option("numPartitions", str(num_partitions))
        .save()
    )

    # ============================================================
    # PK / ANALYZE
    # ============================================================

    conn = abrir_conexao()
    stmt = conn.createStatement()

    try:
        if tem_pk:
            stmt.execute(
                f'alter table "{schema_destino}"."{tabela_destino}" '
                f'add constraint pk_{tabela_destino} primary key ("{pk_postgres}")'
            )

        stmt.execute(f'analyze "{schema_destino}"."{tabela_destino}"')

    finally:
        stmt.close()
        conn.close()

    print(
        f"[POSTGRES] {schema_destino}.{tabela_destino} | linhas={qtd_linhas} | particoes={num_partitions}",
        flush=True
    )