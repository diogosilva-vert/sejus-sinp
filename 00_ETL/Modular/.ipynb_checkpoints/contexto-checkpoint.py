# -*- coding: utf-8 -*-
"""Contexto comum: path, imports e utilitários compartilhados."""


path="/data_lake/gold/intlpris/"

from pyspark.sql import SparkSession
from impala.dbapi import connect
import pandas as pd
import os
import json
import requests
import sys
from datetime import datetime, timedelta

from pyspark.sql import functions as F, types as T
from pyspark.sql.window import Window

# compatibilidade com trechos antigos que usam sf
sf = F

spark = SparkSession.builder \
            .appName("Inteligência Prisional") \
            .config("spark.dynamicAllocation.enabled", "true") \
            .config("spark.dynamicAllocation.initialExecutors", "1") \
            .config("spark.dynamicAllocation.minExecutors", "1") \
            .config("spark.dynamicAllocation.maxExecutors", "4") \
            .config("spark.executor.memory", "4g") \
            .config("spark.executor.cores", "1") \
            .config("spark.driver.memory", "4g") \
            .config("spark.driver.cores", "1") \
            .config("spark.yarn.executor.memoryOverhead", "1g") \
            .config("spark.executor.memoryOverhead", "1g") \
            .config("spark.sql.parquet.int96RebaseModeInWrite", "LEGACY") \
            .config("spark.sql.parquet.datetimeRebaseModeInWrite", "LEGACY") \
            .master('local') \
            .enableHiveSupport() \
            .getOrCreate()

# ===== CELL 2 =====
def get_dtype_df(dataframe):
    string_cols = [col for col, dtype in dataframe.dtypes if dtype == "string"]
    
    # max em BYTES (UTF-8) para cada coluna string
    if string_cols:
        df_max_bytes = (
            dataframe.select([
                sf.max(sf.length(sf.encode(sf.col(col), "UTF-8"))).alias(col)
                for col in string_cols
            ])
            .first()
            .asDict()
        )
    else:
        df_max_bytes = {}
    
    df_schema = pd.json_normalize(json.loads(dataframe.schema.json()), record_path=['fields'])[['name', 'type']]

    def adjust_dtype(name, dtype):
        if dtype == "long":
            return "bigint"
        
        if dtype == 'string':
            max_length = df_max_bytes.get(name) or 1
            
            # ajusta para CHAR(n) se for até 16 caracteres, senão usa VARCHAR(n)
            if max_length <= 16:
                return f"char({max_length})"  # garantir CHAR(1) como mínimo
            else:
                return f"varchar({max_length})" 
            
        else:
            return dtype

    df_schema['type'] = df_schema.apply(lambda x: adjust_dtype(x['name'], x['type']), axis=1)
    return df_schema

def write_impala_table_partioned(df, impala_schema, impala_table, br_path):
    dtype_df = get_dtype_df(df)
    dtypes_sting = ",\n    ".join([f"{row['name']} {row['type']}" for index, row in dtype_df.iterrows()])
    creation_query = f"""CREATE EXTERNAL TABLE {impala_schema}.{impala_table} 
                        ({dtypes_sting})
                        STORED AS PARQUET LOCATION '{br_path}'
                        """
    drop_query = f"""DROP TABLE {impala_schema}.{impala_table} """
    conn = connect(host='worker03-prod.sejus.es.gov.br', 
                   port=21050, 
                   database='bronze', 
                   auth_mechanism='GSSAPI',
                   kerberos_service_name='impala',
                   use_ssl=True,
                   ca_cert="/var/lib/cloudera-scm-agent/agent-cert/cm-auto-global_cacerts.pem")

    cursor = conn.cursor()
    print(creation_query)
    print(drop_query)
    try:
        cursor.execute(drop_query)
        cursor.execute(creation_query)
        cursor.execute(f"COMPUTE STATS {impala_schema}.{impala_table}")
        print(f"Estatísticas atualizadas para {impala_schema}.{impala_table}")
    except: 
        cursor.execute(creation_query)
        cursor.execute(f"COMPUTE STATS {impala_schema}.{impala_table}")
        print(f"Estatísticas atualizadas para {impala_schema}.{impala_table}")
        
def enviar_gold_para_postgres(nome_tabela_origem, pk_postgres):
    """
    Cria/substitui uma tabela no PostgreSQL a partir de uma tabela Spark/Hive
    e define uma chave primária no campo informado, caso pk_postgres seja informado.

    Parâmetros:
        nome_tabela_origem : str
            Nome completo da tabela no Spark, ex: "gold.sinp_pres_loc_atual"

        pk_postgres : str
            Nome do campo que será chave primária no PostgreSQL.
            Se vier "" ou None, a tabela será criada sem PK.

    Premissas:
        - a tabela de origem já existe no Spark
        - o driver JDBC do PostgreSQL está disponível no cluster
        - o schema/tabela de destino no PostgreSQL terão o mesmo nome da origem
          Ex: gold.sinp_pres_loc_atual -> schema "sinp", tabela "sinp_pres_loc_atual"
    """

    url = "jdbc:postgresql://10.242.38.126:5432/sinp_db"
    usuario = "usr_sinp"
    senha = "u9oLzKOato#nksFZ"
    driver = "org.postgresql.Driver"

    partes = nome_tabela_origem.split(".")
    if len(partes) != 2:
        raise ValueError("Informe a tabela no formato schema.tabela. Ex: gold.sinp_pres_loc_atual")

    schema_destino = "sinp"
    tabela_destino = partes[1]

    spark.catalog.clearCache()
    spark.sql(f"REFRESH TABLE {nome_tabela_origem}")
    df = spark.table(nome_tabela_origem).cache()
    df.count()

    colunas_df = df.columns

    tem_pk = pk_postgres is not None and str(pk_postgres).strip() != ""
    pk_postgres = str(pk_postgres).strip() if pk_postgres is not None else ""

    if tem_pk and pk_postgres not in colunas_df:
        raise ValueError(f"A PK '{pk_postgres}' não existe na tabela de origem. Colunas disponíveis: {colunas_df}")

    def mapear_tipo_postgres(campo):
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

    ddl_colunas = []
    for campo in df.schema.fields:
        nome_coluna = campo.name
        tipo_pg = mapear_tipo_postgres(campo)
        ddl_colunas.append(f'"{nome_coluna}" {tipo_pg}')

    ddl_create_schema = f'create schema if not exists "{schema_destino}"'
    ddl_drop_table = f'drop table if exists "{schema_destino}"."{tabela_destino}"'

    if tem_pk:
        ddl_create_table = f'''
            create table "{schema_destino}"."{tabela_destino}" (
                {", ".join(ddl_colunas)},
                constraint pk_{tabela_destino} primary key ("{pk_postgres}")
            )
        '''
    else:
        ddl_create_table = f'''
            create table "{schema_destino}"."{tabela_destino}" (
                {", ".join(ddl_colunas)}
            )
        '''

    jvm = spark._sc._gateway.jvm
    jvm.java.lang.Class.forName(driver)

    conn = jvm.java.sql.DriverManager.getConnection(url, usuario, senha)
    stmt = conn.createStatement()

    try:
        stmt.execute(ddl_create_schema)
        stmt.execute(ddl_drop_table)
        stmt.execute(ddl_create_table)
    finally:
        stmt.close()
        conn.close()

    propriedades = {
        "user": usuario,
        "password": senha,
        "driver": driver
    }

    df.write \
        .mode("append") \
        .jdbc(
            url=url,
            table=f'{schema_destino}.{tabela_destino}',
            properties=propriedades
        )

    print(f"Tabela enviada com sucesso para o PostgreSQL: {schema_destino}.{tabela_destino}")
    if tem_pk:
        print(f"PK definida: {pk_postgres}")
    else:
        print("Tabela criada sem PK.")

