# -*- coding: utf-8 -*-
"""Correção preso_cela, entidades de localização e relação preso/cela."""

from contexto import *

def executar(spark, path=path):
    """Etapa extraída do notebook original."""
    # ===== CELL 10 =====
    #CORRECAO DEVIDO ERRO DE TIPO DE DADO EM PRESO_CELA - APOS CORRIGIDO REMOVER

    from pyspark.sql import functions as F

    origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_CELA.parquet"
    destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/PRESO_CELA_CORR.parquet"

    df_corr = (
        spark.read.parquet(origem)
        .withColumn("dt_entrada_cela", F.col("dt_entrada_cela").cast("timestamp"))
        .withColumn("dt_registro", F.col("dt_registro").cast("timestamp"))
    )

    df_corr.write.mode("overwrite").parquet(destino)
    df_valid = spark.read.parquet(destino)
    df_valid.createOrReplaceTempView("tmp_preso_cela_corr")
    spark.sql("""
    DROP TABLE IF EXISTS bronze.infopen_preso_cela_corr
    """)
    spark.sql(f"""
    CREATE TABLE bronze.infopen_preso_cela_corr
    USING PARQUET
    LOCATION '{destino}'
    """)

    # CORRECAO DEVIDO ERRO DE TIPO DE DADO EM UP_CELA - APOS CORRIGIDO REMOVER

    from pyspark.sql import functions as F

    origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/UP_CELA.parquet"
    destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/UP_CELA_CORR.parquet"

    df_corr = (
        spark.read.parquet(origem)
        .withColumn("dt_desativacao", F.col("dt_desativacao").cast("timestamp"))
    )

    df_corr.write.mode("overwrite").parquet(destino)

    df_valid = spark.read.parquet(destino)
    df_valid.createOrReplaceTempView("tmp_up_cela_corr")

    spark.sql("""
    DROP TABLE IF EXISTS bronze.infopen_up_cela_corr
    """)

    spark.sql(f"""
    CREATE TABLE bronze.infopen_up_cela_corr
    USING PARQUET
    LOCATION '{destino}'
    """)


    path_origem = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/UP_GALERIA.parquet"
    path_destino = "hdfs://hahdfsprod/data_lake/bronze/INFOPEN/UP_GALERIA_CORR.parquet"

    df_gal = (
        spark.read
        .parquet(path_origem)
        .withColumn(
            "dt_desativacao",
            F.to_timestamp(F.col("dt_desativacao").cast("string"), "yyyyMMdd")
        )
    )

    df_gal.write.mode("overwrite").parquet(path_destino)


    spark.sql("drop table if exists bronze.infopen_up_galeria_corr")

    spark.sql(f"""
    create table bronze.infopen_up_galeria_corr
    using parquet
    location '{path_destino}'
    """)


    # ===== CELL 11 =====
    spark.sql("""
    create table if not exists bronze.sinp_estab_pris (
        sigla       string,
        unidade     string,
        municipio   string,
        endereco    string,
        cep         string,
        uf          string,
        pais        string,
        latitude    double,
        longitude   double
    )
    stored as parquet
    location '/data_lake/bronze/intlpris/sinp_estab_pris'
    """)

    spark.sql("""
    insert overwrite table bronze.sinp_estab_pris values
    ('CDPM' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE MARATAÍZES' ,'Marataízes' ,'Rua Espinha de Peixe, s/n, Bairro Acapulco' ,'29345-000' ,'ES','Brasil',-21.021596,-40.820643),
    ('CDPCI' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE CACHOEIRO DE ITAPEMIRIM' ,'Cachoeiro de Itapemirim' ,'Rodovia do Governador Lacerda de Aguiar, Km 01, Bairro Coronel Borges' ,'29306-095' ,'ES','Brasil',-20.8542956,-41.0902809),
    ('APACF' ,'ASSOCIAÇÃO PROTEÇÃO E ASSIST CONDENADOS CACH ITAPEMIRIM - CRS MASC' ,'Cachoeiro de Itapemirim' ,'Rodovia Cachoeiro-Monte Líbano, Village da Luz' ,'29309-500' ,'ES','Brasil',-20.82597,-41.11094),
    ('CPFCI' ,'CENTRO PRISIONAL FEMININO DE CACHOEIRO DE ITAPEMIRIM' ,'Cachoeiro de Itapemirim' ,'Fazenda Monte Líbano, s/n, Zona Rural' ,'29300-970' ,'ES','Brasil',-20.73284,-41.10819),
    ('PRCI' ,'PENITENCIÁRIA REGIONAL DE CACHOEIRO DE ITAPEMIRIM' ,'Cachoeiro de Itapemirim' ,'Fazenda Monte Líbano, s/n, Zona Rural' ,'29300-970' ,'ES','Brasil',-20.7327951,-41.10819),
    ('CDPG' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE GUARAPARI' ,'Guarapari' ,'Rodovia do Sol, Contorno Argilino Dario, Km 51,3, Maxinda' ,'29200-970' ,'ES','Brasil',-20.6777411,-40.509638),
    ('CDPFVV' ,'CENTRO DE DETENÇÃO PROVISÓRIA FEMININO DE VILA VELHA' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Xuri' ,'29100-000' ,'ES','Brasil',-20.4688193,-40.4644965),
    ('PEVV IV' ,'PENITENCIÁRIA ESTADUAL DE VILA VELHA IV' ,'Vila Velha' ,'Rodovia Governador Mario Covas, s/n, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694866,-40.4674836),
    ('PEVV V' ,'PENITENCIÁRIA ESTADUAL DE VILA VELHA V' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694866,-40.4675164),
    ('PEVV III' ,'PENITENCIÁRIA ESTADUAL DE VILA VELHA III' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694669,-40.4674585),
    ('PEVV VI' ,'PENITENCIÁRIA ESTADUAL DE VILA VELHA VI' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694669,-40.4675415),
    ('PEVV II' ,'PENITENCIÁRIA ESTADUAL DE VILA VELHA II' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694366,-40.4674528),
    ('PSVV' ,'PENITENCIÁRIA SEMIABERTA DE VILA VELHA' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694366,-40.4675472),
    ('CDPVV' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE VILA VELHA' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4694444,-40.4675),
    ('CDPVV 2' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE VILA VELHA II' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.4693995,-40.4675),
    ('PEVV I' ,'PENITENCIÁRIA ESTADUAL DE VILA VELHA I' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.46941,-40.4674692),
    ('PSVV II' ,'PENITENCIÁRIA SEMIABERTA DE VILA VELHA II' ,'Vila Velha' ,'Rodovia BR-101 Sul, Km 313, Fazenda Santa Fé, Xuri' ,'29100-000' ,'ES','Brasil',-20.46941,-40.4675308),
    ('CASCUVI' ,'CASA DE CUSTÓDIA DE VIANA' ,'Viana' ,'BR 262 KM 18 5 S N' ,'29130-010' ,'ES','Brasil',-20.382185,-40.4632381),
    ('CDPFV' ,'CENTRO DE DETENÇÃO PROVISÓRIA FEMININO DE VIANA' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821401,-40.4632381),
    ('CDPV II' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE VIANA II' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821472,-40.4632122),
    ('USP' ,'UNIDADE DE SAÚDE PRISIONAL' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821472,-40.463264),
    ('CTV' ,'CENTRO DE TRIAGEM DE VIANA' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821663,-40.4631945),
    ('DIMCME' ,'DIMCME - SERVIÇO PENAL' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821914,-40.4631907),
    ('PSME II' ,'PENITENCIÁRIA DE SEGURANÇA MÉDIA II' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821663,-40.4632817),
    ('EPEN' ,'ESCOLA PENITENCIÁRIA' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3822144,-40.4632019),
    ('PSMA II' ,'PENITENCIÁRIA DE SEGURANÇA MÁXIMA II' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3822144,-40.4632743),
    ('PAES' ,'PENITENCIÁRIA AGRÍCOLA DO ESPÍRITO SANTO' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3822281,-40.4632246),
    ('PSMA I' ,'PENITENCIÁRIA DE SEGURANÇA MÁXIMA I' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3822281,-40.4632516),
    ('PSME I' ,'PENITENCIÁRIA DE SEGURANÇA MÉDIA I' ,'Viana' ,'Rodovia BR-262, Km 18,5' ,'29130-055' ,'ES','Brasil',-20.3821914,-40.4632855),
    ('CASCUVV' ,'CASA DE CUSTÓDIA DE VILA VELHA' ,'Vila Velha' ,'Rua Mestre Gomes, s/n, Pedra D’Água, Glória' ,'29122-100' ,'ES','Brasil',-20.3314703,-40.3137423),
    ('APAC' ,'ASSOCIAÇÃO DE PROTEÇÃO E ASSISTÊNCIA AOS CONDENADOS' ,'Vila Velha' ,'Praça Almirante Tamandaré, 193' ,'29.100-310' ,'ES','Brasil',-20.3321936,-40.300015),
    ('IRS' ,'INSTITUTO DE READAPTAÇÃO SOCIAL' ,'Vila Velha' ,'Rua Mestre Gomes, Glória' ,'29122-100' ,'ES','Brasil',-20.3261111,-40.3141667),
    ('PSC' ,'PENITENCIÁRIA SEMIABERTA DE CARIACICA' ,'Cariacica' ,'Rodovia Governador José Sette, s/n, Tucum' ,'29152-500' ,'ES','Brasil',-20.32244,-40.36908),
    ('CDPC' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE CARIACICA' ,'Cariacica' ,'Rua Mario Valentim, Santana' ,'29154-585' ,'ES','Brasil',-20.30972,-40.38562),
    ('QCGPMES' ,'QUARTEL DO COMANDO GERAL DA POLÍCIA MILITAR DO EST. DO ESPÍRITO SANTO' ,'Vitória' ,'Avenida Maruípe, 2111, São Cristóvão' ,'29048-463' ,'ES','Brasil',-20.2938889,-40.3122222),
    ('CPFC' ,'CENTRO PRISIONAL FEMININO DE CARIACICA' ,'Cariacica' ,'Rua Ofelino Meireles, Bairro Bubu' ,'29157-766' ,'ES','Brasil',-20.2836,-40.40944),
    ('PFC' ,'PENITENCIÁRIA FEMININA CARIACICA' ,'Cariacica' ,'Rua Armélio/Ofelino Meireles, Bubu' ,'29157-766' ,'ES','Brasil',-20.2835551,-40.40944),
    ('HCTP' ,'HOSPITAL DE CUSTÓDIA E TRATAMENTO PSIQUIÁTRICO' ,'Cariacica' ,'Av. José Sette, s/n, Bairro Roças Velhas' ,'29156-970' ,'ES','Brasil',-20.27756,-40.40907),
    ('UCTP' ,'UNIDADE DE CUSTÓDIA E TRATAMENTO PSIQUIÁTRICO' ,'Cariacica' ,'Av. José Sette, s/n, Bairro Roças Velhas' ,'29156-970' ,'ES','Brasil',-20.2776049,-40.40907),
    ('PEF' ,'PENITENCIÁRIA ESTADUAL FEMININA' ,'Cariacica' ,'Av. José Sette / região de Roças Velhas' ,'29156-970' ,'ES','Brasil',-20.2775151,-40.40907),
    ('CDPS' ,'CENTRO DE DETENÇÃO PROVISÓRIA DA SERRA' ,'Serra' ,'Rodovia do Contorno, BR-101, Km 278, Distrito de Queimados' ,'29160-000' ,'ES','Brasil',-20.1283333,-40.3077778),
    ('CDPA' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE ARACRUZ' ,'Aracruz' ,'Estrada Aracruz-Coqueiral, s/n, Fátima' ,'29192-205' ,'ES','Brasil',-19.9285265,-40.1542386),
    ('PRL' ,'PENITENCIÁRIA REGIONAL DE LINHARES' ,'Linhares' ,'Rua Projetada, s/n, Jardim Laguna' ,'29900-970' ,'ES','Brasil',-19.3768823,-40.0552718),
    ('CRL' ,'CENTRO DE RESSOCIALIZAÇÃO DE LINHARES' ,'Linhares' ,'Rodovia ES-440, Km 02, Bebedouro' ,'29900-970' ,'ES','Brasil',-19.4059,-40.0499),
    ('CDPCOL' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE COLATINA' ,'Colatina' ,'Córrego Santa Fé, s/n' ,'29700-970' ,'ES','Brasil',-19.5498025,-40.6272813),
    ('CPFCOL' ,'CENTRO PRISIONAL FEMININO DE COLATINA' ,'Colatina' ,'Córrego Santa Fé, s/n' ,'29700-970' ,'ES','Brasil',-19.5497576,-40.6272813),
    ('PRCOL' ,'PENITENCIÁRIA REGIONAL DE COLATINA' ,'Colatina' ,'Córrego Santa Fé, s/n' ,'29700-970' ,'ES','Brasil',-19.549825,-40.62724),
    ('PSMECOL' ,'PENITENCIÁRIA DE SEGURANÇA MÉDIA DE COLATINA' ,'Colatina' ,'Córrego Santa Fé, s/n' ,'29700-970' ,'ES','Brasil',-19.549825,-40.6273226),
    ('PSMCOL' ,'PENITENCIÁRIA SEMIABERTA MASCULINA DE COLATINA' ,'Colatina' ,'Avenida das Nações, s/n, Bairro Benjamin Carlos dos Santos (IBC)' ,'29712-408' ,'ES','Brasil',-19.517611,-40.61206),
    ('CDPSDN' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE SÃO DOMINGOS DO NORTE' ,'São Domingos do Norte' ,'Córrego Braço do Sul, Km 80, s/n' ,'29745-000' ,'ES','Brasil',-19.0169,-40.5358),
    ('PRBSF' ,'PENITENCIÁRIA REGIONAL DE BARRA DE SÃO FRANCISCO' ,'Barra de São Francisco' ,'Rodovia ES-320, Km 02' ,'29800-000' ,'ES','Brasil',-18.7525,-40.893),
    ('PSSM' ,'PENITENCIÁRIA SEMIABERTA DE SÃO MATEUS' ,'São Mateus' ,'Rodovia Governador Mario Covas, BR-101 Norte, Km 72,5, s/n, Rio Preto da Rodovia' ,'29940-800' ,'ES','Brasil',-18.7167449,-39.8594),
    ('CDPSM' ,'CENTRO DE DETENÇÃO PROVISÓRIA DE SÃO MATEUS' ,'São Mateus' ,'BR-101 Norte, Km 72,5, Fazenda Rancho das Telhas, Zona Rural' ,'29930-000' ,'ES','Brasil',-18.7167,-39.8594),
    ('PRSM' ,'PENITENCIÁRIA REGIONAL DE SÃO MATEUS' ,'São Mateus' ,'Rodovia Governador Mario Covas, BR-101 Norte, Km 72,5, s/n, Rio Preto da Rodovia' ,'29940-800' ,'ES','Brasil',-18.7166551,-39.8594);
    """)


    # ===== CELL 12 =====
    from pyspark.sql import functions as F
    from pyspark.sql.window import Window

    # ============================================================
    # BASE DE LOCALIZAÇÃO DO PRESO
    # ADAPTADA PARA A NOVA VISÃO:
    #   - CELA + GALERIA + ESTABELECIMENTO
    # SEM PERDER A LÓGICA JÁ ESTABELECIDA
    # ============================================================

    df_base_loc = spark.sql("""
    with loc as (
        select
            e.id_estabelecimento,
            c.id_galeria,
            c.id_cela,

            c.nm_cela,
            c.id_tipo_cela,
            c.qt_capacidadeprojetada,
            c.qt_capacidadeadaptada,
            c.qt_metros_cela,
            c.observacao_cela,
            c.sexo_cela,
            c.dt_desativacao as dt_desativacao_cela,

            g.descricao_galeria,
            g.dt_desativacao as dt_desativacao_galeria,

            e.id_orgao,
            e.id_estabelecimentotipo,
            e.id_estabelecimentotiposeguranca,
            e.estabelecimento_nome,
            e.estabelecimento_sigla,
            e.estabelecimento_cnpj,
            e.estabelecimento_email,
            e.estabelecimento_capacidade,
            e.estabelecimento_macrorregiao,
            e.estabelecimento_situacaoregistro,

            row_number() over (
                partition by c.id_estabelecimento, c.id_galeria, c.id_cela
                order by
                    c.dt_desativacao desc nulls last,
                    g.dt_desativacao desc nulls last,
                    c.nm_cela
            ) as rn_cela
        from bronze.infopen_estabelecimentos e
        left join 
        bronze.infopen_up_cela_corr c on c.id_estabelecimento = e.id_estabelecimento
        left join bronze.infopen_up_galeria_corr g
            on c.id_galeria = g.id_galeria
           and c.id_estabelecimento = g.id_estabelecimento
           ),

    base as (
        select
            c.id_presocela,

            l.id_estabelecimento,
            cast(c.id_preso as string) as id_preso,
            p.id_pessoa,
            p.nome_pessoa,
            l.id_galeria,
            l.id_cela,

            l.nm_cela,
            l.id_tipo_cela,
            l.qt_capacidadeprojetada,
            l.qt_capacidadeadaptada,
            l.qt_metros_cela,
            l.observacao_cela,
            l.sexo_cela,
            l.dt_desativacao_cela,

            l.descricao_galeria,
            l.dt_desativacao_galeria,

            l.id_orgao,
            l.id_estabelecimentotipo,
            l.id_estabelecimentotiposeguranca,
            l.estabelecimento_nome,
            l.estabelecimento_sigla,
            l.estabelecimento_cnpj,
            l.estabelecimento_email,
            l.estabelecimento_capacidade,
            l.estabelecimento_macrorregiao,
            l.estabelecimento_situacaoregistro,

            c.st_cela_ativa,
            c.dt_entrada_cela,
            c.id_usuario_registro,
            c.dt_registro,
            c.observacao_presocela,

            r.municipio,
            r.endereco,
            r.cep,
            r.uf,
            r.pais,
            r.latitude,
            r.longitude,

            row_number() over (
                partition by coalesce(
                    cast(c.id_preso as string),
                    concat(
                        'SEM_PRESO_',
                        cast(l.id_estabelecimento as string), '_',
                        coalesce(cast(l.id_galeria as string), '0'), '_',
                        coalesce(cast(l.id_cela as string), '0')
                    )
                )
                order by
                    c.dt_entrada_cela desc nulls last,
                    c.dt_registro desc nulls last,
                    c.id_presocela desc nulls last
            ) as rn
        from bronze.infopen_estabelecimentos e
        left join loc l
            on l.id_estabelecimento = e.id_estabelecimento
           and l.rn_cela = 1
        left join tmp_preso_cela_corr c
            on c.id_estabelecimento = l.id_estabelecimento
           and c.id_galeria = l.id_galeria
           and c.id_cela = l.id_cela
        left join gold.sinp_pnt_pessoa_preso p
            on cast(c.id_preso as string) = cast(p.id_preso as string)
        left join bronze.sinp_estab_pris r
            on upper(trim(e.estabelecimento_sigla)) = upper(trim(r.sigla))

    )

    select *
    from base
    """)

    # ============================================================
    # ENTIDADE: ESTABELECIMENTO
    # ============================================================

    df_ent_estabelecimento = spark.sql("""
        SELECT 
            cast(e.id_estabelecimento as string) as id_estabelecimento,
            cast(e.id_orgao as string) as id_orgao,
            cast(e.id_estabelecimentotipo as string) as id_estabelecimentotipo,
            case
                when upper(trim(e.estabelecimento_nome)) like '%HOSPITAL DE CUSTÓDIA%' then 'UNIDADE DE SAÚDE / CUSTÓDIA PSIQUIÁTRICA'
                when upper(trim(e.estabelecimento_nome)) like '%TRATAMENTO PSIQUIÁTRICO%' then 'UNIDADE DE SAÚDE / CUSTÓDIA PSIQUIÁTRICA'
                when upper(trim(e.estabelecimento_nome)) like '%UNIDADE DE SAÚDE PRISIONAL%' then 'UNIDADE DE SAÚDE / CUSTÓDIA PSIQUIÁTRICA'
                when upper(trim(e.estabelecimento_nome)) like '%UNIDADE DE CUSTÓDIA E TRATAMENTO PSIQUIÁTRICO%' then 'UNIDADE DE SAÚDE / CUSTÓDIA PSIQUIÁTRICA'
                when upper(trim(e.estabelecimento_nome)) like '%ESCOLA PENITENCIÁRIA%' then 'UNIDADE ESPECIAL / APOIO / FORMAÇÃO'
                when upper(trim(e.estabelecimento_nome)) like '%ASSOCIAÇÃO DE PROTEÇÃO E ASSISTÊNCIA AOS CONDENADOS%' then 'APAC / ASSOCIAÇÃO DE PROTEÇÃO E ASSISTÊNCIA'
                when upper(trim(e.estabelecimento_nome)) like '%ASSOCIAÇÃO PROTEÇÃO E ASSIST CONDENADOS%' then 'APAC / ASSOCIAÇÃO DE PROTEÇÃO E ASSISTÊNCIA'
                when upper(trim(e.estabelecimento_nome)) like '%INSTITUTO DE READAPTAÇÃO SOCIAL%' then 'READAPTAÇÃO / RESSOCIALIZAÇÃO'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO DE RESSOCIALIZAÇÃO%' then 'READAPTAÇÃO / RESSOCIALIZAÇÃO'
                when upper(trim(e.estabelecimento_nome)) like '%CASA DE CUSTÓDIA%' then 'CASA DE CUSTÓDIA'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO DE TRIAGEM%' then 'CENTRO DE DETENÇÃO / TRIAGEM / CENTRO PRISIONAL'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO DE DETENÇÃO PROVISÓRIA%' then 'CENTRO DE DETENÇÃO / TRIAGEM / CENTRO PRISIONAL'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO PRISIONAL%' then 'CENTRO DE DETENÇÃO / TRIAGEM / CENTRO PRISIONAL'
                when upper(trim(e.estabelecimento_nome)) like '%QUARTEL DO COMANDO GERAL%' then 'UNIDADE MILITAR / POLICIAL'
                when upper(trim(e.estabelecimento_nome)) like '%PENITENCIÁRIA AGRÍCOLA%' then 'PENITENCIÁRIA AGRÍCOLA'
                when upper(trim(e.estabelecimento_nome)) like '%PENITENCIÁRIA%' then 'PENITENCIÁRIA'
                when upper(trim(e.estabelecimento_nome)) like '%SERVIÇO PENAL%' then 'SERVIÇO PENAL / ADMINISTRATIVO'
                when e.id_estabelecimentotipo = 1  then 'SERVIÇO PENAL / ADMINISTRATIVO'
                when e.id_estabelecimentotipo = 4  then 'PENITENCIÁRIA'
                when e.id_estabelecimentotipo = 5  then 'PENITENCIÁRIA AGRÍCOLA'
                when e.id_estabelecimentotipo = 6  then 'UNIDADE DE SAÚDE / CUSTÓDIA PSIQUIÁTRICA'
                when e.id_estabelecimentotipo = 8  then 'UNIDADE ESPECIAL / APOIO / FORMAÇÃO / APAC'
                when e.id_estabelecimentotipo = 12 then 'READAPTAÇÃO / RESSOCIALIZAÇÃO'
                when e.id_estabelecimentotipo = 14 then 'CENTRO DE DETENÇÃO / TRIAGEM / CENTRO PRISIONAL'
                when e.id_estabelecimentotipo = 15 then 'CASA DE CUSTÓDIA'
                when e.id_estabelecimentotipo = 17 then 'UNIDADE MILITAR / POLICIAL'
                else 'NÃO CLASSIFICADO'
            end as ds_estabelecimentotipo,
            cast(e.id_estabelecimentotiposeguranca as string) as id_estabelecimentotiposeguranca,
            case
                when upper(trim(e.estabelecimento_nome)) like '%SEMIABERTA%' then 'SEMIABERTO / BAIXA CONTENÇÃO / ALTERNATIVO'
                when upper(trim(e.estabelecimento_nome)) like '%APAC%' then 'SEMIABERTO / BAIXA CONTENÇÃO / ALTERNATIVO'
                when upper(trim(e.estabelecimento_nome)) like '%ASSOCIAÇÃO DE PROTEÇÃO E ASSISTÊNCIA AOS CONDENADOS%' then 'SEMIABERTO / BAIXA CONTENÇÃO / ALTERNATIVO'
                when upper(trim(e.estabelecimento_nome)) like '%ASSOCIAÇÃO PROTEÇÃO E ASSIST CONDENADOS%' then 'SEMIABERTO / BAIXA CONTENÇÃO / ALTERNATIVO'
                when upper(trim(e.estabelecimento_nome)) like '%SEGURANÇA MÉDIA%' then 'SEGURANÇA MÉDIA'
                when upper(trim(e.estabelecimento_nome)) like '%SEGURANCA MEDIA%' then 'SEGURANÇA MÉDIA'
                when upper(trim(e.estabelecimento_nome)) like '%SEGURANÇA MÁXIMA%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%SEGURANCA MAXIMA%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%VILA VELHA I%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%VILA VELHA II%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%VILA VELHA III%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%VILA VELHA VI%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%QUARTEL DO COMANDO GERAL%' then 'SEGURANÇA MÁXIMA'
                when upper(trim(e.estabelecimento_nome)) like '%HOSPITAL DE CUSTÓDIA%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%TRATAMENTO PSIQUIÁTRICO%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%UNIDADE DE SAÚDE PRISIONAL%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%UNIDADE DE CUSTÓDIA E TRATAMENTO PSIQUIÁTRICO%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO DE DETENÇÃO PROVISÓRIA%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO DE TRIAGEM%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%CENTRO PRISIONAL%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%CASA DE CUSTÓDIA%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%PENITENCIÁRIA REGIONAL%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%PENITENCIÁRIA ESTADUAL FEMININA%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%PENITENCIÁRIA FEMININA%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when upper(trim(e.estabelecimento_nome)) like '%PENITENCIÁRIA AGRÍCOLA%' then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                when e.id_estabelecimentotiposeguranca = 1 then 'SEMIABERTO / BAIXA CONTENÇÃO / ALTERNATIVO'
                when e.id_estabelecimentotiposeguranca = 2 then 'SEGURANÇA MÉDIA'
                when e.id_estabelecimentotiposeguranca = 3 then 'SEGURANÇA MÁXIMA'
                when e.id_estabelecimentotiposeguranca = 4 then 'PADRÃO / OUTROS / NÃO ESPECIALIZADO'
                else 'NÃO CLASSIFICADO'
            end as ds_estabelecimentotiposeguranca,
            e.estabelecimento_nome,
            e.estabelecimento_sigla,
            e.estabelecimento_cnpj,
            e.estabelecimento_email,
            e.estabelecimento_capacidade,
            e.estabelecimento_macrorregiao,
            e.estabelecimento_situacaoregistro,
            p.municipio,
            p.endereco,
            p.cep,
            p.uf,
            p.pais,
            p.latitude,
            p.longitude
        FROM bronze.infopen_estabelecimentos e
        inner join bronze.sinp_estab_pris p
            on upper(trim(e.estabelecimento_sigla)) = upper(trim(p.sigla))
    """)

    # ============================================================
    # ENTIDADE: GALERIA
    # ============================================================

    df_ent_galeria = (
        df_base_loc
        .filter(F.col("id_galeria").isNotNull())
        .select(
            F.md5(
                F.concat(
                    F.lit("E"),
                    F.coalesce(F.col("id_estabelecimento").cast("string"), F.lit("")),
                    F.lit("G"),
                    F.coalesce(F.col("id_galeria").cast("string"), F.lit(""))
                )
            ).alias("id_galeria"),

            F.col("id_estabelecimento").cast("string").alias("id_estabelecimento"),
            F.col("id_galeria").cast("string").alias("id_galeria_original"),

            F.when(
                F.col("descricao_galeria").isNotNull() & (F.length(F.trim(F.col("descricao_galeria"))) > 0),
                F.trim(F.col("descricao_galeria"))
            ).otherwise(
                F.concat(
                    F.lit("Galeria "),
                    F.when(
                        F.col("id_galeria").cast("string").rlike("^[0-9]+$"),
                        F.lpad(F.col("id_galeria").cast("string"), 3, "0")
                    ).otherwise(F.col("id_galeria").cast("string"))
                )
            ).alias("nome_galeria"),

            F.trim(F.col("descricao_galeria")).alias("descricao_galeria"),
            F.col("dt_desativacao_galeria").alias("dt_desativacao_galeria")
        )
        .distinct()
    )

    # ============================================================
    # ENTIDADE: CELA
    # ============================================================

    df_ent_cela = (
        df_base_loc
        .filter(
            F.col("id_galeria").isNotNull() &
            F.col("id_cela").isNotNull()
        )
        .select(
            F.md5(
                F.concat(
                    F.lit("E"),
                    F.coalesce(F.col("id_estabelecimento").cast("string"), F.lit("")),
                    F.lit("G"),
                    F.coalesce(F.col("id_galeria").cast("string"), F.lit("")),
                    F.lit("C"),
                    F.coalesce(F.col("id_cela").cast("string"), F.lit(""))
                )
            ).alias("id_cela"),

            F.md5(
                F.concat(
                    F.lit("E"),
                    F.coalesce(F.col("id_estabelecimento").cast("string"), F.lit("")),
                    F.lit("G"),
                    F.coalesce(F.col("id_galeria").cast("string"), F.lit(""))
                )
            ).alias("id_galeria"),

            F.col("id_estabelecimento").cast("string").alias("id_estabelecimento_origem"),
            F.col("id_galeria").cast("string").alias("id_galeria_origem"),
            F.col("id_cela").cast("string").alias("id_cela_origem"),

            F.when(
                F.col("nm_cela").isNotNull() & (F.length(F.trim(F.col("nm_cela"))) > 0),
                F.trim(F.col("nm_cela"))
            ).otherwise(
                F.concat(
                    F.lit("Cela "),
                    F.when(
                        F.col("id_cela").cast("string").rlike("^[0-9]+$"),
                        F.lpad(F.col("id_cela").cast("string"), 3, "0")
                    ).otherwise(F.col("id_cela").cast("string"))
                )
            ).alias("nome_cela"),

            F.col("id_tipo_cela").cast("int").alias("id_tipo_cela"),
            F.col("qt_capacidadeprojetada").cast("int").alias("qt_capacidade_projetada"),
            F.col("qt_capacidadeadaptada").cast("int").alias("qt_capacidade_adaptada"),
            F.col("qt_metros_cela").cast("int").alias("qt_metros_cela"),
            F.trim(F.col("observacao_cela")).alias("observacao_cela"),
            F.col("sexo_cela").alias("sexo_cela"),
            F.col("dt_desativacao_cela").alias("dt_desativacao_cela"),
            F.trim(F.col("descricao_galeria")).alias("descricao_galeria"),
            F.col("estabelecimento_nome").alias("estabelecimento_nome"),

            F.concat_ws(
                " > ",
                F.col("estabelecimento_nome"),
                F.col("descricao_galeria"),
                F.when(
                    F.col("nm_cela").isNotNull() & (F.length(F.trim(F.col("nm_cela"))) > 0),
                    F.trim(F.col("nm_cela"))
                ).otherwise(
                    F.concat(
                        F.lit("Cela "),
                        F.when(
                            F.col("id_cela").cast("string").rlike("^[0-9]+$"),
                            F.lpad(F.col("id_cela").cast("string"), 3, "0")
                        ).otherwise(F.col("id_cela").cast("string"))
                    )
                )
            ).alias("ds_localizacao_completa")
        )
        .dropDuplicates(["id_cela"])
    )


    # ===== CELL 13 =====
    # ============================================================
    # ESCRITA GOLD
    # ============================================================

    tabela = "sinp_ent_estabelecimento"
    df_ent_estabelecimento.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_ent_estabelecimento, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_estabelecimento")

    tabela = "sinp_ent_galeria"
    df_ent_galeria.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_ent_galeria, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_galeria")

    tabela = "sinp_ent_cela"
    df_ent_cela.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_ent_cela, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_cela")


    # ===== CELL 14 =====
    rel_passo = spark.sql("""
    select
            id as id_mov,
            trim(cast(infopen as string)) as id_preso,
            nome as nome_livro,
            trim(cast(cela_atual as string)) as cela_atual,
            trim(cast(cela_destino as string)) as cela_destino,
            autorizacao,
            motivo,
            cast(data_registro as timestamp) as dt_movimentacao,
            equipe_id,
            presidio_id
        from bronze.livros_acesso_unidade_trocacela
        where trim(cast(infopen as string)) is not null
          and trim(cast(infopen as string)) <> ''
          and cast(data_registro as timestamp) is not null
          and (
                (trim(cast(cela_atual as string)) is not null and trim(cast(cela_atual as string)) <> '')
             or (trim(cast(cela_destino as string)) is not null and trim(cast(cela_destino as string)) <> '')
          )

    """)

    tabela = "sinp_preso_cela_p1"
    rel_passo.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(rel_passo, "gold", tabela, f"{path}{tabela}")

    #rel_passo.show(30,False)

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_preso_cela_p1")

    rel_passo = spark.sql("""
    select
            s.id_mov as id_evento_origem,
            id_preso,
            s.cela_atual as id_cela_origem_livro,
            s.dt_movimentacao as dt_entrada_uso_cela,
            s.dt_movimentacao as dt_evento_referencia,
            'PRIMEIRA_ORIGEM_INFERIDA' as origem_regra,
            1 as prioridade_regra,
            s.autorizacao as autorizacao,
            s.motivo as motivo,
            s.nome_livro as nome_livro,
            s.equipe_id as equipe_id,
            s.presidio_id as presidio_id
        from (
            select
                id_preso,
                min(
                    named_struct(
                        'dt_movimentacao', dt_movimentacao,
                        'id_mov', id_mov,
                        'cela_atual', cela_atual,
                        'autorizacao', autorizacao,
                        'motivo', motivo,
                        'nome_livro', nome_livro,
                        'equipe_id', equipe_id,
                        'presidio_id', presidio_id
                    )
                ) as s
            from gold.sinp_preso_cela_p1
            where cela_atual is not null
              and cela_atual <> ''
            group by id_preso
        ) a
    """)

    tabela = "sinp_preso_cela_p2"
    rel_passo.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(rel_passo, "gold", tabela, f"{path}{tabela}")

    #rel_passo.show(30,False)

    rel_passo = spark.sql("""
        select
            id_mov as id_evento_origem,
            id_preso,
            cela_destino as id_cela_origem_livro,
            dt_movimentacao as dt_entrada_uso_cela,
            dt_movimentacao as dt_evento_referencia,
            'DESTINO_MOVIMENTACAO' as origem_regra,
            2 as prioridade_regra,
            autorizacao,
            motivo,
            nome_livro,
            equipe_id,
            presidio_id
        from gold.sinp_preso_cela_p1
        where cela_destino is not null
          and cela_destino <> ''
    """)

    tabela = "sinp_preso_cela_p3"
    rel_passo.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(rel_passo, "gold", tabela, f"{path}{tabela}")


    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_preso_cela_p2")
    spark.sql("refresh table gold.sinp_preso_cela_p3")


    #rel_passo.show(30,False)

    rel_passo = spark.sql("""
        select * from gold.sinp_preso_cela_p2
        union all
        select * from gold.sinp_preso_cela_p3
    """)

    tabela = "sinp_preso_cela_p4"
    rel_passo.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(rel_passo, "gold", tabela, f"{path}{tabela}")




    # ===== CELL 15 =====
    spark.sql("drop table if exists gold.sinp_preso_cela_p1")
    spark.sql("drop table if exists gold.sinp_preso_cela_p2")
    spark.sql("drop table if exists gold.sinp_preso_cela_p3")

    spark.catalog.clearCache()
    spark.sql("refresh table gold.sinp_preso_cela_p4")

    rel_passo = spark.sql("""
    select
        md5(concat_ws('||',
            trim(cast(id_preso as string)),
            cast(dt_entrada_uso_cela as string),
            trim(cast(id_cela_origem_livro as string))
        )) as id_preso_data_cela,
        id_evento_origem as id_mov,
        id_preso,
        id_cela_origem_livro,
        dt_entrada_uso_cela,
        dt_evento_referencia,
        origem_regra,
        prioridade_regra,
        autorizacao,
        motivo,
        nome_livro,
        equipe_id,
        presidio_id,
        row_number() over (
            partition by md5(concat_ws('||',
                trim(cast(id_preso as string)),
                cast(dt_entrada_uso_cela as string),
                trim(cast(id_cela_origem_livro as string))
            ))
            order by
                prioridade_regra asc,
                dt_evento_referencia asc,
                origem_regra asc
        ) as rn
    from gold.sinp_preso_cela_p4
    where id_preso is not null
      and trim(cast(id_preso as string)) <> ''
      and dt_entrada_uso_cela is not null
      and id_cela_origem_livro is not null
      and trim(cast(id_cela_origem_livro as string)) <> ''
    """)

    #rel_passo.show(30,False)

    tabela = "sinp_preso_cela_p5"
    rel_passo.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(rel_passo, "gold", tabela, f"{path}{tabela}")


    # ===== CELL 16 =====
    df_ultima_data = spark.sql("""
    select id_preso, max(dt_entrada_uso_cela) as dataRef from gold.sinp_preso_cela_p5 group by 1
    """)
    tabela = "ref_maxData"
    df_ultima_data.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_ultima_data, "gold", tabela, f"{path}{tabela}")

    df_base_preso_cela = spark.sql("""
    select
        p5.id_preso_data_cela as id_rel_preso_cela,
        c.id_cela,
        p.id_pessoa,p.nome_pessoa,
        p5.id_mov,
        p5.id_preso,
        p5.id_cela_origem_livro as numero_cela,
        p5.dt_entrada_uso_cela,
        p5.dt_evento_referencia,
        p5.origem_regra,
        p5.prioridade_regra,
        p5.autorizacao,
        p5.motivo,
        p5.nome_livro,
        p5.equipe_id,
        p5.presidio_id,
        case when m.dataRef is null then 'Anterior' else 'Atual' end as situacao
    from gold.sinp_preso_cela_p5 p5
        inner join gold.sinp_ent_cela c on p5.id_cela_origem_livro = c.id_cela_origem
        inner join gold.sinp_pnt_pessoa_preso p on p5.id_preso = p.id_preso
        left join gold.ref_maxData m on p5.id_preso = m.id_preso and p5.dt_entrada_uso_cela = m.dataRef
    where rn= 1
    """)

    tabela = "sinp_rel_preso_cela"
    df_base_preso_cela.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_preso_cela, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rel_preso_cela")


    df_base_preso_cela_exc = spark.sql("""
    select
        p5.id_preso_data_cela as id_rel_preso_cela,
        c.id_cela,
        p.id_pessoa,p.nome_pessoa,
        p5.id_mov,
        p5.id_preso,
        p5.id_cela_origem_livro as numero_cela,
        p5.dt_entrada_uso_cela,
        p5.dt_evento_referencia,
        p5.origem_regra,
        p5.prioridade_regra,
        p5.autorizacao,
        p5.motivo,
        p5.nome_livro,
        p5.equipe_id,
        p5.presidio_id,
        case when m.dataRef is null then 'Anterior' else 'Atual' end as situacao
    from gold.sinp_preso_cela_p5 p5
        inner join gold.sinp_ent_cela c on p5.id_cela_origem_livro = c.id_cela_origem
        inner join gold.sinp_pnt_pessoa_preso p on p5.id_preso = p.id_preso
        left join gold.ref_maxData m on p5.id_preso = m.id_preso and p5.dt_entrada_uso_cela = m.dataRef
    where rn> 1
    """)

    tabela = "sinp_rel_preso_cela_exc"
    df_base_preso_cela_exc.write.mode("overwrite") \
        .option("maxRecordsPerFile", 1_000_000) \
        .option("compression", "snappy") \
        .parquet(f"{path}{tabela}")
    write_impala_table_partioned(df_base_preso_cela_exc, "gold", tabela, f"{path}{tabela}")
    enviar_gold_para_postgres(f"gold.{tabela}", "id_rel_preso_cela")


