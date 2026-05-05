# -*- coding: utf-8 -*-
"""Snippets auxiliares de debug e validação."""

from contexto import *

def executar(spark, path=path):
    """Etapa extraída do notebook original."""
    # ===== CELL 22 =====
    spark.sql("show tables in bronze like '*familiar*'").show(200, False)
    spark.sql("show tables in bronze like '*famil*'").show(200, False)
    spark.sql("show tables in bronze like '*vinc*'").show(200, False)

    spark.sql("describe bronze.livros_acesso_unidade_visitafamiliar").show(200, False)
    spark.sql("describe bronze.livros_acesso_unidade_controlefamiliares").show(200, False)
    spark.sql("describe bronze.livros_acesso_unidade_interno").show(200, False)


    # ===== CELL 33 =====
    dado = spark.sql("SELECT * FROM bronze.livros_acesso_unidade_historicalpolicial where history_id='7712'")
    dado.show(5,False)
    dado2 = spark.sql("SELECT * FROM bronze.siarhes_servidores where nome_servidor='ADONIS FELIX RODRIGUES'")
    dado2.show(5,False)


