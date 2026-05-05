# -*- coding: utf-8 -*-
"""Orquestrador central do projeto SINP, com execução por import."""

from contexto import spark, path

from etapa_01_ent_pessoa import executar as etapa_01_ent_pessoa
from etapa_02_localizacao_cela import executar as etapa_02_localizacao_cela
from etapa_03_prontuarios_alvaras_encarceramento import executar as etapa_03_prontuarios_alvaras_encarceramento
from etapa_04_fat_visita_advogado import executar as etapa_04_fat_visita_advogado
from etapa_05_fat_visita_familiar import executar as etapa_05_fat_visita_familiar
from etapa_06_fat_visita_religiosa import executar as etapa_06_fat_visita_religiosa
from etapa_07_fat_ocorrencia_infopen import executar as etapa_07_fat_ocorrencia_infopen
from etapa_08_fat_ocorrencia_livro import executar as etapa_08_fat_ocorrencia_livro
from etapa_09_ent_movimentacao import executar as etapa_09_ent_movimentacao

ETAPAS = [
    ("01_ent_pessoa", etapa_01_ent_pessoa),
    ("02_localizacao_cela", etapa_02_localizacao_cela),
    ("03_prontuarios_alvaras_encarceramento", etapa_03_prontuarios_alvaras_encarceramento),
    ("04_fat_visita_advogado", etapa_04_fat_visita_advogado),
    ("05_fat_visita_familiar", etapa_05_fat_visita_familiar),
    ("06_fat_visita_religiosa", etapa_06_fat_visita_religiosa),
    ("07_fat_ocorrencia_infopen", etapa_07_fat_ocorrencia_infopen),
    ("08_fat_ocorrencia_livro", etapa_08_fat_ocorrencia_livro),
    ("09_ent_movimentacao", etapa_09_ent_movimentacao),
]


def executar_todas(stop_on_error=True, spark_session=None, output_path=None):
    spark_session = spark if spark_session is None else spark_session
    output_path = path if output_path is None else output_path

    for nome, funcao in ETAPAS:
        print(f"[INICIO] {nome}")
        try:
            funcao(spark_session, output_path)
            print(f"[FIM] {nome}")
        except Exception as e:
            print(f"[ERRO] {nome}: {e}")
            if stop_on_error:
                raise


if __name__ == "__main__":
    executar_todas()