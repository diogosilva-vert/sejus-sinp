# -*- coding: utf-8 -*-
"""Orquestrador central do projeto SINP, com execução por import."""

from datetime import datetime
from time import perf_counter

from contexto import spark, path

from etapa_01_ent_pessoa import executar as etapa_01_ent_pessoa
from etapa_02_localizacao_cela import executar as etapa_02_localizacao_cela
from etapa_03_prontuarios import executar as etapa_03_prontuarios
from etapa_04_alvaras import executar as etapa_04_alvaras
from etapa_05_encarceramento import executar as etapa_05_encarceramento
from etapa_06_fat_visita_advogado import executar as etapa_06_fat_visita_advogado
from etapa_07_fat_visita_familiar import executar as etapa_07_fat_visita_familiar
from etapa_08_fat_visita_religiosa import executar as etapa_08_fat_visita_religiosa
from etapa_09_fat_ocorrencia_infopen import executar as etapa_09_fat_ocorrencia_infopen
from etapa_10_fat_ocorrencia_livro import executar as etapa_10_fat_ocorrencia_livro
from etapa_11_ent_movimentacao import executar as etapa_11_ent_movimentacao
from etapa_12_ent_enderecos import executar as etapa_12_ent_enderecos
from etapa_13_processos import executar as etapa_13_processos


ETAPAS = [
    ("01_ent_pessoa", etapa_01_ent_pessoa),
    ("02_localizacao_cela", etapa_02_localizacao_cela),
    ("03_prontuarios", etapa_03_prontuarios),
    ("04_alvaras", etapa_04_alvaras),
    ("05_encarceramento", etapa_05_encarceramento),
    ("06_fat_visita_advogado", etapa_06_fat_visita_advogado),
    ("07_fat_visita_familiar", etapa_07_fat_visita_familiar),
    ("08_fat_visita_religiosa", etapa_08_fat_visita_religiosa),
    ("09_fat_ocorrencia_infopen", etapa_09_fat_ocorrencia_infopen),
    ("10_fat_ocorrencia_livro", etapa_10_fat_ocorrencia_livro),
    ("11_ent_movimentacao", etapa_11_ent_movimentacao),
    ("12_ent_enderecos", etapa_12_ent_enderecos),
    ("13_processos", etapa_13_processos),
]


def formatar_duracao(segundos):
    segundos = int(round(segundos))

    horas = segundos // 3600
    minutos = (segundos % 3600) // 60
    segundos = segundos % 60

    return f"{horas:02d}:{minutos:02d}:{segundos:02d}"


def executar_todas(spark_session=spark, output_path=path, stop_on_error=True):
    inicio_geral_dt = datetime.now()
    inicio_geral_perf = perf_counter()

    resumo_execucao = []

    print("=" * 100)
    print(f"[ORQUESTRADOR][INICIO] {inicio_geral_dt.strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 100)

    for nome, funcao in ETAPAS:
        inicio_dt = datetime.now()
        inicio_perf = perf_counter()

        print("-" * 100)
        print(f"[INICIO] {nome} | {inicio_dt.strftime('%Y-%m-%d %H:%M:%S')}")

        status = "OK"
        mensagem_erro = None

        try:
            funcao(spark_session, output_path)

        except Exception as e:
            status = "ERRO"
            mensagem_erro = str(e)

        fim_dt = datetime.now()
        duracao_seg = perf_counter() - inicio_perf
        duracao_fmt = formatar_duracao(duracao_seg)

        resumo_execucao.append({
            "etapa": nome,
            "status": status,
            "inicio": inicio_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "fim": fim_dt.strftime("%Y-%m-%d %H:%M:%S"),
            "duracao": duracao_fmt,
            "erro": mensagem_erro
        })

        if status == "OK":
            print(f"[FIM] {nome} | {fim_dt.strftime('%Y-%m-%d %H:%M:%S')} | DURAÇÃO: {duracao_fmt}")
        else:
            print(f"[ERRO] {nome} | {fim_dt.strftime('%Y-%m-%d %H:%M:%S')} | DURAÇÃO: {duracao_fmt}")
            print(f"[ERRO][DETALHE] {mensagem_erro}")

            if stop_on_error:
                fim_geral_dt = datetime.now()
                duracao_total_fmt = formatar_duracao(perf_counter() - inicio_geral_perf)

                print("=" * 100)
                print(f"[ORQUESTRADOR][INTERROMPIDO] {fim_geral_dt.strftime('%Y-%m-%d %H:%M:%S')} | DURAÇÃO TOTAL: {duracao_total_fmt}")
                print("=" * 100)

                raise Exception(f"Falha na etapa {nome}: {mensagem_erro}")

    fim_geral_dt = datetime.now()
    duracao_total_fmt = formatar_duracao(perf_counter() - inicio_geral_perf)

    print("=" * 100)
    print("[RESUMO EXECUÇÃO]")
    print("=" * 100)

    for item in resumo_execucao:
        print(
            f"{item['status']:>4} | "
            f"{item['etapa']:<32} | "
            f"INÍCIO: {item['inicio']} | "
            f"FIM: {item['fim']} | "
            f"DURAÇÃO: {item['duracao']}"
        )

    print("=" * 100)
    print(f"[ORQUESTRADOR][FIM] {fim_geral_dt.strftime('%Y-%m-%d %H:%M:%S')} | DURAÇÃO TOTAL: {duracao_total_fmt}")
    print("=" * 100)

    return resumo_execucao


if __name__ == "__main__":
    executar_todas()