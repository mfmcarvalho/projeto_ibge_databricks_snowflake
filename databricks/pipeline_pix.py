# Databricks notebook source
# Databricks notebook source

# ============================================================
# PIPELINE_PIX
#
# Orquestração no MESMO contexto Python:
#
# 1. setup_snowflake
# 2. extract_bcb
# 3. load_snowflake
#
# Os notebooks auxiliares são carregados com %run.
# Dessa forma, as funções ficam disponíveis neste notebook
# e os print()/logs aparecem em tempo real durante a execução.
#
# historico=true
#     -> carga histórica completa
#
# historico=false
#     -> carga incremental M0
# ============================================================


# COMMAND ----------
# DEPENDÊNCIAS
#
# Deixe o %pip apenas no pipeline_pix.
#
# Remova dos notebooks auxiliares:
#
#   %pip install ...
#   dbutils.library.restartPython()
#
# ============================================================

%pip install "snowflake-connector-python[pandas]>=4.8,<5" "cryptography>=42,<49"


# COMMAND ----------
# VALIDAR PASTA DO PROJETO
# ============================================================

import os


PROJECT_DIR = (
    "/Workspace/Users/"
    "mfmcarvalho0@gmail.com/"
    "IBGE_BCB"
)


print(
    f"PROJECT_DIR = {PROJECT_DIR}",
    flush=True
)


if not os.path.isdir(
    PROJECT_DIR
):

    raise FileNotFoundError(
        f"Pasta do projeto não encontrada: "
        f"{PROJECT_DIR}"
    )


arquivos_encontrados = sorted(
    os.listdir(
        PROJECT_DIR
    )
)


print(
    "",
    flush=True
)

print(
    "Arquivos encontrados:",
    flush=True
)


for arquivo in arquivos_encontrados:

    print(
        f"  - {arquivo}",
        flush=True
    )


arquivos_obrigatorios = [

    "setup_snowflake.ipynb",

    "extract_bcb.ipynb",

    "load_snowflake.ipynb"

]


arquivos_faltantes = [

    arquivo

    for arquivo
    in arquivos_obrigatorios

    if arquivo
    not in arquivos_encontrados

]


if arquivos_faltantes:

    raise FileNotFoundError(
        "Arquivos obrigatórios não encontrados: "
        f"{arquivos_faltantes}"
    )


print(
    "",
    flush=True
)

print(
    "[OK] Estrutura do projeto validada.",
    flush=True
)


# COMMAND ----------
# CARREGAR FUNÇÕES
# SETUP SNOWFLAKE
#
# IMPORTANTE:
# esta linha precisa ficar sozinha na célula.
# ============================================================

%run ./setup_snowflake.ipynb


# COMMAND ----------
# CARREGAR FUNÇÕES
# EXTRACT BCB / IBGE / SIDRA
# ============================================================

%run ./extract_bcb.ipynb


# COMMAND ----------
# CARREGAR FUNÇÕES
# LOAD SNOWFLAKE
# ============================================================

%run ./load_snowflake.ipynb


# COMMAND ----------
# IMPORTS
# ============================================================

import time
import traceback

from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# LOG DO PIPELINE
# ============================================================

def log_pipeline(
    mensagem
):

    horario = (
        datetime.now(
            ZoneInfo(
                "America/Sao_Paulo"
            )
        )
        .strftime(
            "%H:%M:%S"
        )
    )


    print(
        f"[{horario}] {mensagem}",
        flush=True
    )


# COMMAND ----------
# PARÂMETRO HISTORICO
#
# Se estiver sendo executado manualmente e o widget ainda
# não existir, ele será criado com default = false.
#
# Se estiver rodando via Workflow/Job e o parâmetro já tiver
# sido recebido, o valor será preservado.
# ============================================================

try:

    HISTORICO_STR = (
        dbutils.widgets
        .get(
            "historico"
        )
        .strip()
        .lower()
    )


except Exception:

    dbutils.widgets.dropdown(

        "historico",

        "false",

        [
            "true",
            "false"
        ]

    )


    HISTORICO_STR = (
        dbutils.widgets
        .get(
            "historico"
        )
        .strip()
        .lower()
    )


# ============================================================
# VALIDAR PARÂMETRO
# ============================================================

if HISTORICO_STR not in [

    "true",

    "false"

]:

    raise ValueError(
        f"Parâmetro historico inválido: "
        f"{HISTORICO_STR}. "
        f"Use 'true' ou 'false'."
    )


HISTORICO = (
    HISTORICO_STR
    == "true"
)


# COMMAND ----------
# VALIDAR FUNÇÕES CARREGADAS PELO %run
# ============================================================

FUNCOES_OBRIGATORIAS = {

    "executar_setup_snowflake":

        globals().get(
            "executar_setup_snowflake"
        ),


    "executar_extract_bcb":

        globals().get(
            "executar_extract_bcb"
        ),


    "executar_load_snowflake":

        globals().get(
            "executar_load_snowflake"
        )

}


funcoes_faltantes = [

    nome

    for nome, funcao
    in FUNCOES_OBRIGATORIAS.items()

    if not callable(
        funcao
    )

]


if funcoes_faltantes:

    raise ImportError(

        "As seguintes funções não foram "
        "carregadas pelos notebooks via %run: "

        f"{funcoes_faltantes}. "

        "Verifique se os notebooks "
        "setup_snowflake, extract_bcb e "
        "load_snowflake realmente definem essas funções."

    )


log_pipeline(
    "[OK] Funções carregadas com sucesso."
)


log_pipeline(
    f"HISTORICO = {HISTORICO}"
)


log_pipeline(

    f"MODO = "
    f"{'HISTÓRICO' if HISTORICO else 'INCREMENTAL M0'}"

)


# COMMAND ----------
# FUNÇÃO GENÉRICA PARA EXECUTAR ETAPA
#
# Como as funções estão no mesmo contexto Python,
# seus print() e log() aparecem durante a execução.
# ============================================================

def executar_etapa(

    nome,

    funcao,

    **kwargs

):

    log_pipeline(
        ""
    )


    log_pipeline(
        "=" * 80
    )


    log_pipeline(
        f"INICIANDO: {nome}"
    )


    log_pipeline(
        "=" * 80
    )


    inicio = (
        time.time()
    )


    try:

        retorno = (
            funcao(
                **kwargs
            )
        )


        tempo = (
            time.time()
            - inicio
        )


        log_pipeline(
            ""
        )


        log_pipeline(
            f"[OK] {nome}"
        )


        log_pipeline(

            f"Tempo: "
            f"{tempo / 60:.2f} min"

        )


        return {

            "ETAPA":
                nome,

            "STATUS":
                "OK",

            "TEMPO_SEGUNDOS":
                round(
                    tempo,
                    2
                ),

            "RETORNO":
                retorno

        }


    except Exception as erro:

        tempo = (
            time.time()
            - inicio
        )


        log_pipeline(
            ""
        )


        log_pipeline(
            f"[ERRO] {nome}"
        )


        log_pipeline(

            f"Tempo até falha: "
            f"{tempo / 60:.2f} min"

        )


        log_pipeline(

            f"{type(erro).__name__}: "
            f"{erro}"

        )


        print(
            "",
            flush=True
        )


        print(
            "TRACEBACK:",
            flush=True
        )


        traceback.print_exc()


        raise RuntimeError(

            f"Pipeline interrompido "
            f"na etapa '{nome}'."

        ) from erro


# COMMAND ----------
# INÍCIO DO PIPELINE
# ============================================================

inicio_pipeline = (
    time.time()
)


resultados = []


log_pipeline(
    ""
)


log_pipeline(
    "=" * 80
)


log_pipeline(
    "PIPELINE PIX"
)


log_pipeline(
    "DATABRICKS -> SNOWFLAKE"
)


log_pipeline(

    f"MODO = "
    f"{'HISTÓRICO' if HISTORICO else 'INCREMENTAL M0'}"

)


log_pipeline(
    "=" * 80
)


# COMMAND ----------
# ETAPA 1
# SETUP SNOWFLAKE
# ============================================================

resultado_setup = (
    executar_etapa(

        nome=
            "01_SETUP_SNOWFLAKE",

        funcao=
            executar_setup_snowflake,

        dbutils=
            dbutils

    )
)


resultados.append(
    resultado_setup
)


# COMMAND ----------
# ETAPA 2
# EXTRACT BCB + IBGE + SIDRA
#
# Os logs da extração aparecem em tempo real.
#
# Em historico=true:
#     BCB é consultado em paralelo.
#
# Em historico=false:
#     apenas M0.
# ============================================================

resultado_extract = (
    executar_etapa(

        nome=
            "02_EXTRACT_BCB_IBGE_SIDRA",

        funcao=
            executar_extract_bcb,

        spark=
            spark,

        historico=
            HISTORICO,

        display_fn=
            display

    )
)


resultados.append(
    resultado_extract
)


# COMMAND ----------
# ETAPA 3
# LOAD SNOWFLAKE
#
# Inclui:
#
# Databricks Bronze
#        ↓
# Snowflake Bronze
#        ↓
# Silver
#        ↓
# Gold
#        ↓
# Marts
# ============================================================

resultado_load = (
    executar_etapa(

        nome=
            "03_LOAD_SNOWFLAKE",

        funcao=
            executar_load_snowflake,

        spark=
            spark,

        dbutils=
            dbutils,

        historico=
            HISTORICO,

        display_fn=
            display

    )
)


resultados.append(
    resultado_load
)


# COMMAND ----------
# RESUMO FINAL
# ============================================================

tempo_total = (
    time.time()
    - inicio_pipeline
)


log_pipeline(
    ""
)


log_pipeline(
    "=" * 80
)


log_pipeline(
    "RESUMO FINAL DO PIPELINE"
)


log_pipeline(
    "=" * 80
)


for resultado in resultados:

    log_pipeline(

        f"{resultado['ETAPA']} "

        f"| {resultado['STATUS']} "

        f"| "

        f"{resultado['TEMPO_SEGUNDOS']:.2f}s"

    )


log_pipeline(
    ""
)


log_pipeline(

    f"Tempo total: "
    f"{tempo_total / 60:.2f} min"

)


log_pipeline(
    ""
)


log_pipeline(
    "=" * 80
)


log_pipeline(
    "PIPELINE FINALIZADO COM SUCESSO"
)


log_pipeline(
    "=" * 80
)