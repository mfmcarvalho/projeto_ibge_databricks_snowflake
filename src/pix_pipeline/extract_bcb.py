# Databricks notebook source
"""Extração Databricks Bronze para PIX/IBGE/BCB.

A execução fica dentro de executar_extract_bcb(), permitindo que o
pipeline principal chame a função no mesmo processo Python e receba
os prints/logs em tempo real.
"""

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import requests


def log(mensagem):
    horario = datetime.now(ZoneInfo("America/Sao_Paulo")).strftime("%H:%M:%S")
    print(f"[{horario}] {mensagem}", flush=True)


def executar_extract_bcb(
    spark,
    historico=False,
    display_fn=None,
):
    """Executa a extração Bronze e mostra os logs no chamador."""

    def _display(obj):
        if display_fn is not None:
            display_fn(obj)
            return
        if hasattr(obj, "show"):
            obj.show(truncate=False)
            return
        try:
            print(obj.to_string(), flush=True)
        except Exception:
            print(obj, flush=True)

    # ============================================================
    # CONFIGURAÇÃO DAS CAMADAS
    # ============================================================

    SCHEMA_BRONZE = "bronze"


    spark.sql(
        f"""
        CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}
        """
    )


    log(
        f"[OK] Schema {SCHEMA_BRONZE} disponível."
    )


    # ============================================================
    # PARÂMETRO DE CARGA
    # ============================================================

    if isinstance(historico, str):
        HISTORICO = historico.strip().lower() == "true"
    else:
        HISTORICO = bool(historico)

    log(f"HISTORICO = {HISTORICO}")


    # ============================================================
    # MÊS ATUAL
    # ============================================================

    agora = datetime.now(
        ZoneInfo(
            "America/Sao_Paulo"
        )
    )


    M0 = int(
        agora.strftime(
            "%Y%m"
        )
    )


    log(
        f"M0 = {M0}"
    )


    # ============================================================
    # PRIMEIRO PERÍODO HISTÓRICO PIX
    # ============================================================

    ANOMES_INICIO = 202011


    # ============================================================
    # CONFIGURAÇÃO BCB
    # ============================================================

    BASE_URL_BCB = (
        "https://olinda.bcb.gov.br/olinda/servico/"
        "Pix_DadosAbertos/versao/v1/odata"
    )


    URL_BCB = (
        f"{BASE_URL_BCB}/"
        "TransacoesPixPorMunicipio(DataBase=@DataBase)"
    )


    # ============================================================
    # PARALELISMO / RETENTATIVAS BCB
    # ============================================================

    MAX_WORKERS_BCB = 5
    MAX_TENTATIVAS_HTTP = 3
    BACKOFF_HTTP_SEGUNDOS = 2

    STATUS_RETRY = {
        429,
        500,
        502,
        503,
        504
    }


    # ============================================================
    # CONFIGURAÇÃO IBGE
    # ============================================================

    URL_ESTADOS_IBGE = (
        "https://servicodados.ibge.gov.br/"
        "api/v1/localidades/estados"
    )


    URL_MUNICIPIOS_IBGE = (
        "https://servicodados.ibge.gov.br/"
        "api/v1/localidades/municipios"
    )


    # ============================================================
    # FUNÇÃO AUXILIAR HTTP
    #
    # Retry com backoff para falhas transitórias.
    # ============================================================

    def get_json(
        url,
        params=None,
        timeout=120,
        contexto=None
    ):

        contexto_log = (
            f" | {contexto}"
            if contexto
            else ""
        )

        ultimo_erro = None

        for tentativa in range(
            1,
            MAX_TENTATIVAS_HTTP + 1
        ):

            inicio = time.time()

            try:

                response = requests.get(
                    url,
                    params=params,
                    timeout=(
                        10,
                        timeout
                    )
                )

                tempo = (
                    time.time()
                    - inicio
                )

                log(
                    f"[HTTP]{contexto_log} "
                    f"tentativa={tentativa}/"
                    f"{MAX_TENTATIVAS_HTTP} "
                    f"status={response.status_code} "
                    f"tempo={tempo:.2f}s"
                )

                if (
                    response.status_code
                    in STATUS_RETRY
                ):

                    raise requests.HTTPError(
                        f"HTTP transitório "
                        f"{response.status_code}",
                        response=response
                    )

                response.raise_for_status()

                return response.json()

            except (
                requests.RequestException,
                ValueError
            ) as erro:

                ultimo_erro = erro

                if (
                    tentativa
                    >= MAX_TENTATIVAS_HTTP
                ):
                    break

                espera = (
                    BACKOFF_HTTP_SEGUNDOS
                    * (
                        2
                        ** (
                            tentativa - 1
                        )
                    )
                )

                log(
                    f"[RETRY]{contexto_log} "
                    f"erro={type(erro).__name__}: "
                    f"{erro} | "
                    f"nova tentativa em {espera}s"
                )

                time.sleep(
                    espera
                )

        raise RuntimeError(
            f"Falha HTTP após "
            f"{MAX_TENTATIVAS_HTTP} tentativas"
            f"{contexto_log}: "
            f"{ultimo_erro}"
        )


    # ============================================================
    # GERAR COMPETÊNCIAS YYYYMM
    # ============================================================

    def gerar_competencias(
        inicio,
        fim
    ):

        ano = (
            inicio // 100
        )

        mes = (
            inicio % 100
        )

        ano_fim = (
            fim // 100
        )

        mes_fim = (
            fim % 100
        )

        competencias = []


        while (
            ano < ano_fim
            or (
                ano == ano_fim
                and mes <= mes_fim
            )
        ):

            competencia = (
                ano * 100
                + mes
            )

            competencias.append(
                competencia
            )

            mes += 1

            if mes == 13:

                mes = 1
                ano += 1


        return competencias


    # ============================================================
    # DEFINIR COMPETÊNCIAS DA EXECUÇÃO
    # ============================================================

    if HISTORICO:

        competencias = (
            gerar_competencias(
                ANOMES_INICIO,
                M0
            )
        )

        log(
            "\nMODO: HISTÓRICO"
        )

    else:

        competencias = [
            M0
        ]

        log(
            "\nMODO: INCREMENTAL M0"
        )


    log(
        f"Primeira competência: "
        f"{competencias[0]}"
    )

    log(
        f"Última competência: "
        f"{competencias[-1]}"
    )

    log(
        f"Quantidade: "
        f"{len(competencias)}"
    )


    # ============================================================
    # FUNÇÃO - EXTRAIR E TRATAR UMA COMPETÊNCIA BCB
    #
    # Independente e segura para execução em threads.
    # Nenhuma escrita Spark ocorre dentro das threads.
    # ============================================================

    def extrair_competencia_bcb(
        competencia
    ):

        inicio = time.time()

        params = {
            "@DataBase":
                f"'{competencia}'",

            "$format":
                "json"
        }

        dados = get_json(
            URL_BCB,
            params=params,
            timeout=120,
            contexto=f"BCB {competencia}"
        )

        registros = (
            dados.get(
                "value",
                []
            )
        )

        df_mes = pd.DataFrame(
            registros
        )

        if df_mes.empty:
            return {
                "competencia": competencia,
                "status": "sem_dados",
                "df": None,
                "registros": 0,
                "tempo": time.time() - inicio
            }

        df_mes.columns = [
            coluna.upper()
            for coluna in df_mes.columns
        ]

        if (
            "ANOMES"
            not in df_mes.columns
        ):
            raise ValueError(
                f"Competência {competencia}: "
                f"coluna ANOMES não encontrada."
            )

        df_mes[
            "ANOMES"
        ] = (
            pd.to_numeric(
                df_mes[
                    "ANOMES"
                ],
                errors="coerce"
            )
            .astype("Int64")
        )

        df_mes = (
            df_mes.loc[
                df_mes[
                    "ANOMES"
                ]
                == competencia
            ]
            .copy()
        )

        if df_mes.empty:
            return {
                "competencia": competencia,
                "status": "sem_dados_apos_filtro",
                "df": None,
                "registros": 0,
                "tempo": time.time() - inicio
            }

        if (
            "MUNICIPIO_IBGE"
            in df_mes.columns
        ):
            df_mes[
                "MUNICIPIO_IBGE"
            ] = (
                pd.to_numeric(
                    df_mes[
                        "MUNICIPIO_IBGE"
                    ],
                    errors="coerce"
                )
                .round()
                .astype("Int64")
            )

        return {
            "competencia": competencia,
            "status": "ok",
            "df": df_mes,
            "registros": len(df_mes),
            "tempo": time.time() - inicio
        }


    # ============================================================
    # EXTRAÇÃO BCB EM PARALELO
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "ETAPA 1 - EXTRAÇÃO BCB EM PARALELO"
    )

    log(
        "=" * 80
    )

    log(
        f"Competências: "
        f"{len(competencias)}"
    )

    log(
        f"Workers simultâneos: "
        f"{MAX_WORKERS_BCB}"
    )

    inicio_extracao_bcb = time.time()

    resultados_bcb = {}
    erros_bcb = []
    competencias_sem_dados = []

    workers_efetivos = min(
        MAX_WORKERS_BCB,
        len(competencias)
    )

    with ThreadPoolExecutor(
        max_workers=workers_efetivos,
        thread_name_prefix="bcb"
    ) as executor:

        futuros = {
            executor.submit(
                extrair_competencia_bcb,
                competencia
            ): competencia
            for competencia in competencias
        }

        total_concluido = 0

        for futuro in as_completed(
            futuros
        ):

            competencia = (
                futuros[
                    futuro
                ]
            )

            total_concluido += 1

            try:

                resultado = futuro.result()

                if (
                    resultado[
                        "status"
                    ]
                    == "ok"
                ):

                    resultados_bcb[
                        competencia
                    ] = resultado[
                        "df"
                    ]

                    log(
                        f"[OK] "
                        f"[{total_concluido}/"
                        f"{len(competencias)}] "
                        f"{competencia} | "
                        f"{resultado['registros']:,} registros | "
                        f"{resultado['tempo']:.2f}s"
                    )

                else:

                    competencias_sem_dados.append(
                        competencia
                    )

                    log(
                        f"[AVISO] "
                        f"[{total_concluido}/"
                        f"{len(competencias)}] "
                        f"{competencia} | "
                        f"{resultado['status']} | "
                        f"{resultado['tempo']:.2f}s"
                    )

            except Exception as erro:

                erros_bcb.append(
                    {
                        "ANOMES": competencia,
                        "ERRO": str(erro)
                    }
                )

                log(
                    f"[ERRO] "
                    f"[{total_concluido}/"
                    f"{len(competencias)}] "
                    f"{competencia}: "
                    f"{type(erro).__name__}: "
                    f"{erro}"
                )

    tempo_extracao_bcb = (
        time.time()
        - inicio_extracao_bcb
    )

    log(
        "\n"
        + "=" * 80
    )

    log(
        "RESUMO EXTRAÇÃO BCB"
    )

    log(
        "=" * 80
    )

    log(
        f"Competências solicitadas: "
        f"{len(competencias):,}"
    )

    log(
        f"Competências com dados: "
        f"{len(resultados_bcb):,}"
    )

    log(
        f"Competências sem dados: "
        f"{len(competencias_sem_dados):,}"
    )

    log(
        f"Competências com erro: "
        f"{len(erros_bcb):,}"
    )

    log(
        f"Tempo da extração BCB: "
        f"{tempo_extracao_bcb / 60:.2f} min"
    )

    if erros_bcb:

        erros_ordenados = sorted(
            erros_bcb,
            key=lambda x: x["ANOMES"]
        )

        log(
            "Erros BCB:"
        )

        for item in erros_ordenados:
            log(
                f"  {item['ANOMES']}: "
                f"{item['ERRO']}"
            )

        raise RuntimeError(
            f"Falha na extração de "
            f"{len(erros_bcb)} competência(s) BCB. "
            f"A Bronze não será sobrescrita com "
            f"histórico incompleto."
        )

    dataframes_bcb = [
        resultados_bcb[
            competencia
        ]
        for competencia in sorted(
            resultados_bcb
        )
    ]


    # ============================================================
    # VALIDAR EXTRAÇÃO BCB
    # ============================================================

    if not dataframes_bcb:

        raise ValueError(
            "Nenhuma competência BCB "
            "foi extraída."
        )


    # ============================================================
    # CONSOLIDAR BCB
    # ============================================================

    df_bcb = pd.concat(
        dataframes_bcb,
        ignore_index=True
    )


    df_bcb[
        "ANOMES"
    ] = (
        pd.to_numeric(
            df_bcb[
                "ANOMES"
            ],
            errors="coerce"
        )
        .astype("Int64")
    )


    df_bcb[
        "MUNICIPIO_IBGE"
    ] = (
        pd.to_numeric(
            df_bcb[
                "MUNICIPIO_IBGE"
            ],
            errors="coerce"
        )
        .round()
        .astype("Int64")
    )


    log(
        "\n"
        + "=" * 80
    )

    log(
        "BCB CONSOLIDADO"
    )

    log(
        "=" * 80
    )


    log(
        f"Registros: "
        f"{len(df_bcb):,}"
    )


    log(
        f"Períodos: "
        f"{df_bcb['ANOMES'].nunique():,}"
    )


    log(
        f"Primeiro período: "
        f"{df_bcb['ANOMES'].min()}"
    )


    log(
        f"Último período: "
        f"{df_bcb['ANOMES'].max()}"
    )


    # ============================================================
    # VALIDAÇÃO INCREMENTAL
    # ============================================================

    periodos_extraidos = sorted(
        df_bcb[
            "ANOMES"
        ]
        .dropna()
        .astype(int)
        .unique()
        .tolist()
    )


    if not HISTORICO:

        if (
            periodos_extraidos
            != [M0]
        ):

            raise ValueError(
                f"Carga incremental deveria "
                f"conter somente {M0}. "
                f"Encontrado: "
                f"{periodos_extraidos}"
            )


    # ============================================================
    # SALVAR BCB RAW
    # ============================================================

    spark_bcb = (
        spark.createDataFrame(
            df_bcb
        )
    )


    (
        spark_bcb
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{SCHEMA_BRONZE}."
            "pix_bcb_raw"
        )
    )


    log(
        "\n[OK] "
        "bronze.pix_bcb_raw criada."
    )


    # ============================================================
    # ETAPA 2 - LEITURA IBGE ESTADOS
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "ETAPA 2 - LEITURA IBGE ESTADOS"
    )

    log(
        "=" * 80
    )


    estados_ibge = get_json(
        URL_ESTADOS_IBGE,
        timeout=120
    )


    if not isinstance(
        estados_ibge,
        list
    ):

        raise ValueError(
            "API de estados IBGE "
            "não retornou uma lista."
        )


    log(
        f"Estados recebidos: "
        f"{len(estados_ibge):,}"
    )


    # ============================================================
    # DATAFRAME ESTADOS
    # ============================================================

    registros_estados = []


    for estado in estados_ibge:

        regiao = (
            estado.get(
                "regiao"
            )
            or {}
        )


        registros_estados.append(
            {

                "ESTADO_IBGE":
                    estado.get("id"),

                "SIGLA_UF":
                    estado.get("sigla"),

                "ESTADO":
                    estado.get("nome"),

                "REGIAO_IBGE":
                    regiao.get("id"),

                "SIGLA_REGIAO":
                    regiao.get("sigla"),

                "REGIAO":
                    regiao.get("nome")

            }
        )


    df_estados = pd.DataFrame(
        registros_estados
    )


    for coluna in [
        "ESTADO_IBGE",
        "REGIAO_IBGE"
    ]:

        df_estados[
            coluna
        ] = (
            pd.to_numeric(
                df_estados[
                    coluna
                ],
                errors="coerce"
            )
            .astype("Int64")
        )


    log(
        f"Estados tratados: "
        f"{len(df_estados):,}"
    )


    _display(
        df_estados
    )


    # ============================================================
    # SALVAR ESTADOS RAW/TRATADOS
    # ============================================================

    (
        spark.createDataFrame(
            df_estados
        )
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{SCHEMA_BRONZE}."
            "ibge_estados"
        )
    )


    log(
        "[OK] "
        "bronze.ibge_estados criada."
    )


    # ============================================================
    # ETAPA 3 - LEITURA IBGE MUNICÍPIOS
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "ETAPA 3 - LEITURA IBGE MUNICÍPIOS"
    )

    log(
        "=" * 80
    )


    municipios_ibge = get_json(
        URL_MUNICIPIOS_IBGE,
        timeout=120
    )


    if not isinstance(
        municipios_ibge,
        list
    ):

        raise ValueError(
            "API de municípios IBGE "
            "não retornou uma lista."
        )


    log(
        f"Municípios recebidos: "
        f"{len(municipios_ibge):,}"
    )


    # ============================================================
    # CRIAR DE/PARA MUNICÍPIO -> UF -> REGIÃO
    # ============================================================

    log(
        "\nCriando de/para "
        "Município -> UF -> Região..."
    )


    registros_municipios = []

    municipios_invalidos = []


    for indice, municipio in enumerate(
        municipios_ibge,
        start=1
    ):

        codigo_ibge = (
            municipio.get("id")
        )


        nome_municipio = (
            municipio.get("nome")
        )


        # ========================================================
        # HIERARQUIA ANTIGA
        # ========================================================

        microrregiao = (
            municipio.get(
                "microrregiao"
            )
            or {}
        )


        mesorregiao = (
            microrregiao.get(
                "mesorregiao"
            )
            or {}
        )


        uf = (
            mesorregiao.get(
                "UF"
            )
            or {}
        )


        # ========================================================
        # FALLBACK PARA HIERARQUIA NOVA
        # ========================================================

        if not uf:

            regiao_imediata = (
                municipio.get(
                    "regiao-imediata"
                )
                or {}
            )


            regiao_intermediaria = (
                regiao_imediata.get(
                    "regiao-intermediaria"
                )
                or {}
            )


            uf = (
                regiao_intermediaria.get(
                    "UF"
                )
                or {}
            )


        # ========================================================
        # REGIÃO
        # ========================================================

        regiao = (
            uf.get(
                "regiao"
            )
            or {}
        )


        # ========================================================
        # VALIDAÇÕES
        # ========================================================

        problemas = []


        if codigo_ibge is None:

            problemas.append(
                "MUNICIPIO_IBGE ausente"
            )


        if not nome_municipio:

            problemas.append(
                "MUNICIPIO ausente"
            )


        if uf.get("id") is None:

            problemas.append(
                "ESTADO_IBGE ausente"
            )


        if not uf.get("nome"):

            problemas.append(
                "ESTADO ausente"
            )


        if regiao.get("id") is None:

            problemas.append(
                "REGIAO_IBGE ausente"
            )


        if not regiao.get("nome"):

            problemas.append(
                "REGIAO ausente"
            )


        if problemas:

            municipios_invalidos.append(
                {

                    "MUNICIPIO_IBGE":
                        codigo_ibge,

                    "MUNICIPIO":
                        nome_municipio,

                    "PROBLEMAS":
                        problemas,

                    "REGISTRO_ORIGINAL":
                        municipio

                }
            )


        # ========================================================
        # REGISTRO TRATADO
        # ========================================================

        registros_municipios.append(
            {

                "MUNICIPIO_IBGE":
                    codigo_ibge,

                "MUNICIPIO":
                    nome_municipio,

                "ESTADO_IBGE":
                    uf.get("id"),

                "SIGLA_UF":
                    uf.get("sigla"),

                "ESTADO":
                    uf.get("nome"),

                "REGIAO_IBGE":
                    regiao.get("id"),

                "SIGLA_REGIAO":
                    regiao.get("sigla"),

                "REGIAO":
                    regiao.get("nome")

            }
        )


        if (
            indice == 1
            or indice % 500 == 0
            or indice
            == len(municipios_ibge)
        ):

            percentual = (
                indice
                / len(municipios_ibge)
                * 100
            )

            log(
                f"[PROGRESSO] "
                f"{indice:,}/"
                f"{len(municipios_ibge):,} "
                f"({percentual:.2f}%)"
            )


    # ============================================================
    # DATAFRAME MUNICÍPIOS
    # ============================================================

    df_municipios = (
        pd.DataFrame(
            registros_municipios
        )
    )


    for coluna in [
        "MUNICIPIO_IBGE",
        "ESTADO_IBGE",
        "REGIAO_IBGE"
    ]:

        df_municipios[
            coluna
        ] = (
            pd.to_numeric(
                df_municipios[
                    coluna
                ],
                errors="coerce"
            )
            .astype("Int64")
        )


    # ============================================================
    # VALIDAÇÃO MUNICÍPIOS
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "VALIDAÇÃO IBGE"
    )

    log(
        "=" * 80
    )


    log(
        f"Registros: "
        f"{len(df_municipios):,}"
    )


    log(
        f"Municípios distintos: "
        f"{df_municipios['MUNICIPIO_IBGE'].nunique():,}"
    )


    log(
        f"Estados distintos: "
        f"{df_municipios['ESTADO_IBGE'].nunique():,}"
    )


    log(
        f"Regiões distintas: "
        f"{df_municipios['REGIAO_IBGE'].nunique():,}"
    )


    log(
        f"Municípios inválidos: "
        f"{len(municipios_invalidos):,}"
    )


    # ============================================================
    # DUPLICIDADES
    # ============================================================

    duplicados_ibge = (
        df_municipios
        .duplicated(
            subset=[
                "MUNICIPIO_IBGE"
            ],
            keep=False
        )
    )


    if duplicados_ibge.any():

        _display(
            df_municipios.loc[
                duplicados_ibge
            ]
        )

        raise ValueError(
            "Existem MUNICIPIO_IBGE "
            "duplicados no IBGE."
        )


    log(
        "[OK] MUNICIPIO_IBGE "
        "é único no IBGE."
    )


    # ============================================================
    # SALVAR DE/PARA MUNICÍPIO
    # ============================================================

    (
        spark.createDataFrame(
            df_municipios
        )
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{SCHEMA_BRONZE}."
            "ibge_municipio_uf"
        )
    )


    log(
        "[OK] "
        "bronze.ibge_municipio_uf criada."
    )


    # ============================================================
    # ETAPA 4 - PREPARAÇÃO DO BCB PARA O JOIN
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "ETAPA 4 - CRUZAMENTO BCB x IBGE"
    )

    log(
        "=" * 80
    )


    # ============================================================
    # DIAGNÓSTICO DE MUNICIPIO_IBGE NULO
    # ============================================================

    mask_ibge_nulo = (
        df_bcb[
            "MUNICIPIO_IBGE"
        ]
        .isna()
    )


    qtd_ibge_nulo = int(
        mask_ibge_nulo.sum()
    )


    log(
        f"Registros BCB sem "
        f"MUNICIPIO_IBGE: "
        f"{qtd_ibge_nulo:,}"
    )


    if qtd_ibge_nulo > 0:

        colunas_debug = [
            coluna

            for coluna in [

                "ANOMES",
                "MUNICIPIO_IBGE",
                "MUNICIPIO",
                "ESTADO_IBGE",
                "ESTADO",
                "REGIAO",
                "SIGLA_REGIAO"

            ]

            if coluna
            in df_bcb.columns
        ]


        _display(
            df_bcb.loc[
                mask_ibge_nulo,
                colunas_debug
            ]
        )


    # ============================================================
    # REMOVER REGISTROS SEM CHAVE
    # ============================================================

    df_bcb_valido = (
        df_bcb.loc[
            ~mask_ibge_nulo
        ]
        .copy()
    )


    log(
        f"Registros aptos ao join: "
        f"{len(df_bcb_valido):,}"
    )


    # ============================================================
    # REMOVER GEOGRAFIA ORIGINAL DO BCB
    #
    # IBGE passa a ser fonte oficial para geografia.
    # ============================================================

    colunas_geo_bcb = [

        "MUNICIPIO",

        "ESTADO_IBGE",
        "ESTADO",

        "REGIAO_IBGE",
        "REGIAO",
        "SIGLA_REGIAO",

        "SIGLA_UF"

    ]


    colunas_remover = [

        coluna

        for coluna
        in colunas_geo_bcb

        if coluna
        in df_bcb_valido.columns

    ]


    df_bcb_valido.drop(
        columns=colunas_remover,
        inplace=True
    )


    log(
        "Colunas geográficas "
        "removidas do BCB:"
    )

    log(
        colunas_remover
    )


    # ============================================================
    # JOIN BCB x IBGE
    #
    # BCB:
    # várias linhas por município
    # devido aos meses
    #
    # IBGE:
    # uma linha por município
    # ============================================================

    df_bronze = (
        df_bcb_valido.merge(

            df_municipios,

            how="left",

            on="MUNICIPIO_IBGE",

            validate="many_to_one",

            indicator=True

        )
    )


    log(
        f"\nRegistros após join: "
        f"{len(df_bronze):,}"
    )


    # ============================================================
    # RESULTADO DO JOIN
    # ============================================================

    log(
        "\nResultado do join:"
    )


    log(
        df_bronze[
            "_merge"
        ]
        .value_counts(
            dropna=False
        )
    )


    # ============================================================
    # MUNICÍPIOS NÃO ENCONTRADOS
    # ============================================================

    nao_encontrados = (
        df_bronze.loc[
            df_bronze[
                "_merge"
            ]
            != "both"
        ]
        .copy()
    )


    if not nao_encontrados.empty:

        log(
            "\n[ERRO] "
            "Municípios BCB sem "
            "correspondência IBGE."
        )


        log(
            f"Registros: "
            f"{len(nao_encontrados):,}"
        )


        log(
            f"Municípios: "
            f"{nao_encontrados['MUNICIPIO_IBGE'].nunique():,}"
        )


        _display(
            nao_encontrados[
                [
                    "ANOMES",
                    "MUNICIPIO_IBGE"
                ]
            ]
            .drop_duplicates()
            .sort_values(
                [
                    "ANOMES",
                    "MUNICIPIO_IBGE"
                ]
            )
        )


        raise ValueError(
            "Existem municípios do BCB "
            "sem correspondência no IBGE."
        )


    log(
        "\n[OK] Todos os municípios "
        "foram encontrados no IBGE."
    )


    # ============================================================
    # REMOVER INDICADOR DO MERGE
    # ============================================================

    df_bronze.drop(
        columns=[
            "_merge"
        ],
        inplace=True
    )


    # ============================================================
    # ESTRUTURA FINAL DA BRONZE
    # ============================================================

    COLUNAS_BRONZE = [

        "ANOMES",

        "MUNICIPIO_IBGE",
        "MUNICIPIO",

        "ESTADO_IBGE",
        "ESTADO",

        "REGIAO_IBGE",
        "REGIAO",
        "SIGLA_REGIAO",

        "VL_PAGADORPF",
        "QT_PAGADORPF",

        "VL_PAGADORPJ",
        "QT_PAGADORPJ",

        "VL_RECEBEDORPF",
        "QT_RECEBEDORPF",

        "VL_RECEBEDORPJ",
        "QT_RECEBEDORPJ",

        "QT_PES_PAGADORPF",
        "QT_PES_PAGADORPJ",

        "QT_PES_RECEBEDORPF",
        "QT_PES_RECEBEDORPJ"

    ]


    colunas_faltantes = [

        coluna

        for coluna
        in COLUNAS_BRONZE

        if coluna
        not in df_bronze.columns

    ]


    if colunas_faltantes:

        raise ValueError(
            f"Colunas faltantes: "
            f"{colunas_faltantes}"
        )


    df_bronze = (
        df_bronze[
            COLUNAS_BRONZE
        ]
        .copy()
    )


    # ============================================================
    # DATA DE INGESTÃO
    # ============================================================

    df_bronze[
        "DATA_INGESTAO"
    ] = agora


    # ============================================================
    # VALIDAÇÃO DA GRANULARIDADE
    #
    # 1 registro:
    # ANOMES + MUNICIPIO_IBGE
    # ============================================================

    duplicidades = (
        df_bronze
        .groupby(
            [
                "ANOMES",
                "MUNICIPIO_IBGE"
            ]
        )
        .size()
        .reset_index(
            name="REGISTROS"
        )
    )


    duplicidades = (
        duplicidades.loc[
            duplicidades[
                "REGISTROS"
            ]
            > 1
        ]
    )


    if not duplicidades.empty:

        log(
            "\n[ERRO] "
            "Duplicidades encontradas:"
        )

        _display(
            duplicidades
        )

        raise ValueError(
            "Existem duplicidades em "
            "ANOMES + MUNICIPIO_IBGE."
        )


    log(
        "[OK] Granularidade validada."
    )


    # ============================================================
    # RESUMO FINAL
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "BRONZE FINAL"
    )

    log(
        "=" * 80
    )


    log(
        f"Registros: "
        f"{len(df_bronze):,}"
    )


    log(
        f"Períodos: "
        f"{df_bronze['ANOMES'].nunique():,}"
    )


    log(
        f"Municípios: "
        f"{df_bronze['MUNICIPIO_IBGE'].nunique():,}"
    )


    log(
        f"Estados: "
        f"{df_bronze['ESTADO_IBGE'].nunique():,}"
    )


    log(
        f"Regiões: "
        f"{df_bronze['REGIAO_IBGE'].nunique():,}"
    )


    log(
        f"Primeiro período: "
        f"{df_bronze['ANOMES'].min()}"
    )


    log(
        f"Último período: "
        f"{df_bronze['ANOMES'].max()}"
    )


    # ============================================================
    # AMOSTRA
    # ============================================================

    _display(
        df_bronze.head(20)
    )


    # ============================================================
    # ETAPA 5 - SALVAR BRONZE FINAL
    # ============================================================

    spark_bronze = (
        spark.createDataFrame(
            df_bronze
        )
    )


    (
        spark_bronze
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            f"{SCHEMA_BRONZE}."
            "transacoes_pix_municipio_raw"
        )
    )


    log(
        "\n[OK] "
        "bronze.transacoes_pix_municipio_raw criada."
    )


    # ============================================================
    # ETAPA 6 - SIDRA 5938 / PIB MUNICIPAL
    #
    # Gera bronze.ibge_sidra_5938_raw em formato longo:
    # ANO x MUNICIPIO_IBGE x VARIAVEL_ID.
    #
    # historico=True:
    #     consulta todos os anos disponíveis.
    #
    # historico=False:
    #     mantém o histórico existente e reconsulta o último
    #     ano já carregado + eventuais anos novos.
    #     Se a tabela ainda não existir, faz bootstrap completo.
    # ============================================================

    log("")
    log("=" * 80)
    log("ETAPA 6 - EXTRAÇÃO SIDRA 5938 / PIB MUNICIPAL")
    log("=" * 80)

    BASE_URL_SIDRA = (
        "https://servicodados.ibge.gov.br/"
        "api/v3/agregados"
    )

    TABELA_SIDRA = "5938"

    VARIAVEIS_SIDRA = {
        "37": "Produto Interno Bruto a preços correntes",
        "543": "Impostos líquidos de subsídios sobre produtos",
        "498": "Valor adicionado bruto total",
        "513": "Valor adicionado bruto da agropecuária",
        "517": "Valor adicionado bruto da indústria",
        "6575": "Valor adicionado bruto dos serviços, exclusive administração",
        "525": "Valor adicionado bruto da administração pública",
    }

    VARIAVEIS_SIDRA_STR = ",".join(
        VARIAVEIS_SIDRA.keys()
    )

    MAX_WORKERS_SIDRA = 3


    def converter_valor_sidra(valor):

        if valor is None:
            return None

        texto = str(valor).strip()

        if texto in {"", "...", "-", "X"}:
            return None

        try:
            return float(
                texto.replace(",", ".")
            )
        except ValueError:
            return None


    def listar_periodos_sidra():

        url = (
            f"{BASE_URL_SIDRA}/"
            f"{TABELA_SIDRA}/periodos"
        )

        dados = get_json(
            url,
            timeout=120,
            contexto="SIDRA 5938 períodos"
        )

        if not isinstance(dados, list):
            raise ValueError(
                "Resposta de períodos do SIDRA 5938 "
                "não é uma lista."
            )

        periodos = []

        for item in dados:

            if isinstance(item, dict):
                valor = item.get("id")
            else:
                valor = item

            try:
                ano = int(str(valor).strip())
            except Exception:
                continue

            if 1900 <= ano <= 2100:
                periodos.append(ano)

        periodos = sorted(set(periodos))

        if not periodos:
            raise ValueError(
                "Nenhum período anual válido encontrado "
                "no SIDRA 5938."
            )

        return periodos


    def extrair_sidra_ano(ano):

        inicio_sidra = time.time()

        url = (
            f"{BASE_URL_SIDRA}/"
            f"{TABELA_SIDRA}/"
            f"periodos/{ano}/"
            f"variaveis/{VARIAVEIS_SIDRA_STR}"
            f"?localidades=N6[all]"
        )

        dados = get_json(
            url,
            timeout=300,
            contexto=f"SIDRA 5938 {ano}"
        )

        if not isinstance(dados, list):
            raise ValueError(
                f"SIDRA {ano}: resposta não é uma lista."
            )

        registros = []

        for bloco in dados:

            if not isinstance(bloco, dict):
                continue

            variavel_id = str(
                bloco.get("id", "")
            ).strip()

            if variavel_id not in VARIAVEIS_SIDRA:
                continue

            variavel_nome = (
                bloco.get("variavel")
                or bloco.get("nome")
                or VARIAVEIS_SIDRA[variavel_id]
            )

            resultados = bloco.get(
                "resultados",
                []
            )

            if not isinstance(resultados, list):
                continue

            for resultado in resultados:

                if not isinstance(resultado, dict):
                    continue

                series = resultado.get(
                    "series",
                    []
                )

                if not isinstance(series, list):
                    continue

                for item in series:

                    if not isinstance(item, dict):
                        continue

                    localidade = item.get(
                        "localidade",
                        {}
                    )

                    if not isinstance(localidade, dict):
                        continue

                    nivel = localidade.get(
                        "nivel",
                        {}
                    )

                    if (
                        isinstance(nivel, dict)
                        and str(nivel.get("id", "")) != "N6"
                    ):
                        continue

                    try:
                        municipio_ibge = int(
                            str(localidade.get("id")).strip()
                        )
                    except Exception:
                        continue

                    serie = item.get(
                        "serie",
                        {}
                    )

                    if not isinstance(serie, dict):
                        continue

                    valor = converter_valor_sidra(
                        serie.get(str(ano))
                    )

                    registros.append(
                        {
                            "ANO": ano,
                            "MUNICIPIO_IBGE": municipio_ibge,
                            "VARIAVEL_ID": variavel_id,
                            "VARIAVEL_NOME": str(variavel_nome),
                            "VALOR": valor,
                            "DATA_INGESTAO": agora,
                        }
                    )

        tempo_sidra = time.time() - inicio_sidra

        return {
            "ano": ano,
            "registros": registros,
            "qtd": len(registros),
            "tempo": tempo_sidra,
        }


    periodos_sidra = listar_periodos_sidra()

    log(
        f"SIDRA disponível: "
        f"{periodos_sidra[0]} -> {periodos_sidra[-1]} "
        f"({len(periodos_sidra)} anos)"
    )

    tabela_sidra = (
        f"{SCHEMA_BRONZE}."
        "ibge_sidra_5938_raw"
    )

    sidra_existente = spark.catalog.tableExists(
        tabela_sidra
    )

    if HISTORICO or not sidra_existente:

        anos_sidra_consulta = periodos_sidra

        if not sidra_existente:
            log(
                "SIDRA Bronze ainda não existe: "
                "bootstrap histórico completo."
            )

    else:

        ano_max_existente = (
            spark.table(tabela_sidra)
            .selectExpr("MAX(ANO) AS ANO_MAX")
            .collect()[0]["ANO_MAX"]
        )

        if ano_max_existente is None:
            anos_sidra_consulta = periodos_sidra
        else:
            ano_max_existente = int(ano_max_existente)
            anos_sidra_consulta = [
                ano
                for ano in periodos_sidra
                if ano >= ano_max_existente
            ]

            if not anos_sidra_consulta:
                anos_sidra_consulta = [
                    periodos_sidra[-1]
                ]

    log(
        f"Anos SIDRA que serão consultados: "
        f"{anos_sidra_consulta}"
    )

    resultados_sidra = {}
    erros_sidra = []

    workers_sidra = min(
        MAX_WORKERS_SIDRA,
        len(anos_sidra_consulta)
    )

    with ThreadPoolExecutor(
        max_workers=workers_sidra,
        thread_name_prefix="sidra"
    ) as executor:

        futuros_sidra = {
            executor.submit(
                extrair_sidra_ano,
                ano
            ): ano
            for ano in anos_sidra_consulta
        }

        concluido_sidra = 0

        for futuro in as_completed(
            futuros_sidra
        ):

            ano = futuros_sidra[futuro]
            concluido_sidra += 1

            try:

                resultado = futuro.result()
                resultados_sidra[ano] = resultado["registros"]

                log(
                    f"[OK] SIDRA "
                    f"[{concluido_sidra}/"
                    f"{len(anos_sidra_consulta)}] "
                    f"{ano} | "
                    f"{resultado['qtd']:,} registros | "
                    f"{resultado['tempo']:.2f}s"
                )

            except Exception as erro:

                erros_sidra.append(
                    {
                        "ANO": ano,
                        "ERRO": str(erro),
                    }
                )

                log(
                    f"[ERRO] SIDRA {ano}: "
                    f"{type(erro).__name__}: {erro}"
                )

    if erros_sidra:

        raise RuntimeError(
            f"Falha em {len(erros_sidra)} "
            f"ano(s) do SIDRA 5938. "
            f"A Bronze SIDRA não será publicada."
        )

    registros_sidra = []

    for ano in sorted(resultados_sidra):
        registros_sidra.extend(
            resultados_sidra[ano]
        )

    if not registros_sidra:
        raise ValueError(
            "SIDRA 5938 não retornou registros."
        )

    df_sidra_novo = pd.DataFrame(
        registros_sidra
    )

    duplicados_sidra = (
        df_sidra_novo
        .duplicated(
            subset=[
                "ANO",
                "MUNICIPIO_IBGE",
                "VARIAVEL_ID",
            ]
        )
        .sum()
    )

    if duplicados_sidra > 0:
        raise ValueError(
            f"SIDRA 5938 possui "
            f"{duplicados_sidra:,} duplicidades em "
            f"ANO + MUNICIPIO_IBGE + VARIAVEL_ID."
        )

    spark_sidra_novo = spark.createDataFrame(
        df_sidra_novo
    )

    if HISTORICO or not sidra_existente:

        spark_sidra_final = spark_sidra_novo

    else:

        anos_substituir = sorted(
            set(anos_sidra_consulta)
        )

        anos_sql = ",".join(
            str(ano)
            for ano in anos_substituir
        )

        spark_sidra_antigo = (
            spark.table(tabela_sidra)
            .filter(
                f"ANO NOT IN ({anos_sql})"
            )
        )

        spark_sidra_final = (
            spark_sidra_antigo
            .unionByName(
                spark_sidra_novo,
                allowMissingColumns=True
            )
        )

    (
        spark_sidra_final
        .write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(
            tabela_sidra
        )
    )

    qtd_sidra_final = spark.table(
        tabela_sidra
    ).count()

    log(
        f"[OK] {tabela_sidra} criada/atualizada | "
        f"{qtd_sidra_final:,} registros."
    )


    # ============================================================
    # VALIDAÇÃO FINAL DELTA
    # ============================================================

    spark.sql(
        f"""
        SELECT

            MIN(ANOMES)
                AS PRIMEIRO_PERIODO,

            MAX(ANOMES)
                AS ULTIMO_PERIODO,

            COUNT(
                DISTINCT ANOMES
            )
                AS PERIODOS,

            COUNT(
                DISTINCT MUNICIPIO_IBGE
            )
                AS MUNICIPIOS,

            COUNT(
                DISTINCT ESTADO_IBGE
            )
                AS ESTADOS,

            COUNT(
                DISTINCT REGIAO_IBGE
            )
                AS REGIOES,

            COUNT(*)
                AS REGISTROS

        FROM
            {SCHEMA_BRONZE}.
            transacoes_pix_municipio_raw
        """
    ).show()


    # ============================================================
    # VALIDAÇÃO POR MÊS
    # ============================================================

    spark.sql(
        f"""
        SELECT

            ANOMES,

            COUNT(*)
                AS REGISTROS,

            COUNT(
                DISTINCT MUNICIPIO_IBGE
            )
                AS MUNICIPIOS

        FROM
            {SCHEMA_BRONZE}.
            transacoes_pix_municipio_raw

        GROUP BY
            ANOMES

        ORDER BY
            ANOMES
        """
    ).show(
        100,
        truncate=False
    )


    # ============================================================
    # FIM
    # ============================================================

    log(
        "\n"
        + "=" * 80
    )

    log(
        "EXTRAÇÃO BRONZE CONCLUÍDA"
    )

    log(
        "=" * 80
    )

    log(
        "Tabelas criadas:"
    )

    log(
        "bronze.pix_bcb_raw"
    )

    log(
        "bronze.ibge_estados"
    )

    log(
        "bronze.ibge_municipio_uf"
    )

    log(
        "bronze.transacoes_pix_municipio_raw"
    )

    log(
        "bronze.ibge_sidra_5938_raw"
    )

    return {
        "status": "OK",
        "historico": HISTORICO,
        "m0": M0,
        "competencias": len(competencias),
    }


# COMMAND ----------

# DBTITLE 1,ETAPA 6 - PIB Municipal (SIDRA 5938)
# ============================================================
# ETAPA 6 - PIB MUNICIPAL IBGE (SIDRA TABELA 5938)
#
# Fonte:
#   API SIDRA - IBGE
#
# Variáveis (código):
#   37    - Produto Interno Bruto a preços correntes
#   543   - Impostos, líquidos de subsídios
#   498   - Valor adicionado bruto total
#   513   - VAB agropecuária
#   517   - VAB indústria
#   6575  - VAB serviços, exclusive administração pública
#   525   - VAB administração pública
#
# Resultado:
#   bronze.ibge_sidra_5938_raw
#
# Observação:
#   Executar após a Etapa 1 (usa bronze.ibge_municipio_uf
#   na validação de cobertura, quando disponível).
# ============================================================

import time

import pandas as pd
import requests

from datetime import datetime
from zoneinfo import ZoneInfo


# ============================================================
# LOG
# ============================================================

def log(mensagem):
    horario = datetime.now().strftime("%H:%M:%S")

    print(
        f"[{horario}] {mensagem}",
        flush=True
    )


# ============================================================
# CONFIGURAÇÃO
# ============================================================

SCHEMA_BRONZE = "bronze"

spark.sql(
    f"""
    CREATE SCHEMA IF NOT EXISTS {SCHEMA_BRONZE}
    """
)


# ============================================================
# CONFIGURAÇÃO DA API
#
# A API /values do SIDRA limita o volume por
# requisição: combinações amplas de n6/all + várias
# variáveis + vários períodos retornam HTTP 400.
#
# Por isso a extração é feita em lotes de UM ANO
# (7 variáveis x ~5.570 municípios ≈ 39 mil linhas),
# com fallback para uma requisição por variável
# caso o lote exceda o limite.
# ============================================================

BASE_SIDRA = (
    "https://apisidra.ibge.gov.br/values/t/5938"
)


VARIAVEIS_PIB = (
    "37,498,513,517,525,543,6575"
)


URL_PERIODOS_PIB = (
    "https://servicodados.ibge.gov.br/"
    "api/v3/agregados/5938/periodos"
)


VARIAVEIS_ESPERADAS = {
    "37",
    "498",
    "513",
    "517",
    "525",
    "543",
    "6575"
}


# ============================================================
# FUNÇÃO - LOTE DA API SIDRA
# ============================================================

def extrair_lote_sidra(
    url,
    descricao
):

    log(
        f"[HTTP] GET {descricao}"
    )

    inicio = time.time()

    # Proteção contra limite de requisições:
    # em HTTP 429 (rate limit) ou 5xx, aguarda e
    # tenta novamente com backoff progressivo.
    TENTATIVAS_MAX = 5

    resposta = None

    for tentativa in range(1, TENTATIVAS_MAX + 1):

        resposta = requests.get(
            url,
            timeout=300
        )

        log(
            f"[HTTP] Status: "
            f"{resposta.status_code} | "
            f"{time.time() - inicio:.2f}s"
        )

        if (
            resposta.status_code
            not in (429, 500, 502, 503, 504)
        ):
            break

        if tentativa == TENTATIVAS_MAX:
            break

        pausa = 30 * tentativa

        log(
            f"[RETRY] HTTP {resposta.status_code}. "
            f"Aguardando {pausa}s antes da "
            f"tentativa {tentativa + 1}/"
            f"{TENTATIVAS_MAX}..."
        )

        time.sleep(pausa)

    resposta.raise_for_status()

    dados = resposta.json()

    if (
        not isinstance(dados, list)
        or not dados
    ):
        raise ValueError(
            "Formato inesperado da "
            "resposta do SIDRA."
        )

    # A primeira linha da resposta
    # é o cabeçalho.
    if (
        dados[0].get("V")
        == "Valor"
    ):
        return dados[1:]

    return dados


# ============================================================
# EXTRAÇÃO
# ============================================================

log("")
log("=" * 80)
log("ETAPA 6 - PIB MUNICIPAL (SIDRA 5938)")
log("=" * 80)

# Períodos disponíveis na tabela 5938
periodos = extrair_lote_sidra(
    URL_PERIODOS_PIB,
    "metadados de períodos"
)

anos_pib = sorted(
    periodo["id"]
    for periodo in periodos
)

log(
    f"Anos disponíveis: "
    f"{anos_pib[0]} a {anos_pib[-1]} "
    f"({len(anos_pib)} anos)"
)


registros = []

for indice, ano in enumerate(
    anos_pib,
    start=1
):

    log(
        f"[{indice}/{len(anos_pib)}] Ano {ano}"
    )

    try:

        lote = extrair_lote_sidra(

            f"{BASE_SIDRA}"
            f"/n6/all"
            f"/v/{VARIAVEIS_PIB}"
            f"/p/{ano}"
            f"?format=json",

            f"ano {ano}"
        )

        registros.extend(lote)

    except requests.HTTPError:

        # Lote acima do limite da API:
        # divide em uma requisição
        # por variável.
        log(
            f"[AVISO] Lote do ano {ano} "
            "excedeu o limite da API. "
            "Dividindo por variável..."
        )

        for variavel in sorted(
            VARIAVEIS_ESPERADAS
        ):

            lote = extrair_lote_sidra(

                f"{BASE_SIDRA}"
                f"/n6/all"
                f"/v/{variavel}"
                f"/p/{ano}"
                f"?format=json",

                f"ano {ano} "
                f"variável {variavel}"
            )

            registros.extend(lote)

    time.sleep(0.5)


df_pib = pd.DataFrame(
    registros
)


log(
    f"Registros recebidos: "
    f"{len(df_pib):,}"
)


# ============================================================
# MAPEAR COLUNAS
# ============================================================

df_pib = (
    df_pib[
        [
            "D3C",
            "D1C",
            "D2C",
            "D2N",
            "V"
        ]
    ]
    .rename(
        columns={
            "D3C": "ANO",
            "D1C": "MUNICIPIO_IBGE",
            "D2C": "VARIAVEL_ID",
            "D2N": "VARIAVEL_NOME",
            "V": "VALOR"
        }
    )
)


# ============================================================
# TIPAGEM
# ============================================================

df_pib["ANO"] = (
    pd.to_numeric(
        df_pib["ANO"],
        errors="coerce"
    )
    .astype("Int64")
)

df_pib["MUNICIPIO_IBGE"] = (
    pd.to_numeric(
        df_pib["MUNICIPIO_IBGE"],
        errors="coerce"
    )
    .round()
    .astype("Int64")
)

df_pib["VALOR"] = pd.to_numeric(
    df_pib["VALOR"],
    errors="coerce"
)

df_pib["VARIAVEL_ID"] = (
    df_pib["VARIAVEL_ID"]
    .astype(str)
)

df_pib["VARIAVEL_NOME"] = (
    df_pib["VARIAVEL_NOME"]
    .astype(str)
)


# ============================================================
# REMOVER VALORES MASCARADOS
#
# O SIDRA devolve "...", "-", "X" para valores
# não divulgados. pd.to_numeric já os converteu
# em nulos.
# ============================================================

qtd_mascarados = int(
    df_pib["VALOR"]
    .isna()
    .sum()
)

log(
    f"Registros com valor não divulgado: "
    f"{qtd_mascarados:,}"
)


df_pib = (
    df_pib
    .dropna(
        subset=[
            "ANO",
            "MUNICIPIO_IBGE",
            "VALOR"
        ]
    )
    .copy()
)


# ============================================================
# VALIDAÇÕES
# ============================================================

log("")
log("VALIDAÇÃO SIDRA")
log("=" * 80)

log(
    f"Registros válidos: "
    f"{len(df_pib):,}"
)

log(
    f"Anos: "
    f"{df_pib['ANO'].min()} a "
    f"{df_pib['ANO'].max()} "
    f"({df_pib['ANO'].nunique()} anos)"
)

log(
    f"Municípios: "
    f"{df_pib['MUNICIPIO_IBGE'].nunique():,}"
)


variaveis_encontradas = set(
    df_pib["VARIAVEL_ID"]
    .unique()
)

if (
    variaveis_encontradas
    != VARIAVEIS_ESPERADAS
):
    raise ValueError(
        "Variáveis divergentes: "
        f"{sorted(variaveis_encontradas)}"
    )


log(
    f"Variáveis: "
    f"{sorted(variaveis_encontradas)}"
)


duplicados = (
    df_pib
    .duplicated(
        subset=[
            "ANO",
            "MUNICIPIO_IBGE",
            "VARIAVEL_ID"
        ],
        keep=False
    )
)

if duplicados.any():

    display(
        df_pib.loc[
            duplicados
        ]
    )

    raise ValueError(
        "Existem duplicidades em "
        "ANO + MUNICIPIO_IBGE + "
        "VARIAVEL_ID."
    )


log(
    "[OK] Granularidade "
    "ANO + MUNICIPIO_IBGE + "
    "VARIAVEL_ID validada."
)


# ============================================================
# COBERTURA x MUNICÍPIOS IBGE
# ============================================================

if spark.catalog.tableExists(
    f"{SCHEMA_BRONZE}.ibge_municipio_uf"
):

    municipios_sidra = set(
        df_pib["MUNICIPIO_IBGE"]
        .dropna()
        .astype(int)
        .unique()
    )

    municipios_ibge = set(
        spark
        .table(
            f"{SCHEMA_BRONZE}."
            "ibge_municipio_uf"
        )
        .select("MUNICIPIO_IBGE")
        .distinct()
        .toPandas()["MUNICIPIO_IBGE"]
        .astype(int)
    )

    sem_pib = (
        municipios_ibge
        - municipios_sidra
    )

    log(
        f"Municípios IBGE sem PIB: "
        f"{len(sem_pib):,}"
    )

    if sem_pib:
        log(
            f"[AVISO] Códigos: "
            f"{sorted(sem_pib)[:10]}"
        )


# ============================================================
# DATA DE INGESTÃO
# ============================================================

df_pib["DATA_INGESTAO"] = datetime.now(
    ZoneInfo("America/Sao_Paulo")
)


# ============================================================
# ESTRUTURA FINAL
# ============================================================

df_pib = (
    df_pib[
        [
            "ANO",
            "MUNICIPIO_IBGE",
            "VARIAVEL_ID",
            "VARIAVEL_NOME",
            "VALOR",
            "DATA_INGESTAO"
        ]
    ]
    .copy()
)


display(
    df_pib.head(20)
)


# ============================================================
# SALVAR BRONZE
# ============================================================

(
    spark
    .createDataFrame(df_pib)
    .write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        f"{SCHEMA_BRONZE}."
        "ibge_sidra_5938_raw"
    )
)


log(
    "[OK] "
    "bronze.ibge_sidra_5938_raw criada."
)


# ============================================================
# VALIDAÇÃO FINAL
# ============================================================

spark.sql(
    f"""
    SELECT

        COUNT(*) AS REGISTROS,

        COUNT(DISTINCT ANO) AS ANOS,

        COUNT(
            DISTINCT MUNICIPIO_IBGE
        ) AS MUNICIPIOS,

        COUNT(
            DISTINCT VARIAVEL_ID
        ) AS VARIAVEIS

    FROM
        {SCHEMA_BRONZE}.ibge_sidra_5938_raw
    """
).show()


log("")
log("=" * 80)
log("ETAPA 6 CONCLUÍDA")
log("=" * 80)