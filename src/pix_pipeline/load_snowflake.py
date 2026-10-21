# Databricks notebook source
"""Carga Databricks Bronze -> Snowflake Bronze/Silver/Gold/Marts.

Não instala pacotes nem reinicia o Python. As dependências devem ser
instaladas pelo notebook orquestrador ou como Library do Job/cluster.
"""

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas


def executar_load_snowflake(
    spark,
    dbutils,
    historico=False,
    display_fn=None,
):
    """Executa Bronze -> Silver -> Gold -> Marts no mesmo processo."""

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
    # LOG
    # ============================================================

    def log(mensagem):
        horario = datetime.now(
            ZoneInfo("America/Sao_Paulo")
        ).strftime("%H:%M:%S")

        print(
            f"[{horario}] {mensagem}",
            flush=True
        )


    # ============================================================
    # PARÂMETRO
    # ============================================================

    if isinstance(historico, str):
        HISTORICO = historico.strip().lower() == "true"
    else:
        HISTORICO = bool(historico)

    AGORA = datetime.now(ZoneInfo("America/Sao_Paulo"))
    M0 = int(AGORA.strftime("%Y%m"))

    log(f"HISTORICO = {HISTORICO}")
    log(f"M0 = {M0}")


    # ============================================================
    # CONFIGURAÇÃO SNOWFLAKE
    # ============================================================

    SF_ACCOUNT = "ZZWMHWC-QO26986"
    SF_USER = "MFMCARVALHO0"
    SF_WAREHOUSE = "COMPUTE_WH"
    SF_DATABASE = "PIX_DB"
    SF_ROLE = "ACCOUNTADMIN"

    SF_PASSWORD = dbutils.secrets.get(
        scope="snowflake",
        key="password"
    )

    if not SF_PASSWORD:
        raise ValueError(
            "Senha Snowflake não encontrada no Databricks Secret."
        )


    def conectar_snowflake():

        log("Conectando ao Snowflake...")

        conexao = snowflake.connector.connect(
            account=SF_ACCOUNT,
            user=SF_USER,
            password=SF_PASSWORD,
            warehouse=SF_WAREHOUSE,
            database=SF_DATABASE,
            role=SF_ROLE,
            login_timeout=30,
            network_timeout=120,
            socket_timeout=120
        )

        log("Conexão Snowflake estabelecida.")

        return conexao


    # ============================================================
    # HELPERS
    # ============================================================

    def tabela_databricks_existe(tabela):

        return spark.catalog.tableExists(
            tabela
        )


    def tabela_snowflake_existe(
        conexao,
        schema,
        tabela
    ):

        cursor = conexao.cursor()

        try:
            cursor.execute(
                f"""
                SELECT COUNT(*)
                FROM {SF_DATABASE}.INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                """,
                (
                    schema.upper(),
                    tabela.upper()
                )
            )

            return cursor.fetchone()[0] > 0

        finally:
            cursor.close()


    def contar_snowflake(
        conexao,
        objeto
    ):

        cursor = conexao.cursor()

        try:
            cursor.execute(
                f"SELECT COUNT(*) FROM {objeto}"
            )

            return cursor.fetchone()[0]

        finally:
            cursor.close()


    def obter_colunas_snowflake(
        conexao,
        schema,
        tabela
    ):

        cursor = conexao.cursor()

        try:
            cursor.execute(
                f"""
                SELECT COLUMN_NAME
                FROM {SF_DATABASE}.INFORMATION_SCHEMA.COLUMNS
                WHERE TABLE_SCHEMA = %s
                  AND TABLE_NAME = %s
                ORDER BY ORDINAL_POSITION
                """,
                (
                    schema.upper(),
                    tabela.upper()
                )
            )

            return [
                linha[0]
                for linha in cursor.fetchall()
            ]

        finally:
            cursor.close()


    def preparar_dataframe(
        conexao,
        df_spark,
        schema,
        tabela
    ):

        colunas_target = obter_colunas_snowflake(
            conexao,
            schema,
            tabela
        )

        colunas_source = {
            coluna.upper(): coluna
            for coluna in df_spark.columns
        }

        colunas_auditoria = {
            "DATA_INGESTAO",
            "DATA_PROCESSAMENTO"
        }

        colunas_comuns = [
            coluna
            for coluna in colunas_target
            if (
                coluna in colunas_source
                and coluna not in colunas_auditoria
            )
        ]

        if not colunas_comuns:
            raise ValueError(
                f"Nenhuma coluna em comum entre "
                f"Databricks e Snowflake para "
                f"{schema}.{tabela}."
            )

        log(
            f"{schema}.{tabela} | "
            f"colunas usadas: {len(colunas_comuns)}"
        )

        df_selecionado = df_spark.select(
            *[
                colunas_source[coluna]
                for coluna in colunas_comuns
            ]
        )

        for coluna in df_selecionado.columns:

            if coluna != coluna.upper():

                df_selecionado = (
                    df_selecionado
                    .withColumnRenamed(
                        coluna,
                        coluna.upper()
                    )
                )

        return (
            df_selecionado,
            colunas_comuns
        )


    def criar_tabela_temp(
        conexao,
        schema,
        tabela
    ):

        temp_table = (
            f"{SF_DATABASE}."
            f"{schema}."
            f"TMP_LOAD_{tabela}"
        )

        cursor = conexao.cursor()

        try:
            cursor.execute(
                f"""
                CREATE OR REPLACE TEMPORARY TABLE
                    {temp_table}
                LIKE
                    {SF_DATABASE}.{schema}.{tabela}
                """
            )

        finally:
            cursor.close()

        return temp_table


    def escrever_temp(
        conexao,
        df_pandas,
        schema,
        tabela
    ):

        temp_table_name = (
            f"TMP_LOAD_{tabela}"
        )

        log(
            f"Enviando {len(df_pandas):,} registros para "
            f"{SF_DATABASE}.{schema}.{temp_table_name}..."
        )

        sucesso, chunks, linhas, _ = write_pandas(
            conn=conexao,
            df=df_pandas,
            table_name=temp_table_name,
            database=SF_DATABASE,
            schema=schema,
            quote_identifiers=False,
            auto_create_table=False,
            overwrite=False
        )

        if not sucesso:
            raise RuntimeError(
                f"write_pandas falhou para "
                f"{SF_DATABASE}.{schema}.{temp_table_name}"
            )

        log(
            f"Upload concluído | chunks={chunks} | "
            f"linhas={linhas:,}"
        )


    # ============================================================
    # PUBLICAÇÃO BRONZE
    #
    # DELETE + INSERT é usado no full para manter a operação DML
    # transacional, evitando TRUNCATE dentro da transação.
    # ============================================================

    def carga_completa(
        conexao,
        schema,
        tabela,
        temp_table,
        colunas
    ):

        target = (
            f"{SF_DATABASE}.{schema}.{tabela}"
        )

        lista_colunas = ", ".join(
            colunas
        )

        cursor = conexao.cursor()

        try:
            cursor.execute("BEGIN")

            cursor.execute(
                f"DELETE FROM {target}"
            )

            cursor.execute(
                f"""
                INSERT INTO {target}
                (
                    {lista_colunas}
                )
                SELECT
                    {lista_colunas}
                FROM
                    {temp_table}
                """
            )

            cursor.execute("COMMIT")

        except Exception:
            cursor.execute("ROLLBACK")
            raise

        finally:
            cursor.close()


    def carga_incremental_m0(
        conexao,
        schema,
        tabela,
        temp_table,
        colunas
    ):

        target = (
            f"{SF_DATABASE}.{schema}.{tabela}"
        )

        lista_colunas = ", ".join(
            colunas
        )

        cursor = conexao.cursor()

        try:
            cursor.execute(
                f"""
                SELECT DISTINCT ANOMES
                FROM {temp_table}
                ORDER BY ANOMES
                """
            )

            periodos = [
                int(linha[0])
                for linha in cursor.fetchall()
                if linha[0] is not None
            ]

            if periodos != [M0]:
                raise ValueError(
                    f"Carga incremental deveria conter apenas "
                    f"M0={M0}. Encontrado: {periodos}"
                )

            cursor.execute("BEGIN")

            cursor.execute(
                f"""
                DELETE FROM {target}
                WHERE ANOMES = {M0}
                """
            )

            cursor.execute(
                f"""
                INSERT INTO {target}
                (
                    {lista_colunas}
                )
                SELECT
                    {lista_colunas}
                FROM
                    {temp_table}
                WHERE
                    ANOMES = {M0}
                """
            )

            cursor.execute("COMMIT")

        except Exception:
            cursor.execute("ROLLBACK")
            raise

        finally:
            cursor.close()


    def carregar_tabela_bronze(
        conexao,
        source,
        target,
        estrategia,
        obrigatoria=True
    ):

        schema = "BRONZE"

        log("")
        log("=" * 80)
        log(
            f"{source} -> "
            f"{SF_DATABASE}.{schema}.{target}"
        )
        log("=" * 80)

        if not tabela_databricks_existe(
            source
        ):

            if obrigatoria:
                raise ValueError(
                    f"Tabela Databricks obrigatória "
                    f"{source} não existe."
                )

            log(
                f"[SKIP] Source opcional "
                f"{source} não existe."
            )

            return {
                "status": "skip_source",
                "source": source,
                "target": f"{schema}.{target}",
                "source_rows": None,
                "target_rows": None,
                "estrategia": estrategia
            }

        if not tabela_snowflake_existe(
            conexao,
            schema,
            target
        ):
            raise ValueError(
                f"Target Snowflake "
                f"{SF_DATABASE}.{schema}.{target} "
                f"não existe."
            )

        df_spark = spark.table(
            source
        )

        if (
            estrategia == "monthly"
            and not HISTORICO
        ):

            if "ANOMES" not in df_spark.columns:
                raise ValueError(
                    f"{source} é monthly "
                    f"mas não possui ANOMES."
                )

            df_spark = df_spark.filter(
                f"ANOMES = {M0}"
            )

        qtd_source = df_spark.count()

        log(
            f"Registros source: "
            f"{qtd_source:,}"
        )

        if qtd_source == 0:
            raise ValueError(
                f"{source} não possui registros "
                f"para carregar."
            )

        (
            df_spark,
            colunas
        ) = preparar_dataframe(
            conexao,
            df_spark,
            schema,
            target
        )

        log(
            "Convertendo Spark -> Pandas..."
        )

        df_pandas = df_spark.toPandas()

        # Evita PlanMetrics não serializável
        df_pandas.attrs.clear()

        df_pandas.columns = [
            coluna.upper()
            for coluna in df_pandas.columns
        ]

        df_pandas = (
            df_pandas
            .where(
                pd.notnull(df_pandas),
                None
            )
        )

        temp_table = criar_tabela_temp(
            conexao,
            schema,
            target
        )

        escrever_temp(
            conexao,
            df_pandas,
            schema,
            target
        )

        qtd_temp = contar_snowflake(
            conexao,
            temp_table
        )

        if qtd_temp != len(df_pandas):
            raise ValueError(
                f"Divergência TEMP. "
                f"Pandas={len(df_pandas):,}; "
                f"TEMP={qtd_temp:,}"
            )

        if (
            estrategia == "monthly"
            and not HISTORICO
        ):

            carga_incremental_m0(
                conexao,
                schema,
                target,
                temp_table,
                colunas
            )

        else:

            carga_completa(
                conexao,
                schema,
                target,
                temp_table,
                colunas
            )

        target_completo = (
            f"{SF_DATABASE}.{schema}.{target}"
        )

        qtd_target = contar_snowflake(
            conexao,
            target_completo
        )

        log(
            f"[OK] {target_completo} "
            f"| total={qtd_target:,}"
        )

        return {
            "status": "ok",
            "source": source,
            "target": target_completo,
            "source_rows": len(df_pandas),
            "target_rows": qtd_target,
            "estrategia": estrategia
        }


    # ============================================================
    # BUILD SILVER
    #
    # A DIM_TEMPO passa a ter grão ANUAL.
    # FACT_PIX_MENSAL continua no grão ANOMES x MUNICIPIO,
    # mas recebe ANO para se relacionar com a dimensão temporal.
    # ============================================================

    def build_silver_snowflake(
        conexao
    ):

        log("")
        log("=" * 80)
        log("BUILD SILVER")
        log("=" * 80)

        cursor = conexao.cursor()

        try:

            # ====================================================
            # DIM_REGIAO
            # ====================================================

            log(
                "Construindo SILVER.DIM_REGIAO..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.SILVER.DIM_REGIAO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.SILVER.DIM_REGIAO
                (
                    ID_REGIAO,
                    SIGLA_REGIAO,
                    REGIAO
                )

                SELECT DISTINCT
                    REGIAO_IBGE,
                    TRIM(SIGLA_REGIAO),
                    TRIM(REGIAO)

                FROM
                    PIX_DB.BRONZE.IBGE_ESTADOS

                WHERE
                    REGIAO_IBGE IS NOT NULL
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] SILVER.DIM_REGIAO: "
                f"{contar_snowflake(conexao, 'PIX_DB.SILVER.DIM_REGIAO'):,}"
            )


            # ====================================================
            # DIM_ESTADO
            # ====================================================

            log(
                "Construindo SILVER.DIM_ESTADO..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.SILVER.DIM_ESTADO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.SILVER.DIM_ESTADO
                (
                    ESTADO_IBGE,
                    SIGLA_UF,
                    ESTADO,
                    ID_REGIAO
                )

                SELECT DISTINCT
                    ESTADO_IBGE,
                    TRIM(SIGLA_UF),
                    TRIM(ESTADO),
                    REGIAO_IBGE

                FROM
                    PIX_DB.BRONZE.IBGE_ESTADOS

                WHERE
                    ESTADO_IBGE IS NOT NULL
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] SILVER.DIM_ESTADO: "
                f"{contar_snowflake(conexao, 'PIX_DB.SILVER.DIM_ESTADO'):,}"
            )


            # ====================================================
            # DIM_MUNICIPIO
            # ====================================================

            log(
                "Construindo SILVER.DIM_MUNICIPIO..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.SILVER.DIM_MUNICIPIO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.SILVER.DIM_MUNICIPIO
                (
                    MUNICIPIO_IBGE,
                    MUNICIPIO,
                    ESTADO_IBGE
                )

                SELECT DISTINCT
                    MUNICIPIO_IBGE,
                    TRIM(MUNICIPIO),
                    ESTADO_IBGE

                FROM
                    PIX_DB.BRONZE.IBGE_MUNICIPIO_UF

                WHERE
                    MUNICIPIO_IBGE IS NOT NULL
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] SILVER.DIM_MUNICIPIO: "
                f"{contar_snowflake(conexao, 'PIX_DB.SILVER.DIM_MUNICIPIO'):,}"
            )


            # ====================================================
            # DIM_TEMPO
            #
            # Grão anual.
            #
            # Une os anos existentes em PIX e PIB para que uma
            # única DIM_TEMPO filtre as duas fatos no Power BI.
            # ====================================================

            log(
                "Construindo SILVER.DIM_TEMPO anual..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.SILVER.DIM_TEMPO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.SILVER.DIM_TEMPO
                (
                    ANO
                )

                SELECT DISTINCT
                    ANO

                FROM
                (
                    SELECT
                        FLOOR(ANOMES / 100)::NUMBER(4,0)
                            AS ANO

                    FROM
                        PIX_DB.BRONZE.TRANSACOES_PIX_MUNICIPIO_RAW

                    WHERE
                        ANOMES IS NOT NULL

                    UNION

                    SELECT
                        ANO

                    FROM
                        PIX_DB.BRONZE.IBGE_SIDRA_5938_RAW

                    WHERE
                        ANO IS NOT NULL
                )

                WHERE
                    ANO IS NOT NULL
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] SILVER.DIM_TEMPO: "
                f"{contar_snowflake(conexao, 'PIX_DB.SILVER.DIM_TEMPO'):,}"
            )


            # ====================================================
            # FACT_PIX_MENSAL
            #
            # Grão:
            # ANOMES x MUNICIPIO_IBGE
            #
            # ANO é uma FK para a DIM_TEMPO anual.
            # ====================================================

            log(
                "Construindo SILVER.FACT_PIX_MENSAL..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.SILVER.FACT_PIX_MENSAL
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.SILVER.FACT_PIX_MENSAL
                (
                    ANOMES,
                    ANO,
                    MUNICIPIO_IBGE,

                    VL_PAGADOR_PF,
                    QT_PAGADOR_PF,

                    VL_PAGADOR_PJ,
                    QT_PAGADOR_PJ,

                    VL_RECEBEDOR_PF,
                    QT_RECEBEDOR_PF,

                    VL_RECEBEDOR_PJ,
                    QT_RECEBEDOR_PJ,

                    QT_PES_PAGADOR_PF,
                    QT_PES_PAGADOR_PJ,

                    QT_PES_RECEBEDOR_PF,
                    QT_PES_RECEBEDOR_PJ
                )

                SELECT
                    ANOMES,

                    FLOOR(ANOMES / 100)::NUMBER(4,0)
                        AS ANO,

                    MUNICIPIO_IBGE,

                    VL_PAGADORPF,
                    QT_PAGADORPF,

                    VL_PAGADORPJ,
                    QT_PAGADORPJ,

                    VL_RECEBEDORPF,
                    QT_RECEBEDORPF,

                    VL_RECEBEDORPJ,
                    QT_RECEBEDORPJ,

                    QT_PES_PAGADORPF,
                    QT_PES_PAGADORPJ,

                    QT_PES_RECEBEDORPF,
                    QT_PES_RECEBEDORPJ

                FROM
                    PIX_DB.BRONZE.TRANSACOES_PIX_MUNICIPIO_RAW

                WHERE
                    ANOMES IS NOT NULL
                    AND MUNICIPIO_IBGE IS NOT NULL
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] SILVER.FACT_PIX_MENSAL: "
                f"{contar_snowflake(conexao, 'PIX_DB.SILVER.FACT_PIX_MENSAL'):,}"
            )


            # ====================================================
            # FACT_PIB_MUNICIPIO
            #
            # Grão:
            # ANO x MUNICIPIO_IBGE
            # ====================================================

            log(
                "Construindo SILVER.FACT_PIB_MUNICIPIO..."
            )

            qtd_sidra = contar_snowflake(
                conexao,
                "PIX_DB.BRONZE.IBGE_SIDRA_5938_RAW"
            )

            if qtd_sidra == 0:

                raise ValueError(
                    "PIX_DB.BRONZE.IBGE_SIDRA_5938_RAW está vazia. "
                    "A FACT_PIB é obrigatória no modelo atual."
                )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.SILVER.FACT_PIB_MUNICIPIO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.SILVER.FACT_PIB_MUNICIPIO
                (
                    ANO,
                    MUNICIPIO_IBGE,

                    PIB_TOTAL,
                    IMPOSTOS_LIQUIDOS,
                    VAB_TOTAL,
                    VAB_AGROPECUARIA,
                    VAB_INDUSTRIA,
                    VAB_SERVICOS_EXCLUSIVE_ADM,
                    VAB_ADMINISTRACAO
                )

                SELECT
                    ANO,
                    MUNICIPIO_IBGE,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '37'
                            THEN VALOR
                        END
                    )
                        AS PIB_TOTAL,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '543'
                            THEN VALOR
                        END
                    )
                        AS IMPOSTOS_LIQUIDOS,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '498'
                            THEN VALOR
                        END
                    )
                        AS VAB_TOTAL,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '513'
                            THEN VALOR
                        END
                    )
                        AS VAB_AGROPECUARIA,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '517'
                            THEN VALOR
                        END
                    )
                        AS VAB_INDUSTRIA,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '6575'
                            THEN VALOR
                        END
                    )
                        AS VAB_SERVICOS_EXCLUSIVE_ADM,

                    MAX(
                        CASE
                            WHEN VARIAVEL_ID = '525'
                            THEN VALOR
                        END
                    )
                        AS VAB_ADMINISTRACAO

                FROM
                    PIX_DB.BRONZE.IBGE_SIDRA_5938_RAW

                WHERE
                    ANO IS NOT NULL
                    AND MUNICIPIO_IBGE IS NOT NULL

                GROUP BY
                    ANO,
                    MUNICIPIO_IBGE
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] SILVER.FACT_PIB_MUNICIPIO: "
                f"{contar_snowflake(conexao, 'PIX_DB.SILVER.FACT_PIB_MUNICIPIO'):,}"
            )


            # ====================================================
            # VALIDAÇÕES SILVER
            # ====================================================

            qtd_regioes = contar_snowflake(
                conexao,
                "PIX_DB.SILVER.DIM_REGIAO"
            )

            qtd_estados = contar_snowflake(
                conexao,
                "PIX_DB.SILVER.DIM_ESTADO"
            )

            if qtd_regioes != 5:

                raise ValueError(
                    f"SILVER.DIM_REGIAO deveria possuir 5 registros. "
                    f"Encontrado: {qtd_regioes}"
                )

            if qtd_estados != 27:

                raise ValueError(
                    f"SILVER.DIM_ESTADO deveria possuir 27 registros. "
                    f"Encontrado: {qtd_estados}"
                )

            validacoes = [

                (
                    "SILVER.FACT_PIX_MENSAL sem DIM_TEMPO",
                    """
                    SELECT COUNT(*)

                    FROM
                        PIX_DB.SILVER.FACT_PIX_MENSAL F

                    LEFT JOIN
                        PIX_DB.SILVER.DIM_TEMPO T
                        ON F.ANO = T.ANO

                    WHERE
                        T.ANO IS NULL
                    """
                ),

                (
                    "SILVER.FACT_PIX_MENSAL sem DIM_MUNICIPIO",
                    """
                    SELECT COUNT(*)

                    FROM
                        PIX_DB.SILVER.FACT_PIX_MENSAL F

                    LEFT JOIN
                        PIX_DB.SILVER.DIM_MUNICIPIO M
                        ON F.MUNICIPIO_IBGE = M.MUNICIPIO_IBGE

                    WHERE
                        M.MUNICIPIO_IBGE IS NULL
                    """
                ),

                (
                    "SILVER.FACT_PIB_MUNICIPIO sem DIM_TEMPO",
                    """
                    SELECT COUNT(*)

                    FROM
                        PIX_DB.SILVER.FACT_PIB_MUNICIPIO F

                    LEFT JOIN
                        PIX_DB.SILVER.DIM_TEMPO T
                        ON F.ANO = T.ANO

                    WHERE
                        T.ANO IS NULL
                    """
                ),

                (
                    "SILVER.FACT_PIB_MUNICIPIO sem DIM_MUNICIPIO",
                    """
                    SELECT COUNT(*)

                    FROM
                        PIX_DB.SILVER.FACT_PIB_MUNICIPIO F

                    LEFT JOIN
                        PIX_DB.SILVER.DIM_MUNICIPIO M
                        ON F.MUNICIPIO_IBGE = M.MUNICIPIO_IBGE

                    WHERE
                        M.MUNICIPIO_IBGE IS NULL
                    """
                )

            ]

            for nome, sql in validacoes:

                cursor.execute(sql)

                qtd = cursor.fetchone()[0]

                if qtd > 0:

                    raise ValueError(
                        f"{nome}: {qtd:,} registros."
                    )

            log(
                "[OK] Silver validada."
            )


        except Exception:

            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass

            raise

        finally:
            cursor.close()


    # ============================================================
    # BUILD GOLD
    #
    # Modelo em constelação para Power BI:
    #
    # DIM_TEMPO[ANO] -> FACT_PIX[ANO]
    # DIM_TEMPO[ANO] -> FACT_PIB[ANO]
    # DIM_TEMPO[ANO] -> FACT_VAB_MUNICIPIO[ANO]
    #
    # DIM_MUNICIPIO, DIM_ESTADO e DIM_REGIAO são dimensões
    # independentes e se relacionam diretamente com as fatos.
    #
    # DIM_CATEGORIA_VAB -> FACT_VAB_MUNICIPIO permite analisar
    # e filtrar Agropecuária, Indústria, Serviços e Administração
    # como categorias, sem alterar o grão original da FACT_PIB.
    # ============================================================

    def build_gold_snowflake(
        conexao
    ):

        log("")
        log("=" * 80)
        log("BUILD GOLD")
        log("=" * 80)

        cursor = conexao.cursor()

        try:

            # ====================================================
            # DIM_REGIAO
            # ====================================================

            log(
                "Construindo GOLD.DIM_REGIAO..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.DIM_REGIAO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.DIM_REGIAO
                (
                    ID_REGIAO,
                    SIGLA_REGIAO,
                    REGIAO
                )

                SELECT
                    ID_REGIAO,
                    SIGLA_REGIAO,
                    REGIAO

                FROM
                    PIX_DB.SILVER.DIM_REGIAO
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] GOLD.DIM_REGIAO: "
                f"{contar_snowflake(conexao, 'PIX_DB.GOLD.DIM_REGIAO'):,}"
            )


            # ====================================================
            # DIM_ESTADO
            #
            # Independente de DIM_REGIAO na Gold.
            # ====================================================

            log(
                "Construindo GOLD.DIM_ESTADO..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.DIM_ESTADO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.DIM_ESTADO
                (
                    ESTADO_IBGE,
                    SIGLA_UF,
                    ESTADO
                )

                SELECT
                    ESTADO_IBGE,
                    SIGLA_UF,
                    ESTADO

                FROM
                    PIX_DB.SILVER.DIM_ESTADO
                """
            )

            cursor.execute("COMMIT")

            qtd_estado = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.DIM_ESTADO"
            )

            if qtd_estado != 27:

                raise ValueError(
                    f"GOLD.DIM_ESTADO deveria possuir 27 registros. "
                    f"Encontrado: {qtd_estado}"
                )

            log(
                f"[OK] GOLD.DIM_ESTADO: "
                f"{qtd_estado:,}"
            )


            # ====================================================
            # DIM_MUNICIPIO
            #
            # Independente de DIM_ESTADO e DIM_REGIAO na Gold.
            # ====================================================

            log(
                "Construindo GOLD.DIM_MUNICIPIO..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.DIM_MUNICIPIO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.DIM_MUNICIPIO
                (
                    ID_MUNICIPIO,
                    MUNICIPIO
                )

                SELECT
                    MUNICIPIO_IBGE
                        AS ID_MUNICIPIO,

                    MUNICIPIO

                FROM
                    PIX_DB.SILVER.DIM_MUNICIPIO
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] GOLD.DIM_MUNICIPIO: "
                f"{contar_snowflake(conexao, 'PIX_DB.GOLD.DIM_MUNICIPIO'):,}"
            )


            # ====================================================
            # DIM_TEMPO
            #
            # Grão ANUAL.
            # É a única dimensão temporal das duas fatos.
            # ====================================================

            log(
                "Construindo GOLD.DIM_TEMPO anual..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.DIM_TEMPO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.DIM_TEMPO
                (
                    ANO
                )

                SELECT
                    ANO

                FROM
                    PIX_DB.SILVER.DIM_TEMPO

                WHERE
                    ANO IS NOT NULL
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] GOLD.DIM_TEMPO: "
                f"{contar_snowflake(conexao, 'PIX_DB.GOLD.DIM_TEMPO'):,}"
            )


            # ====================================================
            # DIM_CATEGORIA_VAB
            #
            # Dimensão específica para permitir filtro por categoria
            # de VAB no Power BI.
            # ====================================================

            log(
                "Construindo GOLD.DIM_CATEGORIA_VAB..."
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS
                    PIX_DB.GOLD.DIM_CATEGORIA_VAB
                (
                    ID_CATEGORIA_VAB NUMBER(2,0) NOT NULL,
                    CATEGORIA_VAB VARCHAR(100) NOT NULL,
                    ORDEM NUMBER(2,0) NOT NULL,
                    DATA_PROCESSAMENTO TIMESTAMP_LTZ
                        DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.DIM_CATEGORIA_VAB
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.DIM_CATEGORIA_VAB
                (
                    ID_CATEGORIA_VAB,
                    CATEGORIA_VAB,
                    ORDEM
                )

                SELECT 1, 'Serviços', 1
                UNION ALL
                SELECT 2, 'Indústria', 2
                UNION ALL
                SELECT 3, 'Administração Pública', 3
                UNION ALL
                SELECT 4, 'Agropecuária', 4
                """
            )

            cursor.execute("COMMIT")

            qtd_categoria_vab = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.DIM_CATEGORIA_VAB"
            )

            if qtd_categoria_vab != 4:

                raise ValueError(
                    "GOLD.DIM_CATEGORIA_VAB deveria possuir "
                    f"4 registros. Encontrado: {qtd_categoria_vab}."
                )

            log(
                f"[OK] GOLD.DIM_CATEGORIA_VAB: "
                f"{qtd_categoria_vab:,}"
            )


            # ====================================================
            # FACT_PIX
            #
            # Grão:
            # ANOMES x ID_MUNICIPIO
            #
            # ANOMES permanece na fato.
            # ANO é a FK para DIM_TEMPO.
            # As chaves geográficas ficam diretamente na fato.
            # ====================================================

            log(
                "Construindo GOLD.FACT_PIX..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.FACT_PIX
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.FACT_PIX
                (
                    ANOMES,
                    ANO,

                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,

                    VL_PAGADORPF,
                    QT_PAGADORPF,

                    VL_PAGADORPJ,
                    QT_PAGADORPJ,

                    VL_RECEBEDORPF,
                    QT_RECEBEDORPF,

                    VL_RECEBEDORPJ,
                    QT_RECEBEDORPJ,

                    QT_PES_PAGADORPF,
                    QT_PES_PAGADORPJ,

                    QT_PES_RECEBEDORPF,
                    QT_PES_RECEBEDORPJ
                )

                SELECT

                    F.ANOMES,

                    F.ANO,

                    F.MUNICIPIO_IBGE
                        AS ID_MUNICIPIO,

                    M.ESTADO_IBGE,

                    E.ID_REGIAO,

                    F.VL_PAGADOR_PF
                        AS VL_PAGADORPF,

                    F.QT_PAGADOR_PF
                        AS QT_PAGADORPF,

                    F.VL_PAGADOR_PJ
                        AS VL_PAGADORPJ,

                    F.QT_PAGADOR_PJ
                        AS QT_PAGADORPJ,

                    F.VL_RECEBEDOR_PF
                        AS VL_RECEBEDORPF,

                    F.QT_RECEBEDOR_PF
                        AS QT_RECEBEDORPF,

                    F.VL_RECEBEDOR_PJ
                        AS VL_RECEBEDORPJ,

                    F.QT_RECEBEDOR_PJ
                        AS QT_RECEBEDORPJ,

                    F.QT_PES_PAGADOR_PF
                        AS QT_PES_PAGADORPF,

                    F.QT_PES_PAGADOR_PJ
                        AS QT_PES_PAGADORPJ,

                    F.QT_PES_RECEBEDOR_PF
                        AS QT_PES_RECEBEDORPF,

                    F.QT_PES_RECEBEDOR_PJ
                        AS QT_PES_RECEBEDORPJ

                FROM
                    PIX_DB.SILVER.FACT_PIX_MENSAL F

                INNER JOIN
                    PIX_DB.SILVER.DIM_MUNICIPIO M

                    ON
                        F.MUNICIPIO_IBGE
                        = M.MUNICIPIO_IBGE

                INNER JOIN
                    PIX_DB.SILVER.DIM_ESTADO E

                    ON
                        M.ESTADO_IBGE
                        = E.ESTADO_IBGE
                """
            )

            cursor.execute("COMMIT")

            qtd_fact_pix = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_PIX"
            )

            log(
                f"[OK] GOLD.FACT_PIX: "
                f"{qtd_fact_pix:,}"
            )


            # ====================================================
            # FACT_PIB
            #
            # Grão:
            # ANO x ID_MUNICIPIO
            #
            # Mesmas chaves geográficas e temporais da FACT_PIX.
            # ====================================================

            log(
                "Construindo GOLD.FACT_PIB..."
            )

            qtd_pib_silver = contar_snowflake(
                conexao,
                "PIX_DB.SILVER.FACT_PIB_MUNICIPIO"
            )

            if qtd_pib_silver == 0:

                raise ValueError(
                    "SILVER.FACT_PIB_MUNICIPIO está vazia. "
                    "A FACT_PIB é obrigatória no modelo atual."
                )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.FACT_PIB
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.FACT_PIB
                (
                    ANO,

                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,

                    PIB_TOTAL,
                    IMPOSTOS_LIQUIDOS,
                    VAB_TOTAL,
                    VAB_AGROPECUARIA,
                    VAB_INDUSTRIA,
                    VAB_SERVICOS_EXCLUSIVE_ADM,
                    VAB_ADMINISTRACAO
                )

                SELECT

                    P.ANO,

                    P.MUNICIPIO_IBGE
                        AS ID_MUNICIPIO,

                    M.ESTADO_IBGE,

                    E.ID_REGIAO,

                    P.PIB_TOTAL,
                    P.IMPOSTOS_LIQUIDOS,
                    P.VAB_TOTAL,
                    P.VAB_AGROPECUARIA,
                    P.VAB_INDUSTRIA,
                    P.VAB_SERVICOS_EXCLUSIVE_ADM,
                    P.VAB_ADMINISTRACAO

                FROM
                    PIX_DB.SILVER.FACT_PIB_MUNICIPIO P

                INNER JOIN
                    PIX_DB.SILVER.DIM_MUNICIPIO M

                    ON
                        P.MUNICIPIO_IBGE
                        = M.MUNICIPIO_IBGE

                INNER JOIN
                    PIX_DB.SILVER.DIM_ESTADO E

                    ON
                        M.ESTADO_IBGE
                        = E.ESTADO_IBGE
                """
            )

            cursor.execute("COMMIT")

            log(
                f"[OK] GOLD.FACT_PIB: "
                f"{contar_snowflake(conexao, 'PIX_DB.GOLD.FACT_PIB'):,}"
            )


            # ====================================================
            # FACT_VAB_MUNICIPIO
            #
            # Grão:
            # ANO x ID_MUNICIPIO x ID_CATEGORIA_VAB
            #
            # Converte as quatro colunas de componentes do VAB em
            # linhas para permitir uso de CATEGORIA_VAB em slicers.
            # FACT_PIB permanece inalterada para análises de PIB.
            # ====================================================

            log(
                "Construindo GOLD.FACT_VAB_MUNICIPIO..."
            )

            cursor.execute(
                """
                CREATE TABLE IF NOT EXISTS
                    PIX_DB.GOLD.FACT_VAB_MUNICIPIO
                (
                    ANO NUMBER(4,0) NOT NULL,
                    ID_MUNICIPIO NUMBER(38,0) NOT NULL,
                    ESTADO_IBGE NUMBER(38,0) NOT NULL,
                    ID_REGIAO NUMBER(38,0) NOT NULL,
                    ID_CATEGORIA_VAB NUMBER(2,0) NOT NULL,
                    VALOR_VAB NUMBER(38,6),
                    DATA_PROCESSAMENTO TIMESTAMP_LTZ
                        DEFAULT CURRENT_TIMESTAMP()
                )
                """
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.FACT_VAB_MUNICIPIO
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.FACT_VAB_MUNICIPIO
                (
                    ANO,
                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,
                    ID_CATEGORIA_VAB,
                    VALOR_VAB
                )

                SELECT
                    ANO,
                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,
                    1 AS ID_CATEGORIA_VAB,
                    VAB_SERVICOS_EXCLUSIVE_ADM AS VALOR_VAB
                FROM PIX_DB.GOLD.FACT_PIB

                UNION ALL

                SELECT
                    ANO,
                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,
                    2 AS ID_CATEGORIA_VAB,
                    VAB_INDUSTRIA AS VALOR_VAB
                FROM PIX_DB.GOLD.FACT_PIB

                UNION ALL

                SELECT
                    ANO,
                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,
                    3 AS ID_CATEGORIA_VAB,
                    VAB_ADMINISTRACAO AS VALOR_VAB
                FROM PIX_DB.GOLD.FACT_PIB

                UNION ALL

                SELECT
                    ANO,
                    ID_MUNICIPIO,
                    ESTADO_IBGE,
                    ID_REGIAO,
                    4 AS ID_CATEGORIA_VAB,
                    VAB_AGROPECUARIA AS VALOR_VAB
                FROM PIX_DB.GOLD.FACT_PIB
                """
            )

            cursor.execute("COMMIT")

            qtd_fact_vab = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_VAB_MUNICIPIO"
            )

            log(
                f"[OK] GOLD.FACT_VAB_MUNICIPIO: "
                f"{qtd_fact_vab:,}"
            )


            # ====================================================
            # VALIDAÇÕES GOLD
            # ====================================================

            log(
                "Validando relacionamentos diretos da Gold..."
            )

            validacoes = [

                (
                    "FACT_PIX sem DIM_TEMPO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIX F
                    LEFT JOIN PIX_DB.GOLD.DIM_TEMPO D
                        ON F.ANO = D.ANO
                    WHERE D.ANO IS NULL
                    """
                ),

                (
                    "FACT_PIX sem DIM_MUNICIPIO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIX F
                    LEFT JOIN PIX_DB.GOLD.DIM_MUNICIPIO D
                        ON F.ID_MUNICIPIO = D.ID_MUNICIPIO
                    WHERE D.ID_MUNICIPIO IS NULL
                    """
                ),

                (
                    "FACT_PIX sem DIM_ESTADO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIX F
                    LEFT JOIN PIX_DB.GOLD.DIM_ESTADO D
                        ON F.ESTADO_IBGE = D.ESTADO_IBGE
                    WHERE D.ESTADO_IBGE IS NULL
                    """
                ),

                (
                    "FACT_PIX sem DIM_REGIAO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIX F
                    LEFT JOIN PIX_DB.GOLD.DIM_REGIAO D
                        ON F.ID_REGIAO = D.ID_REGIAO
                    WHERE D.ID_REGIAO IS NULL
                    """
                ),

                (
                    "FACT_PIB sem DIM_TEMPO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIB F
                    LEFT JOIN PIX_DB.GOLD.DIM_TEMPO D
                        ON F.ANO = D.ANO
                    WHERE D.ANO IS NULL
                    """
                ),

                (
                    "FACT_PIB sem DIM_MUNICIPIO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIB F
                    LEFT JOIN PIX_DB.GOLD.DIM_MUNICIPIO D
                        ON F.ID_MUNICIPIO = D.ID_MUNICIPIO
                    WHERE D.ID_MUNICIPIO IS NULL
                    """
                ),

                (
                    "FACT_PIB sem DIM_ESTADO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIB F
                    LEFT JOIN PIX_DB.GOLD.DIM_ESTADO D
                        ON F.ESTADO_IBGE = D.ESTADO_IBGE
                    WHERE D.ESTADO_IBGE IS NULL
                    """
                ),

                (
                    "FACT_PIB sem DIM_REGIAO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_PIB F
                    LEFT JOIN PIX_DB.GOLD.DIM_REGIAO D
                        ON F.ID_REGIAO = D.ID_REGIAO
                    WHERE D.ID_REGIAO IS NULL
                    """
                ),

                (
                    "FACT_VAB_MUNICIPIO sem DIM_TEMPO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO F
                    LEFT JOIN PIX_DB.GOLD.DIM_TEMPO D
                        ON F.ANO = D.ANO
                    WHERE D.ANO IS NULL
                    """
                ),

                (
                    "FACT_VAB_MUNICIPIO sem DIM_MUNICIPIO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO F
                    LEFT JOIN PIX_DB.GOLD.DIM_MUNICIPIO D
                        ON F.ID_MUNICIPIO = D.ID_MUNICIPIO
                    WHERE D.ID_MUNICIPIO IS NULL
                    """
                ),

                (
                    "FACT_VAB_MUNICIPIO sem DIM_ESTADO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO F
                    LEFT JOIN PIX_DB.GOLD.DIM_ESTADO D
                        ON F.ESTADO_IBGE = D.ESTADO_IBGE
                    WHERE D.ESTADO_IBGE IS NULL
                    """
                ),

                (
                    "FACT_VAB_MUNICIPIO sem DIM_REGIAO",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO F
                    LEFT JOIN PIX_DB.GOLD.DIM_REGIAO D
                        ON F.ID_REGIAO = D.ID_REGIAO
                    WHERE D.ID_REGIAO IS NULL
                    """
                ),

                (
                    "FACT_VAB_MUNICIPIO sem DIM_CATEGORIA_VAB",
                    """
                    SELECT COUNT(*)
                    FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO F
                    LEFT JOIN PIX_DB.GOLD.DIM_CATEGORIA_VAB D
                        ON F.ID_CATEGORIA_VAB = D.ID_CATEGORIA_VAB
                    WHERE D.ID_CATEGORIA_VAB IS NULL
                    """
                )

            ]

            for nome, sql in validacoes:

                cursor.execute(sql)

                qtd = cursor.fetchone()[0]

                if qtd > 0:

                    raise ValueError(
                        f"{nome}: {qtd:,} registros."
                    )


            silver_fact_pix = contar_snowflake(
                conexao,
                "PIX_DB.SILVER.FACT_PIX_MENSAL"
            )

            gold_fact_pix = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_PIX"
            )

            if silver_fact_pix != gold_fact_pix:

                raise ValueError(
                    "FACT_PIX divergiu entre Silver e Gold. "
                    f"Silver={silver_fact_pix:,}; "
                    f"Gold={gold_fact_pix:,}."
                )


            silver_fact_pib = contar_snowflake(
                conexao,
                "PIX_DB.SILVER.FACT_PIB_MUNICIPIO"
            )

            gold_fact_pib = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_PIB"
            )

            if silver_fact_pib != gold_fact_pib:

                raise ValueError(
                    "FACT_PIB divergiu entre Silver e Gold. "
                    f"Silver={silver_fact_pib:,}; "
                    f"Gold={gold_fact_pib:,}."
                )


            gold_fact_vab = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_VAB_MUNICIPIO"
            )

            esperado_fact_vab = gold_fact_pib * 4

            if gold_fact_vab != esperado_fact_vab:

                raise ValueError(
                    "FACT_VAB_MUNICIPIO deveria possuir quatro "
                    "categorias por registro da FACT_PIB. "
                    f"Esperado={esperado_fact_vab:,}; "
                    f"Encontrado={gold_fact_vab:,}."
                )


            log(
                "[OK] Relacionamentos da Gold validados."
            )


        except Exception:

            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass

            raise

        finally:
            cursor.close()


    # ============================================================
    # BUILD MARTS GOLD
    #
    # Os marts usam as chaves geográficas diretamente das fatos.
    # DIM_TEMPO é anual, portanto mês e trimestre dos marts PIX
    # são calculados a partir de FACT_PIX.ANOMES.
    # ============================================================

    def build_gold_marts(
        conexao
    ):

        log("")
        log("=" * 80)
        log("BUILD GOLD MARTS")
        log("=" * 80)

        cursor = conexao.cursor()

        try:

            # ====================================================
            # MART 1
            # MUNICÍPIO / MÊS
            # ====================================================

            log(
                "Construindo "
                "GOLD.MART_PIX_MUNICIPIO_MENSAL..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.MART_PIX_MUNICIPIO_MENSAL
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.MART_PIX_MUNICIPIO_MENSAL
                (
                    ID_TEMPO,

                    ANO,
                    MES,
                    TRIMESTRE,

                    ID_MUNICIPIO,
                    MUNICIPIO,

                    ESTADO_IBGE,
                    SIGLA_UF,
                    ESTADO,

                    ID_REGIAO,
                    SIGLA_REGIAO,
                    REGIAO,

                    VL_PAGADOR_PF,
                    VL_PAGADOR_PJ,

                    VL_RECEBEDOR_PF,
                    VL_RECEBEDOR_PJ,

                    QT_PAGADOR_PF,
                    QT_PAGADOR_PJ,

                    QT_RECEBEDOR_PF,
                    QT_RECEBEDOR_PJ,

                    VL_TOTAL_PAGADOR,
                    VL_TOTAL_RECEBEDOR,

                    QT_TOTAL_PAGADOR,
                    QT_TOTAL_RECEBEDOR,

                    TICKET_MEDIO_PAGADOR,
                    TICKET_MEDIO_RECEBEDOR
                )

                SELECT

                    F.ANOMES
                        AS ID_TEMPO,

                    F.ANO,

                    MOD(F.ANOMES, 100)
                        AS MES,

                    CEIL(
                        MOD(F.ANOMES, 100) / 3.0
                    )
                        AS TRIMESTRE,

                    F.ID_MUNICIPIO,

                    M.MUNICIPIO,

                    F.ESTADO_IBGE,

                    E.SIGLA_UF,

                    E.ESTADO,

                    F.ID_REGIAO,

                    R.SIGLA_REGIAO,

                    R.REGIAO,

                    F.VL_PAGADORPF
                        AS VL_PAGADOR_PF,

                    F.VL_PAGADORPJ
                        AS VL_PAGADOR_PJ,

                    F.VL_RECEBEDORPF
                        AS VL_RECEBEDOR_PF,

                    F.VL_RECEBEDORPJ
                        AS VL_RECEBEDOR_PJ,

                    F.QT_PAGADORPF
                        AS QT_PAGADOR_PF,

                    F.QT_PAGADORPJ
                        AS QT_PAGADOR_PJ,

                    F.QT_RECEBEDORPF
                        AS QT_RECEBEDOR_PF,

                    F.QT_RECEBEDORPJ
                        AS QT_RECEBEDOR_PJ,

                    COALESCE(F.VL_PAGADORPF, 0)
                    +
                    COALESCE(F.VL_PAGADORPJ, 0)
                        AS VL_TOTAL_PAGADOR,

                    COALESCE(F.VL_RECEBEDORPF, 0)
                    +
                    COALESCE(F.VL_RECEBEDORPJ, 0)
                        AS VL_TOTAL_RECEBEDOR,

                    COALESCE(F.QT_PAGADORPF, 0)
                    +
                    COALESCE(F.QT_PAGADORPJ, 0)
                        AS QT_TOTAL_PAGADOR,

                    COALESCE(F.QT_RECEBEDORPF, 0)
                    +
                    COALESCE(F.QT_RECEBEDORPJ, 0)
                        AS QT_TOTAL_RECEBEDOR,

                    (
                        COALESCE(F.VL_PAGADORPF, 0)
                        +
                        COALESCE(F.VL_PAGADORPJ, 0)
                    )
                    /
                    NULLIF(
                        COALESCE(F.QT_PAGADORPF, 0)
                        +
                        COALESCE(F.QT_PAGADORPJ, 0),
                        0
                    )
                        AS TICKET_MEDIO_PAGADOR,

                    (
                        COALESCE(F.VL_RECEBEDORPF, 0)
                        +
                        COALESCE(F.VL_RECEBEDORPJ, 0)
                    )
                    /
                    NULLIF(
                        COALESCE(F.QT_RECEBEDORPF, 0)
                        +
                        COALESCE(F.QT_RECEBEDORPJ, 0),
                        0
                    )
                        AS TICKET_MEDIO_RECEBEDOR

                FROM
                    PIX_DB.GOLD.FACT_PIX F

                INNER JOIN
                    PIX_DB.GOLD.DIM_MUNICIPIO M

                    ON
                        F.ID_MUNICIPIO
                        = M.ID_MUNICIPIO

                INNER JOIN
                    PIX_DB.GOLD.DIM_ESTADO E

                    ON
                        F.ESTADO_IBGE
                        = E.ESTADO_IBGE

                INNER JOIN
                    PIX_DB.GOLD.DIM_REGIAO R

                    ON
                        F.ID_REGIAO
                        = R.ID_REGIAO
                """
            )

            cursor.execute("COMMIT")

            qtd_municipio = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.MART_PIX_MUNICIPIO_MENSAL"
            )

            log(
                f"[OK] MART_PIX_MUNICIPIO_MENSAL: "
                f"{qtd_municipio:,}"
            )


            # ====================================================
            # MART 2
            # UF / MÊS
            # ====================================================

            log(
                "Construindo "
                "GOLD.MART_PIX_UF_MENSAL..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.MART_PIX_UF_MENSAL
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.MART_PIX_UF_MENSAL
                (
                    ID_TEMPO,

                    ANO,
                    MES,

                    ESTADO_IBGE,
                    SIGLA_UF,
                    ESTADO,

                    ID_REGIAO,
                    REGIAO,

                    VL_TOTAL_PAGADOR,
                    VL_TOTAL_RECEBEDOR,

                    QT_TOTAL_PAGADOR,
                    QT_TOTAL_RECEBEDOR,

                    MUNICIPIOS
                )

                SELECT

                    F.ANOMES
                        AS ID_TEMPO,

                    F.ANO,

                    MOD(F.ANOMES, 100)
                        AS MES,

                    F.ESTADO_IBGE,

                    E.SIGLA_UF,

                    E.ESTADO,

                    F.ID_REGIAO,

                    R.REGIAO,

                    SUM(
                        COALESCE(F.VL_PAGADORPF, 0)
                        +
                        COALESCE(F.VL_PAGADORPJ, 0)
                    )
                        AS VL_TOTAL_PAGADOR,

                    SUM(
                        COALESCE(F.VL_RECEBEDORPF, 0)
                        +
                        COALESCE(F.VL_RECEBEDORPJ, 0)
                    )
                        AS VL_TOTAL_RECEBEDOR,

                    SUM(
                        COALESCE(F.QT_PAGADORPF, 0)
                        +
                        COALESCE(F.QT_PAGADORPJ, 0)
                    )
                        AS QT_TOTAL_PAGADOR,

                    SUM(
                        COALESCE(F.QT_RECEBEDORPF, 0)
                        +
                        COALESCE(F.QT_RECEBEDORPJ, 0)
                    )
                        AS QT_TOTAL_RECEBEDOR,

                    COUNT(
                        DISTINCT F.ID_MUNICIPIO
                    )
                        AS MUNICIPIOS

                FROM
                    PIX_DB.GOLD.FACT_PIX F

                INNER JOIN
                    PIX_DB.GOLD.DIM_ESTADO E

                    ON
                        F.ESTADO_IBGE
                        = E.ESTADO_IBGE

                INNER JOIN
                    PIX_DB.GOLD.DIM_REGIAO R

                    ON
                        F.ID_REGIAO
                        = R.ID_REGIAO

                GROUP BY
                    F.ANOMES,
                    F.ANO,
                    F.ESTADO_IBGE,
                    E.SIGLA_UF,
                    E.ESTADO,
                    F.ID_REGIAO,
                    R.REGIAO
                """
            )

            cursor.execute("COMMIT")

            qtd_uf = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.MART_PIX_UF_MENSAL"
            )

            log(
                f"[OK] MART_PIX_UF_MENSAL: "
                f"{qtd_uf:,}"
            )


            # ====================================================
            # MART 3
            # REGIÃO / MÊS
            # ====================================================

            log(
                "Construindo "
                "GOLD.MART_PIX_REGIAO_MENSAL..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.MART_PIX_REGIAO_MENSAL
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.MART_PIX_REGIAO_MENSAL
                (
                    ID_TEMPO,

                    ANO,
                    MES,

                    ID_REGIAO,
                    SIGLA_REGIAO,
                    REGIAO,

                    VL_TOTAL_PAGADOR,
                    VL_TOTAL_RECEBEDOR,

                    QT_TOTAL_PAGADOR,
                    QT_TOTAL_RECEBEDOR,

                    MUNICIPIOS
                )

                SELECT

                    F.ANOMES
                        AS ID_TEMPO,

                    F.ANO,

                    MOD(F.ANOMES, 100)
                        AS MES,

                    F.ID_REGIAO,

                    R.SIGLA_REGIAO,

                    R.REGIAO,

                    SUM(
                        COALESCE(F.VL_PAGADORPF, 0)
                        +
                        COALESCE(F.VL_PAGADORPJ, 0)
                    )
                        AS VL_TOTAL_PAGADOR,

                    SUM(
                        COALESCE(F.VL_RECEBEDORPF, 0)
                        +
                        COALESCE(F.VL_RECEBEDORPJ, 0)
                    )
                        AS VL_TOTAL_RECEBEDOR,

                    SUM(
                        COALESCE(F.QT_PAGADORPF, 0)
                        +
                        COALESCE(F.QT_PAGADORPJ, 0)
                    )
                        AS QT_TOTAL_PAGADOR,

                    SUM(
                        COALESCE(F.QT_RECEBEDORPF, 0)
                        +
                        COALESCE(F.QT_RECEBEDORPJ, 0)
                    )
                        AS QT_TOTAL_RECEBEDOR,

                    COUNT(
                        DISTINCT F.ID_MUNICIPIO
                    )
                        AS MUNICIPIOS

                FROM
                    PIX_DB.GOLD.FACT_PIX F

                INNER JOIN
                    PIX_DB.GOLD.DIM_REGIAO R

                    ON
                        F.ID_REGIAO
                        = R.ID_REGIAO

                GROUP BY
                    F.ANOMES,
                    F.ANO,
                    F.ID_REGIAO,
                    R.SIGLA_REGIAO,
                    R.REGIAO
                """
            )

            cursor.execute("COMMIT")

            qtd_regiao = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.MART_PIX_REGIAO_MENSAL"
            )

            log(
                f"[OK] MART_PIX_REGIAO_MENSAL: "
                f"{qtd_regiao:,}"
            )


            # ====================================================
            # MART 4
            # PIX x PIB MUNICÍPIO / ANO
            # ====================================================

            log(
                "Construindo "
                "GOLD.MART_PIX_PIB_MUNICIPIO_ANUAL..."
            )

            cursor.execute("BEGIN")

            cursor.execute(
                """
                DELETE FROM
                    PIX_DB.GOLD.MART_PIX_PIB_MUNICIPIO_ANUAL
                """
            )

            cursor.execute(
                """
                INSERT INTO
                    PIX_DB.GOLD.MART_PIX_PIB_MUNICIPIO_ANUAL
                (
                    ANO,

                    ID_MUNICIPIO,
                    MUNICIPIO,

                    ESTADO_IBGE,
                    SIGLA_UF,
                    ESTADO,

                    ID_REGIAO,
                    REGIAO,

                    PIB_TOTAL,
                    IMPOSTOS_LIQUIDOS,
                    VAB_TOTAL,
                    VAB_AGROPECUARIA,
                    VAB_INDUSTRIA,
                    VAB_SERVICOS_EXCLUSIVE_ADM,
                    VAB_ADMINISTRACAO,

                    VL_PIX_PAGADOR_ANUAL,
                    VL_PIX_RECEBEDOR_ANUAL,

                    QT_PIX_PAGADOR_ANUAL,
                    QT_PIX_RECEBEDOR_ANUAL,

                    PIX_PAGADOR_SOBRE_PIB,
                    PIX_RECEBEDOR_SOBRE_PIB
                )

                WITH PIX_ANUAL AS (

                    SELECT
                        ANO,

                        ID_MUNICIPIO,

                        ESTADO_IBGE,

                        ID_REGIAO,

                        SUM(
                            COALESCE(VL_PAGADORPF, 0)
                            +
                            COALESCE(VL_PAGADORPJ, 0)
                        )
                            AS VL_PIX_PAGADOR_ANUAL,

                        SUM(
                            COALESCE(VL_RECEBEDORPF, 0)
                            +
                            COALESCE(VL_RECEBEDORPJ, 0)
                        )
                            AS VL_PIX_RECEBEDOR_ANUAL,

                        SUM(
                            COALESCE(QT_PAGADORPF, 0)
                            +
                            COALESCE(QT_PAGADORPJ, 0)
                        )
                            AS QT_PIX_PAGADOR_ANUAL,

                        SUM(
                            COALESCE(QT_RECEBEDORPF, 0)
                            +
                            COALESCE(QT_RECEBEDORPJ, 0)
                        )
                            AS QT_PIX_RECEBEDOR_ANUAL

                    FROM
                        PIX_DB.GOLD.FACT_PIX

                    GROUP BY
                        ANO,
                        ID_MUNICIPIO,
                        ESTADO_IBGE,
                        ID_REGIAO
                )

                SELECT

                    P.ANO,

                    P.ID_MUNICIPIO,

                    M.MUNICIPIO,

                    P.ESTADO_IBGE,

                    E.SIGLA_UF,

                    E.ESTADO,

                    P.ID_REGIAO,

                    R.REGIAO,

                    P.PIB_TOTAL,

                    P.IMPOSTOS_LIQUIDOS,

                    P.VAB_TOTAL,

                    P.VAB_AGROPECUARIA,

                    P.VAB_INDUSTRIA,

                    P.VAB_SERVICOS_EXCLUSIVE_ADM,

                    P.VAB_ADMINISTRACAO,

                    X.VL_PIX_PAGADOR_ANUAL,

                    X.VL_PIX_RECEBEDOR_ANUAL,

                    X.QT_PIX_PAGADOR_ANUAL,

                    X.QT_PIX_RECEBEDOR_ANUAL,

                    X.VL_PIX_PAGADOR_ANUAL
                    /
                    NULLIF(
                        P.PIB_TOTAL,
                        0
                    )
                        AS PIX_PAGADOR_SOBRE_PIB,

                    X.VL_PIX_RECEBEDOR_ANUAL
                    /
                    NULLIF(
                        P.PIB_TOTAL,
                        0
                    )
                        AS PIX_RECEBEDOR_SOBRE_PIB

                FROM
                    PIX_DB.GOLD.FACT_PIB P

                LEFT JOIN
                    PIX_ANUAL X

                    ON
                        P.ANO
                        = X.ANO

                        AND

                        P.ID_MUNICIPIO
                        = X.ID_MUNICIPIO

                INNER JOIN
                    PIX_DB.GOLD.DIM_MUNICIPIO M

                    ON
                        P.ID_MUNICIPIO
                        = M.ID_MUNICIPIO

                INNER JOIN
                    PIX_DB.GOLD.DIM_ESTADO E

                    ON
                        P.ESTADO_IBGE
                        = E.ESTADO_IBGE

                INNER JOIN
                    PIX_DB.GOLD.DIM_REGIAO R

                    ON
                        P.ID_REGIAO
                        = R.ID_REGIAO
                """
            )

            cursor.execute("COMMIT")

            qtd_pib_mart = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.MART_PIX_PIB_MUNICIPIO_ANUAL"
            )

            log(
                f"[OK] MART_PIX_PIB_MUNICIPIO_ANUAL: "
                f"{qtd_pib_mart:,}"
            )


            # ====================================================
            # VALIDAÇÕES DOS MARTS
            # ====================================================

            log(
                "Validando integridade dos marts..."
            )

            qtd_fact_pix = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_PIX"
            )

            qtd_mart_municipio = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.MART_PIX_MUNICIPIO_MENSAL"
            )

            if qtd_fact_pix != qtd_mart_municipio:

                raise ValueError(
                    "Divergência entre FACT_PIX e "
                    "MART_PIX_MUNICIPIO_MENSAL. "
                    f"FACT={qtd_fact_pix:,}; "
                    f"MART={qtd_mart_municipio:,}"
                )


            qtd_fact_pib = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.FACT_PIB"
            )

            qtd_mart_pib = contar_snowflake(
                conexao,
                "PIX_DB.GOLD.MART_PIX_PIB_MUNICIPIO_ANUAL"
            )

            if qtd_fact_pib != qtd_mart_pib:

                raise ValueError(
                    "Divergência entre FACT_PIB e "
                    "MART_PIX_PIB_MUNICIPIO_ANUAL. "
                    f"FACT={qtd_fact_pib:,}; "
                    f"MART={qtd_mart_pib:,}"
                )


            cursor.execute(
                """
                SELECT
                    COUNT(*)

                FROM
                    PIX_DB.GOLD.MART_PIX_MUNICIPIO_MENSAL

                WHERE
                    MUNICIPIO IS NULL
                    OR ESTADO IS NULL
                    OR REGIAO IS NULL
                """
            )

            qtd_geo_nulo = cursor.fetchone()[0]

            if qtd_geo_nulo > 0:

                raise ValueError(
                    f"Existem {qtd_geo_nulo:,} registros "
                    f"no mart municipal sem geografia."
                )


            log(
                "[OK] Integridade dos marts validada."
            )


        except Exception:

            try:
                cursor.execute("ROLLBACK")
            except Exception:
                pass

            raise

        finally:
            cursor.close()


    # ============================================================
    # VALIDAÇÃO FINAL
    # ============================================================

    def validar_modelo_final(
        conexao
    ):

        log("")
        log("=" * 80)
        log("VALIDAÇÃO FINAL")
        log("=" * 80)

        objetos = [

            "PIX_DB.BRONZE.PIX_BCB_RAW",
            "PIX_DB.BRONZE.IBGE_ESTADOS",
            "PIX_DB.BRONZE.IBGE_MUNICIPIO_UF",
            "PIX_DB.BRONZE.TRANSACOES_PIX_MUNICIPIO_RAW",
            "PIX_DB.BRONZE.IBGE_SIDRA_5938_RAW",

            "PIX_DB.SILVER.DIM_REGIAO",
            "PIX_DB.SILVER.DIM_ESTADO",
            "PIX_DB.SILVER.DIM_MUNICIPIO",
            "PIX_DB.SILVER.DIM_TEMPO",
            "PIX_DB.SILVER.FACT_PIX_MENSAL",
            "PIX_DB.SILVER.FACT_PIB_MUNICIPIO",

            "PIX_DB.GOLD.DIM_REGIAO",
            "PIX_DB.GOLD.DIM_ESTADO",
            "PIX_DB.GOLD.DIM_MUNICIPIO",
            "PIX_DB.GOLD.DIM_TEMPO",
            "PIX_DB.GOLD.DIM_CATEGORIA_VAB",
            "PIX_DB.GOLD.FACT_PIX",
            "PIX_DB.GOLD.FACT_PIB",
            "PIX_DB.GOLD.FACT_VAB_MUNICIPIO",

            "PIX_DB.GOLD.MART_PIX_MUNICIPIO_MENSAL",
            "PIX_DB.GOLD.MART_PIX_UF_MENSAL",
            "PIX_DB.GOLD.MART_PIX_REGIAO_MENSAL",
            "PIX_DB.GOLD.MART_PIX_PIB_MUNICIPIO_ANUAL"

        ]

        for objeto in objetos:

            qtd = contar_snowflake(
                conexao,
                objeto
            )

            log(
                f"{objeto}: {qtd:,}"
            )


        # ========================================================
        # CONTAGENS FIXAS GEOGRÁFICAS
        # ========================================================

        silver_regiao = contar_snowflake(
            conexao,
            "PIX_DB.SILVER.DIM_REGIAO"
        )

        gold_regiao = contar_snowflake(
            conexao,
            "PIX_DB.GOLD.DIM_REGIAO"
        )

        if (
            silver_regiao != 5
            or gold_regiao != 5
        ):

            raise ValueError(
                "DIM_REGIAO inconsistente: "
                f"Silver={silver_regiao}; "
                f"Gold={gold_regiao}."
            )


        silver_estado = contar_snowflake(
            conexao,
            "PIX_DB.SILVER.DIM_ESTADO"
        )

        gold_estado = contar_snowflake(
            conexao,
            "PIX_DB.GOLD.DIM_ESTADO"
        )

        if (
            silver_estado != 27
            or gold_estado != 27
        ):

            raise ValueError(
                "DIM_ESTADO inconsistente: "
                f"Silver={silver_estado}; "
                f"Gold={gold_estado}."
            )


        gold_categoria_vab = contar_snowflake(
            conexao,
            "PIX_DB.GOLD.DIM_CATEGORIA_VAB"
        )

        if gold_categoria_vab != 4:

            raise ValueError(
                "DIM_CATEGORIA_VAB inconsistente: "
                f"Gold={gold_categoria_vab}; esperado=4."
            )


        # ========================================================
        # PARIDADE DAS FATOS
        # ========================================================

        silver_fact_pix = contar_snowflake(
            conexao,
            "PIX_DB.SILVER.FACT_PIX_MENSAL"
        )

        gold_fact_pix = contar_snowflake(
            conexao,
            "PIX_DB.GOLD.FACT_PIX"
        )

        if silver_fact_pix != gold_fact_pix:

            raise ValueError(
                "FACT_PIX divergiu: "
                f"Silver={silver_fact_pix:,}; "
                f"Gold={gold_fact_pix:,}."
            )


        silver_fact_pib = contar_snowflake(
            conexao,
            "PIX_DB.SILVER.FACT_PIB_MUNICIPIO"
        )

        gold_fact_pib = contar_snowflake(
            conexao,
            "PIX_DB.GOLD.FACT_PIB"
        )

        if silver_fact_pib != gold_fact_pib:

            raise ValueError(
                "FACT_PIB divergiu: "
                f"Silver={silver_fact_pib:,}; "
                f"Gold={gold_fact_pib:,}."
            )


        gold_fact_vab = contar_snowflake(
            conexao,
            "PIX_DB.GOLD.FACT_VAB_MUNICIPIO"
        )

        esperado_fact_vab = gold_fact_pib * 4

        if gold_fact_vab != esperado_fact_vab:

            raise ValueError(
                "FACT_VAB_MUNICIPIO inconsistente: "
                f"Esperado={esperado_fact_vab:,}; "
                f"Gold={gold_fact_vab:,}."
            )


        # ========================================================
        # DIM_TEMPO deve conter todos os anos das fatos Gold
        # ========================================================

        cursor = conexao.cursor()

        try:

            cursor.execute(
                """
                SELECT COUNT(*)

                FROM
                (
                    SELECT DISTINCT
                        ANO

                    FROM
                    (
                        SELECT ANO
                        FROM PIX_DB.GOLD.FACT_PIX

                        UNION

                        SELECT ANO
                        FROM PIX_DB.GOLD.FACT_PIB

                        UNION

                        SELECT ANO
                        FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO
                    )

                    MINUS

                    SELECT ANO
                    FROM PIX_DB.GOLD.DIM_TEMPO
                )
                """
            )

            anos_sem_dim = cursor.fetchone()[0]

            if anos_sem_dim > 0:

                raise ValueError(
                    f"Existem {anos_sem_dim:,} anos das fatos "
                    f"sem correspondência em GOLD.DIM_TEMPO."
                )


            # ----------------------------------------------------
            # Chaves nulas nas fatos Gold
            # ----------------------------------------------------

            cursor.execute(
                """
                SELECT COUNT(*)
                FROM PIX_DB.GOLD.FACT_PIX
                WHERE
                    ANO IS NULL
                    OR ID_MUNICIPIO IS NULL
                    OR ESTADO_IBGE IS NULL
                    OR ID_REGIAO IS NULL
                """
            )

            pix_chaves_nulas = cursor.fetchone()[0]

            if pix_chaves_nulas > 0:

                raise ValueError(
                    f"GOLD.FACT_PIX possui "
                    f"{pix_chaves_nulas:,} registros com chaves nulas."
                )


            cursor.execute(
                """
                SELECT COUNT(*)
                FROM PIX_DB.GOLD.FACT_PIB
                WHERE
                    ANO IS NULL
                    OR ID_MUNICIPIO IS NULL
                    OR ESTADO_IBGE IS NULL
                    OR ID_REGIAO IS NULL
                """
            )

            pib_chaves_nulas = cursor.fetchone()[0]

            if pib_chaves_nulas > 0:

                raise ValueError(
                    f"GOLD.FACT_PIB possui "
                    f"{pib_chaves_nulas:,} registros com chaves nulas."
                )


            cursor.execute(
                """
                SELECT COUNT(*)
                FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO
                WHERE
                    ANO IS NULL
                    OR ID_MUNICIPIO IS NULL
                    OR ESTADO_IBGE IS NULL
                    OR ID_REGIAO IS NULL
                    OR ID_CATEGORIA_VAB IS NULL
                """
            )

            vab_chaves_nulas = cursor.fetchone()[0]

            if vab_chaves_nulas > 0:

                raise ValueError(
                    f"GOLD.FACT_VAB_MUNICIPIO possui "
                    f"{vab_chaves_nulas:,} registros com chaves nulas."
                )


            cursor.execute(
                """
                SELECT COUNT(*)
                FROM
                (
                    SELECT
                        ANO,
                        ID_MUNICIPIO,
                        ID_CATEGORIA_VAB,
                        COUNT(*) AS QT
                    FROM PIX_DB.GOLD.FACT_VAB_MUNICIPIO
                    GROUP BY
                        ANO,
                        ID_MUNICIPIO,
                        ID_CATEGORIA_VAB
                    HAVING COUNT(*) <> 1
                )
                """
            )

            vab_duplicidades = cursor.fetchone()[0]

            if vab_duplicidades > 0:

                raise ValueError(
                    f"GOLD.FACT_VAB_MUNICIPIO possui "
                    f"{vab_duplicidades:,} combinações duplicadas."
                )

        finally:

            cursor.close()


        log(
            "[OK] Modelo final validado."
        )


    # ============================================================
    # MAPEAMENTO
    #
    # Somente BRONZE vem do Databricks.
    #
    # O SIDRA é obrigatório porque FACT_PIB faz parte do modelo
    # analítico final.
    # ============================================================

    TABELAS_BRONZE = [

        {
            "source": "bronze.pix_bcb_raw",
            "target": "PIX_BCB_RAW",
            "estrategia": "monthly",
            "obrigatoria": True
        },

        {
            "source": "bronze.ibge_estados",
            "target": "IBGE_ESTADOS",
            "estrategia": "full",
            "obrigatoria": True
        },

        {
            "source": "bronze.ibge_municipio_uf",
            "target": "IBGE_MUNICIPIOS",
            "estrategia": "full",
            "obrigatoria": True
        },

        {
            "source": "bronze.ibge_municipio_uf",
            "target": "IBGE_MUNICIPIO_UF",
            "estrategia": "full",
            "obrigatoria": True
        },

        {
            "source": "bronze.transacoes_pix_municipio_raw",
            "target": "TRANSACOES_PIX_MUNICIPIO_RAW",
            "estrategia": "monthly",
            "obrigatoria": True
        },

        {
            "source": "bronze.ibge_sidra_5938_raw",
            "target": "IBGE_SIDRA_5938_RAW",
            "estrategia": "full",
            "obrigatoria": True
        }

    ]


    # ============================================================
    # EXECUÇÃO
    # ============================================================

    log("")
    log("=" * 80)
    log("INICIANDO PIPELINE SNOWFLAKE")
    log("=" * 80)

    resultados = []

    conn = conectar_snowflake()

    try:

        cursor = conn.cursor()

        try:

            cursor.execute(
                """
                SELECT
                    CURRENT_ACCOUNT(),
                    CURRENT_USER(),
                    CURRENT_ROLE(),
                    CURRENT_WAREHOUSE(),
                    CURRENT_DATABASE()
                """
            )

            log(
                f"Snowflake session: "
                f"{cursor.fetchone()}"
            )

        finally:

            cursor.close()


        # 1. Databricks Bronze -> Snowflake Bronze
        for config in TABELAS_BRONZE:

            resultado = carregar_tabela_bronze(
                conexao=conn,
                source=config["source"],
                target=config["target"],
                estrategia=config["estrategia"],
                obrigatoria=config["obrigatoria"]
            )

            resultados.append(
                resultado
            )


        # 2. Snowflake Bronze -> Silver
        build_silver_snowflake(
            conn
        )


        # 3. Snowflake Silver -> Gold
        build_gold_snowflake(
            conn
        )


        # 4. Gold -> Marts
        build_gold_marts(
            conn
        )


        # 5. Validação
        validar_modelo_final(
            conn
        )


    finally:

        conn.close()

        log(
            "Conexão Snowflake encerrada."
        )


    df_resultados = pd.DataFrame(
        resultados
    )

    _display(
        df_resultados
    )

    log("")
    log("=" * 80)
    log("LOAD SNOWFLAKE CONCLUÍDO COM SUCESSO")
    log("=" * 80)

    return df_resultados