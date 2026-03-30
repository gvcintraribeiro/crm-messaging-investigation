import duckdb
import pandas as pd


def explorar_dataframe(df: pd.DataFrame) -> None:
    """Exibe um resumo exploratório do DataFrame."""
    print("=== Shape ===")
    print(df.shape)
    print("\n=== Colunas ===")
    print(df.columns.tolist())
    print("\n=== Tipos e nulos ===")
    df.info()
    print("\n=== Primeiras linhas ===")
    df.head()
    print("\n=== Valores únicos por coluna ===")
    print(df.nunique())
    print("\n=== Datas disponíveis em publish_time ===")
    print(df["publish_time"].dt.date.unique())


def sessions_duplicadas(df: pd.DataFrame) -> pd.DataFrame:
    """Calcula participação de valores session_id duplicados em relação ao total"""

    query = """
        WITH total AS (
            SELECT COUNT(DISTINCT session_id) AS qtde_sessions
            FROM df_alias
        ),
        duplicadas AS (
            SELECT COUNT(DISTINCT session_id) AS qtde_duplicadas
            FROM (
                SELECT session_id
                FROM df_alias
                GROUP BY session_id
                HAVING COUNT(*) > 1
            )
        )
        SELECT
            duplicadas.qtde_duplicadas,
            total.qtde_sessions,
            ROUND((duplicadas.qtde_duplicadas * 100.0) / total.qtde_sessions, 2) AS perc_sessoes_duplicadas
        FROM total, duplicadas
    """
    df_alias = df

    df_proporcao = duckdb.query(query).to_df()
    return df_proporcao


def remover_colunas_constantes(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove colunas com apenas um valor único (incluindo nulos).
    Colunas constantes não contribuem para a análise e apenas aumentam o ruído.
    """
    colunas_variantes = [col for col in df.columns if df[col].nunique(dropna=False) > 1]
    colunas_removidas = [col for col in df.columns if col not in colunas_variantes]

    print(f"Colunas removidas ({len(colunas_removidas)}): {colunas_removidas}")
    return df[colunas_variantes]


def buscar_keyword_conversas(
    df_conv: pd.DataFrame, df_camp: pd.DataFrame, pattern: str, coluna_flag: str
) -> pd.DataFrame:
    """
    Busca por um padrão regex na coluna `text` da base de conversas
    e realiza um LEFT JOIN com a base de campanhas via session_id.

    Parâmetros
    ----------
    df_conv     : DataFrame de conversas processadas
    df_camp     : DataFrame de campanhas processadas
    pattern     : Expressão regular a ser buscada (ex: '(?i)CUPOMS26')
    coluna_flag : Nome da coluna indicadora criada no resultado

    Retorno
    -------
    DataFrame filtrado apenas para as linhas onde o padrão foi encontrado.
    """
    query = f"""
        WITH cam AS (
            SELECT DISTINCT session_id AS session_campanha
            FROM df_camp
        ),
        con AS (
            SELECT
                *,
                CASE
                    WHEN regexp_matches(text, '{pattern}') THEN 'sim'
                    ELSE 'não'
                END AS {coluna_flag}
            FROM df_conv
        )
        SELECT *
        FROM con
        LEFT JOIN cam ON cam.session_campanha = con.session_id
        WHERE {coluna_flag} = 'sim'
    """
    return duckdb.query(query).to_df()


def buscar_keyword_logs(
    df_log: pd.DataFrame, pattern: str, coluna_flag: str
) -> pd.DataFrame:
    """
    Busca por um padrão regex na coluna `jsonPayload.message` da base de logs.

    Parâmetros
    ----------
    df_log      : DataFrame de logs processados
    pattern     : Expressão regular a ser buscada (ex: r'(?i)Galaxy\s+S26')
    coluna_flag : Nome da coluna indicadora criada no resultado

    Retorno
    -------
    DataFrame filtrado apenas para as linhas onde o padrão foi encontrado.
    """
    df_alias = df_log

    query = f"""
        SELECT *
        FROM (
            SELECT
                *,
                CASE
                    WHEN regexp_matches("jsonPayload.message", '{pattern}') THEN 'sim'
                    ELSE 'não'
                END AS {coluna_flag}
            FROM df_alias
        )
        WHERE {coluna_flag} = 'sim'
    """
    return duckdb.query(query).to_df()
