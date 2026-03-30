# %%
from pathlib import Path

import duckdb
import pandas as pd

from crm_messaging_investigation.functions.utils import (
    explorar_dataframe,
    sessions_duplicadas,
)

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================================================

DATA_RAW = Path("../data")
DATA_PROCESSED = Path("../data/data_processed")

# =============================================================================
# CARREGAMENTO DOS DADOS
# =============================================================================

# %%
df_conv = pd.read_json(DATA_RAW / "conversas.json")

# %% [markdown]
# ### Dicionário de dados — tabela de conversas
#
# | Variável            | Descrição                                                                                          |
# |---------------------|----------------------------------------------------------------------------------------------------|
# | `session_id`        | Chave de amarração. Identificador que une esta linha ao disparo original da tabela de campanhas.   |
# | `text`              | Conteúdo da mensagem.                                                                              |
# | `author`            | Indica quem enviou a mensagem.                                                                     |
# | `user_id`           | Identificador do cliente. Equivale ao `channel_client_id` da tabela de campanhas.                 |
# | `data`              | Payload bruto. Quando nulo, a informação útil já foi extraída para as demais colunas.             |
# | `attributes`        | Metadados adicionais em JSON. Pode conter IDs de provedores externos (ex: Omnichannel).           |
# | `subscription_name` | Nome da fila de dados de origem.                                                                   |
# | `message_id`        | ID único desta entrada na tabela de conversas. Difere do `message_id` da tabela de campanhas.    |
# | `media_type`        | Formato da mensagem (`text`, `image`, `audio`, `video`).                                           |
# | `publish_time`      | Timestamp UTC de publicação da mensagem.                                                           |

# %%
# =============================================================================
# EXPLORAÇÃO INICIAL
# =============================================================================


explorar_dataframe(df_conv)

# =============================================================================
# SELEÇÃO DE COLUNAS
# =============================================================================

# %%
# As colunas abaixo apresentam baixo valor analítico para o problema em questão
# e foram removidas para simplificar a análise:
#   - author: semanticamente ambígua neste contexto
#   - user_id, data, attributes, subscription_name, media_type: não contribuem
#     para a investigação das campanhas

COLUNAS_DESCARTADAS = [
    "author",
    "user_id",
    "data",
    "attributes",
    "subscription_name",
    "media_type",
]

df_conv = df_conv.drop(columns=COLUNAS_DESCARTADAS)

print("Colunas restantes:", df_conv.columns.tolist())
df_conv.head()

# =============================================================================
# ANÁLISE DE SESSION_ID × MESSAGE_ID
# =============================================================================

# %%
# Verificação: uma session_id pode estar associada a múltiplas mensagens?
# Hipótese: sim — session_id se comporta como uma dimensão (chave de agrupamento),
# enquanto message_id registra eventos individuais dentro da sessão.

query_session_message = """
    SELECT
        session_id,
        COUNT(DISTINCT message_id) AS qtde_distinta
    FROM df_conv
    GROUP BY session_id
    HAVING qtde_distinta > 1
    ORDER BY qtde_distinta DESC
"""

df_session_message = duckdb.query(query_session_message).to_df()
df_session_message.head()

# %%
# Percentual de session_ids com mais de um registro na base
sessions_duplicadas(df_conv)

# =============================================================================
# ORDENAÇÃO E EXPORTAÇÃO
# =============================================================================

# %%
# Ordenação para facilitar a leitura sequencial das conversas:
# session_id → publish_time → message_id

df_conv = df_conv.sort_values(by=["session_id", "publish_time", "message_id"])

df_conv.head()
df_conv.to_csv(DATA_PROCESSED / "conversas_processadas.csv", index=False)

# =============================================================================
# JOIN COM TABELA DE CAMPANHAS
# =============================================================================

# %%
# Nota: o message_id da tabela de campanhas é gerado pelo sistema interno (CRM),
# enquanto o message_id da tabela de conversas é gerado pelo provedor externo
# (ex: Meta / Omnichannel). São identificadores de contextos distintos e não
# devem ser usados como chave de junção.

df_camp_processadas = pd.read_csv(DATA_PROCESSED / "campanhas_processadas.csv")
df_conv_processadas = pd.read_csv(DATA_PROCESSED / "conversas_processadas.csv")


def testar_join(
    df_camp: pd.DataFrame, df_conv: pd.DataFrame, chave: str
) -> pd.DataFrame:
    """Executa e exibe o resultado de um JOIN entre campanhas e conversas pela chave informada."""
    query = f"""
        SELECT *
        FROM df_camp camp
        JOIN df_conv con ON con.{chave} = camp.{chave}
    """
    resultado = duckdb.query(query).to_df()
    print(f"JOIN por '{chave}': {len(resultado)} linhas resultantes.")
    return resultado


# Confirmação: JOIN por message_id não retorna resultados (chaves de contextos distintos)
df_join_message_id = testar_join(df_camp_processadas, df_conv_processadas, "message_id")
df_join_senssion_id = testar_join(
    df_camp_processadas, df_conv_processadas, "session_id"
)

# %%
# JOIN por session_id: melhor candidata à chave de relacionamento entre as tabelas.
# Ressalva: há inconsistências nas datas e no conteúdo das mensagens vinculadas,
# o que pode indicar imperfeições na modelagem original dos dados.

query_join_session_id = """
    SELECT
        camp.session_id,
        camp.publish_time   AS publish_time_campanha,
        camp.message_id     AS message_id_campanha,
        camp.template,
        con.text,
        con.publish_time    AS publish_time_conversa,
        con.message_id      AS message_id_conversa
    FROM df_camp_processadas camp
    JOIN df_conv_processadas con
        ON con.session_id = camp.session_id
"""

df_join_session = duckdb.query(query_join_session_id).to_df()
df_join_session.head()
df_join_session.to_csv(DATA_PROCESSED / "conversas_com_campanhas.csv", index=False)

# %% [markdown]
# ---
# ## Resumo da base de conversas
#
# ### Características gerais
# - As colunas `author`, `user_id`, `data`, `attributes`, `subscription_name` e `media_type`
#   apresentam baixo valor analítico e foram descartadas.
# - Os registros cobrem o período de 20/03. Estranho, pois a apple teve disparos dia 19/03.
# - Uma `session_id` agrega múltiplas mensagens, que podem ser lidas em sequência
#   ao ordenar por `publish_time`.
# - O `message_id` da tabela de conversas tem significado distinto do `message_id`
#   da tabela de campanhas (provedor externo vs. sistema interno).
# - A `session_id` é a única chave identificada para relacionar as duas tabelas,
#   embora haja indícios de inconsistência na modelagem original.
#
# ### Hipóteses levantadas
# - Existem valores na variável `text` correspondentes ao cupom `CUPOMS26`
#   e aos CTAs "Falar com a Lu" ou "Comprar Galaxy S26"?

# %%
