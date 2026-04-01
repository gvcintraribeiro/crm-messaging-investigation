# %%
from pathlib import Path

import duckdb
import pandas as pd
from IPython.display import display

from crm_messaging_investigation.functions.utils import (
    explorar_dataframe,
    session_message,
    sessions_duplicadas,
)

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================================================

DATA_RAW = Path(__file__).resolve().parent.parent / "data"
DATA_PROCESSED = Path(__file__).resolve().parent.parent / "data" / "data_processed"

# =============================================================================
# CARREGAMENTO DOS DADOS
# =============================================================================


# %%
df_camp = pd.read_json(DATA_RAW / "campanhas.json")

# %% [markdown]
# ### Dicionário de dados — tabela de campanhas
#
# Em geral, todas as variáveis são categóricas ou temporais. Mesmo os campos
# numéricos não representam grandezas — não faz sentido somá-los ou calcular
# médias sobre eles.
#
# | Variável            | Descrição                                                                                                                                           |
# |---------------------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
# | `session_id`        | Identificador único da sessão de envio. Agrupa todas as mensagens disparadas em um mesmo lote ou interação.                                         |
# | `source`            | Origem do disparo. O valor `'crm'` indica que o gatilho partiu da ferramenta de gestão de relacionamento com o cliente.                             |
# | `ctwa_clid`         | *Click to WhatsApp Click ID.* Rastreia anúncios que direcionam ao WhatsApp. Valor `nan` indica disparo direto via CRM, sem anúncio pago associado.  |
# | `channel_client_id` | Identificador do destinatário no canal.                                                                                                             |
# | `publish_time`      | Timestamp exato em que a mensagem foi processada pelo sistema de mensageria.                                                                        |
# | `data`              | Payload bruto reservado. Valor `nan` indica que as informações relevantes estão estruturadas nos demais campos.                                     |
# | `attributes`        | Metadados em JSON. Pode conter IDs de rastreio, categorias de campanha ou flags de teste.                                                           |
# | `subscription_name` | Nome da assinatura do tópico de dados (ex: `campaign-source-topic-bq`). Indica qual fluxo alimentou este registro.                                 |
# | `message_id`        | Identificador único de cada evento de mensagem gerado pelo provedor.                                                                                |
# | `template`          | **Campo crítico.** Nome técnico do modelo de mensagem aprovado. Usado para validar se os templates citados pelo analista existem na base.           |
# | `version`           | Versão do template ou do esquema de dados utilizado no momento do disparo.                                                                          |
# %%
# =============================================================================
# EXPLORAÇÃO INICIAL
# =============================================================================

explorar_dataframe(df_camp)

# =============================================================================
# SELEÇÃO DE COLUNAS
# =============================================================================

# %%
# Colunas removidas por baixo valor analítico:
#   - data: contém apenas nulos
#   - channel_client_id, attributes, subscription_name: valor único em toda a base
#     (não diferenciam os registros)

COLUNAS_DESCARTADAS_V1 = [
    "attributes",
    "data",
    "subscription_name",
    "channel_client_id",
]

df_camp = df_camp.drop(columns=COLUNAS_DESCARTADAS_V1)

display(df_camp.head())

# =============================================================================
# VERIFICAÇÕES EXPLORATÓRIAS
# =============================================================================

# %%
# Registros cobrem os dias 19 e 20 de março — nenhum filtro de data necessário.
print("Datas únicas:", df_camp["publish_time"].dt.date.unique())

# %%
# Verificação das versões de template disponíveis na base.
print("Versões únicas:", df_camp["version"].unique())

# %%
# Verificação dos templates presentes.
# Templates esperados mas NÃO encontrados:
#   - crm_cerebro_ads_apple_1903
#   - crm_cerebro_galaxys26
#
# Templates similares encontrados para o Apple:
#   - crm_cerebro_ads_apple_1003
#   - crm_cerebro_ads_apple_1303
#   - crm_cerebro_ads_apple_at
print("Templates disponíveis:")
print(sorted(df_camp["template"].astype(str).unique(), key=str))

# %%
# Validação: source e ctwa_clid são variáveis correlacionadas.
# Disparos via Meta geram um ctwa_clid (rastreio de anúncio),
# enquanto disparos via CRM chegam diretamente ao WhatsApp do cliente, sem clique em anúncio.

query_ctwa_source = """
    SELECT
        source,
        COUNT(DISTINCT ctwa_clid) AS qtde_distinta_ctwa_clid
    FROM df_camp
    GROUP BY source
"""

df_ctwa_source = duckdb.query(query_ctwa_source).to_df()
display(df_ctwa_source)

# =============================================================================
# ANÁLISE DE SESSION_ID × MESSAGE_ID
# =============================================================================

# %%
# Verificação: uma session_id pode estar associada a mais de um message_id?
# Possíveis causas para duplicidade:
#   1. Transmissão em cascata
#   2. Retry logic do sistema
#   3. Double firing (disparo duplicado)

display(session_message(df_camp).head())

# %%
# Percentual de session_ids com mais de um registro na base.

display(sessions_duplicadas(df_camp))

# =============================================================================
# PROCESSAMENTO FINAL E EXPORTAÇÃO
# =============================================================================

# %%
# Remoção das colunas restantes de baixo valor analítico para esta etapa do estudo.

COLUNAS_DESCARTADAS_V2 = ["source", "ctwa_clid", "version"]

df_camp = df_camp.drop(columns=COLUNAS_DESCARTADAS_V2)

# %%
# Nota: o QUALIFY abaixo (comentado) deduplicaria session_ids mantendo apenas
# o registro mais recente. Mantido comentado até confirmar se a deduplicação
# é de fato necessária para a análise.

query_camp_processed = """
    SELECT *
    FROM df_camp
    -- QUALIFY ROW_NUMBER() OVER (PARTITION BY session_id ORDER BY publish_time DESC) = 1
"""

df_camp_processed = duckdb.query(query_camp_processed).to_df()
df_camp_processed.to_csv(DATA_PROCESSED / "campanhas_processadas.csv", index=False)

# %% [markdown]
# ---
# ## Resumo da base de campanhas
#
# ### Características gerais
# - As colunas `attributes`, `data`, `subscription_name`, `source`, `ctwa_clid`,
#   `version` e `channel_client_id` apresentam baixo valor analítico e foram descartadas.
# - Os registros cobrem o período de **19/03 a 20/03**.
# - As variáveis `source` e `ctwa_clid` são correlacionadas: canais Meta apresentam
#   `ctwa_clid` preenchido; disparos via CRM chegam sem clique em anúncio.
# - Algumas `session_id` aparecem mais de uma vez com `message_id` distintos —
#   pode indicar falha no sistema ou uma regra de negócio ainda desconhecida.
# - Os templates **`crm_cerebro_ads_apple_1903`** e **`crm_cerebro_galaxys26`**
#   não foram encontrados na base.
#
# ---
#
# ## Hipóteses levantadas
#
# **Sobre o disparo:**
# - A campanha pode ter encontrado algum erro e simplesmente não foi disparada —
#   os logs podem esclarecer isso.
# - O disparo pode ter sido realizado com um template incorreto.
#
# **Sobre a modelagem do dashboard:**
# - Se o painel estiver relacionando `session_id` da tabela de campanhas diretamente
#   com a tabela de conversas, pode estar ocorrendo uma relação muitos-para-muitos
#   que duplica registros — o que compromete a confiabilidade dos números exibidos.
# - Sem acesso à construção do painel, fica difícil validar essa hipótese.
#   O caminho dependeria da ferramenta utilizada:
#   - **Power BI:** verificar as ligações entre tabelas, checar se o modelo está em
#     *star schema* ou *snowflake schema*, e revisar se as medidas em DAX estão corretas.
#   - **Looker:** analisar como as consultas estão estruturadas nas *explores*,
#     verificar consistência dos nomes de campos entre as views e revisar as relações
#     entre elas.
# %%
