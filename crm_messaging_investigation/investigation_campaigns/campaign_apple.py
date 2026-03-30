# %%
from pathlib import Path

import duckdb
import pandas as pd

from crm_messaging_investigation.functions.utils import (
    buscar_keyword_conversas,
    buscar_keyword_logs,
)

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================================================

DATA_PROCESSED = Path("../data/data_processed")

# =============================================================================
# CARREGAMENTO DOS DADOS
# =============================================================================

# %%
df_camp_pro = pd.read_csv(DATA_PROCESSED / "campanhas_processadas.csv")
df_conv_pro = pd.read_csv(DATA_PROCESSED / "conversas_processadas.csv")
df_log_pro = pd.read_csv(DATA_PROCESSED / "logs_tratados.csv")
df_camp_conv = pd.read_csv(DATA_PROCESSED / "conversas_com_campanhas.csv")

# =============================================================================
# BUSCA POR PALAVRAS-CHAVE — "FALAR COM A LU"
# =============================================================================

# %%
# Foram encontradas conversas com a CTA "Falar com a Lu", porém sem associação
# com nenhuma session_id da tabela de campanhas.
# Ainda não é possível afirmar com certeza que essa CTA pertence exclusivamente
# à campanha Apple — o vínculo permanece inconclusivo.

df_lu_conversas = buscar_keyword_conversas(
    df_conv_pro,
    df_camp_pro,
    pattern=r"(?i)Falar\s+com\s+a\s+Lu",
    coluna_flag="lu_achado",
)
df_lu_conversas.head(100)

# %%
# O mesmo erro de desserialização identificado para o Galaxy S26 também ocorreu
# com a mensagem "Falar com a Lu", reforçando a hipótese de falha no pipeline.

df_lu_logs = buscar_keyword_logs(
    df_log_pro, pattern=r"(?i)Falar\s+com\s+a\s+Lu", coluna_flag="lu_achado"
)
df_lu_logs.head()

# %%
# Inspeção do conteúdo das mensagens de log encontradas.
df_lu_logs["jsonPayload.message"].to_dict()

# =============================================================================
# INVESTIGAÇÃO DE TEMPLATES COM NOMES SIMILARES — CAMPANHA APPLE
# =============================================================================

# %%
# No início da análise foram identificados templates com nomenclatura similar
# ao esperado (crm_cerebro_ads_apple_1903):
#
#   - crm_cerebro_ads_apple_1003
#   - crm_cerebro_ads_apple_1303
#   - crm_cerebro_ads_apple_at
#
# Hipótese: o campo de template pode ser de preenchimento livre, o que abre
# a possibilidade de erro de digitação no cadastro da campanha.

TEMPLATES_SIMILARES_APPLE = [
    "crm_cerebro_ads_apple_1003",
    "crm_cerebro_ads_apple_1303",
    "crm_cerebro_ads_apple_at",
]

# %%
# Contagem de registros associados a cada template similar.
# Apenas dois deles se relacionam com conversas.
# crm_cerebro_ads_apple_1003 e crm_cerebro_ads_apple_1303

query_contagem_templates = f"""
    SELECT
        template,
        COUNT(*) AS qtde_registros
    FROM df_camp_conv
    WHERE template IN ({', '.join(f"'{t}'" for t in TEMPLATES_SIMILARES_APPLE)})
    GROUP BY template
"""

df_contagem_templates = duckdb.query(query_contagem_templates).to_df()
df_contagem_templates

# %%
# Exportação do detalhe completo dos registros com templates similares.
# Apenas um deles menciona explicitamente o nome "apple" no texto da conversa.

query_detalhe_templates = f"""
    SELECT *
    FROM df_camp_conv
    WHERE template IN ({', '.join(f"'{t}'" for t in TEMPLATES_SIMILARES_APPLE)})
"""

df_detalhe_templates = duckdb.query(query_detalhe_templates).to_df()
df_detalhe_templates.to_csv(
    DATA_PROCESSED / "templates_parecidos_apple.csv", index=False
)

# =============================================================================
# BUSCA PELO TERMO "APPLE" NAS CONVERSAS E LOGS
# =============================================================================

# %%
# A palavra "apple" foi encontrada tanto nas conversas quanto nos logs,
# porém sem contexto suficiente para extrair conclusões definitivas.

df_apple_conversas = buscar_keyword_conversas(
    df_conv_pro, df_camp_pro, pattern="(?i)apple", coluna_flag="apple_achado"
)
df_apple_conversas.head()

# %%
df_apple_logs = buscar_keyword_logs(
    df_log_pro, pattern="(?i)apple", coluna_flag="apple_achado"
)
df_apple_logs.head()

# %% [markdown]
# ---
# ## Conclusão Parcial — Causa Raiz Ainda Indeterminada
#
# Não foi possível chegar a uma conclusão definitiva. As hipóteses em aberto são:
#
# | # | Hipótese            | Descrição                                                        |
# |---|---------------------|------------------------------------------------------------------|
# | 1 | **Nome incorreto**  | Campanhas cadastradas com nomes errados no sistema.              |
# | 2 | **Falha na API**    | Problema real na API — indício: mensagem `"Falar com a Lu"` no log. |
# | 3 | **Ambas**           | As duas situações ocorreram simultaneamente.                     |
#
# ---
#
# ### Como descartar uma hipótese
#
# A forma mais direta é **consultar o analista de CRM** com as seguintes perguntas:
#
# > - Você reconhece as campanhas `crm_cerebro_ads_apple_1003` e `crm_cerebro_ads_apple_1303`?
# > - Elas estavam programadas para disparar nos dias **19** e **20**?
#
# **Interpretação da resposta:**
#
# - **Sim** → os nomes estão corretos no CRM; conclui-se que houve um **erro na API**
#   durante o período dos disparos.
#
# - **Não** → a hipótese 1 se confirma; o problema está no **cadastro incorreto**
#   das campanhas.

# %%
