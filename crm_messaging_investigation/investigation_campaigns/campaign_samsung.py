# %%
from pathlib import Path

import pandas as pd
from IPython.display import display

from crm_messaging_investigation.functions.utils import (
    buscar_keyword_conversas,
    buscar_keyword_logs,
)

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================================================

DATA_PROCESSED = Path(__file__).resolve().parent.parent / "data" / "data_processed"

# =============================================================================
# CARREGAMENTO DOS DADOS
# =============================================================================

# %%
df_camp_pro = pd.read_csv(DATA_PROCESSED / "campanhas_processadas.csv")
df_conv_pro = pd.read_csv(DATA_PROCESSED / "conversas_processadas.csv")
df_log_pro = pd.read_csv(DATA_PROCESSED / "logs_tratados.csv")

# =============================================================================
# BUSCA POR PALAVRAS-CHAVE NA BASE DE CONVERSAS
# =============================================================================

# %%
# Busca por menções ao cupom CUPOMS26 nas conversas.
# Foram encontrados 15 registros, porém nenhum associado a um session_id de campanha —
# o que sugere que as mensagens foram enviadas sem registro correspondente na tabela de campanhas.
df_cupom = buscar_keyword_conversas(
    df_conv_pro, df_camp_pro, pattern="(?i)CUPOMS26", coluna_flag="cupom_achado"
)
display(df_cupom.head(100))

# %%
# Busca por menções ao produto Galaxy S26 nas conversas.
# Também foram encontrados registros sem associação com session_id de campanha,
# reforçando a hipótese de falha no pipeline de disparo.
df_s26 = buscar_keyword_conversas(
    df_conv_pro, df_camp_pro, pattern=r"(?i)Galaxy\s+S26", coluna_flag="s26_achado"
)
display(df_s26.head(100))

# =============================================================================
# BUSCA POR PALAVRAS-CHAVE NA BASE DE LOGS
# =============================================================================

# %%
# Investigação nos logs do Omnichannel: existem registros relacionados ao Galaxy S26?
df_log_s26 = buscar_keyword_logs(
    df_log_pro, pattern=r"(?i)Galaxy\s+S26", coluna_flag="lu_achado"
)
display(df_log_s26.head())

# %%
df_log_s26.to_csv(DATA_PROCESSED / "log_samsung_s26.csv", index=False)

# %% [markdown]
# ---
# ## Análise do Erro: `It is not a JSON type and cannot be deserialized`
#
# **Log observado:**
# ```
# It is not a JSON type and cannot be deserialized: Comprar Galaxy S26 e...
# ```
#
# ---
#
# ### Diagnóstico
#
# Esse erro indica uma **falha de comunicação** entre o sistema que produz a mensagem
# e o consumidor que a processa.
#
# O consumidor está configurado para esperar um objeto JSON estruturado, mas recebeu
# uma **string pura** (texto simples).
#
# **O que chegou no tópico:**
# ```
# Comprar Galaxy S26
# ```
#
# **O que era esperado:**
# ```json
# { "produto": "Galaxy S26", "acao": "comprar" }
# ```
#
# Como o texto não inicia com `{` ou `[`, o desserializador interrompe o processo
# e lança o erro.
#
# ---
#
# ### Possíveis causas
#
# | Causa                          | Descrição                                                                          |
# |--------------------------------|------------------------------------------------------------------------------------|
# | **Produtor com dado incorreto**| Sistema de origem enviando string sem serializar para JSON antes do envio.         |
# | **Payload manual**             | Mensagem postada manualmente na fila para testes sem formatação adequada.          |
# | **Deserializer mal configurado**| Consumidor configurado para Avro/JSON enquanto o produtor envia texto plano.      |
#
# ---
#
# ### Como resolver
#
# 1. **Verificar o produtor** — garantir que o objeto seja serializado corretamente:
#    ```python
#    import json
#    payload = json.dumps(dados)  # ✅ correto
#    # payload = str(dados)       # ❌ incorreto
#    ```
#
# 2. **Ajustar o consumidor** — se a mensagem for realmente texto simples, alterar o
#    tipo esperado no listener de `JSON` para `String`.
#
# 3. **Validação de schema** — se utilizar `Pydantic` ou `Marshmallow`, verificar se
#    a entrada chega como `dict` e não como `str` bruta.
#
# ---
#
# ### Perspectiva
#
# As causas levantadas são plausíveis, mas sem acesso direto ao projeto fica difícil
# determinar a raiz do problema. Pode ter sido algo mais pontual — erro de conexão,
# instabilidade de infraestrutura ou deploy com problema naquele dia.
#
# ### Próximos passos recomendados
#
# - **Monitor de logs proativo:** criar um job periódico que varra os logs da aplicação
#   e sinalize ocorrências desse tipo de erro em um canal do Google Chat.
#
# - **Webhook de alertas:** configurar na camada de logs um webhook que dispare eventos
#   em tempo real sempre que esse erro específico for detectado.

# %%
