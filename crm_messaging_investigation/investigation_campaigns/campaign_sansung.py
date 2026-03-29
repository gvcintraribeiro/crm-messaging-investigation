# %%
import duckdb
import pandas as pd

# %%
df_camp_pro = pd.read_csv('../data/data_processed/campanhas_processadas.csv')
df_conv_pro = pd.read_csv('../data/data_processed/conversas_processadas.csv')
df_log_pro = pd.read_csv('../data/data_processed/logs_tratados.csv')

# %%
# Na base de mensagens eu entrei 15 registros com o nome do
# alguma com o nome extamente igual a CUPOMS26
# porem nenhuma se associa com session_id da campanha
# o que parece estranho, porque parece que as mesagens foram enviadas, porem nao existe registro de uma campanha

query_regex_cupon = duckdb.query("""

WITH cam AS (
                                 
SELECT

DISTINCT                           
session_id session_campanha

FROM df_camp_pro                                 

        ),
                    
con AS (
SELECT

*,
CASE
    WHEN regexp_matches(text, '(?i)CUPOMS26')
    THEN 'sim'
    ELSE 'não'
END AS cupom_achado

FROM df_conv_pro)
                
SELECT
                                 
*
FROM con
                                                                
LEFT JOIN cam
ON cam.session_campanha =  con.session_id                            
                                 
WHERE cupom_achado = 'sim'
                                 
""").to_df()

query_regex_cupon.head(100)
# %%
# Encontrei varias mensagens com Comprar Galaxy S26 sem associao com alguma campanha pelo session_id
# aumentando a evidencia de que alguma coisa acontece
query_regex_s26 = duckdb.query("""

WITH cam AS (
                                 
SELECT

DISTINCT                           
session_id session_campanha

FROM df_camp_pro                                 

        ),
                    
con AS (
SELECT

*,
CASE
    WHEN regexp_matches(text, '(?i)Galaxy\\s+S26')
    THEN 'sim'
    ELSE 'não'
END AS s26_achado

FROM df_conv_pro)
                
SELECT
                                 
*
FROM con
                                                                
LEFT JOIN cam
ON cam.session_campanha =  con.session_id                            
                                 
WHERE s26_achado = 'sim'
                                 
""").to_df()

query_regex_s26.head(100)
# %%
# Temos um "Falar com a Lu" tambem, mas nao posso afirma que e relativo ao disparo de sansung
query_regex_lu = duckdb.query("""

WITH cam AS (
                                 
SELECT

DISTINCT                           
session_id session_campanha

FROM df_camp_pro                                 

        ),
                    
con AS (
SELECT

*,
CASE
    WHEN regexp_matches(text, '(?i)Falar\\s+com\\s+a\\s+Lu')
    THEN 'sim'
    ELSE 'não'
END AS s26_achado

FROM df_conv_pro)
                
SELECT
                                 
*
FROM con
                                                                
LEFT JOIN cam
ON cam.session_campanha =  con.session_id                            
                                 
WHERE s26_achado = 'sim'
                                 
""").to_df()

query_regex_lu.head(100)
# %%
# Vou bucar na base de log se acho algo referente a essas palavras chaves

query_regex_log_cupon = duckdb.query(
"""

SELECT 

*

FROM (
SELECT

*,
CASE
    WHEN regexp_matches("jsonPayload.message", '(?i)Galaxy\\s+S26')
    THEN 'sim'
    ELSE 'não'
END AS cupom_achado

FROM df_log_pro)

WHERE cupom_achado = 'sim'


"""
).to_df()

query_regex_log_cupon.head()


# %%
query_regex_log_cupon.to_csv('../data/data_processed/log_sansung_s26.csv', index=False)

# %% [markdown]
# ---
# ## 🔍 Análise do Erro: `It is not a JSON type and cannot be deserialized`
#
# **Log observado:**
# ```
# It is not a JSON type and cannot be deserialized: Comprar Galaxy S26 e...
# ```
#
# ---
#
# ### 🤖 Diagnóstico (via Gemini)
#
# > Esse erro indica uma **falha de comunicação** entre o sistema que envia a mensagem
# > e o que recebe (consome) os dados.
#
# O **consumidor** do sistema de mensageria está configurado para esperar um objeto
# estruturado (JSON), mas recebeu apenas uma **string pura** (texto simples).
#
# #### O que aconteceu na prática?
#
# O código de recebimento esperava algo assim:
# ```json
# {
#   "produto": "Galaxy S26",
#   "acao": "comprar"
# }
# ```
#
# Mas o que chegou no tópico foi apenas:
# ```
# Comprar Galaxy S26
# ```
#
# Como esse texto não começa com `{` (objeto) ou `[` (array), o desserializador
# (ex: `Jackson` no Java ou `json.loads` no Python) interrompe o processo e lança o erro.
#
# ---
#
# #### ⚠️ Possíveis Causas
#
# | Causa | Descrição |
# |---|---|
# | **Produtor com "sujeira"** | Sistema de origem enviando dado sem converter para JSON antes do envio |
# | **Payload incorreto** | Mensagem postada manualmente na fila para testes sem formatação técnica |
# | **Deserializer mal configurado** | Kafka/RabbitMQ com `Value Deserializer` esperando Avro/JSON enquanto chega texto plano |
#
# ---
#
# #### ✅ Como resolver?
#
# 1. **Verifique o Produtor** — garanta que o sistema de origem esteja serializando o objeto:
#    ```python
#    import json
#    payload = json.dumps(dados)  # ✅ correto
#    # payload = str(dados)       # ❌ errado
#    ```
#
# 2. **Ajuste o Consumidor** — se a mensagem for realmente texto simples, mude o tipo
#    esperado no listener de `JSON/Objeto` para `String`.
#
# 3. **Validação de Schema** — se usar `Pydantic` ou `Marshmallow`, verifique se a entrada
#    chega como `dict` e não como `str` bruta.
#
# ---
#
# ### 💬 Minha perspectiva
#
# As causas levantadas são válidas, mas sem o projeto em mãos fica difícil cravar.
# Pode ter sido algo mais trivial — **erro de conexão, instabilidade de infra**,
# ou até um **deploy quebrado** naquele dia.
#
# #### 🚀 Próximos passos que valem a pena
#
# - **Monitor de logs proativo:** criar um job que bata periodicamente nos logs da aplicação
#   e sinalize ocorrências desse tipo de erro em um canal do Google Chat.
#
# - **Webhook de alertas:** configurar na camada de logs da aplicação um webhook que
#   dispare eventos em tempo real quando esse erro específico ocorrer.
#
# ---

# %%