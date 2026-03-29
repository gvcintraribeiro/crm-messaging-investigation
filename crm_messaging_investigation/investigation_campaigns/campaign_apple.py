# %%
import duckdb
import pandas as pd

# %%
df_camp_pro = pd.read_csv('../data/data_processed/campanhas_processadas.csv')
df_conv_pro = pd.read_csv('../data/data_processed/conversas_processadas.csv')
df_log_pro = pd.read_csv('../data/data_processed/logs_tratados.csv')
df_camp_conv = pd.read_csv('../data/data_processed/conversas_com_campanhas.csv')

# %%
# Temos um "Falar com a Lu" tambem, mas nao posso afirma que e relativo ao disparo da Apple
# nao ficou muito claro se a CTA "Falar com a Lu" e de fato da Apple
# porem ela aparece existe uma coversa com essa frase que nao se relaciona com nenhuma session_id da tabala de campanhas

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
# Aconteceu erro com essa mensagem tambem da Lu igual com com Sansung S26
query_regex_log_lu = duckdb.query(
"""

SELECT 

*

FROM (
SELECT

*,
CASE
    WHEN regexp_matches("jsonPayload.message", '(?i)Falar\\s+com\\s+a\\s+Lu')
    THEN 'sim'
    ELSE 'não'
END AS cupom_achado

FROM df_log_pro)

WHERE cupom_achado = 'sim'


"""
).to_df()

query_regex_log_lu.head()
# %%
query_regex_log_lu["jsonPayload.message"].to_dict()
# %%

query_regex_log_lu = duckdb.query(
"""

SELECT 

*

FROM (
SELECT

*,
CASE
    WHEN regexp_matches("jsonPayload.message", '(?i)Falar\\s+com\\s+a\\s+Lu')
    THEN 'sim'
    ELSE 'não'
END AS cupom_achado

FROM df_log_pro)

WHERE cupom_achado = 'sim'


"""
).to_df()

# %%

# No comeco da analise eu indentifique que
# existiam nomes parecidos para a campanha crm_cerebro_ads_apple_1903:

#  'crm_cerebro_ads_apple_1003',
#  'crm_cerebro_ads_apple_1303',
#  'crm_cerebro_ads_apple_at',

# Vou verificar se existe alguma menssagem associada a algumas dessas campanhas e contabilizar elas
# o campo para preencher a campanha, parece ser um campo livre
# e porque nao poderia ter acontecido um erro de preenchimento ?

# Das campanhas, apenas duas se associam as conversas
duckdb.query("""

SELECT
             
template,
COUNT(*) AS qtde_registros_por_template
             
FROM df_camp_conv

WHERE template in ('crm_cerebro_ads_apple_1003',
             'crm_cerebro_ads_apple_1303',
             'crm_cerebro_ads_apple_at')
GROUP BY 1

""").to_df()


# De fato tem varios registros, mas apenas um deles mencionam o nome apple
duckdb.query("""

SELECT
             
*
             
FROM df_camp_conv

WHERE template in ('crm_cerebro_ads_apple_1003',
             'crm_cerebro_ads_apple_1303',
             'crm_cerebro_ads_apple_at')


""").to_df().to_csv('../data/data_processed/templates_parecidos_apple.csv',index=False)
# %%
# Vou dar uma olhada para ver econtro o nome aplle se ele aparece em algum registro de conversa ou log
duckdb.query("""

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
    WHEN regexp_matches(text, '(?i)apple')
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


""").to_df().head()
# %%
duckdb.query("""

SELECT 

*

FROM (
SELECT

*,
CASE
    WHEN regexp_matches("jsonPayload.message", '(?i)apple')
    THEN 'sim'
    ELSE 'não'
END AS cupom_achado

FROM df_log_pro)

WHERE cupom_achado = 'sim'


""").to_df().head()
# %%
# %% [markdown]
# ---
# ## 🧩 Conclusão Parcial — Causa Raiz Ainda Indeterminada
#
# Não foi possível chegar a uma conclusão definitiva. As hipóteses em aberto são:
#
# | # | Hipótese | Descrição |
# |---|---|---|
# | 1 | **Nome incorreto** | Campanhas cadastradas com nomes errados no sistema |
# | 2 | **Falha no log** | Problema real na API — pista: mensagem `"Falar com Lu"` no log |
# | 3 | **Ambas** | As duas situações ocorreram simultaneamente |
#
# ---
#
# ### 🔎 Como descartar uma hipótese
#
# A forma mais direta é **consultar o analista de CRM** com as seguintes perguntas:
#
# > - Você reconhece as campanhas `crm_cerebro_ads_apple_1003` e `crm_cerebro_ads_apple_1303`?
# > - Elas estavam programadas para disparar nos dias **19** e **20**?
#
# #### Interpretação da resposta:
#
# - ✅ **Se sim** → os nomes estão corretos no CRM, portanto podemos concluir que
#   houve de fato um **erro na API** no período dos disparos.
#
# - ❌ **Se não** → a hipótese 1 (nome incorreto) se confirma e o problema
#   está no cadastro das campanhas.
#
# ---
# %%
