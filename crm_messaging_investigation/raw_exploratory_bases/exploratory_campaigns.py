# %% [markdown]
# ### Importação de Bibliotecas e Configuração
# O objetivo é utilizar o **Pandas** para manipulações rápidas e abstrações de código, 
# recorrendo ao **DuckDB** para consultas via SQL, aproveitando a 
# integração nativa entre ambos.


# %%
import duckdb
import pandas as pd

df_camp = pd.read_json('../data/campanhas.json')

# %% [markdown]
### Dicionário de dados — tabela de campanhas
#Em geral, todas as variáveis são categóricas ou temporais. Mesmo os campos numéricos não são varieveis numericas — não faz sentido somá-los ou calcular médias, somas e outras estatística sobre elas.
#Por não estar dentro do negócio, o significado de algumas variáveis não era imediatamente óbvio. Para suprir essa lacuna, consultei uma IA com o contexto do desafio, usando o seguinte prompt:
# > Tenho esse desafio para ser feito e estou criando um dicionário de dados para os dados de campanha. Eles se encontram assim e preciso que você me ajude a ter a intuição de seus significados:
# >
# > ```python
# > {
# >     'session_id':          '3a03ed2e-87d0-47e2-a041-f9cea3cddfd2',
# >     'source':              'crm',
# >     'ctwa_clid':           nan,
# >     'channel_client_id':   5511901234567,
# >     'publish_time':        Timestamp('2026-03-20 13:17:13.833000+0000', tz='UTC'),
# >     'data':                nan,
# >     'attributes':          '{}',
# >     'subscription_name':   'campaign-source-topic-bq',
# >     'message_id':          18932555070477050,
# >     'template':            'crm__crebro_da_lu__mercado_novo',
# >     'version':             '1'
# > }
# > ```

#O Claude alucionou nas respostas, mas o Gemini entregou interpretações que fizeram mais sentido. Seguem os significados inferidos:

# | Variável | Descrição |
# |---|---|
# | `session_id` | Identificador único da sessão de envio. Agrupa todas as mensagens disparadas em um mesmo lote ou interação. |
# | `source` | Origem do disparo. O valor `'crm'` indica que o gatilho partiu da ferramenta de gestão de relacionamento com o cliente. |
# | `ctwa_clid` | *Click to WhatsApp Click ID.* Rastreia a origem de anúncios que direcionam ao WhatsApp. Valor `nan` sugere disparo direto via CRM, sem anúncio pago associado. |
# | `channel_client_id` | Identificador do destinatário no canal|
# | `publish_time` | Timestamp exato em que a mensagem foi processada pelo sistema de mensageria. |
# | `data` | Campo reservado para o corpo da mensagem ou payload bruto. Valor `nan` indica que as informações relevantes estão estruturadas nos demais campos. |
# | `attributes` | Metadados da mensagem em formato JSON. Pode guardar IDs de rastreio, categorias de campanha ou flags de teste. |
# | `subscription_name` | Nome da assinatura do tópico de dados (ex: `campaign-source-topic-bq`). Indica qual fluxo alimentou este registro na tabela. |
# | `message_id` | Identificador único de cada evento de mensagem gerado pelo provedor. |
# | `template` | **Campo crítico.** Nome técnico do modelo de mensagem aprovado. Será usado para validar se os templates citados pelo analista existem na base. |
# | `version` | Versão do template ou do esquema de dados utilizado no momento do disparo. |


# %%
df_camp.shape

# %%
df_camp.columns
# %%
# O campo data pode ser removido, porque ele tem apenas dados null
df_camp.info()

# %%
df_camp.head()
# %%
# Os dados channel_client_id, attributes e subscription_name tem apenas um valor
# Nao faz sentido processeguir com eles na alise, eles nao vao me ajudar a diferenciar os dados
df_camp.nunique()


# %%
df_camp = df_camp.drop(columns=['attributes',
                                'data',
                                'subscription_name',
                                'channel_client_id'])

# %%
df_camp.head()

#%%
# Registros do dia 19 e 20 de Marco
# Noa preciso aplicar filtro de data
df_camp["publish_time"].dt.date.unique()


# %%
# Aqui nao econtramos os valores 835 e 838 para send type
# sera que temos os valores para os templates ?
df_camp["version"].unique()

# %%

# Nao encontrei o crm_cerebro_ads_apple_1903 e crm_cerebro_galaxys26
# Porem achei alguns valores parecidos para crm_cerebro_ads_apple_1903:
#  'crm_cerebro_ads_apple_1003',
#  'crm_cerebro_ads_apple_1303',
#  'crm_cerebro_ads_apple_at',

sorted(df_camp["template"].astype(str).unique(), key=str)

# %%
# Ate o momento ja encontrei os dados que eu tinha que confirma,
# porem quero entender um pouco mais do negocio e por meio de algumas analises
# tirar algums duvidas que tenho da base campanhas

# 1 - O Gemini me informou que ctwa_clid esta diretamente ligado ao source,
# uma vez que o source do tipo meta gera um link a anuncio relacionado a ele
# enquanto o source crm enviomos direto a mensagem no whatsapp do cliente
# para mim faz sentido, mas precisava confirmar e de fato e verdade

query_ctwa_source = """
SELECT

source,
COUNT(DISTINCT ctwa_clid) AS qtde_distinta_ctwa_clid

FROM df_camp

GROUP BY 1

"""

study_ctwa_source = duckdb.query(query_ctwa_source).to_df()

study_ctwa_source.head()


# %%
# 2 - Pela quantidade de valores distintos da varivel
# message_id eu consegui ver que ela e uma varivel que
# nao se repete ao longo do arquivo, porem persebi pela
# mesma analise que a varivel session_id se repete algumas
# vezes e preciso entender um pouco isso, pois acredito
# ela seja elegivem para relacao com a tabela de conversas

# %%

# Aqui podemos ver que um session_id pode ter ate dois menssagem_id
# perguntando para o gemini se isso poderia ser normal
# ele me respondeu que o interresante seria de dados de catalogo tivessem apenas um session_id
# quando aparece duas ou mais vezes pode estar relaciados aos seguintes erros de sistemas:
# 1 - Transmissão em Cascata
# 2- Retry Logic
# 3 - Double Firing


query_session_message = """


SELECT

session_id,
COUNT(DISTINCT message_id) AS qtde_distinta

FROM df_camp

GROUP BY 1


HAVING qtde_distinta > 1

ORDER BY qtde_distinta DESC



"""

study_session_message = duckdb.query(query_session_message).to_df()

study_session_message.head()

# %%
# Apenas para ver o impacto disso na base fiz essa consulta para verificar quantos registros estao assim
# e a % em relacao a base


query_per_session_duplicados = """

WITH qtde_sessions AS (


SELECT

COUNT(DISTINCT session_id) AS qtde_sessions

FROM df_camp),

qtde_sessions_duplicadas AS (


SELECT

COUNT(DISTINCT session_id) qtde_sessions_duplicadas

FROM (
SELECT

session_id,
COUNT(*) qtde_resgistros,


FROM df_camp

GROUP BY 1

HAVING qtde_resgistros > 1

)


)


SELECT 

qtde_sessions_duplicadas,
qtde_sessions,
(qtde_sessions_duplicadas/qtde_sessions) * 100 per_sessoes_duplicadas


FROM qtde_sessions,qtde_sessions_duplicadas
"""

estudo_per_session_duplicada = duckdb.query(query_per_session_duplicados).to_df()

estudo_per_session_duplicada.head()



# %%
# Aqui vou retirar o restante de algumas variaveis, que nao vao me ajudar nesse estudo e fazer uma transformacao
# para pegar apenas uma linha das sessions que aparece mais de uma vez, levando em considerecao a ultima data

df_camp = df_camp.drop(columns=['source',
                                'ctwa_clid',
                                'version'])

query_camp_processed = """

SELECT

*

FROM df_camp

QUALIFY row_number() OVER (PARTITION BY session_id ORDER BY publish_time DESC) = 1;

"""

df_camp_processed = duckdb.query(query_camp_processed).to_df()

df_camp_processed.to_csv('../data/data_processed/campanhas_processadas.csv', index=False)

# %% [markdown]

# Resumo da base de campanhas

## Características gerais

# - As colunas `attributes`, `data`, `subscription_name`, `source`, `ctwa_clid`, `version` e `channel_client_id` têm baixo valor analítico e podem ser descartadas.
# - Os registros cobrem o período de **19/03 a 20/03**.
# - As variáveis `source` e `ctwa_clid` são categóricas nominais relacionadas ao canal de disparo. Canais Meta apresentam `ctwa_clid` preenchido, o que faz todo sentido.
# - Algumas `session_id` aparecem mais de uma vez com `message_id` distintos — pode indicar falha no sistema de campanhas ou uma regra de negócio ainda desconhecida.
# - Os *send types* **835** e **838** não foram encontrados; o campo apresenta muitos nulos e alguns valores fora do padrão.
# - Os templates **`crm_cerebro_ads_apple_1903`** e **`crm_cerebro_galaxys26`** não foram encontrados na base.

# ---

# ## Hipóteses levantadas
# **Sobre o disparo:**
# - A campanha pode ter encontrado algum erro e simplesmente não foi disparada — os logs podem esclarecer isso.
# - O disparo pode ter sido realizado com um template incorreto.

# **Sobre a modelagem do dashboard:**
# - Se o painel estiver relacionando `session_id` da tabela de campanhas diretamente com a tabela de conversas, pode estar ocorrendo uma relação muitos-para-muitos que duplica registros — o que, ainda que não seja necessariamente a causa do problema investigado, compromete a confiabilidade dos números exibidos.
# - Sem acesso à construção do painel, fica difícil validar essa hipótese. O caminho dependeria da ferramenta utilizada:
#   - **Power BI:** verificar as ligações entre tabelas, checar se o modelo está em *star schema* ou *snowflake schema*, e revisar se as medidas em DAX estão corretas.
#   - **Looker:** analisar como as consultas estão estruturadas nas *explores*, se os nomes dos campos são consistentes entre as views e se as relações entre elas estão bem definidas.


# %%
