# %%
import duckdb
import pandas as pd

# %%
df_conv = pd.read_json('../data/conversas.json')

# %% [markdown]
### Dicionário de dados — tabela de conversas

# > Tenho esse desafio para ser feito e estou criando um dicionário de dados para os dados de campanha. Eles se encontram assim e preciso que você me ajude a ter a intuição de seus significados:
# >
# > ```python
# > {
# >     'session_id':          '736cd926-2ab1-4fc0-aeb8-dd3e89ebf517',
# >     'text':              '13834846430',
# >     'author':           request,
# >     'user_id':   5511901234567,
# >     'publish_time':        Timestamp('2026-03-20 13:17:13.833000+0000', tz='UTC'),
# >     'data':                nan,
# >     'attributes':          '{}',
# >     'subscription_name':   'conversation-topic-sub-bq',
# >     'message_id':          18937741078362215,
# >     'media_type':            'text',
# > }
# > ```

#O Claude alucionou nas respostas, mas o Gemini entregou interpretações que fizeram mais sentido. Seguem os significados inferidos:

# | Variável | Descrição |
# |---|---|
# | `session_id` | Chave de Amarração. É o identificador que une esta linha ao disparo original da tabela de campanhas. Fundamental para o seu JOIN |
# | `text` | O conteúdo da mensagem |
# | `author` | Campo Estratégico. Indica quem enviou a mensagem. |
# | `user_id` | O identificador do cliente. Deve ser o mesmo channel_client_id da outra tabela para garantir a integridade.|
# | `data` | Payload bruto. Como está null, a informação útil já foi extraída para as outras colunas. |
# | `attributes` | Metadados adicionais em JSON. Pode conter IDs de provedores externos (como o Omnichannel) |
# | `subscription_name` | Nome da fila de dados. |
# | `message_id` | ID único desta entrada na tabela de conversas. Atenção: Será diferente do message_id da tabela de campanhas. |
# | `media_type` | Define o formato da mensagem (text, image, audio, video). |

#%%
df_conv.shape

# %%
df_conv.columns
# %%
df_conv.info()

# %%
df_conv.head()

# %%
df_conv.nunique()

#%%
df_conv["publish_time"].dt.date.unique()
# %%

# A varivel author ela e um pouco estranha, porque
# nesse dataseat existem mensagens de alguns clientes tambem
# mas a varivel indica que existem apenas autores de request
# talvez tenha um outro significado que nao entendi

# Essas variveis nao fazem muito sentido estarem na analise
# porque elas nao ajudam entender o problema

df_conv = df_conv.drop(columns=['author',
                                'user_id',
                                'data',
                                'attributes',
                                'subscription_name',
                                'media_type'
                                ])

df_conv.head()
# %%
# Aqui eu repeti os mesmo estudo de campanhas paraa verifciar se um session pode estar associada a varias mensagens
# e descobri aqui o numero maior
# Sendo que aqui faz muito sentido uma session_id aparecer para varias menssagens

# Info add: Nessa parte do estudo eu vi que esqueci de adicionar DESC no order by no msm estudo de exploratorio campanhas
# E descobri que um session_id de campanha pode aparecer mais de duas vezes, mas com com conhecimento que adqueri ao logo do estudo
# e com dicas do gemini, tudo me endica que ela deveria se comportar como um informacao dimensao e o dados de conversa como uma tabela
# de resgitros mais proximo de fato com varios eventos


query_session_message = """


SELECT

session_id,
COUNT(DISTINCT message_id) AS qtde_distinta

FROM df_conv

GROUP BY 1


HAVING qtde_distinta > 1

ORDER BY qtde_distinta DESC



"""

study_session_message = duckdb.query(query_session_message).to_df()

study_session_message.head()
# %%
# Apenas para ver em relacao a base

query_per_session_duplicates = """

WITH qtde_sessions AS (


SELECT

COUNT(DISTINCT session_id) AS qtde_sessions

FROM df_conv),

qtde_sessions_duplicadas AS (


SELECT

COUNT(DISTINCT session_id) qtde_sessions_duplicadas

FROM (
SELECT

session_id,
COUNT(*) qtde_resgistros,


FROM df_conv

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


study_per_session_duplicada = duckdb.query(query_per_session_duplicates).to_df()

study_per_session_duplicada.head()

# %%
# Sabendo que a base contem menssagens, que estao agrupadas, resolvi ordenar por session_id, publish_time e essage_i
# Isso me ajudou a entender que estava no caminho ceto, porque lendo alguns valores da varivel text fez sentido dentro de
# uma conversa
df_conv = df_conv.sort_values(by=['session_id', 'publish_time', 'message_id'])

df_conv.head()
# %%
df_conv.to_csv('../data/data_processed/conversas_processadas.csv',index=False)
# %%
# Em uma discussão com o Gemini ele me informou que o menssage_id da tabela de campanhas tem
# um significado distinto da tabela de conversas

#Explicacao Gemini:

# ------------------------------------------------------------------------------------------
# 1. A Tabela de Campanhas (O Pedido na Loja)Quando o analista de CRM aperta o botão "Enviar", o sistema interno gera um registro na tabela campanhas.

# O message_id aqui: 

# É o ID gerado pelo seu sistema interno. Ele prova que o comando de envio foi criado com sucesso no banco de dados da empresa.

# Analogia: É o número do pedido que você recebe logo após clicar em "Comprar".

# 2. A Tabela de Conversas (O Registro do Transportador)
# Depois que o comando é criado, ele é enviado para um provedor de mensageria (como a Meta para WhatsApp ou o Provedor Omnichannel mencionado no desafio). 
# Esse provedor recebe a mensagem, processa em seus próprios servidores e devolve um "recibo".O message_id aqui: É o ID gerado pelo provedor externo (ou pelo tópico de integração conversation-topic-sub-bq) para confirmar que a mensagem entrou na rede de transmissão.
# Analogia: É o código de rastreio dos Correios. A loja (CRM) tem o número do pedido, mas a transportadora (Omnichannel) gera o seu próprio código de controle.
# ------------------------------------------------------------------------------------------


#Para confirmar isso eu fiz um join entre as tabelas com chave menssagem_id e de fato nada se realaciou


df_camp_processadas = pd.read_csv('../data/data_processed/campanhas_processadas.csv')
df_conv_processadas = pd.read_csv('../data/data_processed/conversas_processadas.csv')


query_join_menssage_id = duckdb.query("""

SELECT

*

FROM df_camp_processadas camp

JOIN df_conv_processadas con
ON con.message_id = camp.message_id


""").to_df()

query_join_menssage_id.head()
# %%

# Se existe alguma relacao com a base de campanhas, so pode ser pela session_id, porem ainda tenho minhas duvidas
# porque mesmo alguns registros se ligando, a campanha se relaciona com algumas mensagens nao fazem muito sentido
# em relacao ao templete da campanha
# as datas de publicacao da campanhas tambem nao batem muito com o horario de algumas mensagens
# elas sao maiores que as datas mensagens
# como ainda estou descobrindo um pouco do cenario, vou arriscar que caso tenha um relacao entre as tabelas
# esse parece ser melhor caminho ate o momento
# isso faz refletir com como esta um pouco a modelagem desse dado no dash, sera que eles estivem ligados pelo session_id
# esta tudo correto ? Sera que nao existem mais alguma valor em uma chave de id para ser acrescentada ?

query_join_session_id = duckdb.query("""

SELECT

camp.session_id,
camp.publish_time AS publish_time_camp,
camp.message_id AS message_id_camp,
camp.template,
con.text,
con.publish_time AS publish_time_con,
con.message_id AS message_id_con
                                                                                                               

FROM df_camp_processadas camp

JOIN df_conv_processadas con
ON con.session_id = camp.session_id


""").to_df()

query_join_session_id.head()
query_join_session_id.to_csv('../data/data_processed/conversas_com_campanhas.csv',index=False)
# %% [markdown]

# Resumo da base de conversas

## Características gerais

# - As colunas `author`, `user_id`, `data`, `attributes`, `subscription_name` e `media_type` têm baixo valor analítico e podem ser descartadas.
# - Os registros cobrem o período de 20/03.
# - Um source_id empacota varias menssagens, que podem ser vizualizadas em sequencia ordenando pela public_id.
# - A menssagem_id supostamente tem um significado diferente da tabela de campanhas.
# - A session_id parece ser a unica chave que relaciona as tabelas de menssagens e campanhas, porem olhando para o public_id das duas e para as proprias mensagens, parace haver um erros nessa ligacao, mas vou
# manter ela pois foi a melhor opcao que achei ate o momento.

# ---

# ## Hipóteses levantadas
# **Sobre o disparo:**
# - Sera que existem valores na varivel text do CUPOMS26 e dos CTAs: "Falar com a Lu" ou "Comprar Galaxy S26" ?

# %%
