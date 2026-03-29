# %%
import pandas as pd
import duckdb

df_logs = pd.read_csv("../data/logs_omnichannel.csv",sep=",")
# %%
df_logs.head()
# %%
df_logs.shape
# %%
df_logs.columns
# %%
df_logs.info()
# %%
df_logs.nunique()
# %%
# Aqui eu vou exportar alguns dados para vizualizar
# no google sheets, porque e muito extenso esse dataset
# um pouco inviavel utilizar isso tudo para analisar um informacao
df_logs.loc[:100].to_csv('../data/data_processed/df_logs_limit.csv')

# %%
# Existem muitas variveis que estao com valores iguais e que atribuim nada de importante para um futura analise
# para isso fiz um algoritimo para retirar essas colunas

def remove_constant_columns(df):
    varying = [col for col in df.columns if df[col].nunique(dropna=False) > 1]
    removed = [col for col in df.columns if col not in varying]
    
    print(f"Colunas removidas ({len(removed)}): {removed}")
    return df[varying]

df_logs = remove_constant_columns(df_logs)

df_logs.to_csv('../data/data_processed/logs_tratados.csv')
# %%
