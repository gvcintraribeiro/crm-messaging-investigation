# %%
from pathlib import Path

import pandas as pd

from crm_messaging_investigation.functions.utils import remover_colunas_constantes

# =============================================================================
# CONFIGURAÇÃO DE CAMINHOS
# =============================================================================

DATA_RAW = Path(__file__).resolve().parent.parent / "data"
DATA_PROCESSED = Path(__file__).resolve().parent.parent / "data" / "data_processed"

# =============================================================================
# CARREGAMENTO DOS DADOS
# =============================================================================

# %%
df_logs = pd.read_csv(DATA_RAW / "logs_omnichannel.csv")

# %%
# Exportação de uma amostra para inspeção visual no Google Sheets.
# O volume total do dataset inviabiliza a análise manual completa.
df_logs.head(100).to_csv(DATA_PROCESSED / "df_logs_amostra.csv", index=False)

# =============================================================================
# REMOÇÃO DE COLUNAS CONSTANTES
# =============================================================================


# %%
df_logs = remover_colunas_constantes(df_logs)

# =============================================================================
# EXPORTAÇÃO
# =============================================================================

# %%
df_logs.to_csv(DATA_PROCESSED / "logs_tratados.csv", index=False)

# %%
