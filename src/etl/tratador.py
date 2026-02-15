import pandas as pd

# Padronização de Tipos e Limpeza de Nulos
def transformar_dados(df):
    
    df['data_inversa'] = pd.to_datetime(df['data_inversa'], errors='coerce', dayfirst=True)
    df['horario'] = pd.to_timedelta(df['horario'])
    df['km'] = df['km'].str.replace(',', '.').astype(float)
    df = df.dropna()
    df['id'] = df['id'].astype(int)

    return df
