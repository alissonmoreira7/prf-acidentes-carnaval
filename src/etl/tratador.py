import pandas as pd

# Padronização de Tipos e Limpeza de Nulos
def transformar_dados(df):
    
    #Conversão de data
    data_br = pd.to_datetime(df['data_inversa'], dayfirst=True, errors='coerce')
    data_iso = pd.to_datetime(df['data_inversa'], dayfirst=False, errors='coerce')
    df['data_inversa'] = data_br.fillna(data_iso)


    df['horario'] = pd.to_timedelta(df['horario'], errors='coerce')
    df['km'] = df['km'].str.replace(',', '.').astype(float)

    df = df.dropna(subset=['id'])
    df['id'] = df['id'].astype(int)

    return df

# Adicionar colunas úteis como verificação para saber se é carnaval ou não
def adicionar_colunas(df):
    #Periodo de carnaval nos respectivos anos: 2023, 2024 e 2025
    periodo_carnaval = [
        ('2023-02-17', '2023-02-22'),
        ('2024-02-09', '2024-02-14'),
        ('2025-02-28', '2025-03-05')
    ]

    df['carnaval'] = 0

    for inicio, fim in periodo_carnaval:
        filtro = (df['data_inversa'] >= inicio) & (df['data_inversa'] <= fim)

        df.loc[filtro, 'carnaval'] = 1

    return df