import os
import glob
import pandas as pd

def extrair_dados():
    caminho_arquivos = os.path.join('data', 'raw', '*.csv')
    arquivos = glob.glob(caminho_arquivos)

    print(f'Arquivos encontrados: {len(arquivos)}')

    lista_df = [pd.read_csv(f, sep=';', encoding='latin-1') for f in arquivos]
    df_geral = pd.concat(lista_df, ignore_index=True)

    print(f'Extração concluida! O número total de registros é: {len(df_geral)}')

    return df_geral