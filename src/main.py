from etl.extrator import extrair_dados
from etl.tratador import transformar_dados

def main():
    print('--- Iniciando Pipeline de Dados de Acidentes em Rodóvias Federais ---')

    #Extraindo os dados brutos e unificando em um unico DataFrame
    df_bruto = extrair_dados()

    df_limpo = transformar_dados(df_bruto)

    print('--- Pipeline finalizado com sucesso! ---')

if __name__ == '__main__':
    main()