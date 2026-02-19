from etl.extrator import extrair_dados
from etl.tratador import transformar_dados, adicionar_colunas
from databases.conector import conectar_banco

def main():
    print('--- Iniciando Pipeline de Dados de Acidentes em Rodóvias Federais ---')

    #Extraindo os dados brutos e unificando em um unico DataFrame
    df_bruto = extrair_dados()

    #Limpando o DataFrame
    df_limpo = transformar_dados(df_bruto)

    #Criando coluna para filtrar quais os registros que estão no período de carnaval
    df_final = adicionar_colunas(df_limpo)

    #Cria o motor de conexão para o banco de dados
    engine_conec = conectar_banco()

    

    print('--- Pipeline finalizado com sucesso! ---')

if __name__ == '__main__':
    main()