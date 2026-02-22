import os
from etl.pipeline_etl import PipelineETl

DB_CONNECTION_STRING
def main():
    print('--- Iniciando Pipeline de Dados de Acidentes em Rodóvias Federais ---')

    # Centralização da configuração de caminhos aqui
    caminho_arquivos = os.path.join('data', 'raw', '*.csv')

    pipeline = PipelineETl(caminho_arquivos, engine)


    #Extraindo os dados brutos e unificando em um unico DataFrame
    df_bruto = pipeline.extrair_dados()

    #Limpando o DataFrame
    df_limpo = transformar_dados(df_bruto)

    #Criando coluna para filtrar quais os registros que estão no período de carnaval
    df_final = adicionar_colunas(df_limpo)

    #Cria o motor de conexão para o banco de dados
    engine_conec = conectar_banco()

    #Enviar o DataFrame para o Banco de Dados
    carregar_dados(df_final, engine_conec)

    print('--- Pipeline finalizado com sucesso! ---')

if __name__ == '__main__':
    main()