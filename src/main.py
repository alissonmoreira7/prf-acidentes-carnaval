import os
from dotenv import load_dotenv
from etl.pipeline_etl import PipelineETl
from databases.conector import ConectorBanco
load_dotenv()

def main():
    print('--- Iniciando Pipeline de Dados de Acidentes em Rodóvias Federais ---')

    # Centralização da configuração de caminhos aqui
    caminho_arquivos = os.path.join('data', 'raw', '*.csv')

    prf_db_url = os.getenv('DB_CONNECTION_STRING')
    pipeline = PipelineETl(caminho_arquivos)

    #Cria o motor de conexão para o banco de dados
    conexao = ConectorBanco(prf_db_url)
    engine =  conexao.conectar_banco()

    pipeline.extrair_dados()

    pipeline.transformar_dados()

    pipeline.adicionar_colunas()

    #Enviar o DataFrame para o Banco de Dados
    pipeline.carregar_dados(engine)

    print('--- Pipeline finalizado com sucesso! ---')

if __name__ == '__main__':
    main()