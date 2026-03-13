import os
from dotenv import load_dotenv
from databases.conector import GestaoBanco
from etl.pipeline_etl import PipelineETLAcidentes, PipelineETLMultas
load_dotenv()

def main():
    print('--- Iniciando Pipeline de Dados de Acidentes em Rodóvias Federais ---')
    
    caminho_acidentes = os.getenv('CAMINHO_ACIDENTES')
    caminho_multas = os.getenv('CAMINHO_MULTAS')
    prf_db_url = os.getenv('DB_CONNECTION_STRING')

    conexao = GestaoBanco(prf_db_url)
    engine = conexao.conectar_banco()
    
    pipelineAcidentes = PipelineETLAcidentes(caminho_acidentes, engine)
    pipelineMultasCarnaval = PipelineETLMultas(caminho_multas, engine)
    pipelineMultasNormal = PipelineETLMultas(caminho_multas, engine)

    pipelineAcidentes.extrair_dados()
    pipelineMultasCarnaval.extrair_dados_multas(carnaval=True)
    pipelineMultasNormal.extrair_dados_multas(carnaval=False)

    pipelineAcidentes.transformar_dados()
    pipelineAcidentes.adicionar_colunas()

    # Enviar os DataFrames para o Banco de Dados
    pipelineAcidentes.carregar_dados('acidentes_carnaval')
    pipelineMultasCarnaval.carregar_dados('multas_carnaval')
    pipelineMultasNormal.carregar_dados('multas_normal')    

    print('--- Pipeline finalizado com sucesso! ---')

if __name__ == '__main__':
    main()