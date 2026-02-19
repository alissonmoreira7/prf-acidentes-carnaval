import os
from dotenv import load_dotenv
from sqlalchemy import create_engine

load_dotenv()
def conectar_banco():
    acid_conec_db = os.getenv('DB_CONNECTION_STRING')

    try:
        # Cria o motor de conexão
        engine = create_engine(acid_conec_db)
        print("Conexão com o MySQL efetuada!")
        return engine
    
    except Exception as e:
        print(f"Erro ao preparar a conexão: {e}")
        return None