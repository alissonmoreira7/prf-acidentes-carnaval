from dotenv import load_dotenv
from sqlalchemy import create_engine
load_dotenv()

class ConectorBanco():
    def __init__(self, prf_db_url):
        self.prf_db_url = prf_db_url
        
    def conectar_banco(self):
        """
        Cria o motor de conexão com o banco de dados usando SQLAlchemy
        e realiza um teste de ping para garantir que o banco está online.
        """
        try:
            # Cria o motor de conexão
            engine = create_engine(self.prf_db_url)

            # Testa a conexão
            with engine.connect():
                print("Conexão com o MySQL testada e efetuada com sucesso!")

            return engine
        
        except Exception as e:
            raise ConnectionError(f"Falha crítica ao conectar no banco de dados: {e}")