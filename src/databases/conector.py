import pandas as pd
from sqlalchemy import create_engine

class ConectorBanco():
    def __init__(self, db_url):
        self.db_url = db_url
        
    def conectar_banco(self):
        """
        Cria o motor de conexão com o banco de dados usando SQLAlchemy
        e realiza um teste de ping para garantir que o banco está online.
        """
        try:
            # Cria o motor de conexão
            engine = create_engine(self.db_url)

            # Testa a conexão
            with engine.connect():
                print("Conexão com o MySQL testada e efetuada com sucesso!")

            return engine
        
        except Exception as e:
            raise ConnectionError(f"Falha crítica ao conectar no banco de dados: {e}")
        
    def carregar_dataframe_acidentes(self, engine):
        """
            Coleta os dados diretamente do banco.
        """
        query = "SELECT * FROM acidentes_carnaval"

        df = pd.read_sql(query, con = engine)

        return df