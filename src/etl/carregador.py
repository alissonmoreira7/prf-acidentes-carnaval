from sqlalchemy.types import Time

#Envio do DataFrame para o banco de dados relacional 
def carregar_dados(df, eng):
    try:

        df.to_sql(
            name='acidentes_carnaval',
            con=eng,
            if_exists='replace',
            index=False,
            chunksize=5000,
        )
        print("Envio para o banco bem sucedido!")

    except Exception as e:
        print(f"Erro ao carregar os dados para o banco relacionnal: {e}")