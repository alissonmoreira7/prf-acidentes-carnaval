class AnaliseEstatistica:
    """
    Calcula a média de acidentes por dia.
    Parâmetro: carnaval (bool) - Filtra os dados de acordo com a flag.
    """
    def __init__(self, df):
        self.df =  df

    def calcular_media_diaria(self, eh_carnaval=1):
        """
            Calcula a média de acidentes por dia.
            Parâmetro: carnaval (bool) - Filtra os dados de acordo com a flag.
        """
        # Filtra o dataframe com base no parâmetro
        df_filtrado = self.df[self.df['carnaval'] == eh_carnaval]
        
        total_acidentes = len(df_filtrado)
        total_dias = df_filtrado['data_inversa'].nunique()

        return total_acidentes/total_dias
    
    def calcular_variacao_percentual(self, media_carnaval, media_normal):
        """
         Calcula a variação percentual entre o período de Carnaval e dias normais.
        """
        variacao_percentual = ((media_carnaval-media_normal)/media_normal)*100

        return variacao_percentual

