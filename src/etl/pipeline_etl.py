import glob
import pandas as pd

class PipelineETl:
    def __init__(self, file_path):
        self.file_path = file_path
        self.df = None

    def extrair_dados(self):
        """
        Busca arquivos CSV no diretório especificado e os consolida no estado interno da classe.
        """
        arquivos = glob.glob(self.file_path)

        if not arquivos:
            raise FileNotFoundError(f"Nenhum arquivo encontrado no caminho: {self.file_path}")
        
        print(f'Arquivos encontrados: {len(arquivos)}')

        # Unifica os arquivos
        lista_df = [pd.read_csv(f, sep=';', encoding='latin-1') for f in arquivos]
        
        self.df = pd.concat(lista_df, ignore_index=True)

        print(f'Extração concluida! O número total de registros é: {len(self.df)}')

    def transformar_dados(self):
        """
        Realiza a higienização do DataFrame: padronização de datas, extração de horários 
        e garantia de integridade da chave primária (ID).
        """
        if not self.df:
            raise ValueError("O DataFrame está vazio. Execute 'extrair_dados' primeiro.")


        #Conversão de datas 
        data_br = pd.to_datetime(self.df['data_inversa'], dayfirst=True, errors='coerce')
        data_iso = pd.to_datetime(self.df['data_inversa'], dayfirst=False, errors='coerce')
        self.df['data_inversa'] = data_br.fillna(data_iso)

        self.df['horario'] = pd.to_timedelta(self.df['horario'], errors='coerce')

        self.df['horario'] = self.df['horario'].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')[0]
        self.df['km'] = self.df['km'].str.replace(',', '.').astype(float)

        self.df = self.df.dropna(subset=['id'])
        self.df['id'] = self.df['id'].astype(int)

    def adicionar_colunas(self):
        """
        Adiciona a coluna para saber se é carnaval ou não
        """
        if self.df is None:
            raise ValueError("O DataFrame está vazio. Execute 'extrair_dados' primeiro.")


        #Periodo de carnaval nos respectivos anos: 2023, 2024 e 2025
        periodo_carnaval = [
            ('2023-02-17', '2023-02-22'),
            ('2024-02-09', '2024-02-14'),
            ('2025-02-28', '2025-03-05')
        ]

        self.df['carnaval'] = 0

        for inicio, fim in periodo_carnaval:
            filtro = (self.df['data_inversa'] >= inicio) & (self.df['data_inversa'] <= fim)

            self.df.loc[filtro, 'carnaval'] = 1

    def carregar_dados(self, engine):
        """
        Envio do DataFrame para o banco de dados relacional 
        """
        try:
            self.df.to_sql(
                name = 'acidentes_carnaval',
                con = engine,
                if_exists = 'replace',
                index = False,
                chunksize = 5000,
            )
            print("Envio para o banco bem sucedido!")

        except Exception as e:
            raise RuntimeError(f"Erro ao carregar os dados para o banco relacionnal: {e}")