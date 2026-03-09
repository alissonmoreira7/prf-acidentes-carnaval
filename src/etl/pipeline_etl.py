import glob
import logging as log
from tqdm import tqdm
import pandas as pd
from abc import abstractmethod

class PipelineETl:
    def __init__(self, file_path, database_engine):
        self.file_path = file_path
        self.engine = database_engine
        self.df = None

    def extrair_dados(self):
        """
        Busca arquivos CSV no diretório especificado e os consolida no estado interno da classe.
        """
        arquivos = glob.glob(self.file_path)

        if not arquivos:
            raise FileNotFoundError(f"Nenhum arquivo encontrado no caminho: {self.file_path}")
        
        log.info(f'Arquivos encontrados: {len(arquivos)}')

        # Unifica os arquivos
        lista_df = [pd.read_csv(f, sep=';', encoding='latin-1') for f in arquivos]
        
        self.df = pd.concat(lista_df, ignore_index=True)

        log.info(f'Extração concluida! O número total de registros é: {len(self.df)}')

    def carregar_dados(self, nome_tabela):
        """
            Envio do DataFrame para o banco de dados relacional 
        """
        try:
            self.df.to_sql(
                name = nome_tabela,
                con = self.engine,
                if_exists = 'replace',
                index = False,
                chunksize = 5000,
            )
            log.info("Envio para o banco bem sucedido!")

        except Exception as e:
            log.critical(f"Erro ao carregar os dados para o banco relacional: {e}")
    
    @abstractmethod 
    def transformar_dados(self):
       pass

    def adicionar_colunas(self):
        pass

    # Acessando o DataFrame bruto
    def acessar_df(self):
        if self.df is None:
            log.critical('O DataFrame está vazio. Execute "extrair_dados" primeiro.')

        return self.df
        
class PipelineETLAcidentes(PipelineETl):
    def transformar_dados(self):
        """
            Realiza a higienização do DataFrame: padronização de datas, extração de horários 
            e garantia de integridade da chave primária (ID) para a classe PipelineETLAcidentes.
        """
        
        if self.df is None:
            raise ValueError('O DataFrame está vazio. Execute "extrair_dados" primeiro.')

        #Conversão de datas 
        data_br = pd.to_datetime(self.df['data_inversa'], dayfirst=True, errors='coerce')
        data_iso = pd.to_datetime(self.df['data_inversa'], dayfirst=False, errors='coerce')
        self.df['data_inversa'] = data_br.fillna(data_iso)

        self.df['horario'] = pd.to_timedelta(self.df['horario'], errors='coerce')
        self.df['horario'] = self.df['horario'].astype(str).str.extract(r'(\d{2}:\d{2}:\d{2})')[0]

        self.df['km'] = self.df['km'].str.replace(',', '.').astype(float)

        self.df = self.df.dropna(subset=['id'])
        self.df['id'] = self.df['id'].astype(int)

        log.info("Tratamento bem sucedido!")

        return super().transformar_dados()
    
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

            log.info("Adição de coluna bem sucedida!")

            return super().adicionar_colunas()

class PipelineETLMultas(PipelineETl):
    def extrair_dados(self):
        arquivos = glob.glob(self.file_path)

        if not arquivos:
            raise FileNotFoundError(f"Nenhum arquivo encontrado no caminho: {self.file_path}")
        
        log.info(f'Arquivos encontrados: {len(arquivos)}')

        periodo_carnaval = [
                ('2023-02-17', '2023-02-22'),
                ('2024-02-09', '2024-02-14'),
                ('2025-02-28', '2025-03-05')
            ]
        
        lista_df_filtrado = []    
        for arquivo in tqdm(arquivos, desc="Processando arquivos"):

            chunks = pd.read_csv(
                arquivo,
                sep=';',
                encoding='latin-1',
                chunksize=100000,
                low_memory=False,
                usecols=[0, 1, 5, 6, 8, 16, 21]
            )

            for chunk in chunks:
                chunk['data_temporaria'] = pd.to_datetime(chunk['Data da Infração (DD/MM/AAAA)'], format='%Y-%m-%d', errors='coerce')

                filtro_geral = pd.Series(False, index=chunk.index)

                for inicio, fim in periodo_carnaval:
                    filtro_ano = (chunk['data_temporaria'] >= inicio) & (chunk['data_temporaria'] <= fim)
                    filtro_geral = filtro_geral | filtro_ano

                chunk_filtrado = chunk[filtro_geral].copy()
                
                chunk_filtrado = chunk_filtrado.drop(columns=['data_temporaria'])

                if not chunk_filtrado.empty:
                    lista_df_filtrado.append(chunk_filtrado)
                    
        if lista_df_filtrado:
            self.df = pd.concat(lista_df_filtrado, ignore_index=True)
        else:
            self.df = pd.DataFrame()

        log.info(f'Extração filtrada de multas concluída! O número total de registros isolados é: {len(self.df)}')