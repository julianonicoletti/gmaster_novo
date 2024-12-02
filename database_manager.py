import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine
import pandas as pd
import numpy as np

class DatabaseConnectionManager:
    def __init__(self):
        self.supported_dbs = {
            'postgres': {
                'env_file': 'postgres_config.env',
                'connection_string': "postgresql+psycopg2://{user}:{password}@{host}:{port}/{database}"
            },
            'mysql': {
                'env_file': 'mysql_config.env',
                'connection_string': "mysql+pymysql://{user}:{password}@{host}:{port}/{database}"
            },
            'sqlite': {
                'env_file': 'sqlite_config.env',
                'connection_string': "sqlite:///{database}"
            },
            'mssql': {
                'env_file': 'mssql_config.env',
                'connection_string': "mssql+pyodbc://{user}:{password}@{host}:{port}/{database}?driver=ODBC+Driver+17+for+SQL+Server"
            }
        }
        self.current_db_type = None
        self.engine = None

    def load_db_config(self, db_type: str):
        if db_type not in self.supported_dbs:
            raise ValueError(f"Tipo de banco de dados não suportado. Opções válidas: {list(self.supported_dbs.keys())}")
            
        db_config = self.supported_dbs[db_type]
        env_path = Path(__file__).resolve().parent / db_config['env_file']
        if not env_path.exists():
            raise FileNotFoundError(f"Arquivo de configuração {env_path} não encontrado.")
        
        load_dotenv(dotenv_path=env_path, verbose=True)
        config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME')
        }
        return config, db_config['connection_string']
    
    def configure_connection(self, db_type: str):
        """Configura a conexão com o banco de dados."""
        config, connection_string = self.load_db_config(db_type)
        if db_type == 'sqlite':
            self.engine = create_engine(connection_string.format(database=config['database']))
        else:
            self.engine = create_engine(connection_string.format(**config))
        self.current_db_type = db_type
    
    def load_table_data(self, table_name: str):
        
        """Carrega os dados de uma tabela do banco configurado."""
        if not self.engine:
            raise ValueError("Conexão com o banco de dados não configurada.")
        if not table_name:
            raise ValueError("Nome da tabela não fornecido.")
        
        df = pd.read_sql_table(table_name, con=self.engine)
        print(df.head())
        df = df.map(lambda x: None if isinstance(x, float) and np.isnan(x) else x)
        for col in df.select_dtypes(include=['datetime64']):
            df[col] = df[col].astype(str)
        df = df.applymap(lambda x: list(x) if isinstance(x, set) else x)
        return df.fillna("null").to_dict(orient='records')
