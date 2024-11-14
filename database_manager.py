import os
from dotenv import load_dotenv
from pathlib import Path
from sqlalchemy import create_engine

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
        """Carrega configurações do banco de dados especificado"""
        if db_type not in self.supported_dbs:
            raise ValueError(f"Tipo de banco de dados não suportado. Opções válidas: {list(self.supported_dbs.keys())}")
            
        db_config = self.supported_dbs[db_type]
        env_path = Path('.') / db_config['env_file']
        
        print(f"Tentando carregar configurações de: {env_path.absolute()}")
        
        # Adicione uma verificação se o arquivo existe
        if not env_path.exists():
            print(f"Arquivo de configuração {env_path} não encontrado.")
            raise FileNotFoundError(f"Arquivo de configuração {env_path} não encontrado.")
        
        # Tentar carregar o arquivo .env com verbose
        load_dotenv(dotenv_path=env_path, verbose=True)

        config = {
            'user': os.getenv('DB_USER'),
            'password': os.getenv('DB_PASSWORD'),
            'host': os.getenv('DB_HOST'),
            'port': os.getenv('DB_PORT'),
            'database': os.getenv('DB_NAME')
        }
        
        print(f"Configurações carregadas: DB_USER={config['user']}, DB_HOST={config['host']}, DB_NAME={config['database']}")
        
        return config, db_config['connection_string']
    
    def get_db_connection(self, db_type: str):
        """Cria e retorna uma conexão com o banco de dados especificado"""
        try:
            config, connection_string = self.load_db_config(db_type)
            
            if db_type == 'sqlite':
                self.engine = create_engine(connection_string.format(database=config['database']))
            else:
                self.engine = create_engine(connection_string.format(**config))
            
            self.current_db_type = db_type
            return self.engine
            
        except Exception as e:
            print(f"Erro ao conectar ao banco de dados {db_type}: {e}")
            raise

