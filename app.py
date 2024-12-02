from flask import Flask, render_template, request, jsonify, send_file, session
from flask_session import Session
import pandas as pd
import redis
import os
import numpy as np
import xml.etree.ElementTree as ET
from io import StringIO, BytesIO
import chardet
import re
import zipfile
from database_manager import DatabaseConnectionManager



app = Flask(__name__, template_folder="templates")
app.secret_key = os.urandom(24)

# app.config['SESSION_TYPE'] = 'redis'
# app.config['SESSION_PERMANENT'] = False
# app.config['SESSION_USE_SIGNER'] = True  # Para assinar cookies
# app.config['SESSION_REDIS'] = redis.StrictRedis(host='localhost', port=6379, db=0)

# Session(app)

# Instância global do gerenciador de banco de dados
db_manager = DatabaseConnectionManager()

# @app.route('/check_session', methods=['GET'])
# def check_session():
#     # Obtém o histórico diretamente da sessão
#     history = session.get('history', [])
    
#     # Verifique se o histórico foi inicializado
#     if history:
#         print("Histórico encontrado:", history)
#     else:
#         print("Nenhum histórico encontrado na sessão.")
    
#     # Retorna o histórico como JSON
#     return jsonify(history)


# @app.before_request
# def ensure_history_initialized():
#     print("Verificando a inicialização do histórico...")
#     if 'history' not in session:
#         print("Inicializando histórico na sessão.")
#         session['history'] = []  # Inicializa o histórico
#     else:
#         print(f"Histórico existente na sessão: {session['history']}")

# def initialize_history():
#     if 'history' not in session:
#         print("Iniciando sessão de histórico")
#         session['history'] = []
#     else:
#         print("Sessão de histórico ja iniciada")

# def save_state(df, operation):
#     # Verifique se o 'history' existe na sessão
#     if 'history' not in session:
#         print("Inicializando 'history' na sessão...")
#         session['history'] = []  # Iniciar o histórico, se não existir
#     print(f'Salvando operação no histórico: {operation}')
#     # Salvar o estado
#     session['history'].append({
#         'data': df.to_dict(orient='records'),
#         'operation': operation
#     })
#     session.modified = True  # Garante que a sessão seja marcada como modificada

# @app.route('/get_history', methods=['GET'])
# def get_history():
#     return jsonify(session.get('history', []))

# @app.route('/undo', methods=['POST'])
# def undo():
#     history = session.get('history', [])
#     if len(history) > 1:
#         history.pop()  # Remove a última operação
#         session['history'] = history
#         last_state = history[-1]['data']  # Obtém o estado anterior
#         global df
#         df = pd.DataFrame(last_state)  # Restaura o DataFrame
#         return jsonify(last_state)
#     return jsonify({"error": "Nenhuma operação para desfazer"}), 400
    

@app.route('/database', methods=['POST'])
def handle_database_request():
    try:
        data = request.json
        if not data:
            return jsonify({"error": "Dados não fornecidos na requisição."}), 400
        
        action = data.get("action")
        if action not in ["set_database", "load_table"]:
            return jsonify({"error": "Ação inválida. Use 'set_database' ou 'load_table'."}), 400
        
        if action == "set_database":
            db_type = data.get("db_type")
            if not db_type:
                return jsonify({"error": "Tipo de banco de dados não especificado."}), 400
            db_manager.configure_connection(db_type)
            return jsonify({
                "message": f"Conexão configurada com sucesso para {db_type}",
                "db_type": db_type
            })
        
        elif action == "load_table":
            table_name = data.get("table_name")
            if not table_name:
                return jsonify({"error": "Nome da tabela não fornecido."}), 400
            data = db_manager.load_table_data(table_name)
            return jsonify({
                "message": f"Dados carregados com sucesso da tabela '{table_name}'",
                "data": data,
                "row_count": len(data)
            })
    
    except ValueError as e:
        return jsonify({"error": str(e)}), 400
    except Exception as e:
        return jsonify({"error": f"Erro inesperado: {str(e)}"}), 500

@app.route('/')
def index():
    return render_template('index.html')

@app.route('/upload', methods=['POST'])
def upload_file():
    global df
    if 'file' not in request.files:
        return jsonify({"error": "No file uploaded"}), 400

    file = request.files['file']

    try:
        # Verifica se é um arquivo .zip
        if file.filename.endswith('.zip'):
            print("Tentando processar um arquivo ZIP...")
            with zipfile.ZipFile(BytesIO(file.read()), 'r') as zip_ref:
                zip_ref.extractall('extracted_files')
                print("Arquivos extraídos com sucesso.")

            # Processar arquivos dentro do .zip
            extracted_files = os.listdir('extracted_files')
            data = []
            for extracted_file in extracted_files:
                file_path = os.path.join('extracted_files', extracted_file)
                if extracted_file.endswith('.xlsx'):
                    print(f"Tentando ler o arquivo XLSX dentro do ZIP: {extracted_file}...")
                    df = pd.read_excel(file_path)
                    for col in df.select_dtypes(include=['datetime']):
                        df[col] = df[col].astype(str)
                    data.extend(df.to_dict(orient='records'))
                    print(f"Arquivo XLSX {extracted_file} lido com sucesso.")

                elif extracted_file.endswith('.json'):
                    print(f"Tentando ler o arquivo JSON dentro do ZIP: {extracted_file}...")
                    try:
                        with open(file_path, 'r') as f:
                            file_data = json.load(f)
                            if isinstance(file_data, list) and all(isinstance(item, dict) for item in file_data):
                                data.extend(file_data)
                                print(f"Arquivo JSON {extracted_file} lido com sucesso.")
                            else:
                                print(f"Estrutura de JSON inesperada no arquivo {extracted_file}. Esperado: lista de dicionários.")
                                return jsonify({"error": f"Formato de JSON inválido no arquivo {extracted_file}. Esperado uma lista de objetos."}), 400
                    except json.JSONDecodeError as e:
                        print(f"Erro de decodificação JSON no arquivo {extracted_file}: {e}")
                        return jsonify({"error": f"Erro ao decodificar o arquivo JSON {extracted_file}."}), 400

                elif extracted_file.endswith('.xml'):
                    print(f"Tentando ler o arquivo XML dentro do ZIP: {extracted_file}...")
                    import xml.etree.ElementTree as ET
                    tree = ET.parse(file_path)
                    root = tree.getroot()
                    xml_data = [{child.tag: child.text for child in elem} for elem in root]
                    data.extend(xml_data)
                    print(f"Arquivo XML {extracted_file} lido com sucesso.")

                elif extracted_file.endswith('.csv'):
                    print(f"Tentando ler o arquivo CSV dentro do ZIP: {extracted_file}...")
                    with open(file_path, 'rb') as f:
                        raw_data = f.read()
                        result = chardet.detect(raw_data)
                        encoding = result['encoding']
                        print(f'Codificação detectada para {extracted_file}: {encoding}')

                        content = raw_data.decode(encoding, errors='replace')
                        df = pd.read_csv(StringIO(content), sep=None, on_bad_lines='skip', quotechar='"', skipinitialspace=True)
                        print(df.head())
                        df = df.where(pd.notnull(df), 'N/A')
                        data.extend(df.to_dict(orient='records'))
                        print(f"Arquivo CSV {extracted_file} lido com sucesso.")

                else:
                    print(f"Tipo de arquivo {extracted_file} não suportado dentro do ZIP.")
                    return jsonify({"error": f"Tipo de arquivo {extracted_file} não suportado dentro do ZIP."}), 400

            # Limpa a pasta temporária de extração
            for extracted_file in extracted_files:
                os.remove(os.path.join('extracted_files', extracted_file))
            os.rmdir('extracted_files')

        elif file.filename.endswith('.xlsx'):
            print("Tentando ler o arquivo XLSX...")
            df = pd.read_excel(BytesIO(file.read()))
            for col in df.select_dtypes(include=['datetime']):
                df[col] = df[col].astype(str)
            data = df.to_dict(orient='records')
            print("Arquivo XLSX lido com sucesso.")

        elif file.filename.endswith('.json'):
            print("Tentando ler o arquivo JSON...")
            try:
                data = json.load(file)
                if isinstance(data, list) and all(isinstance(item, dict) for item in data):
                    print("Arquivo JSON lido com sucesso.")
                    df = pd.DataFrame(data)
                else:
                    print("Estrutura de JSON inesperada. Esperado: lista de dicionários.")
                    return jsonify({"error": "Formato de JSON inválido. Esperado uma lista de objetos."}), 400
            except json.JSONDecodeError as e:
                print(f"Erro de decodificação JSON: {e}")
                return jsonify({"error": "Erro ao decodificar o arquivo JSON."}), 400

        elif file.filename.endswith('.xml'):
            print("Tentando ler o arquivo XML...")
            try:
                # Lê o XML diretamente como DataFrame
                df = pd.read_xml(BytesIO(file.read()))

                # Exibe as primeiras linhas
                print("Primeiras linhas do DataFrame:")
                print(df.head())
                
                # Preenche valores nulos e converte para dicionário
                data = df.fillna("null").to_dict(orient='records')
                print("Arquivo XML lido com sucesso.")
            except ValueError as e:
                print(f"Erro ao processar o arquivo XML com Pandas: {e}")
                data = []

        elif file.filename.endswith('.csv'):
            print("Tentando ler o arquivo CSV...")
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            print(f'Codificação detectada: {encoding}')

            content = raw_data.decode(encoding, errors='replace')
            df = pd.read_csv(StringIO(content), sep=None, on_bad_lines='skip', quotechar='"', skipinitialspace=True)
            
            print(df.head())
            # initialize_history()
            # save_state(df, "Arquivo carregado")

            data = df.fillna("null").to_dict(orient='records')
            print("Arquivo CSV lido com sucesso.")
            
        elif file.filename.endswith('.txt'):
            try:
                print("Tentando ler o arquivo TXT...")
                # Lê o arquivo como um DataFrame, ignorando a coluna inicial e final (delimitadores vazios)
                df = pd.read_csv(BytesIO(file.read()), sep='|', header=None, engine='python')
                
                # Remove as colunas vazias (delimitadores extras no início e fim)
                df = df.iloc[:, 1:-1]

                # Exibe as primeiras linhas para depuração
                print("Primeiras linhas do DataFrame:")
                print(df.head())
                data = df.fillna("null").to_dict(orient='records')
                print("Arquivo XML lido com sucesso.")
                
                
            except Exception as e:
                print(f"Erro ao processar o arquivo TXT: {e}")
                return None

        else:
            print("Tipo de arquivo não suportado.")
            return jsonify({"error": "File type not supported"}), 400
        
        return jsonify(data)

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return jsonify({"error": f"Failed to process the file: {str(e)}"}), 500
    
@app.route('/clean_data', methods=['POST'])
def clean_data():
    global df
    try:
        data = request.get_json().get("data", [])
        if not data:
            return jsonify({"error": "Nenhum dado para limpar."}), 400

        cleaned_data = [row for row in data if all(value.strip() for value in row.values())]

        return jsonify(cleaned_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/calcular_nova_coluna', methods=['POST'])
def calcular_nova_coluna():
    global df   
    data = request.json
    print(data.get('formula'))
    print(data.get('new_column'))
    df= pd.DataFrame(data['data'])
    formula = data.get('formula')
    new_column_name = data.get('new_column')
    print(formula)

    if not formula:
        return jsonify({"error": "Fórmula não fornecida"}), 400
    if not new_column_name:
        new_column_name = f"{formula} (Nova)"

    # Substituir os nomes das colunas por df['coluna']
    for col in df.columns:
        if col in formula:
            # Converter a coluna para tipo numérico, substituindo erros por NaN
            df[col] = pd.to_numeric(df[col], errors='coerce')
            formula = formula.replace(col, f"df['{col}']")
    
    try:
        # Avaliar a fórmula
        df[new_column_name] = eval(formula)

        # Converter NaN para "null" para compatibilidade JSON
        data = df.fillna("null").to_dict(orient='records')
        return jsonify(data)
    except Exception as e:
        print("Erro ao aplicar fórmula:", str(e))
        return jsonify({"error": str(e)}), 500
    

@app.route('/transpor', methods=['POST'])
def transpor():
    global df
    try:
        data = request.get_json()
        
        if 'data' not in data:
            return jsonify({"error": "Dados não fornecidos."}), 400
        
        # Cria um DataFrame a partir dos dados recebidos
        df = pd.DataFrame(data['data'])
        
        columns_as_first_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
        
        # Adiciona os nomes das colunas como a primeira linha
        df = pd.concat([columns_as_first_row, df], ignore_index=True)
        
        df_transposto = df.T
        result = df_transposto.to_dict(orient='records')
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/rename_column', methods=['POST'])
def rename_column():
    global df
    data = request.json
    current_column = data.get('currentColumn')
    new_column_name = data.get('newColumnName')
    df = pd.DataFrame(data['rawData'])

    if not current_column or not new_column_name:
        return jsonify({"error": "Nome atual e novo nome são necessários."}), 400

    # Verifique se a coluna atual existe no DataFrame
    if current_column not in df.columns:
        return jsonify({"error": f"A coluna '{current_column}' não existe."}), 400

    # Renomeia a coluna
    df.rename(columns={current_column: new_column_name}, inplace=True)
    # save_state(df, f"Renomeou coluna '{current_column}' para '{new_column_name}'")
    print(df.head())
    # Retorne o DataFrame atualizado
    data = df.fillna("null").to_dict(orient='records')
    return jsonify(data)

@app.route('/replace_value', methods=['POST'])
def replace_value():
    global df
    data = request.json
    column = data.get('column')
    old_value = data.get('oldValue')
    new_value = data.get('newValue')
    df = pd.DataFrame(data['data'])
    print(column, old_value, new_value)

    # Verifica se os parâmetros estão presentes
    if not column or old_value is None or new_value is None:
        return jsonify({"error": "Parâmetros incompletos"}), 400

    try:
        # Converte os valores para numéricos (inteiros ou flutuantes)
        old_value = float(old_value)
        new_value = float(new_value)

        # Converte a coluna para tipo numérico antes de substituir os valores
        df[column] = pd.to_numeric(df[column], errors='coerce')

        # Substitui o valor antigo pelo novo
        df[column] = df[column].replace(old_value, new_value)

        print(df.head())
        
        # Preenche valores NaN com "null" para compatibilidade JSON
        updated_data = df.fillna("null").to_dict(orient='records')
        return jsonify(updated_data)
    except Exception as e:
        return jsonify({"error": str(e)}), 500



if __name__ == '__main__':
    app.run(debug=True)
    
    
