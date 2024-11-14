from flask import Flask, render_template, request, jsonify, send_file
import pandas as pd
import os
import xml.etree.ElementTree as ElementTree
from io import StringIO, BytesIO
import chardet
import zipfile
from database_manager import DatabaseConnectionManager

app = Flask(__name__, template_folder="templates")

# Instância global do gerenciador de banco de dados
db_manager = DatabaseConnectionManager()

@app.route('/set_database', methods=['POST'])
def set_database():
    """Rota para configurar o tipo de banco de dados a ser usado"""
    try:
        data = request.get_json()
        if not data or 'db_type' not in data:
            return jsonify({
                "error": "Tipo de banco de dados não especificado no corpo da requisição"
            }), 400
            
        db_type = data['db_type']
        print(f"Tentando configurar conexão para banco de dados: {db_type}")
        
        db_manager.get_db_connection(db_type)
        
        return jsonify({
            "message": f"Conexão estabelecida com sucesso para {db_type}",
            "db_type": db_type
        })
        
    except ValueError as e:
        print(f"Erro de validação: {str(e)}")
        return jsonify({
            "error": str(e),
            "supported_dbs": list(db_manager.supported_dbs.keys())
        }), 400
        
    except Exception as e:
        print(f"Erro ao configurar banco de dados: {str(e)}")
        return jsonify({
            "error": f"Erro ao configurar conexão com o banco de dados: {str(e)}"
        }), 500

@app.route('/load_from_db', methods=['POST'])
def load_from_db():
    """Rota para carregar dados de uma tabela específica"""
    try:
        if not db_manager.engine:
            return jsonify({
                "error": "Conexão com banco de dados não configurada. Use /set_database primeiro."
            }), 400
            
        table_name = request.args.get("table")
        if not table_name:
            return jsonify({
                "error": "Nome da tabela não fornecido na URL"
            }), 400

        print(f"Tentando carregar dados da tabela: {table_name}")
        print(f"Usando banco de dados: {db_manager.current_db_type}")
        
        df = pd.read_sql_table(table_name, con=db_manager.engine)
        df = df.map(lambda x: None if isinstance(x, float) and np.isnan(x) else x)
        
        for col in df.select_dtypes(include=['datetime64']):
            df[col] = df[col].astype(str)
        
        data = df.to_dict(orient="records")
        
        print(f"Dados carregados com sucesso: {len(data)} registros encontrados")
        return jsonify({
            "message": "Dados carregados com sucesso",
            "data": data,
            "row_count": len(data)
        })
    
    except Exception as e:
        print(f"Erro ao carregar dados do banco de dados: {str(e)}")
        return jsonify({
            "error": f"Erro ao carregar dados do banco de dados: {str(e)}"
        }), 500

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
            import xml.etree.ElementTree as ET
            tree = ET.parse(file)
            root = tree.getroot()
            data = [{child.tag: child.text for child in elem} for elem in root]
            df = pd.DataFrame(data)
            print("Arquivo XML lido com sucesso.")

        elif file.filename.endswith('.csv'):
            print("Tentando ler o arquivo CSV...")
            raw_data = file.read()
            result = chardet.detect(raw_data)
            encoding = result['encoding']
            print(f'Codificação detectada: {encoding}')

            content = raw_data.decode(encoding, errors='replace')
            df = pd.read_csv(StringIO(content), sep=None, on_bad_lines='skip', quotechar='"', skipinitialspace=True)
            df = df.where(pd.notnull(df), 'N/A')
            print(df.head())

            data = df.to_dict(orient='records')
            print("Arquivo CSV lido com sucesso.")

        else:
            print("Tipo de arquivo não suportado.")
            return jsonify({"error": "File type not supported"}), 400

        # Processa os dados
        for item in data:
            for key, value in item.items():
                if isinstance(value, (int, float)):
                    item[key] = str(value)
                elif pd.isna(value):
                    item[key] = "N/A"

        return jsonify(data)

    except Exception as e:
        print(f"Erro ao processar o arquivo: {e}")
        return jsonify({"error": f"Failed to process the file: {str(e)}"}), 500
    
@app.route('/download_csv', methods=['POST'])
def download_csv():
    global df
    file = request.files.get('file')
    if not file:
        return jsonify({"error": "Nenhum arquivo foi enviado"}), 400

    try:
        df = pd.read_csv(
            file.stream,
            encoding='latin1',
            delimiter='|',
            skipinitialspace=True,
            on_bad_lines='skip'
        )
        df.columns = ['Column1', 'Column2', 'Column3', 'Column4', 'Column5', 'Column6', 'Column7', 'Column8']
        df = df.dropna(axis=1, how='all')

        output = BytesIO()
        df.to_csv(output, index=False, encoding='utf-8')
        output.seek(0)

        return send_file(output, mimetype='text/csv', as_attachment=True, download_name='arquivo_ajustado.csv')

    except Exception as e:
        return jsonify({"error": str(e)}), 500


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

@app.route('/apply_formula', methods=['POST'])
def apply_formula():
    # global df
    data = request.json
    df = pd.DataFrame(data['data'])
    column1 = data.get('coluna1')
    column2 = data.get('coluna2')
    operation = data.get('operador')
    new_column_name = data.get('new_column')
    if not column1 or not column2 or not operation:
        return jsonify({"error": "Parâmetros incompletos"}), 400
    
    if not new_column_name:
        new_column_name = f"{column1}_{operation}_{column2}"
    
    print(new_column_name)
    # Verificação se as colunas e operação são válidas
    

    if column1 not in df.columns or column2 not in df.columns:
        return jsonify({"error": "Colunas inválidas"}), 400

    try:
        # Converte as colunas para tipo numérico, substituindo erros por NaN
        df[column1] = pd.to_numeric(df[column1], errors='coerce')
        df[column2] = pd.to_numeric(df[column2], errors='coerce')

        # Realizando a operação solicitada
        if operation == '+':
            df[new_column_name] = df[column1].fillna(0) + df[column2].fillna(0)
            df[new_column_name] = df[new_column_name].where(df[column1].notna() | df[column2].notna(), None)
        elif operation == '-':
            df[new_column_name] = df[column1] - df[column2]
        elif operation == '*' or operation == 'x':
            df[new_column_name] = df[column1] * df[column2]
        elif operation == '/':
            # Tratamento para evitar divisão por zero
            df[new_column_name] = df[column1] / df[column2].replace(0, None)
            df[new_column_name] = round(df[new_column_name], 4)
        else:
            return jsonify({"error": "Operação inválida"}), 400
        
        print(df.head())
        data = df.fillna("null").to_dict(orient='records')

        return jsonify(data)
    except Exception as e:
        # Exibe o erro detalhado no console do Flask
        print("Erro ao adicionar nova coluna:", str(e))
        return jsonify({"error": str(e)}), 500

@app.route('/transpor', methods=['POST'])
def transpor():
    try:
        # Recebe os dados da tabela como JSON
        data = request.get_json()
        
        # Verifica se os dados foram recebidos corretamente
        if 'data' not in data:
            return jsonify({"error": "Dados não fornecidos."}), 400
        
        # Cria um DataFrame a partir dos dados recebidos
        df = pd.DataFrame(data['data'])
        
        columns_as_first_row = pd.DataFrame([df.columns.tolist()], columns=df.columns)
        
        # Adiciona os nomes das colunas como a primeira linha
        df = pd.concat([columns_as_first_row, df], ignore_index=True)
        
        # Transpõe o DataFrame
        df_transposto = df.T
        
        # Converte o DataFrame transposto de volta para uma lista de dicionários
        result = df_transposto.to_dict(orient='records')
        
        # Retorna os dados transpostos como JSON
        return jsonify(result)

    except Exception as e:
        return jsonify({"error": str(e)}), 500

@app.route('/rename_column', methods=['POST'])
def rename_column():
    data = request.json
    current_column = data.get('currentColumn')
    new_column_name = data.get('newColumnName')

    if not current_column or not new_column_name:
        return jsonify({"error": "Nome atual e novo nome são necessários."}), 400

    # Verifique se a coluna atual existe no DataFrame
    if current_column not in df.columns:
        return jsonify({"error": f"A coluna '{current_column}' não existe."}), 400

    # Renomeia a coluna
    df.rename(columns={current_column: new_column_name}, inplace=True)

    # Retorne o DataFrame atualizado
    data = df.to_dict(orient="records")
    return jsonify(data)


if __name__ == '__main__':
    app.run(debug=True)
    
    
