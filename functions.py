import aiohttp
import pandas as pd
import json
from mysql.connector import pooling
import pyodbc
from dotenv import load_dotenv
import os

load_dotenv()


BASE_URL = os.getenv('BASE_URL')
USER = os.getenv('USER')
PASSWORD = os.getenv('PASSWORD')
TENANT = os.getenv('TENANT')
HOST = os.getenv('HOST')
PORT = os.getenv('PORT')
DATABASE = os.getenv('DATABASE')
USER_BD = os.getenv('USER_BD')
PASSWORD_BD = os.getenv('PASSWORD_BD')
DRIVER = os.getenv('DRIVER')

async def login(base_url, user, password, tenant):
    login_url = f'{base_url}/users/login'
    data = {
        'user': user,
        'password': password,
        'tenant': tenant
    }
    
    headers = {
        'Content-Type': 'application/json'
    }
    
    async with aiohttp.ClientSession() as session:
        try:
            async with session.post(login_url, headers=headers, json=data) as response:
                if response.status == 200:
                    try:
                        response_text = await response.text()
                        response_json = json.loads(response_text)
                        session_id = response_json['data']['Session']['id']
                        return session_id
                    except json.JSONDecodeError:
                        response_text = await response.text()
                        print(f'Resposta não é JSON. Conteúdo: {response_text}')
                        return None
                else:
                    print(f'Erro de login: {response.status}')
                    print(f'Mensagem de erro: {await response.text()}')
                    return None
        except aiohttp.ClientError as e:
            print(f'Erro ao fazer requisição: {e}')
            return None
        except Exception as e:
            print(f'Erro inesperado: {e}')
            return None
        
def get_sql_server_connection():
    connection_string = (
        f"DRIVER={{{DRIVER}}};"
        f"SERVER={HOST},{PORT};"
        f"DATABASE={DATABASE};"
        f"UID={USER_BD};"
        f"PWD={PASSWORD_BD};"
        "Encrypt=yes;"
        "TrustServerCertificate=yes;"
    )
    return pyodbc.connect(connection_string)

async def fetch_data(session, url, params):
    try:
        async with session.get(url, params=params) as response:
            if response.status == 200:
                try:
                    response_text = await response.text()
                    response_json = json.loads(response_text)
                    return response_json['data'] if 'data' in response_json else []
                except json.JSONDecodeError:
                    response_text = await response.text()
                    print(f'Erro ao fazer parse do JSON: {response_text}')
                    return []
            else:
                print(f'Erro na requisição: {response.status}')
                print(f'Mensagem de erro: {await response.text()}')
                return []
    except Exception as e:
        print(f'Exceção ao fazer requisição: {e}')
        return []
   
def truncate_table(table):
    try:
        cnx = get_sql_server_connection()
        if cnx is None:
            return 0

        cursor = cnx.cursor()
        query = f"TRUNCATE TABLE {table}"
        cursor.execute(query)
        cnx.commit()
        cursor.close()
        cnx.close()
        return 0
    except pyodbc.Error as err:
        print(f"Erro ao conectar ao SQL Server: {err}")
        return 0
    
async def insert_data(data_insert, table):
    try:
        cnx = get_sql_server_connection()
        if cnx is None:
            return

        cursor = cnx.cursor()
        columns = ', '.join(data_insert[0].keys())
        placeholders = ', '.join(['?'] * len(data_insert[0]))
        insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        data_batch = [tuple(item.values()) for item in data_insert]

        if data_batch:
            cursor.executemany(insert_query, data_batch)
            cnx.commit()
            # print(f'{len(data_batch)} registros inseridos com sucesso')
        else:
            print("Nenhum registro novo para inserir")
    except pyodbc.Error as err:
        print(f"Erro ao conectar ao SQL Server: {err}")
    finally:
        cursor.close()
        cnx.close()
        
async def insert_data_in_batches(data_insert, table, batch_size):
    try:
        cnx = get_sql_server_connection()
        if cnx is None:
            return

        cursor = cnx.cursor()
        columns = ', '.join(data_insert[0].keys())
        placeholders = ', '.join(['?'] * len(data_insert[0]))
        insert_query = f"INSERT INTO {table} ({columns}) VALUES ({placeholders})"
        
        for i in range(0, len(data_insert), batch_size):
            batch = data_insert[i:i+batch_size]
            # print(f"Total de dados para inserir {len(batch)}")
            data_batch = [tuple(item.values()) for item in batch]
            if data_batch:
                cursor.executemany(insert_query, data_batch)
                cnx.commit()
            print(f'{len(data_batch)} registros inseridos com sucesso')

    except pyodbc.Error as err:
        print(f"Erro ao conectar ao SQL Server: {err}")
    finally:
        cursor.close()
        cnx.close()
        
async def delete_records(table, column, ids):
    try:
        cnx = get_sql_server_connection()
        if cnx is None:
            return

        cursor = cnx.cursor()
        ids_placeholder = ', '.join(['?'] * len(ids))
        query = f"DELETE FROM {table} WHERE {column} IN ({ids_placeholder})"
        cursor.execute(query, list(ids))
        cnx.commit()
        cursor.close()
        cnx.close()
        # print(f"Total de registros deletados: {len(ids)}")
    except pyodbc.Error as err:
        print(f"Erro ao conectar ao SQL Server: {err}")
        