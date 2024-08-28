import mariadb
import pandas as pd
import pandas_gbq
import numpy as np
import sys
from google.oauth2 import service_account

# Funções para conexão ao Banco de Dados

def conectar_banco(host, port, user, password, database):
    """
    Conecta ao banco de dados MariaDB usando as credenciais fornecidas.

    :param host: Endereço do host do banco de dados.
    :param port: Porta de conexão do banco de dados.
    :param user: Nome de usuário para autenticação.
    :param password: Senha para autenticação.
    :param database: Nome do banco de dados.
    :return: Objeto de conexão MariaDB ou None em caso de erro.
    """
    try:
        conn = mariadb.connect(
               user=user,
               password=password,
               host=host,
               port=port,
               database=database
        )
        return conn
    except mariadb.Error as e:
        print(f"Error connecting to MariaDB Platform: {e}")
        return None


def obter_cursor(conn):
    """
    Obtém um cursor a partir da conexão ao banco de dados.

    :param conn: Objeto de conexão MariaDB.
    :return: Cursor MariaDB ou None em caso de erro.
    """
    if conn is not None:
        try:
            return conn.cursor()
        except mariadb.Error as e:
            print(f"Error creating cursor: {e}")
    return None


def fechar_conexao(conn, cur):
    """
    Fecha a conexão e o cursor do banco de dados.

    :param conn: Objeto de conexão MariaDB.
    :param cur: Cursor MariaDB.
    """
    try:
        if cur:
            cur.close()
        if conn:
            conn.close()
    except mariadb.Error as e:
        print(f"Error closing connection: {e}")


def executar_consulta(cur, query):
    """
    Executa uma consulta SQL e retorna os resultados.

    :param cur: Cursor MariaDB.
    :param query: Consulta SQL a ser executada.
    :return: Resultados da consulta ou None em caso de erro.
    """
    if cur is not None:
        try:
            cur.execute(query)
            return cur.fetchall()
        except mariadb.Error as e:
            print(f"Error executing query: {e}")
    return None

# Função principal

def main(mariadb_user, mariadb_password, mariadb_host, mariadb_port, mariadb_db, query):
    """
    Função principal que executa a consulta e retorna um DataFrame.

    :param mariadb_user: Nome de usuário para o banco de dados MariaDB.
    :param mariadb_password: Senha para o banco de dados MariaDB.
    :param mariadb_host: Endereço do host do banco de dados MariaDB.
    :param mariadb_port: Porta de conexão do banco de dados MariaDB.
    :param mariadb_db: Nome do banco de dados MariaDB.
    :param query: Consulta SQL a ser executada.
    :return: DataFrame com os resultados da consulta.
    """
    # Conectar ao banco de dados
    conn = conectar_banco(mariadb_host, mariadb_port,
                          mariadb_user, mariadb_password, mariadb_db)
    if conn is None:
        sys.exit(1)

    # Obter o cursor
    cur = obter_cursor(conn)
    if cur is None:
        fechar_conexao(conn, None)
        sys.exit(1)

    # Executar a consulta
    rows = executar_consulta(cur, query)
    if rows is not None:
        # Obter os nomes das colunas
        columns = [desc[0] for desc in cur.description]

        # Criar DataFrame com os resultados
        df = pd.DataFrame(rows, columns=columns)

    # Fechar cursor e conexão
    fechar_conexao(conn, cur)
    return df

# Função para envio ao BigQuery

def send_to_bigquery(destino_bq, dados, cert):
    """
    Envia um DataFrame para uma tabela no BigQuery.

    :param destino_bq: Nome da tabela de destino no BigQuery.
    :param dados: DataFrame a ser enviado.
    :param cert: Caminho para o arquivo de chave do serviço do BigQuery.
    """
    if not dados.empty:
        # Remove linhas duplicadas
        dados = dados.drop_duplicates()
        key_path = cert  # Caminho para o arquivo de chave do serviço
        credentials = service_account.Credentials.from_service_account_file(key_path, scopes=[
            "https://www.googleapis.com/auth/cloud-platform", "https://www.googleapis.com/auth/drive"])
        # Enviar DataFrame para BigQuery
        pandas_gbq.to_gbq(dataframe=dados,
                          destination_table=destino_bq,
                          credentials=credentials,
                          if_exists='replace')
        print(f"Dataframe {destino_bq} enviado ao BigQuery")
    else:
        print(f"Dataframe {destino_bq} vazio, não será enviado ao BigQuery")

# Funções para ajuste de tipos de dados das colunas

def adjust_type_string(column, df):
    """
    Ajusta a coluna para o tipo string.

    :param column: Nome da coluna a ser ajustada.
    :param df: DataFrame contendo a coluna.
    """
    if not df.empty:
        if column in df.columns:
            df[column] = df[column].astype("str", errors='ignore')
        else:
            df[column] = ''
            df[column] = df[column].astype("str", errors='ignore')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

def adjust_type_integer(column, df):
    """
    Ajusta a coluna para o tipo inteiro.

    :param column: Nome da coluna a ser ajustada.
    :param df: DataFrame contendo a coluna.
    """
    if not df.empty:
        if column in df.columns:
            # Substituir valores em branco por zero
            df[column] = df[column].fillna(0)
            df[column] = df[column].astype("int64", errors='ignore')
        else:
            df[column] = np.nan
            df[column] = df[column].astype("int64", errors='ignore')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

def adjust_type_timestamp_yf(column, df):
    """
    Ajusta a coluna para o tipo timestamp (ano primeiro).

    :param column: Nome da coluna a ser ajustada.
    :param df: DataFrame contendo a coluna.
    """
    if not df.empty:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], yearfirst=True, errors='coerce')
        else:
            df[column] = pd.NaT
            df[column] = pd.to_datetime(
                df[column], yearfirst=True, errors='coerce')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

def adjust_type_timestamp_df(column, df):
    """
    Ajusta a coluna para o tipo timestamp (dia primeiro).

    :param column: Nome da coluna a ser ajustada.
    :param df: DataFrame contendo a coluna.
    """
    if not df.empty:
        if column in df.columns:
            df[column] = pd.to_datetime(
                df[column], dayfirst=True, errors='coerce')
        else:
            df[column] = pd.NaT
            df[column] = pd.to_datetime(
                df[column], dayfirst=True, errors='coerce')
    else:
        print(f"Dataframe {df} Vazio, não será ajustado")

def adjust_type_float(column, df):
    """
    Ajusta a coluna para o tipo float.

    :param column: Nome da coluna a ser ajustada.
    :param df: DataFrame contendo a coluna.
    """
    if not df.empty:
        if column in df.columns:
            # Substituir valores em branco por zero
            df[column] = df[column].fillna(0.0)
            
            # Converter a coluna para tipo numérico
            df[column] = pd.to_numeric(df[column], errors='coerce')
        else:
            print(f"Coluna '{column}' não encontrada no DataFrame, não será ajustado")
    else:
        print("DataFrame vazio, não será ajustado")

def adjust_type_boolean(column, df):
    """
    Ajusta a coluna para o tipo booleano.

    :param column: Nome da coluna a ser ajustada.
    :param df: DataFrame contendo a coluna.
    :return: DataFrame com a coluna ajustada.
    """
    if not df.empty:
        if column in df.columns:
            try:
                df[column] = df[column].astype(bool)
            except ValueError as e:
                print(
                    f"Erro ao converter a coluna '{column}' para booleano
