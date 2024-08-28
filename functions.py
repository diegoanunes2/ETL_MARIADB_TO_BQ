import mariadb
import pandas as pd
import pandas_gbq
import numpy as np
import sys
from google.oauth2 import service_account
from configparser import ConfigParser
import os

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
        print(
            f"(1) Erro ao conectar ao Banco de dados\n > Detalhes do Erro:\n{e}")
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
            print(f"> Erro ao criar o cursor\n> Detalhes do Erro:\n{e}")
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
        print(f"(!) Erro ao encerrar a conexão: \n> Detalhes do Erro:\n{e}")


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
            print(f"(!) Erro ao executar a query\n> Detalhes do Erro:\n{e}")
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
    print(
        f'> Conectando ao Banco de Dados {mariadb_db} no host {mariadb_host}:{mariadb_port}')
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
    print('> Solicitando dados ao host...')
    rows = executar_consulta(cur, query)
    if rows is not None:
        # Obter os nomes das colunas
        columns = [desc[0] for desc in cur.description]

        # Criar DataFrame com os resultados
        print('> Criando o Dataframe')
        df = pd.DataFrame(rows, columns=columns)
        print('> Dataframe criado com sucesso!')

    # Fechar cursor e conexão
    print(f'> Fechando a conexão com o host {mariadb_host}:{mariadb_port}')
    fechar_conexao(conn, cur)
    print('> Conexão fechada')
    return df

# Função para envio ao BigQuery


def send_to_bigquery(destino_bq, dados, cert):
    """
    Envia um DataFrame para uma tabela no BigQuery.

    :param destino_bq: Nome da tabela de destino no BigQuery.
    :param dados: DataFrame a ser enviado.
    :param cert: Caminho para o arquivo de chave do serviço do BigQuery.
    """
    print('> Enviando dados para o Bigquery')
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
        print(f"\n> Dataframe salvo em {destino_bq} no Google Bigquery")
    else:
        print(
            f"(!) Dataframe {destino_bq} vazio, não será enviado ao BigQuery")

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
        print(f"(!) Dataframe {df} Vazio, não há dados a serem ajustados")


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
        print(f"(!) Dataframe {df} Vazio, não há dados a serem ajustados")


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
        print(f"(!) Dataframe {df} Vazio, não há dados a serem ajustados")


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
        print(f"(!) Dataframe {df} Vazio, não há dados a serem ajustados")


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
            print(
                f"(!) Coluna '{column}' não encontrada no DataFrame, não será ajustado")
    else:
        print(f"(!) Dataframe {df} Vazio, não há dados a serem ajustados")


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
                    f"(!) Erro ao converter a coluna '{column}' para booleano")
    else:
        print(f"(!) Dataframe {df} Vazio, não há dados a serem ajustados")


def ler_configuracoes(arquivo_config):
    """Lê o arquivo de configuração e retorna um dicionário com as configurações.

    Args:
        arquivo_config (str): Caminho completo para o arquivo de configuração.

    Returns:
        dict: Dicionário com as configurações, ou None se ocorrer algum erro.
    """

    if not os.path.exists(arquivo_config):
        print(
            f"\n(!) O arquivo de configuração '{arquivo_config}' não foi encontrado.\nCertifique-se que ele se encontra na raiz do diretório desta aplicação")
        return None

    config = ConfigParser(interpolation=None)
    config.read(arquivo_config, encoding='utf-8')

    configuracoes = {}
    try:
        # Sessão [credentials_maria_db]
        configuracoes['user_db'] = config.get(
            'credentials_maria_db', 'user_db')
        configuracoes['password_db'] = config.get(
            'credentials_maria_db', 'password_db')
        configuracoes['host_db'] = config.get(
            'credentials_maria_db', 'host_db')
        configuracoes['port_db'] = int(
            config.get('credentials_maria_db', 'port_db'))
        configuracoes['db'] = config.get('credentials_maria_db', 'db')

        # Sessão [credentials_google]
        configuracoes['destino_bq'] = config.get(
            'credentials_google', 'destino_bq')
        configuracoes['cert'] = config.get('credentials_google', 'cert')
    except (configparser.NoSectionError, configparser.NoOptionError) as e:
        print(f"(!) Erro ao ler o arquivo de configuração: {e}")
        return None

    return configuracoes
