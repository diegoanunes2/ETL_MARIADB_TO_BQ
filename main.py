import query_list as query
import functions as fn
from configparser import ConfigParser
import column_types as ct
import sys

# =========================================== #
# === Ler os dados do arquivo config.ini  === #
# =========================================== #

# Inicializa o ConfigParser sem interpolação para ler o arquivo de configuração
config = ConfigParser(interpolation=None)
# Lê o arquivo de configuração e passa a codificação UTF-8
config.read('config.ini', encoding='utf-8')

# Cria variáveis com as informações de configuração do arquivo config.ini

# Sessão [credentials_maria_db]: Obtém as credenciais de acesso ao banco de dados MariaDB
user_db = config.get('credentials_maria_db', 'user_db')
password_db = config.get('credentials_maria_db', 'password_db')
host_db = config.get('credentials_maria_db', 'host_db')
port_db = int(config.get('credentials_maria_db', 'port_db'))
db = config.get('credentials_maria_db', 'db')

# Sessão [credentials_google]: Obtém informações para o envio ao BigQuery
destino_bq = config.get('credentials_google', 'destino_bq')
cert = config.get('credentials_google', 'cert')

# =========================================== #
# =====  Inicia a execução do programa  ===== #
# =========================================== #

# Faz a conexão com o banco e recebe os dados em formato de um DataFrame na variável 'dados'
try:
    # Conecta ao banco de dados e executa a consulta, retornando os dados como um DataFrame
    dados = fn.main(mariadb_user=user_db,
                    mariadb_password=password_db,
                    mariadb_host=host_db,
                    mariadb_port=port_db, 
                    mariadb_db=db, 
                    query=query.query)
                    
    print('Dataframe Criado')
except Exception as e:
    # Exibe a mensagem de erro e encerra o programa com código 1 se a conexão ou consulta falhar
    print(f'Erro ao conectar ao banco de dados ou executar a consulta: {e}')
    sys.exit(1)
# ==============# Ajuste dos tipos de dados ============== #
try:
    print('Iniciando o ajuste dos tipos de dados:')
    
    # =============== Ajusta os tipos de dados =============== #

    # ===================(String)============================= #
    try:
        # Converte as colunas especificadas para o tipo de dados string
        print('Convertendo os dados no formato String...')
        for coluna in ct.string:
            fn.adjust_type_string(column=coluna, df=dados)
    except Exception as e:
        # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para String
        print(f'Ocorreu uma exceção ao converter os dados para String: \n {e}')

    # ==============(Timestamp Year first)=================== #
    try:
        # Converte as colunas especificadas para o tipo de dados timestamp (ano primeiro)
        print('Convertendo os dados no formato Timestamp (ano primeiro)...')
        for coluna in ct.date_yf:
            fn.adjust_type_timestamp(column=coluna, df=dados)
    except Exception as e:
        # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Timestamp (ano primeiro)
        print(f'Ocorreu uma exceção ao converter os dados para Timestamp (ano primeiro): \n {e}')

    # ===================(Timestamp Day First)======================= #
    try:
        # Converte as colunas especificadas para o tipo de dados timestamp (dia primeiro)
        print('Convertendo os dados no formato Timestamp (dia primeiro)...')
        for coluna in ct.date_df:
            fn.adjust_type_timestamp_df(column=coluna, df=dados)
    except Exception as e:
        # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Timestamp (dia primeiro)
        print(f'Ocorreu uma exceção ao converter os dados para Timestamp (dia primeiro): \n {e}')

    # ===================(Float)=======================
    try:
        # Converte as colunas especificadas para o tipo de dados float
        print('Convertendo os dados no formato Float...')
        for coluna in ct.float:
            fn.adjust_type_float(column=coluna, df=dados)
    except Exception as e:
        # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Float
        print(f'Ocorreu uma exceção ao converter os dados para Float: \n {e}')

    # Descomentar, caso necessário usar

    # =====================(Integer)====================
    try:
        # Converte as colunas especificadas para o tipo de dados integer
        print('Convertendo os dados no formato Integer...')
        for coluna in ct.integer:
            fn.adjust_type_integer(column=coluna, df=dados)
    except Exception as e:
        # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Integer
        print(f'Ocorreu uma exceção ao converter os dados para Integer: \n {e}')

    # =====================(Boolean)=====================
    try:
        # Converte as colunas especificadas para o tipo de dados booleano
        print('Convertendo os dados no formato Boolean...')
        for coluna in ct.bool:
            fn.adjust_type_boolean(column=coluna, df=dados)
    except Exception as e:
        # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Boolean
        print(f'Ocorreu uma exceção ao converter os dados para Boolean: \n {e}')

except Exception as e:
    # Exibe uma mensagem de erro se ocorrer uma exceção geral durante o ajuste dos tipos de dados
    print(f'Ocorreu uma exceção com a conversão de dados e o programa irá encerrar\nMotivo: {e}')
    sys.exit(1)  # Encerra o programa com erro se o ajuste de tipos de dados falhar

try:
    print('Enviando os dados tratados para o BigQuery')
    # Envia o DataFrame tratado para a tabela especificada no BigQuery
    fn.send_to_bigquery(destino_bq, dados, cert)
    print('Dados enviados para o BigQuery com sucesso!')
    print('Extração encerrada')
    sys.exit(0)  # Encerra o programa sem erros se o envio for bem-sucedido
except Exception as e:
    # Exibe uma mensagem de erro se ocorrer uma exceção durante o envio dos dados para o BigQuery
    print(f'Ocorreu uma exceção ao enviar os dados para o BigQuery > {e}')
    sys.exit(1)  # Encerra o programa com erro se o envio falhar
