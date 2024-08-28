import query_list as query
import functions as fn
from configparser import ConfigParser
import column_types as ct
import sys

# =========================================== #
# === Ler os dados do arquivo config.ini  === #
# =========================================== #

# Chamar a função para ler as configurações
configuracoes = fn.ler_configuracoes('config.ini')

if configuracoes:
    # Sessão [credentials_maria_db]: Obtém as credenciais de acesso ao banco de dados MariaDB
    user_db = configuracoes['user_db']
    password_db = configuracoes['password_db']
    host_db = configuracoes['host_db']
    port_db = int(configuracoes['port_db'])
    db = configuracoes['db']

    # Sessão [credentials_google]: Obtém informações para o envio ao BigQuery
    destino_bq = configuracoes['destino_bq']
    cert = configuracoes['cert']
else:
    sys.exit(1)
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

except Exception as e:
    # Exibe a mensagem de erro e encerra o programa com código 1 se a conexão ou consulta falhar
    print(
        f'(!) Erro ao conectar ao banco de dados ou executar a consulta: {e}')
    sys.exit(1)
# ==============# Ajuste dos tipos de dados ============== #
try:

    # =============== Ajusta os tipos de dados =============== #
    # ===================(String)============================= #
    if ct.string:
        try:
            # Converte as colunas especificadas para o tipo de dados string
            print('> Convertendo os dados no formato String...')
            for coluna in ct.string:
                fn.adjust_type_string(column=coluna, df=dados)
        except Exception as e:
            # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para String
            print(
                f'(!) Ocorreu uma exceção ao converter os dados para String: \n {e}')
    else:
        print('> Não há colunas do tipo String a serem ajustadas.')

    # ==============(Timestamp Year first)=================== #
    if ct.date_yf:
        try:
            # Converte as colunas especificadas para o tipo de dados timestamp (ano primeiro)
            print('> Convertendo os dados no formato Timestamp (AAAA/MM/DD)...')
            for coluna in ct.date_yf:
                fn.adjust_type_timestamp_yf(column=coluna, df=dados)
        except Exception as e:
            # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Timestamp (ano primeiro)
            print(
                f'(!) Ocorreu uma exceção ao converter os dados para Timestamp (ano primeiro): \n {e}')
    else:
        print('(!) Não há colunas do tipo Timestamp (AAAA/MM/DD) a serem ajustadas.')

    # ===================(Timestamp Day First)======================= #
    if ct.date_df:
        try:
            # Converte as colunas especificadas para o tipo de dados timestamp (dia primeiro)
            print('> Convertendo os dados no formato Timestamp (DD/MM/AAAA)...')
            for coluna in ct.date_df:
                fn.adjust_type_timestamp_df(column=coluna, df=dados)
        except Exception as e:
            # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Timestamp (dia primeiro)
            print(
                f'(!) Ocorreu uma exceção ao converter os dados para Timestamp (dia primeiro): \n {e}')
    else:
        print('(!) Não há colunas do tipo Timestamp (DD/MM/AAAA) a serem ajustadas.')
    # ===================(Float)=======================
    if ct.date_df:
        try:
            # Converte as colunas especificadas para o tipo de dados float
            print('> Convertendo os dados no formato Float...')
            for coluna in ct.float:
                fn.adjust_type_float(column=coluna, df=dados)
        except Exception as e:
            # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Float
            print(
                f'(!) Ocorreu uma exceção ao converter os dados para Float: \n {e}')
    else:
        print('(!) Não há colunas do tipo Float a serem ajustadas.')

    # Descomentar, caso necessário usar

    # =====================(Integer)====================
    if ct.integer:
        try:
            # Converte as colunas especificadas para o tipo de dados integer
            print('> Convertendo os dados no formato Integer...')
            for coluna in ct.integer:
                fn.adjust_type_integer(column=coluna, df=dados)
        except Exception as e:
            # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Integer
            print(
                f'(!) Ocorreu uma exceção ao converter os dados para Integer: \n {e}')
    else:
        print('(!) Não há colunas do tipo Inteiro a serem ajustadas.')

    # =====================(Boolean)=====================
    if ct.integer:
        try:
            # Converte as colunas especificadas para o tipo de dados booleano
            print('> Convertendo os dados no formato Boolean...')
            for coluna in ct.bool:
                fn.adjust_type_boolean(column=coluna, df=dados)
        except Exception as e:
            # Exibe uma mensagem de erro se ocorrer uma exceção durante a conversão para Boolean
            print(
                f'(!) Ocorreu uma exceção ao converter os dados para Boolean: \n {e}')
    else:
        print('(!) Não há colunas do tipo Bool a serem ajustadas.')

except Exception as e:
    # Exibe uma mensagem de erro se ocorrer uma exceção geral durante o ajuste dos tipos de dados
    print(
        f'> Ocorreu uma exceção com a conversão de dados e o programa irá encerrar\nMotivo: {e}')
    sys.exit(1)  # Encerra o programa com erro se o ajuste de tipos de dados falhar

try:
    # Envia o DataFrame tratado para a tabela especificada no BigQuery
    fn.send_to_bigquery(destino_bq, dados, cert)
    print('> Extração encerrada')
    sys.exit(0)  # Encerra o programa sem erros se o envio for bem-sucedido
except Exception as e:
    # Exibe uma mensagem de erro se ocorrer uma exceção durante o envio dos dados para o BigQuery
    print(f'(!) Ocorreu uma exceção ao enviar os dados para o BigQuery > {e}')
    sys.exit(1)  # Encerra o programa com erro se o envio falhar
