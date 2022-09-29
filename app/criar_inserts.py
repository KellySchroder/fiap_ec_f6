# Importarção das blibliotecas padrões (built-in)
import json
import os
import shutil
import urllib3
from datetime import datetime

# Importarção das blibliotecas externas
import oracledb
import requests
from dotenv import load_dotenv

# Suprimir aviso do SSL do site do governo
urllib3.disable_warnings()

# Carregamento do arquivo com as variaveis de ambiente
# essas variaveis contém dados sensiveis
#    ex: conexão com o banco de dados
load_dotenv()

# Declaração das constantes
DIRETORIO_CSV = 'csv'
DIRETORIO_SQL = 'sql'
DIRETORIO_JSON = 'json'

ORDEM_ARQUIVOS_TABELAS = [
    'divisoes_regionais_esp.csv',
    'estabelecimentos_saude.csv',
    'populacao2022.csv',
    'saude_leitos_mun_ano.csv',
    'saude_med_enf_mun_ano.csv'
]

DB_USER = os.getenv('DB_USER')
DB_PASSWORD = os.getenv('DB_PASSWORD')
DB_HOST = os.getenv('DB_HOST')
DB_PORT = int(os.getenv('DB_PORT', 0))
DB_SID = os.getenv('DB_SID')


def criar_carga_de_dados(arquivo_nome):
    """
    Função responsável ler o arquivo .csv passado no parâmetro arquivo_nome
    e retornar os INSERTS que serão executados para carga dos dados
    :param arquivo_nome: Nome do arquivo csv que será utilizado para gerar os inserts
    :return: Retorna uma lista de inserts
    """

    # Montagem do caminho dos arquivos que serão utilizados/criados
    arquivo_csv = os.path.join(DIRETORIO_CSV, arquivo_nome)
    arquivo_json = os.path.join(DIRETORIO_JSON, arquivo_nome + '.json')
    arquivo_sql = os.path.join(DIRETORIO_SQL, arquivo_nome + '.sql')

    # Leitura do arquivo de estrutura json
    estrutura = None
    with open(arquivo_json, 'r', encoding="UTF-8") as f:
        estrutura = json.loads(f.read())

    # Download do arquivo CSV
    download_arquivo_csv(estrutura.get('url'), arquivo_csv)

    # Leitura do arquivo de carga csv
    conteudo_arquivo = None
    with open(arquivo_csv, 'r', encoding="UTF-8") as f:
        conteudo_arquivo = f.readlines()

    # Leitura dos nomes das colunas do arquivo csv
    cabecalho = [
        nome_coluna.strip()
            for nome_coluna in conteudo_arquivo[0].split(';')
    ]

    # Cria o arquivo SQL
    script = open(arquivo_sql, 'w', encoding="UTF-8")

    # Inicialização da variável de retorno
    insert_sql = []

    # Iteração do arquivo csv, iniciando pela segunda
    # linha e ignorando o cabeçalho
    for linha in conteudo_arquivo[1:]:
        # Separando os valores da linha pelo ponto e virgula
        linha = linha.split(';')

        # Inicializando a variavel que conterá os valores validos
        valores_validos = {}

        # Iteração pelas colunas existentes no arquivo csv
        for ix in range(0, len(cabecalho)):
            # Verifica se o valor do cabeçalho na posição ix está
            # entre as colunas declaradas no json de estrutura
            if cabecalho[ix] in estrutura.get("colunas").keys():
                # Verifica se o cabeçalho na posição ix é cod_ibge
                # e se linha na posição ix é vazia
                if estrutura.get("colunas").get(cabecalho[ix]).get('alias') == 'cod_ibge' \
                   and not linha[ix].strip():
                    # Não considerar a linha como valida
                    # e interrompe a iteração do resto das colunas
                    valores_validos = {}
                    break

                # Verifica os tipos das colunas para montar os valores_validos
                if estrutura.get("colunas").get(cabecalho[ix]).get('tipo') == 'str':
                    # Se for texto escapa o apóstrofo (ajuste para o SQL)
                    # e coloca o valor entre aspas simples
                    linha[ix] = linha[ix].strip().replace("'", "''")
                    valores_validos[cabecalho[ix]] = f"'{linha[ix]}'"
                elif estrutura.get("colunas").get(cabecalho[ix]).get('tipo') == 'float':
                    # Se for decimal substitui virgula por ponto
                    valores_validos[cabecalho[ix]] = linha[ix].strip().replace(',', '.')
                elif estrutura.get("colunas").get(cabecalho[ix]).get('tipo') == 'int':
                    # Se for inteiro remove os pontos de formatação existentes
                    valores_validos[cabecalho[ix]] = linha[ix].strip().replace('.', '')
                else:
                    # Caso não atenda a nenhum dos critérios anteriores,
                    # recebe o valor da variavel sem alterações
                    valores_validos[cabecalho[ix]] = linha[ix].strip()

        # Verifica se a variavel está preenchida
        if valores_validos:
            # Inicializa a lista do alias das colunas
            colunas_alias = []

            # Itera sobre as chaves(nomes das colunas) em valores_validos
            for coluna in valores_validos.keys():
                # Insere o valor do alias contido na estrutura com
                # base no nome original da coluna
                colunas_alias.append(estrutura.get("colunas").get(coluna).get('alias'))

            # Itera sobre os items de colunas_extras
            for key, value in estrutura.get("colunas_extras", {}).items():
                # Insere o nome da coluna
                colunas_alias.append(key)

                # Verifica os tipos das colunas para montar os valores_validos
                if value.get('tipo') == 'str':
                    # Se for texto coloca o valor default entre aspas simples
                   valores_validos[key] = f"'{value.get('default')}'"
                else:
                    # Caso não atenda a nenhum dos critérios anteriores,
                    # recebe o valor da variavel sem alterações
                   valores_validos[key] = value.get('default')

            # Monta o comando de INSERT para a tabela com base nas
            # variaveis colunas_alias e valores_validos
            sql = f'INSERT INTO {estrutura.get("tabela")}({", ".join(colunas_alias)}) VALUES ' \
                  f'({", ".join(valores_validos.values())})\n'

            # Escreve uma nova linha no arquivo SQL
            script.write(sql)

            # Adiciona o comando SQL na lista de retorno
            insert_sql.append(sql)

    # Fecha o arquivo SQL
    script.close()

    return insert_sql


def executar_inserts(inserts):
    """
    Função responsável realizar a conexão com o banco de dados
    e executar os inserts passados por parâmetro
    :param inserts: Lista de inserts
    :return: None
    """

    # Abrir conexão com o banco de dados
    with oracledb.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sid=DB_SID
    ) as connection:
        # Abrir o cursor de execução
        with connection.cursor() as cursor:
            # Iteração sobre os inserts
            for item_insert in inserts:
                try:
                    # Executa o comando de insert
                    cursor.execute(item_insert)
                except Exception as e:
                    # Em caso de erro imprime no console o comando
                    # e retorna o erro para o script
                    print(item_insert)
                    raise Exception(str(e))
        # Realiza o commit da transação
        connection.commit()


def truncates():
    """
    Função responsável realizar a conexão com o banco de dados
    e limpar o conteudo das tabelas
    :param: None
    :return: None
    """

    # Lista de tabelas que serão limpas
    tabelas = ['T_ESTAB_SAUDE', 'T_POPULACAO','T_LEITOS','T_PROF_SAUDE','T_MUNICIPIOS']

    # Abrir conexão com o banco de dados
    with oracledb.connect(
        user=DB_USER,
        password=DB_PASSWORD,
        host=DB_HOST,
        port=DB_PORT,
        sid=DB_SID
    ) as connection:
        # Abrir o cursor de execução
        with connection.cursor() as cursor:
            # Itera sobre a lista de tabelas
            for nome_tabela in tabelas:
                # Executa o comando TRUNCATE TABLE
                cursor.execute(f'TRUNCATE TABLE {nome_tabela}')
                # Realiza o commit da transação
                connection.commit()


def download_arquivo_csv(url, arquivo_csv):
    """
    Função responsável realizar o download do arquivo csv para o disco local
    :param url: URL do arquivo
    :param arquivo_csv: Nome do arquivo que será salvo
    :return: None
    """

    # Executa a requisição do arquivo
    response = requests.request("GET", url, verify=False, allow_redirects=True)
    # Escreve o arquivo no disco local
    with open(arquivo_csv, "w", encoding="UTF-8") as f:
        f.write(response.text.replace('\r\n', '\n'))


if __name__ == '__main__':

    # Verifica se o diretório existe
    if os.path.isdir(DIRETORIO_SQL):
        # Remove o diretório e seus arquivos
        shutil.rmtree(DIRETORIO_SQL)
    # Cria o diretório
    os.mkdir(DIRETORIO_SQL)

    # Verifica se o diretório existe
    if os.path.isdir(DIRETORIO_CSV):
        # Remove o diretório e seus arquivos
        shutil.rmtree(DIRETORIO_CSV)
    # Cria o diretório
    os.mkdir(DIRETORIO_CSV)

    # Variavel para controle de tempo da execução
    inicio = datetime.now()
    # Executa truncates
    truncates()
    # Imprime o tempo de execução
    print(datetime.now() - inicio, 'limpar tabelas')

    # Itera sobre a lista de arquivos seguindo a ordem de declaração
    for arquivo in ORDEM_ARQUIVOS_TABELAS:
        # Variavel para controle de tempo da execução
        inicio = datetime.now()
        # Recebe a lista de inserts da função criar_carga_de_dados
        insert_sql = criar_carga_de_dados(arquivo)
        # Imprime o tempo de execução
        print(datetime.now() - inicio, 'criar inserts do arquivo', arquivo)

        # Variavel para controle de tempo da execução
        inicio = datetime.now()
        # Executa os inserts
        executar_inserts(insert_sql)
        # Imprime o tempo de execução
        print(datetime.now() - inicio, 'executar inserts do arquivo', arquivo)
