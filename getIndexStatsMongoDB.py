# -*- coding: utf-8 -*-

## import modules
import csv
import sqlite3
import os
import io
import dotenv
import socket
import pyodbc as po
from datetime import datetime
from pymongo import MongoClient
from removeLogAntigo import removeLogs
from sendMsgChatGoogle import sendMsgChatGoogle


## Carrega os valores do .env
dotenv.load_dotenv()
myhost = socket.gethostname()

### Variaveis do local do script e log mongodb
dirapp = os.path.dirname(os.path.realpath(__file__))

## funcao que retorna data e hora Y-M-D H:M:S
def obterDataHora():
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    return datahora

## funcao de gravacao de log
def GravaLog(strValue, strAcao):

    ## Path LogFile
    datahoraLog = datetime.now().strftime('%Y-%m-%d')
    pathLog = os.path.join(dirapp, 'log')
    pathLogFile = os.path.join(pathLog, 'logInfoIndeMongoDB_{0}.txt'.format(datahoraLog))

    if not os.path.exists(pathLog):
        os.makedirs(pathLog)
    else:
        pass

    msg = strValue
    with io.open(pathLogFile, strAcao, encoding='utf-8') as fileLog:
        fileLog.write('{0}\n'.format(strValue))

    return msg


## funcao de envio de exception para google chat
def enviaExceptionGChat(msgGChat):
    URL_WEBHOOK_DBA = os.getenv("URL_WEBHOOK_DBA")
    datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    msgWebHook = '*Host: {0} | ETL - Obter Dados de Uso dos Indices MongoDB | {1}*\n{2}'.format(myhost, datahora, msgGChat)
    sendMsgChatGoogle(URL_WEBHOOK_DBA, msgWebHook)
    #GravaLog(msgWebHook, 'a')


## funcao de formacao da connection string Database Destino
def strConnectionDatabaseDestino():
    
    #variaveis de conexao database_bi
    server   = os.getenv("SERVER_BI_SQL")
    port     = os.getenv("PORT_BI_SQL")
    database = os.getenv("DATABASE_BI_SQL")
    username = os.getenv("USERNAME_BI_SQL")
    password = os.getenv("PASSWORD_BI_SQL")

    strConnection = 'DRIVER={{ODBC Driver 17 for SQL Server}};\
        SERVER={v_server};\
        PORT={v_port};\
        DATABASE={v_database};\
        UID={v_username};\
        PWD={v_password}'.format(v_server = server, v_port = port, v_database = database, v_username = username, v_password = password)

    return strConnection

## funcao para obter dados de indices no MongoDB
def obterInfoIndexMongoDB(v_prefix_name_db):

    ## grava log
    datahora = obterDataHora()
    msgLog = 'Obtendo dados estatisticos dos indices MongoDB (Inicio): {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    try:

        ## variaveis de conexao
        DBUSERNAME = os.getenv("USERNAME_MONGODB")
        DBPASSWORD = os.getenv("PASSWORD_MONGODB")
        MONGO_HOST = os.getenv("SERVER_MONGODB")
        DBAUTHDB   = os.getenv("DBAUTHDB_MONGODB")
        #MONGO_URI = 'mongodb://' + DBUSERNAME + ':' + DBPASSWORD + '@' + MONGO_HOST + '/' + DBAUTHDB + '?directConnection=true'
        MONGO_URI = 'mongodb://{0}:{1}@{2}/{3}?directConnection=true'.format(DBUSERNAME, DBPASSWORD, MONGO_HOST, DBAUTHDB)
        
        # Lista para armazenar os resultados
        listReturnMongoDb = []
        
        # Timestamp atual
        current_timestamp = datetime.now().strftime("%Y-%m-%d")
        
        # Conectando ao MongoDB
        #with MongoClient(MONGO_URI) as client:
        client = MongoClient(MONGO_URI)
        db_names = client.list_database_names()
    
        for db_name in db_names:
            #if db_name.startswith(v_prefix_name_db) and db_name[4:].isdigit():
            if db_name.startswith(v_prefix_name_db):
                print("--- Database: {0} ---".format(db_name))
                db = client[db_name]
                
                collection_names = db.list_collection_names()
                
                for coll_name in collection_names:
                    if coll_name:
                        
                        ## obtem dados estatisticos da colecao
                        coll_stats = db.command("collstats", coll_name)
                        
                        ## acessa a colecao
                        collection = db[coll_name]
                        
                        ## obtem dados estatisticos dos indices da colecao
                        index_stats = list(collection.aggregate([{"$indexStats": {}}]))
                        
                        for index in index_stats:
                            
                            ## extrai informacoes especificas dos indices
                            index_name = index['name']
                            access_ops = index['accesses']['ops']
                            
                            ## ajusta formato de data accesses.since de YYYY-MM-DD hh:mm:ss para YYYY-MM-DD
                            access_since = str(index['accesses']['since']).split(".")[0] ## formata data YYYY-MM-DD hh:mm:ss
                            access_since = datetime.strptime(access_since, "%Y-%m-%d %H:%M:%S").date() ## ajusta formato para YYYY-MM-DD
                            
                            ix_specification = str(index['spec'])

                            ## obtem tamanho do indices atual
                            indexSizeBytes = coll_stats['indexSizes'][index_name]
                            
                            ## cria lista auxiliar vazia
                            values = []

                            ## insere dados na lista auxiliar
                            values.insert(0, db_name)
                            values.insert(1, coll_name)
                            values.insert(2, index_name)
                            values.insert(3, access_ops)
                            values.insert(4, access_since)
                            values.insert(5, indexSizeBytes)
                            values.insert(6, ix_specification)
                            values.insert(7, current_timestamp)
                            
                            ## insere os dados da lista auxiliar na lista principal
                            listReturnMongoDb.append(values)
        

    except Exception as e:
        msgException = "{0}".format(e)
        msgLog = 'Error (MONGODB): {0}'.format(msgException)
        enviaExceptionGChat(msgLog)
        print(GravaLog(msgLog, 'a'))

    finally:
        
        ## fecha conexao mongodb
        client.close()

        ## verificacoes adicionais da lista obtida
        if not listReturnMongoDb:
            datahora = obterDataHora()
            msgLog = 'Nao foi possivel obter dados do MongoDB\n'
            msgLog = '{0}***** Fim da aplicacao: {1}\n'.format(msgLog, datahora)
            enviaExceptionGChat(msgLog)
            print(GravaLog(msgLog, 'a'))
            exit()
        else:
            datahora = obterDataHora()
            msgLog = 'Obtendo dados dos databases MongoDB (Fim): {0}'.format(datahora)
            msgLog = '{0}\nTamanho lista obtida do MongoDB: {1}'.format(msgLog, len(listReturnMongoDb))
            print(GravaLog(msgLog, 'a'))

            return listReturnMongoDb


## exibe dados em formato CSV usando pipe "|"
def exibeformatoCsV(listReturnMongoDb):
    
    tamlist =  range(len(listReturnMongoDb))
    for i in tamlist:
        p_dbname            = str(listReturnMongoDb[i][0])
        p_collname          = str(listReturnMongoDb[i][1])
        p_indexname         = str(listReturnMongoDb[i][2])
        p_access_ops        = str(listReturnMongoDb[i][3])
        p_access_since      = str(listReturnMongoDb[i][4])
        p_indexSizeBytes    = str(listReturnMongoDb[i][5])
        p_ix_specification  = str(listReturnMongoDb[i][6])
        p_current_timestamp = str(listReturnMongoDb[i][7])
        
        #print("'{0}', '{1}', '{2}', {3}, '{4}', {5}, '{6}', '{7}'".format(p_dbname, p_collname, p_indexname, p_access_ops, p_access_since, p_indexSizeBytes, p_ix_specification, p_current_timestamp))
        values = [p_dbname, p_collname, p_indexname, p_access_ops, p_access_since, p_indexSizeBytes, p_ix_specification, p_current_timestamp]
        print("|".join("{0}".format(str(value)) for i, value in enumerate(values)))
        #print("|".join("'{0}'".format(value) if (i != 3 or i != 5) else str(value) for i, value in enumerate(values)))


## Grava dados em arquivo CSV
def escreveCSV(listValues):

    pathCsv = os.path.join(dirapp, 'csv')
    pathCsvFile = os.path.join(pathCsv, 'indexStatsMongoDB.csv')

    if not os.path.exists(pathCsv):
        os.makedirs(pathCsv)

    with io.open(pathCsvFile, mode="w", encoding='utf-8', newline="") as filecsv:
        writer = csv.writer(filecsv, delimiter="|")
        writer.writerow(['Database', 'Collection', 'IndexName', 'UsageCount', 'LastUsed', 'IndexSizeBytes', 'IndexSpecification', 'DataColeta'])
        writer.writerows(listValues)


## Funcao de criacao do database e tabela caso nao exista
def create_tables(dbname_sqlite3):
    
    ## script sql de criacao da tabela
    # pode ser adicionado a criacao de mais e uma tabela
    # separando os scripts por virgulas
    sql_statements = [
        """
        CREATE TABLE IF NOT EXISTS "infoIndexesMongoDB" (
            infoIndexesMongoDBId INTEGER,
            Database             VARCHAR(20),
            Collection           VARCHAR(60),
            IndexName            VARCHAR(30),
            UsageCount           BIGINT,
            LastUsed             DATETIME,
            IndexSizeBytes       BIGINT,
            IndexSpecification   VARCHAR(3000),
            DataColeta           DATE,
            PRIMARY KEY("infoIndexesMongoDBId" AUTOINCREMENT)
        )        
        """
    ]

    # variaveis da conexao ao database
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    
    # cria o diretorio caso nao exista
    if not os.path.exists(path_dir_db):
        os.makedirs(path_dir_db)
    else:
        pass
    

    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:
            cursor = conn.cursor()
            for statement in sql_statements:
                cursor.execute(statement)
            
            conn.commit()

    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Criar tabela SQlite3 [infoIndexesMongoDB] Error (SQLITE): {0}\n{1}'.format(datahora, msgException)
        enviaExceptionGChat(msgLog)
        print(GravaLog(msgLog, 'a'))

    finally:
        msgLog = 'Criado tabela [infoIndexesMongoDB] no database [{0}]'.format(dbname_sqlite3)
        print(GravaLog(msgLog, 'a'))


## gera comandos de inserts conforme valores da lista passada
def gravaDadosSqlite(v_ListValues):
    dbname_sqlite3 = os.getenv("DBNAME_SQLITE")
    path_dir_db = os.path.join(dirapp, 'db')
    path_full_dbname_sqlite3 = os.path.join(path_dir_db, dbname_sqlite3)
    RowCount = 0

    ## verifica se banco de dados existe 
    # caso nao exista realizada a chamada da funcao de criacao
    if not os.path.exists(path_dir_db):
        create_tables(dbname_sqlite3)
    else:
        pass

    
    try:
        with sqlite3.connect(path_full_dbname_sqlite3) as conn:

            cur = conn.cursor()
            sqlcmdDelete = '''DELETE FROM infoIndexesMongoDB WHERE DataColeta < date('now', '-30 days')'''
            cur.execute(sqlcmdDelete)
            RowDelete = conn.total_changes

            sqlcmd = '''
            INSERT INTO infoIndexesMongoDB
                (Database, Collection, IndexName, UsageCount, LastUsed, IndexSizeBytes, IndexSpecification, DataColeta) 
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?);
            '''

            cur.executemany(sqlcmd, v_ListValues)
            RowCount = conn.total_changes - RowDelete
            conn.commit()
    
    except sqlite3.Error as e:
        datahora = obterDataHora()
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim Insert tabela SQlite3 [infoIndexesMongoDB] Error (SQLITE): {0}\n{1}'.format(datahora, msgException)
        enviaExceptionGChat(msgLog)
        print(GravaLog(msgLog, 'a'))

    finally:
        msgLog = 'Quantidade de Registros Inseridos na tabela [infoIndexesMongoDB]: {0} registro(s)'.format(RowCount)
        print(GravaLog(msgLog, 'a'))


## Grava dados no destino
def gravaDadosDestinoSQLServer(v_ListValues):

    try:
        ## Connection string
        connString = str(strConnectionDatabaseDestino())
        cnxn = po.connect(connString)
        cnxn.autocommit = False
        RowCount = 0
        
        ## query de busca
        cursor = cnxn.cursor()
        

        ## apaga registros anteriores
        #sqlcmdDelete = 'DELETE FROM [dbo].[infoIndexesMongoDB]'
        #cursor.execute(sqlcmdDelete)

        """
        ESTRUTURA DA TABELA NO SQL SERVER

        CREATE TABLE [infoIndexesMongoDB] (
            [infoIndexesMongoDBId] BIGINT IDENTITY(1,1), 
            [Database]             VARCHAR(20), 
            [Collection]           VARCHAR(60), 
            [IndexName]            VARCHAR(30), 
            [UsageCount]           BIGINT, 
            [LastUsed]             DATE, 
            [IndexSizeBytes]       BIGINT, 
            [IndexSpecification]   VARCHAR(3000), 
            [DataColeta]           DATE, 
			CONSTRAINT [PK_infoIndexesMongoDB] PRIMARY KEY CLUSTERED ([infoIndexesMongoDBId] ASC)
        );

        """

        for i in range(len(v_ListValues)):
            Database           = str(v_ListValues[i][0])
            Collection         = str(v_ListValues[i][1])
            IndexName          = str(v_ListValues[i][2])
            UsageCount         = str(v_ListValues[i][3])
            LastUsed           = str(v_ListValues[i][4])
            IndexSizeBytes     = str(v_ListValues[i][5])
            IndexSpecification = str(v_ListValues[i][6])
            DataColeta         = str(v_ListValues[i][7])
        
            sqlcmd = """  
            INSERT INTO [dbo].[infoIndexesMongoDB] 
                ([Database], [Collection], [IndexName], [UsageCount], [LastUsed], [IndexSizeBytes], [IndexSpecification], [DataColeta])
            VALUES 
                (?, ?, ?, ?, ?, ?, ?, ?);  
            """

            paramSql = (Database, Collection, IndexName, UsageCount, LastUsed, IndexSizeBytes, IndexSpecification, DataColeta)
            cursor.execute(sqlcmd, paramSql)
            RowCount = RowCount + cursor.rowcount
            


    except Exception as e:
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgException = "Error: {0}".format(e)
        msgLog = 'Fim insercao de dados no destino - Error (SQL SERVER): {0}\n{1}'.format(datahora, msgException)
        enviaExceptionGChat(msgLog)
        print(GravaLog(msgLog, 'a'))
        cnxn.rollback()

    else:
        cnxn.commit()
        
    finally:

        ## Close the database connection
        cursor.close()
        del cursor
        cnxn.close()
        datahora = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        msgLog = 'Quantidade de Registros Inseridos no destino: {0}\n'.format(RowCount)
        print(GravaLog(msgLog, 'a'))



## FUNCAO INICIAL
def main():

    ## log do inicio da aplicacao
    datahora = obterDataHora()
    msgLog = '\n***** Inicio da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

    ## remocao dos logs antigos acima de xx dias
    diasRemover = 30
    dirLog = os.path.join(dirapp, 'log')
    msgLog = removeLogs(diasRemover, dirLog)
    print(GravaLog(msgLog, 'a'))
    
    prefix_name_db = 'dat_'
    listIndex = obterInfoIndexMongoDB(prefix_name_db) 
    
    #exibeformatoCsV(listIndex)
    escreveCSV(listIndex)
    gravaDadosSqlite(listIndex)
    #gravaDadosDestinoSQLServer(listIndex)

    ## log do final da aplicacao
    datahora = obterDataHora()
    msgLog = '***** Final da aplicacao: {0}'.format(datahora)
    print(GravaLog(msgLog, 'a'))

#### inicio da aplicacao ####
if __name__ == "__main__":
    ## chamada da funcao inicial
    main()