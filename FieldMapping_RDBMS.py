###############################################################################################################################################
#  Date                                               : 2021-09-08
#  Author                                             : Ayush Ranjan, Ayush Kumar
#  Description                                        : This script compares the datatypes of the rdbms source tables with the Hive target tables
#  Comments                                           : This is the current location of script /var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/
#  Parameters to be passed                            : rundate,run number,blob upload flag, blob path and source system name
###############################################################################################################################################

import pyodbc
import cx_Oracle
import os
import json
import pandas as pd
import sys
import azure.core.exceptions
import azure.storage.blob
from azure.storage.blob import BlobServiceClient, BlobClient
from azure.storage.blob import ContentSettings, ContainerClient
from io import BytesIO
import pyorc as p
import re
import shlex
import logging
import subprocess
from datetime import datetime,timedelta
import pyarrow as pa
import pyarrow.orc as orc
import csv

# To get the parameters at the run time
try:
    rundate=str(sys.argv[1].strip())
    run_no=str(sys.argv[2].strip())
    orc_upload=str(sys.argv[3].strip())
    target_blob=str(sys.argv[4].strip())
    src_syst=str(sys.argv[5].strip())
    exec_date=str(sys.argv[6].strip())

except:
    sys.exit("Please enter the required arguments to run the script")

# To configure the logging

op_time = datetime.now().strftime("%Y-%m-%d")
log_path = "/data/02/informatica/1040/logs/NODE01_ATRD_PRD/services/DataIntegrationService/disLogs/python_orchestration_logs/"
log_file_name = log_path + "/" + src_syst + "field_mapping_" + op_time + ".log"
Log_Format = "%(asctime)s - %(message)s"
logging.basicConfig(filename = log_file_name,
                    filemode = "w",
                    format = Log_Format, level = logging.ERROR)

logger = logging.getLogger()
logger.setLevel(logging.INFO)

#used to create the blob connection string in the proper format
class RedactedStr(str):
    __repr__ = object.__repr__

#To create the blob connection string
def connection_str(account, key, protocol: str = 'https', suffix: str = 'core.windows.net'):
    return RedactedStr(f'DefaultEndpointsProtocol={protocol};AccountName={account};'
                       f'AccountKey={key};EndpointSuffix={suffix}')


#To generate the rundate at runtime
def date(rundate):
  if len(rundate)>0:
     return rundate
  else:
      now = datetime.date.today()
      last_month = now.month-1 if now.month > 1 else 12
      year = now.year if last_month!=12 else now.year-1
      last_month_date = datetime.date(day=1,month=last_month,year=year)
      valuation = last_month_date - datetime.timedelta(days=1)
      rundate = valuation.strftime("%Y%m%d")
      return(rundate)



if src_syst == "SYM":

    tgt_syst="Symphony"
    configfile="/var/datahub/config/sym_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]
    db_server_port="1515"
    db_server_add="sl05.atradiusnet.com"
    db_name="SYMP.atradiusnet.com"
    db_user="GLIFRS1"

    config_table="/var/datahub/config/Symphony.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "LCT":

    tgt_syst="LargeCaseTool"
    configfile="/var/datahub/config/lct_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    tables = []
    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/Large_Case_Tool.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []
elif src_syst == "NST":

    tgt_syst="Natstar"
    configfile="/var/datahub/config/nst_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    tables = []
    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = "svc_ifrs17_nst_dev"
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/Natstar.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []    

elif src_syst == "DOR":

    tgt_syst="Dora"
    configfile="/var/datahub/config/dor_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    #jdbc:oracle:thin:@10.20.13.227:1515:SYMP
    #db_server_port=data["dbServerPort"]
    db_server_port="1515"
    #db_server_add=data["dbServerAdd"]
    db_server_add="10.20.13.227"
    #db_name = data["dbName"]
    db_name="SYMP.atradiusnet.com"
    #db_user = data["dbUser"]
    db_user="GLIFRS1"
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/DORA.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "BAL":

    tgt_syst="Balloon"
    configfile="/var/datahub/config/bal_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/Balloon.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "ORF":

    tgt_syst="OracleFinancials"
    configfile="/var/datahub/config/orf_config_gl.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    #db_server_address=data["dbServerAddress"]
    #db_server_address="jdbc:oracle:thin:@gl50.atradiusnet.com"
    db_server_address="jdbc:oracle:thin:@lpora901.atradiusnet.com"
    #db_server_port=data["dbServerPort"]
    db_server_port="1523"
    #db_server_add=data["dbServerAdd"]
    #db_server_add="gl50.atradiusnet.com"
    db_server_add="lpora901.atradiusnet.com"
    #db_name = data["dbName"]
    db_name = "ORFP"
    db_user = "GLIFRS1"
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/Oracle_Financials.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "ALC":

    tgt_syst="Allocated_Capital"
    configfile="/var/datahub/config/alc_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()
 
    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/Allocated_Capital.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []


elif src_syst == "MIX":

    tgt_syst="MIX"
    configfile="/var/datahub/config/mix_config_oradmart1.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/MIX.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "SYR":

    tgt_syst="SymRRead"
    configfile="/var/datahub/config/syr_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/SymphonyRRead.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "NAV":

    tgt_syst="NAV"
    configfile="/var/datahub/config/nav_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    db_server_address=data["dbServerAddress"]
    db_server_port=data["dbServerPort"]
    db_server_add=data["dbServerAdd"]
    db_name = data["dbName"]
    db_user = data["dbUser"]
    keytab_file_datahub= data["keytab_file_datahub"]

    #print(db_server_address,db_server_port,db_server_add,db_name,db_user)

    config_table="/var/datahub/config/Navision1.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []

elif src_syst == "CIC":

    tgt_syst="CICS_Cyclope"
    configfile="/var/datahub/config/cic_config.json"
    f = open(configfile,)
    data1 = json.load(f)
    f.close()

    data = data1["Configs"]
    subdbname=data["subDBName"]
    keytab_file_datahub= data["keytab_file_datahub"]

    config_table="/var/datahub/config/CICS_Cyclope.json"
    tbl = open(config_table,)
    tbl_dtl = json.load(tbl)
    tbl.close()
    tables = []


accname="prdifrssl1"
acckey="cfxWBefZ3+GhkZiOm7MJK9M/qg868z4arP2gxuP/BLdbqIj9VN+Jps7gmF6moJ7zQEybMVMZWSxlOU4FgPMjJQ=="
#acckey=os.getenv("BLOB_KEY")



#function to run a unix command
def run_cmd(args_list):
    logger.debug(' '.join(args_list))
    proc = subprocess.Popen(args_list,stdout=subprocess.PIPE,stderr=subprocess.PIPE,shell=False)
    s_output,s_err = proc.communicate()
    s_return = proc.returncode
    return  s_return,s_output,s_err

#function to retrieve pid
def get_pid(keytab_file_datahub):
    cmd = ['klist', '-kt', '/etc/security/keytabs/' + keytab_file_datahub]
    ret, out, err = run_cmd(cmd)
    principle_id = out.decode("utf-8").rstrip().split(" ")[-1]
    if ret == 0:
        logger.info("PID for service account{1} is : {0}".format(principle_id, keytab_file_datahub))
    else:
        logger.error("Exit8, PID could not be retrieved due to {0}".format(err))
        exit(8)

    return principle_id

#function too authenticate service account
def auth_service_account(keytab_file_datahub, principle_id):
    cmd = 'kinit -kt /etc/security/keytabs/' + keytab_file_datahub+' '+ principle_id
    ret, out, err = run_cmd(shlex.split(cmd))
    if ret == 0:
        logger.info("Authenticated into service account: {0}".format(keytab_file_datahub))
    else:
        logger.error("Exit8, Authentication failed into service account: {0} due to : {1}".format(keytab_file_datahub, err))
        exit(8)

# To extract the datatypes of the columns from oracle source
def oracle_source_data():
    for table_dict in tbl_dtl['tables']:
        tablename=table_dict["tablename"]
        table_name=tablename.upper()

        if("upload" in table_dict.keys() and str(table_dict["upload"]) == "True"):
            tables.append(table_name)


    #if data["abbreviation"] == src_syst:
     #   db_password = os.getenv("{0}Pwd".format(src_syst.lower()))
    #else:
     #   logger.error("Exit8,src_system is not matching with the config provided")

    if src_syst=="DOR" or src_syst=="SYM":
        if data["abbreviation"] == src_syst:
            db_password = os.getenv("symPwdFDM")
        else:
            logger.error("Exit8,src_system is not matching with the config provided") 

    elif src_syst=="ORF":
        #db_password = "fg_99_O0"
        if data["abbreviation"] == src_syst:
            db_password = os.getenv("orfPwdFDM")
        else:
            logger.error("Exit8,src_system is not matching with the config provided")
    elif data["abbreviation"] == src_syst:
        db_password = os.getenv("{0}Pwd".format(src_syst.lower()))		 
    else:
        logger.error("Exit8,src_system is not matching with the config provided")

    constr = '{0}/{1}@{2}:{3}/{4}'.format(db_user, db_password, db_server_add,
                                              db_server_port,
                                              db_name)
    print(constr)

    connect = cx_Oracle.connect(constr)

    try:
        df_src = pd.DataFrame()
        for table in tables:

            if src_syst=="ORF":
                query = "SELECT table_name ,column_name ,"+ "          data_type " + "       || CASE "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) > 0 "+ "              THEN " + "                  '(' || data_precision || ',' || data_scale || ')' "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) = 0 "+ "              THEN " + "                  '(' || data_precision || ')' "+ "              WHEN data_precision IS NULL AND data_scale IS NOT NULL "+ "              THEN " + "                  '(*,' || data_scale || ')' "+ "              WHEN char_length > 0 " + "              THEN " + "                     '('"+ "                  || char_length " + "                  || ')'"+ "          END as DATA_TYPE" + "  FROM dba_tab_columns " + " WHERE table_name ='{0}'".format(table)
            else:
                query = "SELECT table_name ,column_name ,"+ "          data_type " + "       || CASE "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) > 0 "+ "              THEN " + "                  '(' || data_precision || ',' || data_scale || ')' "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) = 0 "+ "              THEN " + "                  '(' || data_precision || ')' "+ "              WHEN data_precision IS NULL AND data_scale IS NOT NULL "+ "              THEN " + "                  '(*,' || data_scale || ')' "+ "              WHEN char_length > 0 " + "              THEN " + "                     '('"+ "                  || char_length " + "                  || ')'"+ "          END as DATA_TYPE" + "  FROM all_tab_columns " + " WHERE table_name ='{0}'".format(table)

            df_ora = pd.read_sql(query, con=connect)
            df_src = df_src.append(df_ora)

        df_src = df_src.rename(columns={"TABLE_NAME": "SourceTable","COLUMN_NAME": "SourceColumn","DATA_TYPE": "SourceDataType"})
        logger.info("Successfully extracted the source schema details for all the tables")
        srcfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_source_data.csv"
        Source_csv = df_src.to_csv(srcfile, index = False, sep = "|")
        print(df_src)
        connect.close()
        return df_src

    except Exception as exc:
        print(exc)
        connect.close()
        sys.exit()


# To extract the datatypes of the columns from sql server source
def sql_source_data():

    server = '{0},{1}'.format(db_server_add, db_server_port)

    for table_dict in tbl_dtl['tables']:
        tablename=table_dict["tablename"]
        table_name=tablename.upper()

        if("upload" in table_dict.keys() and str(table_dict["upload"]) == "True"):
            tables.append(table_name)


    if data["abbreviation"] == src_syst:
        db_password = os.getenv("{0}Pwd".format(src_syst.lower()))
    else:
        logger.error("Exit8,src_system is not matching with the config provided")


    src_field_data = []

    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};ColumnEncryption=Enabled;SERVER=' + server + ';DATABASE=' + db_name + ';UID=' + db_user +
        ';PWD=' + db_password)

    tbl_list = []
    col_list = []
    datatype_list = []
    for i in tables:

        cursor = cnxn.cursor()
        query = 'SELECT COLUMN_NAME,* FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?;'
        data_params = (i)
        table_schema = cursor.execute(query, data_params)
        for row in cursor:
            column_name = '{0}'.format(row.COLUMN_NAME)
            column_type_src = row.DATA_TYPE.lower()
            column_precision = row.NUMERIC_PRECISION
            column_scale = row.NUMERIC_SCALE
            #src_field_data.append([i], [column_name], [column_type_src])
            tbl_list.append(i)
            col_list.append(column_name)
            datatype_list.append(column_type_src)


    src_field = [tbl_list, col_list, datatype_list]
    for tbl,col,typ in zip(*src_field):
        source = [tbl, col, typ]
        src_field_data.append(source)


    logger.info("Successfully extracted the source schema details for all the tables")
    cursor.close()
    cnxn.close()

    df_src = pd.DataFrame(src_field_data,columns=['SourceTable','SourceColumn','SourceDataType'])
    srcfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_source_data.csv"
    source_csv = df_src.to_csv(srcfile, index = True, sep = "|")
    print(df_src)
    logger.info("Successfully created the file for source data")

def nav_source_data():

    server = '{0},{1}'.format(db_server_add, db_server_port)
    
    for table_dict in tbl_dtl['tables']:        
        tablename=table_dict["tablename"]
        table_name=tablename
        
        if("consumption" in table_dict.keys() and str(table_dict["consumption"]) == "True"):
            tables.append(table_name)

    print(tables)        
    if data["abbreviation"] == src_syst:
        db_password = os.getenv("{0}Pwd".format(src_syst.lower()))
    else:
        logger.error("Exit8,src_system is not matching with the config provided")


    src_field_data = []
    
    cnxn = pyodbc.connect(
        'DRIVER={ODBC Driver 17 for SQL Server};ColumnEncryption=Enabled;SERVER=' + server + ';DATABASE=' + db_name + ';UID=' + db_user +
        ';PWD=' + db_password)


    #print(cnxn)
    tbl_list = []
    col_list = []
    datatype_list = []

    for i in tables:

        cursor = cnxn.cursor()
        query = 'SELECT COLUMN_NAME,* FROM INFORMATION_SCHEMA.COLUMNS WHERE TABLE_NAME = ?;'
        data_params = (i)
        table_schema = cursor.execute(query, data_params)
        for row in cursor:
            column_name = '{0}'.format(row.COLUMN_NAME)
            column_name = column_name.replace(" ","")
            column_name = column_name.replace("(","_")
            column_name = column_name.replace(")","_")
            column_name = column_name.replace("-","_")
            column_name = column_name.replace("%","_")
            column_name = column_name.replace(",","_")
            column_type_src = row.DATA_TYPE.lower()
            column_precision = row.NUMERIC_PRECISION
            column_scale = row.NUMERIC_SCALE
            #src_field_data.append([i], [column_name], [column_type_src])
            i = i.replace("Atradius Bonding$","")
            i = i.replace(" ","_")
            i = i.replace("__","_")
            tbl_list.append(i)
            col_list.append(column_name)
            datatype_list.append(column_type_src)


    src_field = [tbl_list, col_list, datatype_list]
    for tbl,col,typ in zip(*src_field):
        source = [tbl, col, typ]
        src_field_data.append(source)
      

    print(src_field_data)
    logger.info("Successfully extracted the source schema details for all the tables")
    cursor.close()
    cnxn.close()

    df_src = pd.DataFrame(src_field_data,columns=['SourceTable','SourceColumn','SourceDataType'])
    srcfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_source_data.csv"
    source_csv = df_src.to_csv(srcfile, index = True, sep = "|")
    print(df_src)
    logger.info("Successfully created the file for source data")

def informix_source_data():

    for table_dict in tbl_dtl['tables']:
        tablename=table_dict["tablename"]
        table_name=tablename

        if("consumption" in table_dict.keys() and str(table_dict["consumption"]) == "True"):
            tables.append(table_name)

    print(tables)
    if data["abbreviation"] == src_syst:
        db_password = os.getenv("{0}Pwd".format(src_syst.lower()))
    else:
        logger.error("Exit8,src_system is not matching with the config provided")


    src_field_data = []

    dsn_name='{0}'.format(subdbname)
    conn = pyodbc.connect(dsn_name + ';ColumnEncryption=Enabled')
    conn.setdecoding(pyodbc.SQL_WCHAR, encoding='UTF-8')
    conn.setdecoding(pyodbc.SQL_CHAR, encoding='UTF-8')
    conn.setencoding(encoding='UTF-8')


    tbl_list = []
    col_list = []
    datatype_list = []

    for i in tables:
        cursor = conn.cursor()
        query = "SELECT  TRIM(t.tabname) table, TRIM(c.colname) table_column, CASE WHEN MOD(coltype,256)=0 THEN 'CHAR'" \
                "WHEN MOD(coltype,256)=1 THEN 'SMALLINT' WHEN MOD(coltype,256)=2 THEN 'INTEGER' " \
                "WHEN MOD(coltype,256)=3 THEN 'FLOAT'   WHEN MOD(coltype,256)=4 THEN 'SMALLFLOAT' " \
                "WHEN MOD(coltype,256)=5 THEN 'DECIMAL' WHEN MOD(coltype,256)=6 THEN 'SERIAL' " \
                "WHEN MOD(coltype,256)=7 THEN 'DATE'   WHEN MOD(coltype,256)=8 THEN 'MONEY' " \
                "WHEN MOD(coltype,256)=9 THEN 'NULL' WHEN MOD(coltype,256)=10 THEN 'DATETIME' " \
                "WHEN MOD(coltype,256)=11 THEN 'BYTE'   WHEN MOD(coltype,256)=12 THEN 'TEXT' " \
                "WHEN MOD(coltype,256)=13 THEN 'VARCHAR' WHEN MOD(coltype,256)=14 THEN 'INTERVAL' " \
                "WHEN MOD(coltype,256)=15 THEN 'NCHAR'   WHEN MOD(coltype,256)=16 THEN 'NVARCHAR' " \
                "WHEN MOD(coltype,256)=17 THEN 'INT8' WHEN MOD(coltype,256)=18 THEN 'SERIAL8' " \
                "WHEN MOD(coltype,256)=19 THEN 'SET'   WHEN MOD(coltype,256)=20 THEN 'MULTISET' " \
                "WHEN MOD(coltype,256)=21 THEN 'LIST' WHEN MOD(coltype,256)=22 THEN 'ROW (unnamed)' " \
                "WHEN MOD(coltype,256)=23 THEN 'COLLECTION'   WHEN MOD(coltype,256)=40 THEN 'LVARCHAR fixed-length opaque types' " \
                "WHEN MOD(coltype,256)=41 THEN 'BLOB, BOOLEAN, CLOB variable-length opaque types'   " \
                "WHEN MOD(coltype,256)=43 THEN 'LVARCHAR (client-side only)'   WHEN MOD(coltype,256)=45 THEN 'BOOLEAN' WHEN MOD(coltype,256)=52 THEN 'BIGINT'   " \
                "WHEN MOD(coltype,256)=53 THEN 'BIGSERIAL'   WHEN MOD(coltype,256)=2061 THEN 'IDSSECURITYLABEL'   WHEN MOD(coltype,256)=4118 THEN 'ROW (named)' " \
                "ELSE TO_CHAR(coltype) END AS table_column_type, BITAND(coltype,256)=256 AS NotNull " \
                "FROM systables AS t JOIN syscolumns AS c ON t.tabid = c.tabid WHERE t.tabname = ? AND t.tabid >= 100"

        data_params = (i)
        cursor.execute(query, data_params)
        table_schema = cursor.fetchall()
        for j in table_schema:
            column_name = str(j[1])
            column_type = str(j[2])

            tbl_list.append(i)
            col_list.append(column_name)
            datatype_list.append(column_type)

    src_field = [tbl_list, col_list, datatype_list]
    print(src_field)
    for tbl,col,typ in zip(*src_field):
        source = [tbl, col, typ]
        src_field_data.append(source)


    logger.info("Successfully extracted the source schema details for all the tables")
    cursor.close()
    conn.close()

    df_src = pd.DataFrame(src_field_data,columns=['SourceTable','SourceColumn','SourceDataType'])
    srcfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_source_data.csv"
    source_csv = df_src.to_csv(srcfile, index = True, sep = "|" )
    print(df_src)
    logger.info("Successfully created the file for source data")


# To extract the datatypes of the tables from azure container
def target_data():
    target = []
    fol_name = str(exec_date)+"-"+str(run_no).zfill(4)
    print(fol_name)
    #accounts = {}
    accounts = dict(prdifrssl1=acckey)

    logger.info("Connecting to azure container for the target schema")
    print("Connecting to azure container for the target schema")
    for account, connection in zip(accounts, map(connection_str, accounts, accounts.values())):
        with azure.storage.blob.BlobServiceClient.from_connection_string(connection) as blob_service_client:
            try:
                for container in blob_service_client.list_containers():
                    if container.name.__contains__("prd-sl1-blob-container"):

                       run= date(rundate)
                       container_client = blob_service_client.get_container_client(container.name)
                       blob_list = container_client.list_blobs()

                       #To check the blob names for the given run date

                       for blob in blob_list:
                           #if blob.name.__contains__(tgt_syst) and blob.name.__contains__("000000_0") and blob.name.__contains__(f'{run}_RUN{run_no}'):
                           if blob.name.__contains__(tgt_syst) and blob.name.__contains__("000000_0") and blob.name.__contains__("Reporting") and blob.name.__contains__(f'{run}') and blob.name.__contains__(f'{fol_name}'):
                              tableName = blob.name.split("/")[8]
                              fileName = tableName + ".orc"
                              print(fileName)
                              #To break out from the loop for the additional orc files
                              if tableName.__contains__(f'{run}'):
                                 break

                              blobClient = container_client.get_blob_client(blob=blob.name)
                              with BytesIO() as f:
                                   blobClient.download_blob().readinto(f)
                                   reader = p.Reader(f)
                                   a = str(reader.schema).upper()
                                   a=a.replace("STRUCT<", "").replace(">","")
                                   l=re.split(r',\s*(?![^()]*\))|:',a)
                                   col_name = []
                                   data_type = []
                                   for i in range (0,len(l)):
                                      if i%2 == 0:
                                          col_name.append(l[i])
                                      else:
                                          typ=l[i]
                                          data_type.append(typ)
                                   table_struct = [col_name,data_type]
                                   for col_name,data_type in zip(*table_struct):

                                       table_target = [tableName,col_name,data_type]
                                       target.append(table_target)
                print("\nTARGET DATA CREATION STARTED..")
                df_tgt = pd.DataFrame(target)
                logger.info("Created the dataframe for the target schema")
                df_tgt.columns =['TargetTable', 'TargetColumn', 'TargetDataType']
                print(df_tgt)

                #Loading the target dataframe data into a csv file
                tgtfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_target_data.csv"
                target_csv = df_tgt.to_csv(tgtfile, sep = "|")
                return df_tgt

            except Exception as exc:
                print(exc)
                logger.error("Error occured:"+ str(exc))

#To create and upload the orc report to the azure container
'''def orc_upload_to_sl1():
    logger.info("Generating the orc file of the report")
    print("Generating the orc file of the report")

    outfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_comparison_data.csv"
    df_result = pd.read_csv(outfile, sep=",\s*(?![^()]*\)|)", engine="python" )
    print(df_result)
    df_result['SourceConfidentiality'] = df_result['SourceConfidentiality'].astype('str')
    print("Done")
    table = pa.Table.from_pandas(df_result, preserve_index=False)

    outorc="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_comparison_data.orc"
    orc.write_table(table, outorc)
    logger.info("Generated the orc file of the report")
    print("Generated the orc file of the report")

    MY_CONNECTION_STRING =  "DefaultEndpointsProtocol=https;AccountName=prdifrssl1;AccountKey=" + acckey + ";EndpointSuffix=core.windows.net"
    container_sl1 = "prd-sl1-blob-container"

    blob_local_path = "/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_comparison_data.orc"

    blob_service_client =  BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)

    blob_full_path = target_blob
    blob_client = blob_service_client.get_blob_client(container=container_sl1,
                                                          blob = blob_full_path)

    content_settings = ContentSettings(content_type='orc')

    upload_file_path = blob_local_path

    try:

        with open(upload_file_path, "rb") as data:
            blob_client.upload_blob(data,overwrite=True, content_settings=content_settings)
            print("Successfully uploaded")
        logger.info("Uploaded the field mapping report to the SL1 container")
    except Exception as err:
        print(err)
        logger.error("Error occured:"+ str(err))'''

def main():

    pid = get_pid(keytab_file_datahub)
    auth_service_account(keytab_file_datahub, pid)

    if src_syst=="DOR" or src_syst=="SYM" or src_syst=="ORF" or src_syst=="SYR" or src_syst=="MIX" or src_syst=="NST" or src_syst=="ALC" :

        print("SOURCE DATA CREATION STARTED..")
        oracle_source_data()

    elif src_syst=="LCT" or src_syst=="BAL" :

        print("SOURCE DATA CREATION STARTED..")
        sql_source_data()
    elif src_syst=="NAV":
        print("SOURCE DATA CREATION STARTED..")
        nav_source_data()
    elif src_syst=="CIC":
        print("SOURCE DATA CREATION STARTED..")
        informix_source_data()

        
    print("TARGET DATA CREATION STARTED..")
    target_data()

    try:
        srcfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_source_data.csv"
        df_src = pd.read_csv(srcfile, sep = "|")

        tgtfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_target_data.csv"
        df_tgt = pd.read_csv(tgtfile, sep = "|")


        logger.info("Started comparing the datatypes")
        print("Started comapring the datatypes")
        check_null=0
        df_result = pd.DataFrame()
        if len(df_src) >= len(df_tgt):
            for i in range(len(df_src)):
                src_table=df_src.loc[i, "SourceTable"]
                src_column=df_src.loc[i, "SourceColumn"]
                src_datatype=df_src.loc[i, "SourceDataType"]
                src_table=src_table.upper()
                src_column=src_column.upper()
                src_datatype=src_datatype.upper()

                for j in range(len(df_tgt)):

                    tgt_table=df_tgt.loc[j, "TargetTable"]
                    tgt_column=df_tgt.loc[j, "TargetColumn"]
                    tgt_datatype=df_tgt.loc[j, "TargetDataType"]
                    tgt_table=tgt_table.upper()
                    tgt_column=tgt_column.lower()
                    tgt_datatype=tgt_datatype.lower()

                    if src_table.upper()==tgt_table.upper() and src_column.upper()==tgt_column.upper() :

                        df_result=df_result.append({'SourceTable':src_table, 'SourceColumn':src_column, 'SourceDatatype':src_datatype, 'TargetTable':tgt_table, 'TargetColumn':tgt_column, 'TargetDatatype':tgt_datatype, 'SourceSystem':src_syst, 'SourceConfidentiality':'NULL', 'TargetSystem':tgt_syst},ignore_index=True)

                        check_null=1
                        break

                    else:
                        check_null=2

                if check_null==2:
                    df_result=df_result.append({'SourceTable':src_table, 'SourceColumn':src_column, 'SourceDatatype':src_datatype, 'TargetTable':'NULL', 'TargetColumn':'NULL', 'TargetDatatype':'NULL', 'SourceSystem':src_syst, 'SourceConfidentiality':'NULL', 'TargetSystem':tgt_syst},ignore_index=True)


        else:
            for i in range(len(df_tgt)):
                tgt_table=df_tgt.loc[i, "TargetTable"]
                tgt_column=df_tgt.loc[i, "TargetColumn"]
                tgt_datatype=df_tgt.loc[i, "TargetDataType"]
                tgt_table=tgt_table.upper()
                tgt_column=tgt_column.lower()
                tgt_datatype=tgt_datatype.lower()

                for j in range(len(df_src)):

                    src_table=df_src.loc[j, "SourceTable"]
                    src_column=df_src.loc[j, "SourceColumn"]
                    src_datatype=df_src.loc[j, "SourceDataType"]

                    src_table=src_table.upper()
                    src_column=src_column.upper()
                    src_datatype=src_datatype.upper()

                    if src_table.upper()==tgt_table.upper() and src_column.upper()==tgt_column.upper() :

                        df_result=df_result.append({'SourceTable':src_table, 'SourceColumn':src_column, 'SourceDatatype':src_datatype, 'TargetTable':tgt_table, 'TargetColumn':tgt_column, 'TargetDatatype':tgt_datatype, 'SourceSystem':src_syst, 'SourceConfidentiality':'NULL', 'TargetSystem':tgt_syst},ignore_index=True)

                        check_null=1
                        break

                    else:
                        check_null=2

                if check_null==2:
                    df_result=df_result.append({'SourceTable':'NULL', 'SourceColumn':'NULL', 'SourceDatatype':'NULL', 'TargetTable':tgt_table, 'TargetColumn':tgt_column, 'TargetDatatype':tgt_datatype, 'SourceSystem':src_syst, 'SourceConfidentiality':'NULL', 'TargetSystem':tgt_syst},ignore_index=True)

        logger.info("Data comparison completed successfully")
        print("Data comparison completed successfully")
        df_result = df_result.reindex(columns=["SourceSystem","SourceTable","SourceColumn","SourceDatatype","SourceConfidentiality","TargetSystem","TargetTable","TargetColumn","TargetDatatype"])
        print(df_result)
        
        table = pa.Table.from_pandas(df_result, preserve_index=False)

        outorc="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_comparison_data.orc"
        orc.write_table(table, outorc)

        logger.info("Generated the orc file of the report")
        print("Generated the orc file of the report")

        MY_CONNECTION_STRING =  "DefaultEndpointsProtocol=https;AccountName=prdifrssl1;AccountKey=" + acckey + ";EndpointSuffix=core.windows.net"
        container_sl1 = "prd-sl1-blob-container"

        blob_local_path = "/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_comparison_data.orc"

        blob_service_client =  BlobServiceClient.from_connection_string(MY_CONNECTION_STRING)

        blob_full_path = target_blob
        blob_client = blob_service_client.get_blob_client(container=container_sl1,
                                                          blob = blob_full_path)

        content_settings = ContentSettings(content_type='orc')

        upload_file_path = blob_local_path

        try:

            with open(upload_file_path, "rb") as data:
                blob_client.upload_blob(data,overwrite=True, content_settings=content_settings)
                print("Successfully uploaded")
                logger.info("Uploaded the field mapping report to the SL1 container")
        except Exception as err:
            print(err)
            logger.error("Error occured:"+ str(err))

        
        

        outfile="/var/datahub/prod/recon/Validation/SCRIPTS/DATATYPE_MAPPING/" + src_syst + "_comparison_data.csv"
    
                                           
        final_output = df_result.to_csv(outfile, index=False , sep = "|")

        with open(outfile, 'r+') as f:
            text = f.read()
            text = re.sub('\"', '', text)
            f.seek(0)
            f.write(text)
            f.truncate()

        #if orc_upload == "yes":
         #   orc_upload_to_sl1()
          #  print("Orc uploaded to the SL1")


    except Exception as exc:
        print(exc)
        logger.error("Error occured:"+ str(exc))

if __name__ == '__main__':
    main()
