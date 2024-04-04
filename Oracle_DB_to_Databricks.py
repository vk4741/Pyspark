%sh
pip install oracledb

from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import IntegerType,StringType,StructType,StructField

import pandas as pd
import oracledb
import os
import datetime as dt
spark = SparkSession.builder.appName("source-connectivity").getOrCreate()

source1_user = dbutils.secrets.get(scope = '', key = '')
source1_passwd = dbutils.secrets.get(scope = '', key = '')

# To get the parameters at the run time
def params_input():
  try:
    src_syst=str(dbutils.widgets.get('src_syst').strip())
    return src_syst
  except Exception as e:
    print("Please enter the required arguments to run the script")


#To extract the datatypes of the columns from sql server source
def sql_source_data(src_syst):
  if src_syst == "source1":
    database_host = "00.00.0.00"
    database_port = "8080"
    database_name = "dbname"
    user = source1user
    password = source1pwd

  catalog_target_path = f"{catalog_name}.{src_syst.lower()}_source_data"
  try:
    all_src_df = spark.read.option('header',True).json("dbfs:/path of json")
    tables_src_df = all_src_df.filter(all_src_df.source == src_syst).select("table_list")
    for row in tables_src_df.collect():
      tables_list = row['table_list']
    driver = "com.microsoft.sqlserver.jdbc.SQLServerDriver"
    db_table = "INFORMATION_SCHEMA.COLUMNS"
    url = f"jdbc:sqlserver://{database_host}:{database_port};database={database_name};encrypt=true;trustServerCertificate=true"

    read_schema_table = (spark.read
        .format("jdbc")
        .option("driver", driver)
        .option("url", url)
        .option("dbtable", db_table)
        .option("user", user)
        .option("password", password)
        .load()
        )
    df_sch = StructType([StructField('SourceTable',StringType(),True),StructField('SourceColumn',StringType(),True),StructField('SourceDataType',StringType(),True)])
        final_src_df = spark.createDataFrame([],df_sch)
        for table in tables_list:
            src_df = read_schema_table.where(read_schema_table.TABLE_NAME == table).select("TABLE_NAME","COLUMN_NAME","DATA_TYPE")
            required_src_df = src_df.withColumn('DATA_TYPE', lower(col('DATA_TYPE'))).withColumnRenamed("TABLE_NAME","SourceTable").withColumnRenamed("COLUMN_NAME","SourceColumn").withColumnRenamed("DATA_TYPE","SourceDataType")
            final_src_df = final_src_df.union(required_src_df)
        final_src_df.write.saveAsTable(name = catalog_target_path,  mode = "overwrite")
        print(f"{src_syst} Source data created")
   except Exception as e:
        print("Error occured while creating the comparison data:" + str(e))
        sys.exit()



def oracle_source_data(src_syst):
    
    if src_syst=="DOR" or src_syst=="SYM" or src_syst=="BRX_SYM" :
        user=sym_username
        password=sym_passwd
        db_host = "10.20.14.103"
        db_port = "1519"
        db_name = "SYMF.atradiusnet.com"
        
    elif src_syst == "NST":
        user=nst_username
        password=nst_passwd
        db_host = "10.20.13.242"
        db_port = "1562"
        db_name = "BFBN.atradiusnet.com"

    elif src_syst == "SYR":
        user=syr_username
        password=syr_passwd
        db_host = "10.20.13.236"
        db_port = "1562"
        db_name = "SYMR.atradiusnet.com"

    else:
        print(f"{src_syst} src_system is not matching with the config provided")

    dsn_connection = f"{db_host}:{db_port}/{db_name}"
    catalog_target_path = f"{catalog}.field_data_mappings.{src_syst.lower()}_source_data"
    try:
        all_src_df = spark.read.option('header',True).json("dbfs:/databricks/field_mapping_Json/All_srcs.json")
        tables_src_df = all_src_df.filter(all_src_df.source == src_syst).select("table_list")
        for row in tables_src_df.collect():
            tables = row['table_list']
        
        if src_syst=="BRX_SYM":
            tables.remove('MAPA_CONTROLE')
            tables.remove('SINISTROS_PENDENTES')
            tables.remove('CALCULO_RESERVAS_DIREITO_CRED')
        
        connection = oracledb.connect(user=user, password=password, dsn= dsn_connection)
        df_src = pd.DataFrame()
        for table in tables:

            if src_syst=="ORF":
                query = "SELECT table_name ,column_name ,"+ "          data_type " + "       || CASE "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) > 0 "+ "              THEN " + "                  '(' || data_precision || ',' || data_scale || ')' "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) = 0 "+ "              THEN " + "                  '(' || data_precision || ')' "+ "              WHEN data_precision IS NULL AND data_scale IS NOT NULL "+ "              THEN " + "                  '(*,' || data_scale || ')' "+ "              WHEN char_length > 0 " + "              THEN " + "                     '('"+ "                  || char_length " + "                  || ')'"+ "          END as DATA_TYPE" + "  FROM dba_tab_columns " + " WHERE table_name ='{0}'".format(table)
            else:
                query = "SELECT table_name ,column_name ,"+ "          data_type " + "       || CASE "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) > 0 "+ "              THEN " + "                  '(' || data_precision || ',' || data_scale || ')' "+ "              WHEN data_precision IS NOT NULL AND NVL (data_scale, 0) = 0 "+ "              THEN " + "                  '(' || data_precision || ')' "+ "              WHEN data_precision IS NULL AND data_scale IS NOT NULL "+ "              THEN " + "                  '(*,' || data_scale || ')' "+ "              WHEN char_length > 0 " + "              THEN " + "                     '('"+ "                  || char_length " + "                  || ')'"+ "          END as DATA_TYPE" + "  FROM all_tab_columns " + " WHERE table_name ='{0}'".format(table)

            df_ora = pd.read_sql(query, con=connection)
            df_src = df_src.append(df_ora)

        df_src = df_src.rename(columns={"TABLE_NAME": "SourceTable","COLUMN_NAME": "SourceColumn","DATA_TYPE": "SourceDataType"})
        print(f"{src_syst} Successfully extracted the source schema details for all the tables")
        connection.close()
        spark_df = spark.createDataFrame(df_src)
        spark_df.write.saveAsTable(name = catalog_target_path,  mode = "overwrite")

    except Exception as exc:
        print(exc)
        connection.close()
        sys.exit()

def main():
    src_sys = params_input()
    src_systems = ["NAV", "LCT", "SYM", "SYR","BAL","DOR","BRX_SYM","ORF","NST"]

    if(len(src_sys) > 0):
        try:
            for src_syst in src_sys:
                if src_syst=="DOR" or src_syst=="SYM" or src_syst=="SYR" or src_syst=="NST" or src_syst=="BRX_SYM"  :

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    oracle_source_data(src_syst)

                elif src_syst=="LCT" or src_syst=="BAL" :

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    sql_source_data(src_syst)
                elif src_syst=="NAV":

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    nav_source_data()

                elif src_syst=="ORF":
                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    orf_source_data()
        except Exception as e:
            print("Error occured :" + str(e))
    else:
        try:
            for src_syst in src_systems:
                if src_syst=="DOR" or src_syst=="SYM" or src_syst=="SYR" or src_syst=="NST" or src_syst=="BRX_SYM"  :

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    oracle_source_data(src_syst)

                elif src_syst=="LCT" or src_syst=="BAL" :

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    sql_source_data(src_syst)
                elif src_syst=="NAV":

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    nav_source_data()
                elif src_syst=="ORF":

                    print(f"{src_syst} SOURCE DATA CREATION STARTED..")
                    orf_source_data()
        except Exception as e:
            print("Error occured :" + str(e))


  if __name__ == '__main__':
    main()

  
      
