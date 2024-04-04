import logging
import re
import subprocess
import datetime

import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, when

logger = logging.getLogger('SPX')


def run_cmd(args_list):
    proc = subprocess.Popen(' '.join(args_list), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    logger.debug(' '.join(args_list))
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    return s_return, s_output, s_err


def create_local_output_path(output_csv_path):
    cmd = ['!', '-d', output_csv_path]
    ret, out, err = run_cmd(cmd)
    if ret == 0:
        logger.info('Folder Not exist in {0}'.format(output_csv_path))
        cmd = ['mkdir', '-p', output_csv_path]
        ret, out, err = run_cmd(cmd)
        if ret == 0:
            logger.info('Folder created in {0}'.format(output_csv_path))
        else:
            logger.error("Exit8,Local Path cannot be created due to following error:{0}".format(err))
            exit(8)
    else:
        logger.info('Folder exist in {0}'.format(output_csv_path))


def create_hdfs_path(hdp_excel_path):
    cmd = ['hadoop', 'fs', '-test', '-e', hdp_excel_path]
    ret, out, err = run_cmd(cmd)
    if ret == 0:
        logger.info('Folder exist in {0}'.format(hdp_excel_path))
    else:
        cmd = ['hadoop', 'fs', '-mkdir', '-p', hdp_excel_path]
        (ret, out, err) = run_cmd(cmd)
        if ret == 0:
            logger.info('Successfully created : {0}'.format(hdp_excel_path))
        else:
            logger.error("Exit8,Path cannot be created due to following error:{0}".format(err))
            exit(8)


def copy_to_hdfs(csv_file_name, hdp_excel_path, sheet_name):
    csv_file_in_hdfs = hdp_excel_path + sheet_name + "_formatted.csv"

    logger.info("Checking the file {0} available in HDFS".format(csv_file_in_hdfs))
    cmd = ['hadoop', 'fs', '-test', '-e', csv_file_in_hdfs]
    ret, out, err = run_cmd(cmd)
    if ret == 0:
        logger.info("{0} file exists in provided path ".format(csv_file_in_hdfs))
        logger.info("Removing the file {0} in HDFS".format(csv_file_in_hdfs))
        cmd = ['hadoop', 'fs', '-rm', csv_file_in_hdfs]
        (ret, out, err) = run_cmd(cmd)
        if ret == 0:
            logger.info("File removed from HDFS for {0}".format(csv_file_in_hdfs))
        else:
            logger.error("Exit8, Unable to remove file from HDFS for {0} due to {1}".format(csv_file_in_hdfs, err))
            exit(8)
    else:
        logger.info("{0} file not exists in hadoop provided path for {1}".format(csv_file_name, hdp_excel_path))
    logger.info("Copying a file from local to hdfs using put command")
    cmd = ['hadoop', 'fs', '-put', csv_file_name, hdp_excel_path]
    (ret, out, err) = run_cmd(cmd)
    if ret == 0:
        logger.info("File copied from local : {0} to hdfs {1} using put command".format(csv_file_name,
                                                                                        hdp_excel_path))
    else:
        logger.error("Exit8, Unable to copy file from local : {0} to hdfs {1} due to {2}".format(csv_file_name,
                                                                                                 hdp_excel_path, err))
        exit(8)


def rename_avro(avro_table_path, sheet_name):
    logger.info("Renaming avro file")
    avro_file = avro_table_path + "/part*.avro"
    avro_file_rename = avro_table_path + "/" + sheet_name + ".avro"

    avro_files = get_files_list(avro_file).splitlines()
    for file in avro_files:
        rename(file, avro_file_rename)
    logger.info("Avro file renamed as :{0}".format(avro_file_rename))


def rename_orc(orc_table_path, sheet_name,time):
    logger.info("Renaming Orc file")
    orc_file = orc_table_path + "/part*.orc"
    time=time.replace(":", "-")
    orc_file_rename = orc_table_path + "/INIT_" + sheet_name+"_" +time+ ".snappy.orc"
    avro_files = get_files_list(orc_file).splitlines()
    for file in avro_files:
        rename(file, orc_file_rename)
    logger.info("Orc file renamed as :{0}".format(orc_file_rename))


def rename(original_file_name, new_filename):
    """This functions rename and a file in hadoop
    Still the command -mv used for renaming and moving
    """
    cmd = ['hdfs', 'dfs', '-mv', original_file_name, new_filename]
    ret, out, err = run_cmd(cmd)
    if ret == 0:
        logger.info('file renamed successfully from older name : {0} to new name : {1}'.format(original_file_name,
                                                                                               new_filename))
    else:
        logger.error("Exit8, Error failed while renaming the files on native zone:{0}".format(err))
        exit(8)


def get_files_list(files_path):
    """This function returns the files in the given location
        And it returns the files in list
    """
    logger.info("Acquiring the files in hadoop path for {0}".format(files_path))
    cmd = ['hdfs', 'dfs', '-ls', files_path, "|", " awk '{ print $8 }'"]
    ret, out, err = run_cmd(cmd)
    if ret == 0:
        logger.info("Files available in hadoop path for {0}".format(files_path))
    else:
        logger.error("Exit8, Files not available in hadoop path for {0} due to ".format(files_path, err))
        exit(8)
    return out

def run_cmd_conversion(args_list):

    proc = subprocess.Popen(' '.join(args_list), stdout=subprocess.PIPE, stderr=subprocess.PIPE, shell=True)
    s_output, s_err = proc.communicate()
    s_return = proc.returncode
    if s_return == 0:
        logger.debug("Command: {0} is executed successfully".format(args_list))
    else:
        logger.error("Command: {0} returned {1}".format(args_list,s_return))
    return s_return, s_output, s_err


def get_pid(keytab_file_datahub):
    cmd = ['klist', '-kt', '/etc/security/keytabs/'+keytab_file_datahub, "|", " awk 'END{print}'"]
    ret, out, err = run_cmd_conversion(cmd)
    pid= out.decode("utf-8").rstrip()
    if ret == 0:
        logger.info("PID for service account{1} is : {0}".format(pid,keytab_file_datahub))
    else:
        logger.error("Exit8,PID could not be retrieved due to {0}".format(err))
        exit(8)
    return pid

def auth_service_account(keytab_file_datahub, pid):
    cmd = ['kinit', '-kt', '/etc/security/keytabs/'+keytab_file_datahub, pid]
    ret, out, err = run_cmd_conversion(cmd)
    if ret == 0:
        logger.info("Authenticated into service account: {0}".format(keytab_file_datahub))
    else:
        logger.error("Exit8,Authentication failed into service account: {0} due to : {1}".format(keytab_file_datahub, err))
        exit(8)


def csv_to_csv(csv_output_file, filtered_csv, sheet_name):
    logger.info(" started for csv to csv")
    try:
        logger.info("Reading csv file : {0}".format(sheet_name))

        if sheet_name == "BOUND_POLICY_DATA":
            column_to_handle = 'Policy_Number'
            primary_key = 'Policy_count'

            data = pd.read_csv(csv_output_file, sep="|")

            data[column_to_handle] = data[column_to_handle].str.replace(r'[\s()]+', ' ')
            data[column_to_handle] = data[column_to_handle].str.rstrip()
            data = data.join(data[column_to_handle].str.split(' ', expand=True).add_prefix(column_to_handle))
            data.columns = data.columns.str.replace('.', '_dup')
            data = data.dropna(how='all')
            data = data.dropna(subset=[primary_key])
            columns_and_datatypes = list(data.columns.values.tolist())
            columns_to_convert = list(data.dtypes.values.tolist())
            for data_columns, data_types in zip(columns_and_datatypes, columns_to_convert):
                if not str(data_types).__contains__("float64"):
                    data[data_columns] = data[data_columns].str.replace(re.escape("\n"), " ")
            data.to_csv(filtered_csv, sep='|', header=True, index=False, encoding='utf-8')

        elif sheet_name == "OBLIGOR_DETAILS":
            column_to_handle = 'Policy_Number'
            column_to_handle_date = 'Risk_Data_input_Date'

            data = pd.read_csv(csv_output_file, sep="|")
            data[column_to_handle] = data[column_to_handle].str.replace(r'[\s()]+', ' ')
            data[column_to_handle] = data[column_to_handle].str.rstrip()
            data = data.join(data[column_to_handle].str.split(' ', expand=True).add_prefix(column_to_handle))
            data = data.join(data[column_to_handle_date].str.
                             extract('(?P<{0}0>\d+)?(?P<{0}1>\D+)?'.format(column_to_handle_date)).fillna(''))
            data = data.dropna(how='all')
            data = data.dropna(subset=[column_to_handle])
            data.to_csv(filtered_csv, sep='|', header=True, index=False, encoding='utf-8')
        logger.info("conversion ended for csv to csv")
        logger.info("Csv file read complete for : {0}".format(sheet_name))
    except Exception as e:
        logger.error(e)
        logger.exception("Error while running csv_to_csv : {0}".format(e))
        exit(8)


def csv_to_avro(csv_file_in_hadoop, avro_path, sheet_name):
    logger.info("spark started for csv to avro")
    try:
        spark = SparkSession.builder \
            .appName('sp_symphony_spcl_product_CsvToAvro') \
            .getOrCreate()
        df_final=''
        if sheet_name == "BOUND_POLICY_DATA":
            column_to_handle = 'Policy_Number'

            df_csv = spark.read.format("csv").option("header", "true").option("delimiter", "|").load(csv_file_in_hadoop)

            current_schema = df_csv.columns

            result = [i for i in current_schema if i.startswith(column_to_handle)]
            del result[0]

            for value in result:
                current_schema.remove(value)

            all_columns = []
            all_columns=get_cols_bpd(current_schema, column_to_handle, result, all_columns)
            csv_columns = ','.join('{0}'.format(w) for w in all_columns)  # convert list to string

            logger.info("Selecting final columns for writing : {0} for sheet :{1}".format(csv_columns,sheet_name))

            df_final = df_csv.select(csv_columns.split(","))

        elif sheet_name == "OBLIGOR_DETAILS":
            column_to_handle = 'Policy_Number'
            column_to_handle_date = 'Risk_Data_input_Date'
            df_csv = spark.read.format("csv").option("header", "true").option("delimiter", "|").load(csv_file_in_hadoop)
            df_csv = df_csv.withColumn("{0}0".format(column_to_handle_date),
                                       when(df_csv["{0}0".format(column_to_handle_date)] != "",
                                            df_csv[column_to_handle_date]).otherwise(
                                           df_csv["{0}0".format(column_to_handle_date)]))

            current_schema = df_csv.columns

            result = [i for i in current_schema if i.startswith(column_to_handle)]
            del result[0]

            result1 = [i for i in current_schema if i.startswith(column_to_handle_date)]
            del result1[0]

            for value in result:
                current_schema.remove(value)

            for value in result1:
                current_schema.remove(value)

            all_columns = []
            all_columns=get_cols_ob(current_schema, column_to_handle, result, all_columns, column_to_handle_date, result1)
            csv_columns = ','.join('{0}'.format(w) for w in all_columns)  # convert list to string

            logger.info("Selecting final columns for writing : {0}".format(csv_columns))

            df_final = df_csv.select(csv_columns.split(","))
            df_final=df_final.withColumnRenamed("Risk_Data_input_Date0", "Risk_Data_input_Date")
            df_final=df_final.withColumnRenamed("Risk_Data_input_Date1", "Risk_Data_input_comments")

        df_final.coalesce(1).write.mode("overwrite").format("com.databricks.spark.avro").save(avro_path)
        logger.info("spark ended for csv to avro")
    except Exception as e:
        logger.error(e)
        logger.exception("Error while running csv_to_avro : {0}".format(e))
        exit(8)

def get_cols_bpd(current_schema,column_to_handle,result,all_columns):
    for eachvalue in current_schema:
        if eachvalue == column_to_handle:
            for req_columns in result:
                all_columns.append(req_columns)
        else:
            all_columns.append(eachvalue)
    return all_columns
def get_cols_ob(current_schema,column_to_handle,result,all_columns,column_to_handle_date,result1):
    for eachvalue in current_schema:
        if eachvalue == column_to_handle:
            for req_columns in result:
                all_columns.append(req_columns)
        elif eachvalue == column_to_handle_date:
            for req_columns in result1:
                all_columns.append(req_columns)
        else:
            all_columns.append(eachvalue)
    return all_columns

def avro_to_orc(avro_path, orc_path, sheet_name):
    logger.info("spark started for avro to orc")
    try:
        spark = SparkSession.builder \
            .appName('sp_symphony_spcl_product_Avro_to_orc') \
            .getOrCreate()

        new_columns = []
        new_columns_date = []
        data_type = "Decimal (38,12)"
        data_type_date = "DATE"

        if sheet_name == "BOUND_POLICY_DATA":
            new_columns = ["Net_line_amount_Curr", "Net_line_amount_EUR", "Gross_line_amount_EUR",
                           "Min_premium_Curr", "Min_premium_EUR", "Est_premium_Curr", "Est_premium_EUR",
                           "Earned_premium_per_day_EUR", "Portfolio_value_EUR", "Brokerage_EUR", "Closing_exchange_rate", "Indemnity",
                           "Brokerage_percent","Ceding_Commission_percent"]

            new_columns_date = ["Inception_date", "Cover_start", "Cover_end", "Credit_Limit_Review_Date",
                                "Policy_Expiry_Date","Final_Checklist_Completed_Date"]
        elif sheet_name == "OBLIGOR_DETAILS":
            new_columns = ["Net_C_L_amount_Curr", "Net_C_L_amount_EUR","Exchange_rate"]
            new_columns_date = ["Off_risk_date", "Risk_Data_input_Date"]

        df_avro = spark.read.format("com.databricks.spark.avro").load(avro_path)
        time = datetime.datetime.now().strftime("%Y-%m-%d %H-%M-%S")
        df_adding_three_columns = df_avro.withColumn("op_ts", lit(time)) \
            .withColumn("pos", lit("0000000001")).withColumn("op_type", lit("I"))
        current_avro_schema = df_avro.columns

        req_transaction_columns = ["op_ts", "pos", "op_type"]

        all_columns = req_transaction_columns

        for eachvalue in current_avro_schema:
            all_columns.append(eachvalue)

        avro_columns = ','.join('{0}'.format(w) for w in all_columns)  # convert list to string

        logger.info("Selecting final columns for writing into orc : {0}".format(avro_columns))

        df_read_avro_init_final = df_adding_three_columns.select(avro_columns.split(","))

        logger.info("Casting column datatypes to {0} for {1}".format(data_type, new_columns))

        for column_name in new_columns:
            df_read_avro_init_final = df_read_avro_init_final.withColumn(column_name, col(column_name).cast(data_type))

        logger.info("Casting column datatypes to {0} for {1}".format(data_type_date, new_columns_date))

        for column_name_date in new_columns_date:
            df_read_avro_init_final = df_read_avro_init_final.withColumn(column_name_date,
                                                                         col(column_name_date).cast(data_type_date))
        if sheet_name=="BOUND_POLICY_DATA":
            df_read_avro_init_final = df_read_avro_init_final.withColumn("Tenor_days", col("Tenor_days").cast("BIGINT"))
            logger.info("Casting column to BIGINT for Tenor_days for {0}".format(sheet_name))

        logger.info("Writing data into Orc")

        df_read_avro_init_final.coalesce(1).write.mode("append").orc(orc_path)

        nz_count = df_read_avro_init_final.count()

        logger.info("Records count for {0} : {1}".format(sheet_name, nz_count))

        logger.info("spark ended for avro to orc")
    except Exception as e:
        logger.error(e)
        logger.exception("Error while running avro_to_orc : {0}".format(e))
        exit(8)
