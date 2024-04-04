import pandas as pd
from pyspark.sql.functions import *
from pyspark.sql import SparkSession
from pyspark.sql.types import *

fileName = "Test"
filePath = f"provide path"

def insert_record(df,row):
    insert_loc=df.index.max()
    if pd.isna(insert_loc):
        df.loc[0] = row
    else:
        df.loc[insert_loc+1] = row


from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pandas as pd

schema = "provide schema name"
s = spark.sql("show tables in {0}".format(schema))
tables = s.collect()

df=pd.DataFrame(columns = ['tablename','count'])
df = df.astype( dtype={'tablename':str,'count':int})

for row in tables:
    s=spark.read.table(schema+"."+row.tableName)
    ctr =s.count()
    #print(row.tableName)
    #print(ctr)

    insert_record(df, [row.tableName,ctr])


DF2 = spark.createDataFrame(df)
#df.to_csv("path"+output_filename,index = False, header=True)
DF2.show()

def export_csv(df, fileName, filePath):
    filePathDestTemp = filePath + ".dir/" 
    df\
    .write.options(header='True')\
    .csv(filePathDestTemp) # use .csv to save as csv
    listFiles = dbutils.fs.ls(filePathDestTemp)
    for subFiles in listFiles:
        if subFiles.name[-4:] == ".csv":
            dbutils.fs.cp (filePathDestTemp + subFiles.name,  filePath + fileName+ '.csv')
    dbutils.fs.rm(filePathDestTemp, recurse=True)

export_csv(DF2,fileName,filePath)
