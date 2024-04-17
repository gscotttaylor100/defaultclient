# Databricks notebook source
# MAGIC %run "../Schemas/orders_schema" 

# COMMAND ----------

# MAGIC %md
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC Azure Settings

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

#Storage Account Name - 
adls_account = "storaccentdatamarts01"

#Storage Container Name
bronze_container = 'bronze01'
silver_container = 'silver01'
gold_container = 'gold01'

#The subscription where the Resource group is located
sp_subscriptionId = "US - Managed Enterprise Production"


entity = 'orders'
databaseName = 'datamarts'
log_path = 'abfss://common@storaccentdatamarts01.dfs.core.windows.net/table_logs/orders/orders_log'
bronze_source_path = 'abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_bronze'
bronze_rejects_path = 'abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_bronze_rejects'
bronze_duplicates_path = 'abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_bronze_duplicates'



table_log_name = 'datamarts.orders_log'
databasetable = databaseName + '.' + 'bronze_' + entity

#set the batch timestamp
BatchTimestamp = datetime.datetime.now()

#create the hive database
spark.sql('CREATE DATABASE IF NOT EXISTS {}'.format(databaseName))


# COMMAND ----------

def file_exists(path):
    try:
        dbutils.fs.ls(path)
        return True
    except Exception as e:
        if 'java.io.FileNotFoundException' in str(e):
            return False
        else:
            raise

# COMMAND ----------

# MAGIC %md
# MAGIC Get SAS Token from key vault for Storage Account

# COMMAND ----------

#Get SAS Token from key vault and establish a connection to ADLS
spark.conf.set("fs.azure.account.auth.type.storaccentdatamarts01.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storaccentdatamarts01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storaccentdatamarts01.dfs.core.windows.net", dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="storage-account-sas-code03"))

# COMMAND ----------

# MAGIC %md
# MAGIC Initialize Auto Loader for Checkpoints in Raw Zone
# MAGIC

# COMMAND ----------

# Import functions
from pyspark import Row
from delta.tables import *
from pyspark.sql.functions import *
from pyspark.sql.types import *

# Define variables used in code below
sp_subscriptionId = dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="subscription-prod-01")
sp_conn_string = dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="sp-conn-stringn01")
sp_tenantId = dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="tenant-id-prod-01")
sp_clientId = dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="databricks-autoloader-app-id")
sp_clientKey = dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="databricks-server-principal")
sp_rgName = "enterprisedatamarts01-prd-rg01"

file_path = "abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/raw/*/*/*/*/*.txt"

checkpoint_path = "abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_checkpoint"
staging_path = "abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_staging"

#schema from **FILE*** coming in from message queue. 
#this schema applies to incoming messages in file format.

# cloudFile settings
cloudFilesConf = {
  "cloudFiles.subscriptionId": sp_subscriptionId,
  "cloudFiles.format": "text",
  "cloudFiles.tenantId": sp_tenantId,
  "cloudFiles.clientId": sp_clientId,
  "cloudFiles.clientSecret": sp_clientKey,
  "cloudFiles.resourceGroup": sp_rgName,
  "cloudFiles.useNotifications": "true",
  "cloudFiles.includeExistingFiles": "true",
  "cloudFiles.validateOptions": "true"  
}

# Configure Auto Loader to ingest new message data to a Delta table
message_stream_df = (spark.readStream.format("cloudFiles")
    .schema("message STRING")   
    .options(**cloudFilesConf)
    .option("checkpointLocation", checkpoint_path)
    .option("recursiveFileLookup", "true")
    .option("ignoreMissingFiles", "true")
    .option("Header", "False")
    .load(file_path))


autoloader = (message_stream_df.writeStream.format("delta")
  .outputMode("append")
  .option("checkpointLocation", checkpoint_path)
  .trigger(once=True)
  .start(staging_path))

#wait until delta table is populated.
autoloader.awaitTermination()


# COMMAND ----------

# MAGIC %md
# MAGIC Load Incoming Orders and Perform Delimiter Checks
# MAGIC
# MAGIC

# COMMAND ----------

#read in the staging table from Autoloader. 
df_new_msg = spark.read.format('delta').load(staging_path)

if (df_new_msg.count() == 0): 
    dbutils.notebook.exit('stop')

source_record_count = df_new_msg.count()
display(source_record_count)
#display(df_new_msg)

# COMMAND ----------

# Read in the incoming data, perform initial checks on Orders data.# 
# Apply Orders schema to transaction data #

import os
from pyspark.sql.functions import input_file_name
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import *
from pyspark.sql.functions import sha2, concat_ws


#split the orders data by the ^ delimter. place that string and the 
#delimter count in a data frame.
df_1 = df_new_msg.withColumn("delimiter_count", F.size(F.split("message",r"\^") )-1)


#wrong # of delimters. write out to reject table. 
df_bad_delim = df_1.filter(df_1.delimiter_count != 655)
#display(df_bad_delim)

df_bad_delim_count = df_bad_delim.count()

#anything with bad # of delimiters, write it out to the reject table
#with the header information
df_reject =  (df_bad_delim.withColumn("BATCH_TIMESTAMP", F.lit(BatchTimestamp))
             .withColumn("dm_source_system",F.trim(F.substring('message', 1,30)))
             .withColumn("dm_name",F.trim(F.substring('message', 31,30)))
             .withColumn("dm_source_div",F.trim(F.substring('message', 61,6)))
             .withColumn("dm_batch_id",F.trim(F.substring('message', 67,14)))
             .withColumn("dm_action",F.trim(F.substring('message', 81,3)))
             .withColumn("dm_version",F.trim(F.substring('message', 84,3)))
             .withColumn("dm_key",F.trim(F.substring('message', 87,150)))      
             .withColumn("error_message",F.lit("Invalid Number of Delimiters in File. The delimiter count should be 655.").cast(StringType()))
             .drop('delimiter_count')
             .drop('message')
            )

#rejects for bad delimiter
if file_exists(bronze_rejects_path):
        if(df_reject.count() > 0):
            df_reject.write.format('delta').mode('append').option('path', bronze_rejects_path).saveAsTable('datamarts.orders_bronze_rejects')
else:
        if(df_reject.count() > 0):
            df_reject.write.format('delta').mode('overwrite').option('path', bronze_rejects_path).saveAsTable('datamarts.orders_bronze_rejects')
    
    
# place all rows with correct # of delimters
# in seperate data frame.
df_2 = df_1.filter(df_1.delimiter_count == 655)
#display(df_2)

#next load the schema for all data that has the correct # of delimiters
#from the above dataframe, split the data again and apply orders schema.

df1 = df_2.select(F.split(df_2.message,r"\^")).rdd.flatMap(lambda x: x).toDF(schema=orders_schema)


#df1.printSchema()
#print(orders_schema)
#display(df1)
#df1.count()


# COMMAND ----------

# MAGIC %md
# MAGIC Create the header for the record along with the Hash Key

# COMMAND ----------

#Build the Header for the record. Validate the header, then create the Hash Key#
import pyspark.sql.functions as F
from pyspark.sql import Window


#take the order information above, without the full header, add the action
#the action field along with orders columns makes up the hash_key
df_hash = (df1.withColumn("dm_action",F.substring('header', 81,3))
              .drop ('header')
          ) 

#display(df_hash)

# #build final dataframe with hashkey. 
# #Add the error messages columns here for later validation.
df = (df1.withColumn("dm_source_system",F.trim(F.substring('header', 1,30)))
          .withColumn("dm_name",F.trim(F.substring('header', 31,30)))
          .withColumn("dm_source_div",F.trim(F.substring('header', 61,6)))
          .withColumn("dm_batch_id",F.trim(F.substring('header', 67,14)))
          .withColumn("dm_action",F.trim(F.substring('header', 81,3)))
          .withColumn("dm_version",F.trim(F.substring('header', 84,3)))
          .withColumn("dm_key",F.trim(F.substring('header', 87,150)))      
          .withColumn("sales_ord_no",F.trim(F.substring('header', 247,100)))
          .withColumn("hash_key", sha2(concat_ws("^", *df_hash.columns), 256))
          .withColumn("error_message",F.lit(None).cast(StringType()))
          .withColumn("error_message_concat",F.lit(None).cast(StringType()))
          .withColumn("BATCH_TIMESTAMP", F.lit(BatchTimestamp))
          .withColumn("FullLoadDivision", F.lit('N'))
          .drop('header')
      )

###convert any empty strings to null
df = df.transform(convert_empty_string_to_null())

#change the action type of PUR to DEL for deletes
df = df.withColumn("dm_action", when(df.dm_action == "PUR", "DEL").otherwise(df.dm_action))


df = df.withColumn("error_message", F.when((F.col('dm_action') == 'ADD') &
                                            (F.isnull(F.col('dm_source_system')) | (F.col('dm_source_system') == '0') |
                                            F.isnull(F.col('dm_source_div')) | (F.col('dm_source_div') == '0') |
                                            F.isnull(F.col('dm_batch_id')) |  (F.col('dm_batch_id') == '0') | 
                                            F.isnull(F.col('dm_version')) |  (F.col('dm_version') == '0') |
                                            F.isnull(F.col('dm_key')) | (F.col('dm_key') == '0') |
                                            F.isnull(F.col('sales_ord_no')) | (F.col('sales_ord_no') == '0')|
                                            F.isnull(F.col('dm_action')) | (F.col('dm_action') == '0')|
                                            F.isnull(F.col('dm_name')) | (F.col('dm_name') == '0')) 
                                            ,F.lit("Bad Header Row. Data is missing in Header fields.")).otherwise(None))


df = df.withColumn("error_message", F.when((F.col('dm_action') == 'DEL') &
                                            (F.isnull(F.col('dm_source_div')) | (F.col('dm_source_div') == '0') |
                                            F.isnull(F.col('dm_key')) | (F.col('dm_key') == '0'))
                                            ,F.lit("Bad Header Row. Data is missing in Header fields.")).otherwise(None))


#bad_header_df = df.select(F.length(F.trim(F.col('error_message'))) > 0)

bad_header_df = df.filter(df.error_message.isNotNull())

#write bad headers to rejects table.
if file_exists(bronze_rejects_path):
     if(bad_header_df.count() > 0):
            df.select(['BATCH_TIMESTAMP',
                           'dm_source_system','dm_name',
 'dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','error_message']).filter(df.error_message.isNotNull()).write.format("delta").mode("append").option('path', bronze_rejects_path).saveAsTable('datamarts.orders_bronze_rejects')
else:
     if(bad_header_df.count() > 0):
            df.select(['BATCH_TIMESTAMP',
                           'dm_source_system','dm_name',
'dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','error_message']).filter(df.error_message.isNotNull()).write.format("delta").mode("overwrite").option('path', bronze_rejects_path).saveAsTable('datamarts.orders_bronze_rejects')

#count before dup check
#display(df.count())        

# #check for dups coming in from MQ. 
#w = Window.partitionBy('dm_source_div','dm_key', 'hash_key')
#duplicates_df = df.select('*', F.count("dm_key").over(w).alias('dm_key_dups'))\
#      .where('dm_key_dups > 1')\
#      .drop('dm_key_dups')


w = Window.partitionBy('dm_source_div','dm_key', 'hash_key')
duplicates_df = df.select('dm_source_system','dm_name','dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','hash_key','BATCH_TIMESTAMP',F.count("dm_key").over(w).alias('dm_key_dups'))\
      .where('dm_key_dups > 1')\
      .drop('dm_key_dups')

# #***********************
# #remove those duplicates.
#duplicates_df = duplicates_df.drop('error_message_concat')

#### #write out any duplicates that just came in.
if file_exists(bronze_duplicates_path):
           if(duplicates_df.count() > 0):
            duplicates_df.select(['dm_source_system','dm_name','dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','hash_key','BATCH_TIMESTAMP']).distinct()\
                .write.format("delta").mode("append").option('path', bronze_duplicates_path).saveAsTable('datamarts.orders_bronze_duplicates')
else:
           if(duplicates_df.count() > 0):
            duplicates_df.select(['dm_source_system','dm_name','dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','hash_key','BATCH_TIMESTAMP']).distinct()\
                .write.format("delta").mode("overwrite").option('path', bronze_duplicates_path).saveAsTable('datamarts.orders_bronze_duplicates')

#distinct count of duplicates
dup_count = duplicates_df.distinct().count()

# #drop duplicates coming from messge queue to continute on.
df = df.dropDuplicates(subset=['dm_source_div','dm_key','hash_key'])

df = df.filter((F.col('error_message').isNull()))

##########commented displays#############
#display(df.filter(df.dm_action == 'DEL'))
#display(duplicates_df)
#display(dup_count)
#display(df.count())
#display(duplicates_df.filter(duplicates_df.hash_key == '23d008ff557f4accd11e982e6547bd9eecceb1957331ed12c21f38638373b5d8'))
#display(df.filter(df.hash_key == '23d008ff557f4accd11e982e6547bd9eecceb1957331ed12c21f38638373b5d8'))


# COMMAND ----------

# MAGIC %md Check the Data Types
# MAGIC

# COMMAND ----------

from pyspark.sql.functions import *
from pyspark.sql.types import *


#check data types in order bronze. 
#remove rows from final table where fields cannot be cast to the target data type
#write the bad rows to the reject table

#load the data types from the lookup
datatype_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/datatypecheck/OrderRecordDataTypes.csv").distinct()

#decimals
decimal_rows = datatype_df.select(trim("FIELD_NAME"), trim("DATA_TYPE")).filter(datatype_df.DATA_TYPE == "Decimal").collect()
for row in decimal_rows:
    df = df.withColumn("error_message", F.when((F.length(F.trim(F.col(row[0]))) > 0) & (F.isnull(F.col(row[0]).cast(row[1]))),F.lit("Data Invalid for Column " + row[0] + "Value: " + F.col(row[0]) + " Cannot be cast to decimal type."))
        .otherwise(None))
    df = df.withColumn("error_message_concat",F.concat_ws(",",F.col("error_message"),F.col("error_message_concat")))
    
#dates
#try to cast each value as a date. If it can't it will return null
#if null, flag as invalid. Also check length of field. 
datetime_rows = datatype_df.select(trim("FIELD_NAME")).filter(datatype_df.DATA_TYPE == "Datetime").collect()
for row in datetime_rows:
    df = df.withColumn("error_message", F.when(F.isnull(to_date(F.substring(F.col(row[0]),1,8),"yyyyMMdd")),F.lit("Invalid Date Format for: " + row[0]))
    .when((F.length(F.trim(F.col(row[0]))) > 0) & (F.length(F.trim(F.col(row[0]))) < 14),F.lit("Invalid Date Format for: " + row[0]))
    .otherwise(None))
    df = df.withColumn("error_message_concat",F.concat_ws(",",F.col("error_message"),F.col("error_message_concat")))


#texts and timezones
#text_rows = datatype_df.select(trim("FIELD_NAME"), trim("LENGTH").cast("int")).filter(datatype_df.DATA_TYPE == "Text").collect()
#for row in text_rows:
#    df.withColumn(row[0], when((F.length(F.trim(F.col(row[0]))) == 0),F.lit(None)).otherwise(F.substring(row[0], 1, row[1])))   
    # df.withColumn(row[0], F.substring(row[0], 1, row[1]))
    

df = df.drop('error_message').withColumnRenamed('error_message_concat','error_message')


#get a count of the bad data type records.
df_bad_data_type = df.filter(F.length(F.trim(F.col('error_message'))) > 0)
#display(df_bad_data_type.count())

df_bad_data_type_count = df_bad_data_type.count()


#write bad data types to rejects table.
if file_exists(bronze_rejects_path):
    if(df_bad_data_type.count() > 0):
        df.select(['BATCH_TIMESTAMP',
                          'dm_source_system','dm_name',
'dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','error_message']).filter(F.length(F.trim(F.col('error_message'))) > 0).write.format("delta").mode("append").option('path', bronze_rejects_path).saveAsTable('datamarts.orders_bronze_rejects')
else:
    if(df_bad_data_type.count() > 0):
        df.select(['BATCH_TIMESTAMP',
                          'dm_source_system','dm_name',
'dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','error_message']).filter(F.length(F.trim(F.col('error_message'))) > 0).write.format("delta").mode("overwrite").option('path', bronze_rejects_path).saveAsTable('datamarts.orders_bronze_rejects')

#remove rejects from main data frame.
df = df.filter(F.length(F.trim(F.col('error_message'))) == 0)

#display(df)
#display(df.count())

# COMMAND ----------

# MAGIC %md
# MAGIC De-Duplication of incoming orders

# COMMAND ----------

import pyspark.sql.functions as F
from pyspark.sql import Window


#certain divisions send a full load. update those without the de-dup process.
#remove those divisions from the main df. append them at the end. 
full_load_lov_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/Order_Full_Load_Divisions.csv").distinct()
 
df_total = df.join(full_load_lov_df, full_load_lov_df.DIVISION_6DIGIT == df.dm_source_div, "left")

df_total = df_total.withColumn("FullLoadDivision", F.when((F.col("DIVISION_6DIGIT").isNotNull()), F.lit('Y')).otherwise(F.col("FullLoadDivision")))

df = df_total.filter(df_total.FullLoadDivision == "N")
df = df.drop('_c3')\
       .drop('DIVISION_6DIGIT')\
       .drop('ORD_OVERRIDE')\
       .drop('ORD_SIGNOFF')

full_load_df = df_total.filter(df_total.FullLoadDivision == "Y")
full_load_df = full_load_df.drop('_c3')\
       .drop('DIVISION_6DIGIT')\
       .drop('ORD_OVERRIDE')\
       .drop('ORD_SIGNOFF')

if file_exists(bronze_source_path):
    #read up the full table.
    #Check incoming orders against existing orders for duplicates.#
    df_orders = spark.read.format("delta").load('abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_bronze')
    df_orders = df_orders.filter(df_orders.FullLoadDivision == "N")
    
    unique_df = df.join(df_orders, (df_orders.hash_key == df.hash_key) & (df_orders.dm_source_div == df.dm_source_div) & (df_orders.dm_key == df.dm_key), "leftanti") 

    dup_df = df.join(df_orders, (df_orders.hash_key == df.hash_key) & (df_orders.dm_source_div == df.dm_source_div) & (df_orders.dm_key == df.dm_key), "leftsemi")

    total_record_count = unique_df.count()
    duplicate_record_count = dup_count + dup_df.count()
     
    #####write the unique and duplicates out to the data lake.
    unique_df.write.format("delta").mode("append").option('path', bronze_source_path).saveAsTable('datamarts.orders_bronze')
  
    destination_record_count = total_record_count
    
    ####write out full orders
    if (full_load_df.count() > 0):
        full_load_df.write.format("delta").mode("append").option('path', bronze_source_path).saveAsTable('datamarts.orders_bronze')
    
    ####dups, check if table exists.
    if file_exists(bronze_duplicates_path):
            dup_df.select(['dm_source_system','dm_name','dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','hash_key','BATCH_TIMESTAMP']).distinct()\
                .write.format("delta").mode("append").option('path', bronze_duplicates_path).saveAsTable('datamarts.orders_bronze_duplicates')
    else:
            dup_df.select(['dm_source_system','dm_name','dm_source_div','dm_batch_id','dm_action','dm_version','dm_key','hash_key','BATCH_TIMESTAMP']).distinct()\
                .write.format("delta").mode("overwrite").option('path', bronze_duplicates_path).saveAsTable('datamarts.orders_bronze_duplicates')
else:
    #first time through create the delta table. if not orders table, then no duplicates
    #print('In else')
    df.write.format("delta").mode("overwrite").option('path', bronze_source_path).saveAsTable('datamarts.orders_bronze')  
    full_load_df.write.format("delta").mode("append").option('path', bronze_source_path).saveAsTable('datamarts.orders_bronze')
    total_record_count = df.count()
    duplicate_record_count = dup_count
    destination_record_count = total_record_count


# COMMAND ----------

# MAGIC %md
# MAGIC Log the load information into the Log Table

# COMMAND ----------

import datetime
from pyspark.sql.functions import *

print(f"BatchTime : {BatchTimestamp}")



##Capture error log
DataTimestamp = datetime.datetime.now()
Database = 'Datamart'
TableName = 'Orders'
DataLakeLayer = 'bronze'
ErrorMessage = None

#LogTableSchema
tableRun = []


tableRun.append(Row(TableName=TableName, 
                    DataAsOfTime = DataTimestamp, 
                    MaxBatchTimestamp=BatchTimestamp, 
                    ExportTimestamp = None, 
                    TotalRecordCount = source_record_count,
                    DuplicateCount = duplicate_record_count,
                    BadDilimiterRejectCount = df_bad_delim_count,
                    BadDataTypeRejectCount = df_bad_data_type_count,
                    DivisionRejectCount = 0,
                    LOVRejectCount = 0,
                    SourceCount = total_record_count,
                    DestinationCount=destination_record_count, 
                    SourcePath= file_path, 
                    DestinationPath= bronze_source_path, 
                    ErrorMessage = ErrorMessage,
                    RunID=None, 
                    DataLakeLayer = DataLakeLayer)
               )

#Write to table log
if file_exists(log_path):
    spark.createDataFrame(tableRun, schema=LogTableSchema).write.mode('append').format('delta').option('path', log_path).saveAsTable(table_log_name)
else:
    spark.createDataFrame(tableRun, schema=LogTableSchema).write.mode('overwrite').format('delta').option('path', log_path).saveAsTable(table_log_name)

# COMMAND ----------

# MAGIC %md
# MAGIC Truncate Staging Table for next run if all loads are successful

# COMMAND ----------

#after writing to bronze layer. truncate staging table here.
#if there is an error along the way, staging table will stay in tact, 
#and can process the failed records.
spark.sql("TRUNCATE TABLE delta.`abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/orders/orders_staging`")

# COMMAND ----------

from delta.tables import *

from datetime import datetime
from pytz import timezone
from delta.tables import *

tz = timezone('EST')
currentDateAndTime = datetime.now(tz)

deltaTable = DeltaTable.forPath(spark, bronze_source_path)
deltaTable.optimize().executeCompaction()


