# Databricks notebook source
# MAGIC %run "../Schemas/logging_schema"

# COMMAND ----------

# MAGIC %run "../Schemas/orders_gold_schema"

# COMMAND ----------

# MAGIC %run "../Common/transformations"

# COMMAND ----------

#Get SAS Token from key vault and establish a connection to ADLS - Dev Workspace
spark.conf.set("fs.azure.account.auth.type.storaccentdatamarts01.dfs.core.windows.net", "SAS")
spark.conf.set("fs.azure.sas.token.provider.type.storaccentdatamarts01.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")
spark.conf.set("fs.azure.sas.fixed.token.storaccentdatamarts01.dfs.core.windows.net", dbutils.secrets.get(scope="dbrx-entdatamarts-01", key="storage-account-sas-code03"))

# COMMAND ----------

entity = 'Orders'
databaseName = 'datamarts'
log_path = 'abfss://common@storaccentdatamarts01.dfs.core.windows.net/table_logs/{}/{}_log'.format('orders','orders')

datalake_source_path = 'abfss://bronze01@storaccentdatamarts01.dfs.core.windows.net/{}/orders_bronze'.format('orders')

datalake_sink_path = 'abfss://silver01@storaccentdatamarts01.dfs.core.windows.net/{}/{}_silver'.format('orders','orders')
datalake_reject_path = 'abfss://silver01@storaccentdatamarts01.dfs.core.windows.net/{}/rejects'.format('orders')

# COMMAND ----------

import os
import datetime 
from pyspark.sql.functions import input_file_name
from datetime import datetime
from pyspark.sql import Row
from pyspark.sql import functions as F
from pyspark.sql import SQLContext
from pyspark.sql.window import Window
from pyspark.sql.types import *
from delta.tables import *


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

#Create database
spark.sql('CREATE DATABASE IF NOT EXISTS {}'.format(databaseName))

# COMMAND ----------

# DBTITLE 1,Read log and get the  silver  max date for last write
from datetime import *
silver_log_df = spark.read.load(log_path).filter(F.col('DataLakeLayer')== 'silver')

if (silver_log_df.count() > 0) :
    print('We have silver records in log file')
    max_date = silver_log_df.select('MaxBatchTimestamp').groupBy().agg(F.max(F.col('MaxBatchTimestamp')).alias('MaxTimestamp')).collect()[0][0]
    silverRecords = True
else:
    print('We have NO silver records in log file')      
    silverRecords = False
    
if silverRecords :  
    print(max_date)  


# COMMAND ----------


if silverRecords:
    messages_df = spark.read.load(datalake_source_path).filter(F.col('BATCH_TIMESTAMP') > max_date)
    print('Incremental Load...')
else:
    print('Initial Load...')
    messages_df = spark.read.load(datalake_source_path)
    

if (messages_df.count() == 0): 
    dbutils.notebook.exit('stop')    

# Get the max timestamp of the new records
maxBatchTimestamp = messages_df.select('BATCH_TIMESTAMP').groupBy().agg(F.max(F.col('BATCH_TIMESTAMP')).alias('MaxTimestamp')).collect()[0][0]

messages_df = messages_df.withColumn('pop_classification_regional',F.regexp_replace('pop_classification_regional', '"', ''))\
                         .withColumn('dm_source_system',F.regexp_replace('dm_source_system', '"', ''))\
                         .withColumn('cust_required_dt',F.to_timestamp(messages_df.cust_required_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('original_promise_dt',F.to_timestamp(messages_df.original_promise_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('entered_dt',F.to_timestamp(messages_df.entered_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('promise_dt',F.to_timestamp(messages_df.promise_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('created_dt',F.to_timestamp(messages_df.created_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('last_maint_dt',F.to_timestamp(messages_df.last_maint_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('sales_amt_mth_ph_currency_rate_dt',F.to_timestamp(messages_df.sales_amt_mth_ph_currency_rate_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('sales_amt_domestic_gl_currency_rate_dt',F.to_timestamp(messages_df.sales_amt_domestic_gl_currency_rate_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('lst_price_domestic_gl_currency_rate_dt',F.to_timestamp(messages_df.lst_price_domestic_gl_currency_rate_dt, 'yyyyMMddHHmmss'))\
                         .withColumn('lst_price_mth_ph_currency_rate_dt',F.to_timestamp(messages_df.lst_price_mth_ph_currency_rate_dt, 'yyyyMMddHHmmss'))

Total_record_count = messages_df.count()

if Total_record_count > 0 :
    
    #convert column names to upper case
    print('Convert to upper case...')
    messages_df= messages_df.select([F.col(col).alias(col.upper()) for col in messages_df.columns])
    
    #Apply schema for gold 
    print('Apply schema...')
    messages_df = messages_df.withColumn('FULLLOADDIVISION',messages_df.FULLLOADDIVISION)
    messages_df = messages_df.transform(apply_schema(orders_gold_schema))

    #Add error message fields to dataframe
    messages_df = messages_df.drop('error_message')
    messages_df = messages_df.withColumn("ERROR_MESSAGE",F.lit(None).cast(StringType()))\
                             .withColumn("ERROR_MESSAGE_CONCAT",F.lit(None).cast(StringType()))\

print(Total_record_count)
print(maxBatchTimestamp)



# COMMAND ----------

# DBTITLE 1,Create LOV Lists
if Total_record_count > 0:
    
    #LOAD Full_Load_Divisions
    Order_Full_Load_Divisions_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/Order_Full_Load_Divisions.csv").distinct()
    
    
    #Load main lov
    LOV1_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/LOV1.csv").distinct()
    LOV1_df = LOV1_df.withColumn('VAL1',F.trim(F.col("VAL1")))\
                     .withColumn('LOV',F.when(F.col('LOV') == 'Part on Item Master LOV','Part On Item Master LOV').otherwise(F.col('LOV')))\
                     .withColumn("VAL1", F.upper('VAL1'))
    
    #Load cst_mth_exch_rate table
    cst_mth_exch_rate = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/cst_mth_exch_rate.csv").distinct()\
                                                .withColumnRenamed('CURCY_CD','curcy_cd_cst_mth_exch_rate')
    #Load cst_amt_ann_ph_currency_rate table
    cst_amt_ann_ph_currency_rate = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/cst_amt_ann_ph_currency_rate.csv").distinct()\
                                                           .withColumnRenamed('CURCY_CD','curcy_cd_cst_amt_ann_ph_currency_rate')
    #Load division lookup table
    Division_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/Division.csv").distinct()
    
    #Load fields used with lov table
    Order_LOV_Fields_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/Datamart_field_lov.csv")
    Order_LOV_Fields_df = (Order_LOV_Fields_df.withColumnRenamed('LOV','ListOfVaues')\
                                              .withColumnRenamed('_c2','Active')\
                                              .filter(F.col('Active') == 'TRUE')\
                                              .distinct())
    
    #Join LOV Fields table with the main lov table
    validation_df = (Order_LOV_Fields_df.join(LOV1_df,Order_LOV_Fields_df
                                             .ListOfVaues == LOV1_df.LOV,'left')\
                                             .drop('DESC','CREATED_TS','CREATED_BY','UPDATED_TS','UPDATED_BY','_c2','ListOfVaues','LOV')\
                                             .withColumnRenamed('datamart_field','ColumnName'))
    
    validation_df = validation_df.withColumn('ColumnName',F.upper(F.col('ColumnName')))


# COMMAND ----------

# DBTITLE 1,Convert Columns to Uppercase
if Total_record_count > 0:
    messages_df= ( messages_df.withColumn("APP_CODE_CORP", F.upper(messages_df.APP_CODE_CORP))
                              .withColumn("BILL_TO_ACCT_LANG_CODE", F.upper(messages_df.BILL_TO_ACCT_LANG_CODE))
                              .withColumn("BILL_TO_CONTACT_CTRY_CODE", F.upper(messages_df.BILL_TO_CONTACT_CTRY_CODE))
                              .withColumn("BILL_TO_CONTACT_LANG_CODE", F.upper(messages_df.BILL_TO_CONTACT_LANG_CODE))
                              .withColumn("BILL_TO_CTRY_CODE", F.upper(messages_df.BILL_TO_CTRY_CODE))
                              .withColumn("CONTRACT_AWARDED_TZ", F.upper(messages_df.CONTRACT_AWARDED_TZ))
                              .withColumn("CONTRACT_EFFECTIVE_TZ", F.upper(messages_df.CONTRACT_EFFECTIVE_TZ))
                              .withColumn("CREATED_TZ", F.upper(messages_df.CREATED_TZ))
                              .withColumn("CST_CURRENCY_CODE", F.upper(messages_df.CST_CURRENCY_CODE))
                              .withColumn("CUST_REQUIRED_TZ", F.upper(messages_df.CUST_REQUIRED_TZ))
                              .withColumn("DM_ACTION", F.upper(messages_df.DM_ACTION))
                              .withColumn("DM_NAME", F.upper(messages_df.DM_NAME))
                              .withColumn("END_USE_ACCT_LANG_CODE", F.upper(messages_df.END_USE_ACCT_LANG_CODE))
                              .withColumn("END_USE_CONTACT_CTRY_CODE", F.upper(messages_df.END_USE_CONTACT_CTRY_CODE))
                              .withColumn("END_USE_CONTACT_LANG_CODE", F.upper(messages_df.END_USE_CONTACT_LANG_CODE))
                              .withColumn("END_USE_CTRY_CODE", F.upper(messages_df.END_USE_CTRY_CODE))
                              .withColumn("ENTERED_TZ", F.upper(messages_df.ENTERED_TZ))
                              .withColumn("IN_PROCESS_QTY_NORMALIZED_UOM", F.upper(messages_df.IN_PROCESS_QTY_NORMALIZED_UOM))
                              .withColumn("IN_PROCESS_QTY_NORMALIZED_UOM_DESC", F.upper(messages_df.IN_PROCESS_QTY_NORMALIZED_UOM_DESC))
                              .withColumn("LAST_MAINT_TZ", F.upper(messages_df.LAST_MAINT_TZ))
                              .withColumn("LST_PRICE_ANN_PH_CURRENCY_RATE_TZ", F.upper(messages_df.LST_PRICE_ANN_PH_CURRENCY_RATE_TZ))
                              .withColumn("LST_PRICE_CURRENCY_CODE", F.upper(messages_df.LST_PRICE_CURRENCY_CODE))
                              .withColumn("LST_PRICE_DOMESTIC_GL_CURRENCY_CODE", F.upper(messages_df.LST_PRICE_DOMESTIC_GL_CURRENCY_CODE))
                              .withColumn("LST_PRICE_DOMESTIC_GL_CURRENCY_RATE_TZ", F.upper(messages_df.LST_PRICE_DOMESTIC_GL_CURRENCY_RATE_TZ))
                              .withColumn("LST_PRICE_MTH_PH_CURRENCY_RATE_TZ", F.upper(messages_df.LST_PRICE_MTH_PH_CURRENCY_RATE_TZ))
                              .withColumn("ORD_TAXABLE_FLG", F.upper(messages_df.ORD_TAXABLE_FLG))
                              .withColumn("ORIGINAL_ORD_QTY_NORMALIZED_UOM_DESC_OVERALL", F.upper(messages_df.ORIGINAL_ORD_QTY_NORMALIZED_UOM_DESC_OVERALL))
                              .withColumn("ORIGINAL_ORD_QTY_NORMALIZED_UOM_OVERALL", F.upper(messages_df.ORIGINAL_ORD_QTY_NORMALIZED_UOM_OVERALL))
                              .withColumn("ORIGINAL_PROMISE_TZ", F.upper(messages_df.ORIGINAL_PROMISE_TZ))
                              .withColumn("PARENT_SOLD_TO_ACCT_LANG_CODE", F.upper(messages_df.PARENT_SOLD_TO_ACCT_LANG_CODE))
                              .withColumn("PARENT_SOLD_TO_CTRY_CODE", F.upper(messages_df.PARENT_SOLD_TO_CTRY_CODE))
                              .withColumn("PART_ON_ITEM_MASTER", F.upper(messages_df.PART_ON_ITEM_MASTER))
                              .withColumn("PART_TYP", F.upper(messages_df.PART_TYP))
                              .withColumn("PH_ACQUIRE_TZ", F.upper(messages_df.PH_ACQUIRE_TZ))
                              .withColumn("PH_DIVEST_TZ", F.upper(messages_df.PH_DIVEST_TZ))
                              .withColumn("PROMISE_TZ", F.upper(messages_df.PROMISE_TZ))
                              .withColumn("PROPRIETARY_PART", F.upper(messages_df.PROPRIETARY_PART))
                              .withColumn("SALES_AMT_ANN_PH_CURRENCY_RATE_TZ", F.upper(messages_df.SALES_AMT_ANN_PH_CURRENCY_RATE_TZ))
                              .withColumn("SALES_AMT_CURRENCY_CODE", F.upper(messages_df.SALES_AMT_CURRENCY_CODE))
                              .withColumn("SALES_AMT_DOMESTIC_GL_CURRENCY_CODE", F.upper(messages_df.SALES_AMT_DOMESTIC_GL_CURRENCY_CODE))
                              .withColumn("SALES_AMT_DOMESTIC_GL_CURRENCY_RATE_TZ", F.upper(messages_df.SALES_AMT_DOMESTIC_GL_CURRENCY_RATE_TZ))
                              .withColumn("SALES_AMT_MTH_PH_CURRENCY_RATE_TZ", F.upper(messages_df.SALES_AMT_MTH_PH_CURRENCY_RATE_TZ))
                              .withColumn("SHIP_TO_ACCT_LANG_CODE", F.upper(messages_df.SHIP_TO_ACCT_LANG_CODE))
                              .withColumn("SHIP_TO_CONTACT_CTRY_CODE", F.upper(messages_df.SHIP_TO_CONTACT_CTRY_CODE))
                              .withColumn("SHIP_TO_CONTACT_LANG_CODE", F.upper(messages_df.SHIP_TO_CONTACT_LANG_CODE))
                              .withColumn("SHIP_TO_CTRY_CODE", F.upper(messages_df.SHIP_TO_CTRY_CODE))
                              .withColumn("SHIPPED_QTY_NORMALIZED_UOM", F.upper(messages_df.SHIPPED_QTY_NORMALIZED_UOM))
                              .withColumn("SHIPPED_QTY_NORMALIZED_UOM_DESC", F.upper(messages_df.SHIPPED_QTY_NORMALIZED_UOM_DESC))
                              .withColumn("SOLD_TO_ACCT_LANG_CODE", F.upper(messages_df.SOLD_TO_ACCT_LANG_CODE))
                              .withColumn("SOLD_TO_CONTACT_CTRY_CODE", F.upper(messages_df.SOLD_TO_CONTACT_CTRY_CODE))
                              .withColumn("SOLD_TO_CONTACT_LANG_CODE", F.upper(messages_df.SOLD_TO_CONTACT_LANG_CODE))
                              .withColumn("SOLD_TO_CTRY_CODE", F.upper(messages_df.SOLD_TO_CTRY_CODE))
                              .withColumn("THIRD_PARTY_ACCT_LANG_CODE", F.upper(messages_df.THIRD_PARTY_ACCT_LANG_CODE))
                              .withColumn("THIRD_PARTY_CONTACT_CTRY_CODE", F.upper(messages_df.THIRD_PARTY_CONTACT_CTRY_CODE))
                              .withColumn("THIRD_PARTY_CONTACT_LANG_CODE", F.upper(messages_df.THIRD_PARTY_CONTACT_LANG_CODE))
                              .withColumn("THIRD_PARTY_CTRY_CODE", F.upper(messages_df.THIRD_PARTY_CTRY_CODE))
                              .withColumn("UNSCHEDULED_QTY_NORMALIZED_UOM", F.upper(messages_df.UNSCHEDULED_QTY_NORMALIZED_UOM))
                              .withColumn("UNSCHEDULED_QTY_NORMALIZED_UOM_DESC", F.upper(messages_df.UNSCHEDULED_QTY_NORMALIZED_UOM_DESC))
                              .withColumn("UNSHIPPED_QTY_NORMALIZED_UOM", F.upper(messages_df.UNSHIPPED_QTY_NORMALIZED_UOM))
                 )

# COMMAND ----------

# DBTITLE 1,Get the LOVs to compare message data field values
if Total_record_count > 0:

    data = []
    
    #Create schema for lov dataframe
    messages_schema = StructType([
            StructField("columnName", StringType(), True),  
            StructField("value", StringType(), True), 
    
        ])
    
    #Create an empty lov dataframe
    lov_df = spark.createDataFrame(data,messages_schema)
    
    x= 0
    post_columnName = ''
    acc_column_value = ''
    post_columnValue = ''
    
    
    #Loop through the LOV's and create a string of values for each column to check
    rows =validation_df.sort("ColumnName","VAL1").collect()
    for row in rows:
        columnName = row['ColumnName']
        
        if ( x ==0 ):
            column_value = row['VAL1']
            acc_column_value = "'" + column_value.strip()  
            x = 1 
            
        elif (post_columnName == columnName.strip()) :
            column_value = row['VAL1']
            acc_column_value = acc_column_value + "," + column_value.strip()
            
        else :
            acc_column_value = acc_column_value + "," + str(row['VAL1']) +"'"
            x=0
    
            #Add record to dataframe
            rows = [(post_columnName, acc_column_value)]
            lov2_df = spark.createDataFrame(rows,schema=messages_schema)
            lov_df = lov_df.union(lov2_df)
    
        post_columnName = columnName.strip() 
    
    #Add last column
    rows = [(columnName, ([acc_column_value]))]
    lov2_df = spark.createDataFrame(rows,messages_schema)
    
    #Dataframe of column names and values
    lov_df = lov_df.union(lov2_df)


# COMMAND ----------

# DBTITLE 1,Validate LOV values with incoming data
if Total_record_count > 0:
    #loop through the lov dataframe and validate each column value in the messages dataframe 
    rows =lov_df.sort("columnName").collect()

    for row in rows:
        columnValue = row['value'].replace("'","").replace(",","~")
        messages_df = messages_df.withColumn('ERROR_MESSAGE',
                                                              F.when(~F.col(row['columnName']).isin(list(columnValue.split('~'))),
                                                                      F.lit("Invalid value for " + row['columnName'])).otherwise(None))\
                                 .withColumn('ERROR_MESSAGE_CONCAT',F.concat_ws(',',F.col('ERROR_MESSAGE'),F.col('ERROR_MESSAGE_CONCAT')))

    messages_df = messages_df.drop('ERROR_MESSAGE').withColumnRenamed('ERROR_MESSAGE_CONCAT','ERROR_MESSAGE')
    
    #set empty string to NULL
    messages_df = messages_df.withColumn('ERROR_MESSAGE',F.when(messages_df.ERROR_MESSAGE == '',F.lit(None)).otherwise(messages_df.ERROR_MESSAGE))

# COMMAND ----------

# DBTITLE 1,Get Exchange Rates
if Total_record_count > 0:
    #cst_mth_exch_rate
    messages_df = messages_df.join(cst_mth_exch_rate,messages_df.CST_CURRENCY_CODE == cst_mth_exch_rate.curcy_cd_cst_mth_exch_rate,'left')
    
    #CST_AMT_ANN_PH_CURRENCY_RATE
    messages_df = messages_df.join(cst_amt_ann_ph_currency_rate,messages_df.CST_CURRENCY_CODE == cst_amt_ann_ph_currency_rate.curcy_cd_cst_amt_ann_ph_currency_rate,'left')
    

# COMMAND ----------

# DBTITLE 1,Division Lookups for some derived columns
if Total_record_count > 0:
    # #Load division lookup table
    Division_df = spark.read.format('csv').option("Header","True").load("abfss://common@storaccentdatamarts01.dfs.core.windows.net/lov/Division.csv").distinct()
    Division_df = Division_df.drop('DIVISION_3CHAR','DIVISION_2CHAR','DIVISION_NAME','CREATED_TS','CREATED_BY','UPDATED_TS','UPDATED_BY','SOURCE_SEGMENT')
    
    Division_df = Division_df.withColumnRenamed('GROUP_2CHAR','GROUP_2CHAR_2')\
                             .withColumnRenamed('GROUP_NAME','GROUP_NAME_2')\
                             .withColumnRenamed('DIVISION_6DIGIT','DIVISION_6DIGIT_2')
    
    messages_df = messages_df.join(Division_df,messages_df.PH_PART_OWNING_DIV_ACCT_NO_CORP == Division_df.DIVISION_6DIGIT_2,'left')\
                      .withColumn('PH_PART_OWNING_GROUP',Division_df.GROUP_2CHAR_2)\
                      .withColumn('PH_PART_OWNING_GROUP_NAME',Division_df.GROUP_NAME_2)
    messages_df = messages_df.drop('GROUP_2CHAR','GROUP_2CHAR_2','GROUP_NAME_2','DIVISION_6DIGIT_2')
    
    messages_df = messages_df.join(Division_df,messages_df.PH_REPATRIATED_LOC_ACCT_NO_CORP == Division_df.DIVISION_6DIGIT_2,'left')\
                      .withColumn('PH_REPATRIATED_GROUP',Division_df.GROUP_2CHAR_2)\
                      .withColumn('PH_REPATRIATED_GROUP_NAME',Division_df.GROUP_NAME_2)\
                      .withColumn('PH_REPATRIATED_LOC_ACCT_NO_CORP',Division_df.GROUP_NAME_2)
    messages_df = messages_df.drop('GROUP_2CHAR','GROUP_2CHAR_2','GROUP_NAME_2','DIVISION_6DIGIT_2')
    
    messages_df = messages_df.join(Division_df,messages_df.PH_SELLING_LOC_ACCT_NO_CORP == Division_df.DIVISION_6DIGIT_2,'left')\
                      .withColumn('PH_SELLING_GROUP',Division_df.GROUP_2CHAR_2)\
                      .withColumn('PH_SELLING_GROUP_NAME',Division_df.GROUP_NAME_2)
    messages_df = messages_df.drop('GROUP_2CHAR','GROUP_2CHAR_2','GROUP_NAME_2','DIVISION_6DIGIT_2')
 

# COMMAND ----------

# DBTITLE 1,Derived Columns
if Total_record_count > 0:
    messages_df= (messages_df.withColumn('CONTRACT_AWARDED_TM', F.substring(messages_df.CONTRACT_AWARDED_DT.cast(StringType()),11,9))
                             .withColumn('CONTRACT_EFFECTIVE_TM', messages_df.CONTRACT_AWARDED_DT)
                             .withColumn('CREATED_TM', messages_df.CREATED_DT)
                             .withColumn('CST_AMT_TOTAL', messages_df.CST_AMT)   
                             .withColumn('CST_AMT_TOTAL_ANN_PH', messages_df.CST_AMT_ANN_PH)               
                             .withColumn('CST_AMT_TOTAL_DOMESTIC_GL', messages_df.CST_AMT_DOMESTIC_GL)
                             .withColumn('CST_AMT_TOTAL_MTH_PH', messages_df.CST_AMT_MTH_PH)
                             .withColumn('CUST_REQUIRED_YEAR',  F.when(((F.year(messages_df.CUST_REQUIRED_DT)< 2000) | (F.year(messages_df.CUST_REQUIRED_DT)> 2070)),0).otherwise(F.year(messages_df.CUST_REQUIRED_DT)))
                             .withColumn('CUST_REQUIRED_TM', F.substring(messages_df.CUST_REQUIRED_DT.cast(StringType()),11,9))
                             .withColumn('CUST_REQUIRED_TRANS_MTH', F.when(((F.year(messages_df.CUST_REQUIRED_DT)< 2000) | (F.year(messages_df.CUST_REQUIRED_DT)> 2070)),0).otherwise(F.month(messages_df.CUST_REQUIRED_DT)))
                             .withColumn('DM_ARCHIVE_TM', F.lit(None))
                             .withColumn('DM_CREATED_DT', F.current_timestamp())
                             .withColumn('DM_CREATED_TM', F.substring(F.current_timestamp().cast(StringType()),11,9))
                             .withColumn('DM_PROCESS_ID', F.substring(messages_df.BATCH_TIMESTAMP.cast(StringType()),11,9))
                             .withColumn('DM_TIMESTAMP', messages_df.BATCH_TIMESTAMP)              
                             .withColumn('DM_TRANS_SOURCE_TYP', F.lit('ETL'))
                             .withColumn('END_USE_CONTACT_CITY_ENG', messages_df.END_USE_CONTACT_CITY)   
                             .withColumn('ENTERED_YEAR',  F.when(((F.year(messages_df.ENTERED_DT)< 2000) | (F.year(messages_df.ENTERED_DT)> 2070)),0).otherwise(F.year(messages_df.ENTERED_DT))) 
                             .withColumn('ENTERED_TM',  F.substring(messages_df.ENTERED_DT.cast(StringType()),11,9))  
                             .withColumn('LAST_MAINT_TM',  F.substring(messages_df.LAST_MAINT_DT.cast(StringType()),11,9))                
                             .withColumn('LST_PRICE_ANN_PH_CURRENCY_RATE_TM',  F.substring(messages_df.LAST_MAINT_DT.cast(StringType()),11,9))                
                             .withColumn('LST_PRICE_DOMESTIC_GL_CURRENCY_RATE_TM',  F.substring(messages_df.LST_PRICE_DOMESTIC_GL_CURRENCY_RATE.cast(StringType()),11,9)) 
                             .withColumn('ORIGINAL_PROMISE_YEAR',  F.when(((F.year(messages_df.ORIGINAL_PROMISE_DT)< 2000) | 
                                                                           (F.year(messages_df.ORIGINAL_PROMISE_DT)> 2070)),0).otherwise(F.year(messages_df.ORIGINAL_PROMISE_DT)))              
                             .withColumn('ORIGINAL_PROMISE_TM', F.substring(messages_df.ORIGINAL_PROMISE_DT.cast(StringType()),11,9))
                             .withColumn('PH_ACQUIRE_TM', F.substring(messages_df.PH_ACQUIRE_DT.cast(StringType()),11,9))  
                             .withColumn('PH_DIVEST_TM', F.substring(messages_df.PH_DIVEST_DT.cast(StringType()),11,9))  
                             .withColumn('PROMISE_YEAR',  F.when(((F.year(messages_df.PROMISE_DT)< 2000) | 
                                                                  (F.year(messages_df.PROMISE_DT)> 2070)),0).otherwise(F.year(messages_df.PROMISE_DT)))  
                             .withColumn('PROMISE_TM', F.substring(messages_df.PROMISE_DT.cast(StringType()),11,9))                             
                             .withColumn('SALES_AMT_ANN_PH_CURRENCY_RATE_TM', F.substring(messages_df.SALES_AMT_ANN_PH_CURRENCY_RATE_DT.cast(StringType()),11,9))       
                             .withColumn('SALES_AMT_DOMESTIC_GL_CURRENCY_RATE_TM', F.substring(messages_df.SALES_AMT_DOMESTIC_GL_CURRENCY_RATE.cast(StringType()),11,9))               
                             .withColumn('SALES_AMT_MTH_PH_CURRENCY_RATE_TM', F.substring(messages_df.SALES_AMT_MTH_PH_CURRENCY_RATE_TM.cast(StringType()),11,9))  
    
                     )
    
    messages_df= (messages_df.withColumn('CUST_REQUIRED_FISCAL_MTH',F.when(messages_df.CUST_REQUIRED_YEAR == 0,0))
                             .withColumn('CUST_REQUIRED_FISCAL_MTH',F.when(((messages_df.CUST_REQUIRED_YEAR != 0) & 
                                                                             (messages_df.CUST_REQUIRED_TRANS_MTH < 7)),messages_df.CUST_REQUIRED_TRANS_MTH +6).otherwise(messages_df.CUST_REQUIRED_TRANS_MTH -6))
                             .withColumn('CUST_REQUIRED_FISCAL_QUARTER',F.when((messages_df.CUST_REQUIRED_YEAR ==0),0).otherwise((F.quarter(messages_df.CUST_REQUIRED_DT))))
                             .withColumn('CUST_REQUIRED_FISCAL_YEAR', F.when((messages_df.CUST_REQUIRED_YEAR ==0),0))
                             .withColumn('CUST_REQUIRED_FISCAL_YEAR', F.when(((messages_df.CUST_REQUIRED_YEAR !=0)&
                                                                              (messages_df.CUST_REQUIRED_TRANS_MTH < 7)),messages_df.CUST_REQUIRED_YEAR).otherwise(messages_df.CUST_REQUIRED_YEAR + 1))
                             .withColumn('CUST_REQUIRED_SERIAL_MTH_NORMALIZED', F.when((messages_df.CUST_REQUIRED_YEAR == 0),0)
                                                                                  .otherwise((((messages_df.CUST_REQUIRED_YEAR-1980) * 12) + messages_df.CUST_REQUIRED_TRANS_MTH)))
                             .withColumn('ENTERED_TRANS_MTH', F.when(messages_df.ENTERED_YEAR == 0,0))
                             .withColumn('ENTERED_TRANS_MTH', F.when(messages_df.ENTERED_YEAR != 0,F.month(messages_df.ENTERED_DT)))              
                             .withColumn('ENTERED_FISCAL_MTH', F.when(messages_df.ENTERED_YEAR == 0, 0))
                             .withColumn('ENTERED_FISCAL_MTH', F.when(((messages_df.ENTERED_YEAR != 0) & (F.month(messages_df.ENTERED_DT)> 6)),F.month(messages_df.ENTERED_DT)- 6)
                                                                 .otherwise(F.month(messages_df.ENTERED_DT)+ 6))
                             .withColumn('ENTERED_FISCAL_QUARTER', F.when(messages_df.ENTERED_YEAR == 0, 0))
                             .withColumn('ENTERED_FISCAL_QUARTER', F.when(messages_df.ENTERED_YEAR != 0, F.quarter(messages_df.ENTERED_DT)))
                             .withColumn('ENTERED_FISCAL_YEAR', F.when(messages_df.ENTERED_YEAR  == 0, 0))
                             .withColumn('ENTERED_FISCAL_YEAR', F.when((messages_df.ENTERED_YEAR  != 0)& (F.month(messages_df.ENTERED_DT)< 7),messages_df.ENTERED_YEAR).otherwise((messages_df.ENTERED_YEAR +1)))                                    .withColumn('ENTERED_SERIAL_MTH_NORMALIZED', F.when((messages_df.ENTERED_YEAR == 0),0)
                                                                                  .otherwise((((messages_df.ENTERED_YEAR-1980) * 12) + F.month(messages_df.ENTERED_DT))))  
                             .withColumn('ORIGINAL_PROMISE_FISCAL_YEAR', F.when((messages_df.ORIGINAL_PROMISE_YEAR != 0)& 
                                                                                (F.month(messages_df.ORIGINAL_PROMISE_DT)< 7),messages_df.PROMISE_YEAR).otherwise((messages_df.PROMISE_YEAR +1))) 
                             .withColumn('ORIGINAL_PROMISE_FISCAL_MTH', F.when(messages_df.ORIGINAL_PROMISE_YEAR == 0, 0)) 
                             .withColumn('ORIGINAL_PROMISE_FISCAL_MTH',F.when(((messages_df.ORIGINAL_PROMISE_YEAR != 0) & (F.month(messages_df.ORIGINAL_PROMISE_DT)< 7)),F.month(messages_df.ORIGINAL_PROMISE_DT)+ 6)
                                                                 .otherwise(F.month(messages_df.ORIGINAL_PROMISE_DT)- 6)) 
                             .withColumn('ORIGINAL_PROMISE_FISCAL_QUARTER', F.when(messages_df.ORIGINAL_PROMISE_YEAR == 0, 0))  
                             .withColumn('ORIGINAL_PROMISE_FISCAL_QUARTER', F.when(messages_df.ORIGINAL_PROMISE_YEAR != 0, F.quarter(messages_df.ORIGINAL_PROMISE_DT)))
                             .withColumn('ORIGINAL_PROMISE_SERIAL_MTH_NORMALIZED',F.when((messages_df.ORIGINAL_PROMISE_YEAR == 0),0)
                                                                                  .otherwise((((messages_df.ORIGINAL_PROMISE_YEAR-1980) * 12) + F.month(messages_df.ORIGINAL_PROMISE_DT))))
                             .withColumn('ORIGINAL_PROMISE_TRANS_MTH', F.when(messages_df.ORIGINAL_PROMISE_YEAR == 0, 0))
                             .withColumn('ORIGINAL_PROMISE_TRANS_MTH', F.month(messages_df.ORIGINAL_PROMISE_DT)) 
                             .withColumn('ORIGINAL_PROMISE_FISCAL_MTH', F.when(messages_df.ORIGINAL_PROMISE_YEAR == 0, 0)) 
                             .withColumn('PROMISE_FISCAL_MTH', F.when(messages_df.PROMISE_YEAR == 0, 0))              
                             .withColumn('PROMISE_FISCAL_MTH',  F.when(((messages_df.PROMISE_YEAR != 0) & (F.month(messages_df.PROMISE_DT)< 7)),F.month(messages_df.PROMISE_DT)+ 6)
                                                                 .otherwise(F.month(messages_df.PROMISE_DT)- 6))               
                             .withColumn('PROMISE_FISCAL_QUARTER', F.when(messages_df.PROMISE_YEAR == 0, 0))  
                             .withColumn('PROMISE_FISCAL_QUARTER', F.when(messages_df.PROMISE_YEAR != 0, F.quarter(messages_df.PROMISE_DT)))   
                             .withColumn('PROMISE_FISCAL_YEAR', F.when(messages_df.PROMISE_YEAR  == 0, 0))
                             .withColumn('PROMISE_FISCAL_YEAR', F.when((messages_df.PROMISE_YEAR  != 0)& (F.month(messages_df.PROMISE_DT)< 7),messages_df.PROMISE_YEAR).otherwise((messages_df.PROMISE_YEAR +1)))
                             .withColumn('PROMISE_SERIAL_MTH_NORMALIZED', F.when((messages_df.PROMISE_YEAR == 0),0)
                                                                           .otherwise((((messages_df.PROMISE_YEAR-1980) * 12) + F.month(messages_df.PROMISE_DT))))   
                             .withColumn('PROMISE_TRANS_MTH', F.when(messages_df.PROMISE_YEAR == 0,0))
                             .withColumn('PROMISE_TRANS_MTH', F.when(messages_df.PROMISE_YEAR != 0,F.month(messages_df.PROMISE_DT)))                       
                             .withColumn('ANN_CST_AMT_FOR_CALC', F.when(messages_df.CST_CURRENCY_CODE != messages_df.SALES_AMT_CURRENCY_CODE,                                                                      (messages_df.CST_AMT * messages_df.CST_AMT_ANN_PH_CURRENCY_RATE)/ messages_df.SALES_AMT_ANN_PH_CURRENCY_RATE_TM).otherwise(messages_df.CST_AMT))
                )


# COMMAND ----------

# DBTITLE 1,Create Calculated columns
if Total_record_count > 0:
    messages_df =(messages_df.withColumn('SALES_AMT_TOTAL',messages_df.SALES_AMT)
                             .withColumn('SALES_AMT_TOTAL_ANN_PH',messages_df.SALES_AMT_DOMESTIC_GL - messages_df.CST_AMT_MTH_PH )              
                             .withColumn('SALES_AMT_TOTAL_DOMESTIC_GL',messages_df.SALES_AMT_ANN_PH - messages_df.CST_AMT_DOMESTIC_GL )                            
                             .withColumn('SALES_AMT_TOTAL_MTH_PH',messages_df.SALES_AMT_MTH_PH - messages_df.CST_AMT_MTH_PH )                                          
                             .withColumn('CST_AMT_TOTAL_ANN_PH',F.when((F.col('CST_CURRENCY_CODE') != F.col('SALES_AMT_CURRENCY_CODE')),                                                                  
                                                                      (messages_df.CST_AMT * messages_df.CST_AMT_ANN_PH_CURRENCY_RATE)/messages_df.SALES_AMT_ANN_PH_CURRENCY_RATE).otherwise(messages_df.CST_AMT))
                             .withColumn('CST_AMT_TOTAL_MTH_PH',F.when((messages_df.CST_CURRENCY_CODE != messages_df.SALES_AMT_CURRENCY_CODE), 
                                                                    (messages_df.CST_AMT * messages_df.curcy_cd_cst_mth_exch_rate)/messages_df.SALES_AMT_ANN_PH_CURRENCY_RATE).otherwise(messages_df.CST_AMT))
                             .withColumn('MARGIN_AMT_TOTAL',F.when((messages_df.CST_CURRENCY_CODE != messages_df.SALES_AMT_CURRENCY_CODE), 
                                                                    (messages_df.CST_AMT * messages_df.curcy_cd_cst_mth_exch_rate)/messages_df.SALES_AMT_ANN_PH_CURRENCY_RATE).otherwise(messages_df.CST_AMT))
                             .withColumn('MARGIN_AMT_UNSHIPPED',messages_df.SALES_AMT_ANN_PH - messages_df.CST_AMT_MTH_PH)
                             .withColumn('MARGIN_AMT_UNSHIPPED_ANN_PH', messages_df.SALES_AMT_ANN_PH - messages_df.CST_AMT_MTH_PH)
                             .withColumn('MARGIN_AMT_UNSHIPPED_DOMESTIC_GL', messages_df.SALES_AMT_DOMESTIC_GL - messages_df.CST_AMT_DOMESTIC_GL)
                             .withColumn('MARGIN_AMT_UNSHIPPED_MTH_PH', messages_df.SALES_AMT_MTH_PH - messages_df.CST_AMT_MTH_PH) 
                             .withColumn('MARGIN_PERCENT',F.when((messages_df.SALES_AMT == F.lit(0)), F.lit(None)).otherwise((messages_df.SALES_AMT - messages_df.ANN_CST_AMT_FOR_CALC) / messages_df.SALES_AMT))              
                 )

# COMMAND ----------

# DBTITLE 1,Get Full Load Divisions
if Total_record_count > 0:

    #Get the full load divisions
    full_load_divisions_df = messages_df.filter(F.col('FULLLOADDIVISION')== 'Y')\
                                        .withColumn('BATCH_TIMESTAMP_DT',F.to_date(F.from_utc_timestamp('BATCH_TIMESTAMP', 'America/New_York')))

    #filter buy divsion key maxtimestamp row nunmber
    full_load_divisions_df = full_load_divisions_df.select('*',(F.row_number().over(Window.partitionBy('DM_KEY','DM_SOURCE_DIV').orderBy(F.col('BATCH_TIMESTAMP_DT').desc()))).alias("SEQ_NUM")) 
    
    #Get lastest list of full load
    latest_load_df = full_load_divisions_df.filter(F.col('SEQ_NUM')==1 )\

    #get a list of SOURCE_DIV with the max batch-timestamp date (delete these from silver before adding the new ones unless they have been added today)
    latest_load_df2 = latest_load_df.groupBy('DM_SOURCE_DIV').agg( F.max("BATCH_TIMESTAMP_DT").alias("BATCH_TIMESTAMP_DT"))

    #Set as current record
    latest_load_df = latest_load_df.withColumn('DM_CURR_FLG', F.lit('Y'))                
    
    full_count = latest_load_df.count()  
    #if we have full load division records write them out
    if full_count > 0:
        if file_exists(datalake_sink_path):
            #Get existing Silver records
            deltaTable = delta.DeltaTable.forPath(spark, datalake_sink_path)
    
            #Delete the records if they exist in silver
            deltaTable.alias("silvertable").merge( latest_load_df2.alias("updates"),("silvertable.DM_SOURCE_DIV = updates.DM_SOURCE_DIV"))\
                                           .whenMatchedDelete( "silvertable.BATCH_TIMESTAMP_DT < updates.BATCH_TIMESTAMP_DT").execute() 

# COMMAND ----------

# DBTITLE 1,Write to Silver table
if Total_record_count > 0:

    #remove FullLoadDivisions from messages_df
    messages_df = messages_df.filter(F.col('FULLLOADDIVISION')=='N').drop('SEQ_NUM')

    messages_df = messages_df.withColumn('DM_ARCHIVE_TM',F.lit(None).cast(StringType()) )\
                             .withColumn('BATCH_TIMESTAMP_DT',F.to_date(F.from_utc_timestamp('BATCH_TIMESTAMP', 'America/New_York')))

    SourceCount = messages_df.count()
     
    # see if there multiple versions of an order and mark the latest as current and the others as not current
    messages_df = messages_df.select('*',(F.row_number().over(Window.partitionBy('DM_KEY','DM_SOURCE_DIV').orderBy(F.col('DM_BATCH_ID').desc(),F.col('DM_ACTION').asc()))).alias("SEQ_NUM"))

    #print('Set current record flag...')    
    messages_df = messages_df.withColumn('DM_CURR_FLG', F.when((messages_df.SEQ_NUM == 1)& (messages_df.DM_ACTION == 'ADD'),F.lit("Y")).otherwise(F.lit("N")))
    
    #Select the current records from the incoming data and set the  DM_CURR_FLG = N in the datalake on the matching records
    messages_Y_df = messages_df.filter((F.col('SEQ_NUM')=='1')) 
    #messages_Y_df = messages_df.select('DM_SOURCE_DIV','DM_KEY','SEQ_NUM').filter((F.col('SEQ_NUM')=='1')) 
    
    ##*********  Combine full load divisions and incremental division  ************
    if full_count > 0 :
        messages_df = messages_df.union(latest_load_df)
    
    #Drop fileds
    messages_df = messages_df.drop('ANN_CST_AMT_FOR_CALC','CONTRACT_EFFECTIVE_TM','CREATED_TM','curcy_cd_cst_amt_ann_ph_currency_rate',
                                  'curcy_cd_cst_mth_exch_rate','CUST_REQUIRED_YEAR','DIVISION_2CHAR','DIVISION_3CHAR',
                                  'DIVISION_6DIGIT','DIVISION_NAME','ENTERED_YEAR','ERROR_MESSAGE','EXCH_RATE','GROUP_NAME',
                                  'ORIGINAL_PROMISE_FISCAL_YEAR','ORIGINAL_PROMISE_YEAR','PROMISE_YEAR','SOURCE_SEGMENT',
                                  'CST_AMT_ANN_PH_CURRENCY_RATE','CONTRACT_EFFECTIVE_TM','CREATED_TM','ENTERED_YEAR','ORIGINAL_PROMISE_YEAR',
                                  'PROMISE_YEAR','ORIGINAL_PROMISE_FISCAL_YEAR','ANN_CST_AMT_FOR_CALC',
                                  'SEQ_NUM')
    
    print('Write out records...')
    #check for existance of the silver table, if exists then merge if not exists overwrite
    if file_exists(datalake_sink_path):

        print('Silver exists...')      
        deltaTable = DeltaTable.forPath(spark, datalake_sink_path)
        
        #Update the dm_curr_flg to N for the exising records that match the join criteria
        deltaTable.alias("silvertable")\
                  .merge(messages_Y_df.alias("updates"),
                      "silvertable.DM_SOURCE_DIV = updates.DM_SOURCE_DIV and silvertable.DM_KEY = updates.DM_KEY" )\
                  .whenMatchedUpdate(set = {"DM_CURR_FLG": "'N'"})\
                  .execute()

        print('Write out Silver...')
        messages_df.write.mode('append').option('mergeSchema','true').option('path',datalake_sink_path).saveAsTable('datamarts.orders_silver')

    else:
       
        print('Silver does not exist...') 
        print('Write all to Silver...')
        #If the silver data lake dir does not exist write new records
        messages_df.write.mode('overwrite').option('mergeSchema','true').option('path',datalake_sink_path).saveAsTable('datamarts.orders_silver')
    
    

# COMMAND ----------

# DBTITLE 1,Capture Log info about the load
from datetime import datetime


DestinationCount = messages_df.count()
DataTimestamp = datetime.now()
    
tableRun = []

tableRun.append(Row(
                    TableName = entity, 
                    DataAsOfTime=DataTimestamp, 
                    MaxBatchTimestamp = maxBatchTimestamp, 
                    ExportTimestamp = None, 
                    TotalRecordCount = Total_record_count,   
                    DuplicateCount = 0,
                    BadDilimiterRejectCount = 0,
                    BadDataTypeRejectCount = 0,
                    DivisionRejectCount = 0,
                    LOVRejectCount = 0,
                    SourceCount= SourceCount,     
                    DestinationCount=DestinationCount, 
                    SourcePath= datalake_source_path,    
                    DestinationPath= datalake_sink_path,
                    ErrorMessage= None, 
                    RunID = None, 
                    DataLakeLayer = 'silver')
               )


if file_exists(log_path):
    #write out the lov rejects
    spark.createDataFrame(tableRun, schema=LogTableSchema).write.mode('append').option('path',log_path).saveAsTable('datamarts.orders_log')
    
else:
    #write out the lov rejects
    spark.createDataFrame(tableRun, schema=LogTableSchema).write.mode('overwrite').option('path',log_path).saveAsTable('datamarts.orders_log')
 

# COMMAND ----------

from delta.tables import *
deltaTable = DeltaTable.forPath(spark, log_path)
deltaTable.optimize().executeCompaction()

# COMMAND ----------

from datetime import datetime
from pytz import timezone
from delta.tables import *

deltaTable = DeltaTable.forPath(spark, datalake_sink_path)
deltaTable.optimize().executeCompaction()
    
