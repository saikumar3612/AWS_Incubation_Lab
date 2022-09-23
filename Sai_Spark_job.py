from pyspark.sql import SparkSession
import json, sys
from pyspark.sql.types import DecimalType, StringType
from pyspark.sql.functions import col,concat_ws, sha2, lit
from pyspark.sql import functions as f
from pyspark.conf import SparkConf
from pyspark.sql.utils import AnalysisException

spark = SparkSession.builder.appName("Delta-Lake-MS8") \
    .config("spark.jars.packages", \
            "io.delta:delta-core_2.12:0.8.0") \
    .config("spark.sql.extensions", \
            "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog",\
            "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.databricks.delta.retentionDurationCheck.enabled",\
            "false") \
    .getOrCreate()

spark.sparkContext.addPyFile("s3://saikumar-landing-zone"\
                             +"/dependencyfiles/"\
                             +"delta-core_2.12-0.8.0.jar")

from delta import *

class Transformation:

    def __init__(self):
        """
        From the JSON data read through the configuration all the values are assigned 
        accordingly to its respective variables.
        """
        self.config_data = self.read_config()
        # Assigning the data from config_data for Configuration file parameters
        self.ingest_actives_source             = self.config_data['ingest-actives']['source']['data-location']
        self.ingest_actives_destination        = self.config_data['ingest-actives']['destination']['data-location']
        self.ingest_viewership_source          = self.config_data['ingest-viewership']['source']['data-location']
        self.ingest_viewership_destination     = self.config_data['ingest-viewership']['destination']['data-location']
        
        self.ingest_raw_actives_source           = self.config_data['transformed-actives']['source']['data-location']
        self.ingest_stage_actives_destination    = self.config_data['transformed-actives']['destination']['data-location']
        
        self.ingest_raw_viewership_source        = self.config_data['transformed-viewership']['source']['data-location']
        self.ingest_stage_viewership_destination = self.config_data['transformed-viewership']['destination']['data-location']
        
        self.transformation_cols_actives         = self.config_data['transformed-actives']['transformation-cols']
        self.transformation_cols_viewership      = self.config_data['transformed-viewership']['transformation-cols']

        self.masking_cols_actives                = self.config_data['transformed-actives']['masking-cols']
        self.masking_cols_viewership             = self.config_data['transformed-viewership']['masking-cols']

        self.actives_partition_columns           = self.config_data['transformed-actives']['partition-cols']
        self.viewership_partition_columns        = self.config_data['transformed-viewership']['partition-cols']

        self.actives_pii_cols                    = self.config_data['pii-cols-actives']
        self.viewership_pii_cols                 = self.config_data['pii-cols-viewership']

        self.look_up                             = self.config_data['lookup-dataset']
    
    def read_config(self) -> json:
        """
        This function will read the configuration file data from S3 bucket and return in json format
        """
        # app_config_path = spark.conf.get("spark.path")
        app_config_path = "s3://saikumar-landing-zone/config/app-config.json"
        data = ''.join(spark.sparkContext.textFile(app_config_path).collect())
        jsonData = json.loads(data)
        return jsonData
    
    def read_parquet_data(self, file_path):
        """
        This method reads the parquet data from given specified path.        
        file_path   :  path of parquet file
        """
        try:
            dataFrame = spark.read.parquet(file_path)
            return dataFrame
        except Exception as err:
            print("Exception occured:" + str(err))
  
    def casting_columns(self, df, col_cast):
        """
        This method will type caste the given list of columns in the passed dataframe.
        
        df              : Dataframe which holds the data read from parquet file.
        columns_to_cast : List of column names that need to be type casted.        
        """
        for key in col_cast.keys():
            if col_cast[key].split(",")[0] == "DecimalType":
                df = df.withColumn(key, df[key].cast(DecimalType(10, int(col_cast[key].split(",")[1]))))
            elif col_cast[key] == "ArrayType-StringType":
                df.withColumn(key, f.concat_ws(",", col(key)))
        return df
        
    def masking_columns(self, df, columns_to_mask):
        """
        This method will mask(encrypts) the data of given list of columns in the dataframe.
        
        df  : Dataframe with the transformed columns
        columns_to_mask : List of column names which has to be masked.
        
        returns Dataframe with the masked data
        """
        for column in columns_to_mask:
            df = df.withColumn(column, sha2(col(column), 256))
        return df

    def write_transformed_dataframe(self, df, destination_path, partition_columns = []):
        """
        This method writes the transformed dataframe into a parquet file in given file path
        
        df  : Transformed dataframe
        destination_path  : file path where transformed data need to be stored in parquet format
        partition_columns: List of column names based on which the dataframe has to be partitioned
        """
        try:
            if partition_columns:
                df.write.mode("overwrite").partitionBy(partition_columns).parquet(destination_path)
            else:
                df.write.mode("overwrite").parquet(destination_path)
        except Exception as write_err:
            print("Exception invoked while writing data:" + str(write_err))
    
    def scd2_write(delta_df, df_source, targetTable, file_name_type):
        """
        This function merges the data into delta table in case of new
        records or update made to old records
        :param delta_df: delta table dataframe.
        :param df_source: transformed dataframe.
        :param targetTable: delta table object
        :param file_name_type: file type to check
        :return: flag: status flag. e.g. if data is merged
        """
        flag = False
        delta_col = [x for x in delta_df.columns if x not in ['begin_date', 'update_date', 'flag_active']]

        delta_df = delta_df.select(*(col(x).alias('target_' + x) for x in delta_df.columns))
        cond = [df_source.advertising_id == delta_df.target_advertising_id]

        left_join_df = df_source.join(delta_df, cond, "leftouter") \
            .select(df_source['*'], delta_df['*'])

        if file_name_type == "actives":
            new_entries = left_join_df.filter("target_user_id is null")
            filter_df = left_join_df.filter(left_join_df.user_id != left_join_df.target_user_id)
            mergeDf = filter_df.withColumn("MERGEKEY",
                                           f.concat(filter_df.advertising_id, filter_df.target_user_id))
        elif file_name_type == "viewership":
            new_entries = left_join_df.filter("target_channel_name is null")
            filter_df = left_join_df.filter(left_join_df.channel_name != left_join_df.target_channel_name)
            mergeDf = filter_df.withColumn("MERGEKEY",
                                           f.concat(filter_df.advertising_id, filter_df.target_channel_name))
        else:
            print("File name not matching, Hence terminating the execution")
            sys.exit(1)

        filter_df = filter_df.union(new_entries)

        if filter_df.count() != 0:

            dummyDf = filter_df.filter("target_advertising_id is not null").withColumn("MERGEKEY", lit(None))

            scdDF = mergeDf.union(dummyDf)

            Insertable = {x: "source." + x for x in delta_col}
            Insertable.update({"begin_date": "current_date", "update_date": "null", "flag_active": "True"})

            cond_ = "concat(target.advertising_id, target.user_id) = source.MERGEKEY and target.flag_active = 'true'"
            if file_name_type == "viewership":
                cond_ = cond_.replace('user_id', 'channel_name')

            targetTable.alias("target").merge(
                source=scdDF.alias("source"),
                condition=cond_
            ).whenMatchedUpdate(set={
                "update_date": "current_date",
                "flag_active": "False",
            }).whenNotMatchedInsert(values=Insertable
            ).execute()
            flag = True

        return flag

    def scd2_implementation(self, df, look_up_dict, file_name_delta):
        """
        This function is used for scd type 2 implementation process.
        :param df: DataFrame after transforming
        :param look_up_dict: Dictionary fetched from app_config
        :param file_name_delta: delta table file name
        :return: df : DataFrame after transforming
        """
        file_name_delta = file_name_delta.lower()
        delta_location = look_up_dict['data-location'] + '/' + file_name_delta
        str_value = file_name_delta + '_pii-cols'
        pii_cols = look_up_dict[str_value]
        df_source = df.withColumn("begin_date", f.current_date())
        df_source = df_source.withColumn("update_date", f.lit("null"))
        flag = False
        pii_cols = [i for i in pii_cols if i in df_source.columns]
        columns_needed = []

        for col1 in pii_cols:
            if col1 in df_source.columns:
                columns_needed += [col1, "Masked_" + col1]

        if file_name_delta == "viewership":
            str_n = "Masked_" + pii_cols[1]
            columns_needed.remove(str_n)

        source_columns_used = columns_needed + ['begin_date', 'update_date']
        
        df_source = df_source.withColumn("Masked_advertising_id", df_source['advertising_id'] )
        df_source = df_source.withColumn("Masked_user_id", df_source['user_id'])
        df_source = df_source.select(*source_columns_used)

        try:
            targetTable = DeltaTable.forPath(spark, delta_location)
            delta_df = targetTable.toDF()
        except AnalysisException:
            print('Table does not exist')
            df_source = df_source.withColumn("flag_active", lit("true"))
            df_source.write.format("delta").mode("overwrite").save(delta_location)
            print('Table Created Sucessfully!')
            targetTable = DeltaTable.forPath(spark, delta_location)
            delta_df = targetTable.toDF()
            delta_df.show(100)

        flag = Transformation.scd2_write(delta_df, df_source, targetTable, file_name_delta)
        if flag:
            print("Successfully updated delta table")
        else:
            print("No changes made in delta table")

        for i in pii_cols:
            df = df.drop(i).withColumnRenamed("Masked_" + i, i)

        return df

# Objects creation:
transform_obj = Transformation()

# Steps:
# 1. Read data from raw zone
# 2. Perform casting operations
# 3. Perform masking operations
# 4. Write data into staging zone

# Actives

if len(sys.argv) <= 2 and sys.argv[1] == "actives":
    actives_raw_df    = transform_obj.read_parquet_data(transform_obj.ingest_raw_actives_source)
    actives_casted_df = transform_obj.casting_columns(actives_raw_df, transform_obj.transformation_cols_actives)
    actives_masked_df = transform_obj.masking_columns(actives_casted_df, transform_obj.masking_cols_actives)
    transform_obj.write_transformed_dataframe(actives_masked_df, transform_obj.ingest_stage_actives_destination, transform_obj.actives_partition_columns)
    dataset = transform_obj.scd2_implementation(actives_masked_df, transform_obj.look_up , file_name_delta="actives")

# Viewership
elif len(sys.argv) <= 2 and sys.argv[1] == "viewership":
    viewership_raw_df    = transform_obj.read_parquet_data(transform_obj.ingest_raw_viewership_source)
    # viewership_raw_df    = transform_obj.read_parquet_data(transform_obj.ingest_raw_viewership_source + filename + fileformat)
    viewership_casted_df = transform_obj.casting_columns(viewership_raw_df, transform_obj.transformation_cols_viewership)
    viewership_masked_df = transform_obj.masking_columns(viewership_casted_df, transform_obj.masking_cols_viewership)
    transform_obj.write_transformed_dataframe(viewership_masked_df, transform_obj.ingest_stage_viewership_destination, transform_obj.viewership_partition_columns)
    dataset = transform_obj.scd2_implementation(viewership_masked_df, transform_obj.look_up , file_name_delta= "viewership")
    
else:
    pass

# Spark session terminates after execution
# spark.stop()