from pyspark.sql import SparkSession
from pyspark.sql.functions import col, substring, to_date, when, lit, length, trim
from pyspark.sql.types import StructType, StructField, StringType
from pymongo import MongoClient
from datetime import datetime
from bson.codec_options import CodecOptions
import pymongo
from pyspark.sql.functions import to_timestamp, date_format
from pyspark.sql.types import DateType, TimestampType
import pandas as pd
import json
import logging
from google.cloud import storage
import io
import time

class GCSHandler(logging.Handler):
    def __init__(self, bucket_name, blob_name):
        super().__init__()
        self.bucket_name = bucket_name
        self.blob_name = blob_name
        self.client = storage.Client()
        self.bucket = self.client.get_bucket(bucket_name)
        self.blob = self.bucket.blob(blob_name)
        self.buffer = io.StringIO()

    def emit(self, record):
        msg = self.format(record)
        self.buffer.write(msg + '\n')

    def flush(self):
        if self.buffer.tell() > 0:
            self.blob.upload_from_string(self.buffer.getvalue())
            self.buffer.seek(0)
            self.buffer.truncate()

def setup_logging(config):
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(__name__)

    log_bucket = config['log_bucket']
    log_path = config['log_path']
    gcs_handler = GCSHandler(log_bucket, f"{log_path}/etl_log_{time.strftime('%Y%m%d-%H%M%S')}.log")
    gcs_handler.setLevel(logging.INFO)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    gcs_handler.setFormatter(formatter)

    logger.addHandler(gcs_handler)
    return logger

def load_config(spark, config_path):
    try:
        df = spark.read.json(config_path, multiLine=True)
        if df.count() == 0:
            raise ValueError("Config file is empty")
        return df.first().asDict()
    except Exception as e:
        raise Exception(f"Error loading config from {config_path}: {str(e)}")

def load_file_layout(spark, file_layout_path):
    try:
        df = spark.read.json(file_layout_path, multiLine=True)
        if df.count() == 0:
            raise ValueError("File layout is empty")
        return df.first().asDict()
    except Exception as e:
        raise Exception(f"Error loading file layout from {file_layout_path}: {str(e)}")

def create_spark_session(config):
    return SparkSession.builder \
        .appName(config['spark_config']['app_name']) \
        .master(config['spark_config']['master']) \
        .getOrCreate()

def create_schema(file_layout):
    fields = file_layout['memberFileLayout'][0]['fields']
    return StructType([StructField(field['fieldName'], StringType(), True) for field in fields])

def read_fixed_width_file(spark, file_path, schema, file_layout):
    raw_data = spark.read.text(file_path)
    fields = file_layout['memberFileLayout'][0]['fields']
    for field in fields:
        raw_data = raw_data.withColumn(
            field['fieldName'],
            trim(substring(col('value'), field['position'], field['length']))
        )
    return raw_data.select([col(field['fieldName']) for field in fields])

def validate_data(df, file_layout):
    fields = file_layout['memberFileLayout'][0]['fields']
    logger.info("Starting data validation process")

    for field in fields:
        field_name = field['fieldName']
        validation_type = field['validationType'].lower()
        reaction_code = field['reactionCode']
        reaction_type = field['reactionType']

        logger.info(f"Validating field: {field_name}, Type: {validation_type}")

        if validation_type == 'required':
            df = df.withColumn(
                'validation_status',
                when(col(field_name).isNull() | (col(field_name) == ''),
                     lit(reaction_code)).otherwise(col('validation_status'))
            )
        elif validation_type == 'date':
            df = df.withColumn(
                'validation_status',
                when(to_date(col(field_name), 'yyyy-MM-dd').isNull(),
                     lit(reaction_code)).otherwise(col('validation_status'))
            )
        elif validation_type == 'list':
            valid_values_str = field.validationValues 
            logger.info(f"valid_values: {valid_values_str}")
            
            try:
             
                valid_values = json.loads(valid_values_str)
                
               
                if isinstance(valid_values, list) and field_name in df.columns:
                    df = df.withColumn(
                        'validation_status',
                        when(~col(field_name).isin(valid_values),
                             lit(reaction_code)).otherwise(col('validation_status'))
                    )
                else:
                    logger.error(f"Invalid valid_values or column {field_name} does not exist")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse valid values: {e}")

        elif validation_type == 'checkstringlengthbounds':
            validation_values_str = field.validationValues
            logger.info(f"validation_values: {validation_values_str}")

            try:
               
                validation_values = json.loads(validation_values_str)
                
                min_length = int(validation_values.get('min'))
                max_length = int(validation_values.get('max'))
                
               
                if field_name in df.columns:
                    df = df.withColumn(
                        'validation_status',
                        when(
                            (length(trim(col(field_name))) < min_length) | 
                            (length(trim(col(field_name))) > max_length),
                            lit(reaction_code)
                        ).otherwise(col('validation_status'))
                    )
                else:
                    logger.error(f"Column {field_name} does not exist in the DataFrame")
            except json.JSONDecodeError as e:
                logger.error(f"Failed to parse validation values: {e}")
            except ValueError as e:
                logger.error(f"Invalid min or max values: {e}")

        elif validation_type == 'regex':
            pattern = field.validationValues
            df = df.withColumn(
                'validation_status',
                when(~col(field_name).rlike(pattern),
                     lit(reaction_code)).otherwise(col('validation_status'))
            )

        df = df.withColumn('reaction_type',
            when((col('validation_status').isNotNull()) & (col('validation_status') != ''),
                when(lit(reaction_type) == 'Reject Record',
                    when(lit(validation_type) != 'optional', lit('Reject'))
                    .otherwise(lit('Report')))
                .otherwise(lit('Report')))
            .otherwise(lit('Load')))

    logger.info("Data validation process completed")
    return df

def save_to_mongodb(df, mongodb_uri, database_name, collection_name):
    try:
        # Create MongoDB client
        client = MongoClient(mongodb_uri)
        db = client[database_name]
        collection = db[collection_name]
        
        # Convert Spark DataFrame to Pandas and then to dictionary
        pandas_df = df.toPandas()
        records = pandas_df.to_dict('records')
        
        # Insert data into MongoDB
        collection.insert_many(records)
        logger.info(f"Successfully loaded {len(records)} records to MongoDB")
        
    except Exception as e:
        logger.error(f"Error saving to MongoDB: {str(e)}")
        raise
    finally:
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")

def convert_datetime_columns(df):
    """Convert all date/datetime columns to timestamp string format"""
    for field in df.schema.fields:
        if isinstance(field.dataType, (DateType, TimestampType)):
            df = df.withColumn(
                field.name,
                date_format(to_timestamp(df[field.name]), "yyyy-MM-dd'T'HH:mm:ss.SSS'Z'")
            )
    return df

def save_to_mongodb(df, mongodb_uri, database_name, collection_name):
    try:
        df_processed = convert_datetime_columns(df)
        
        pandas_df = df_processed.toPandas()
        records = pandas_df.to_dict('records')
        
        client = pymongo.MongoClient(mongodb_uri)
        db = client[database_name]
        collection = db.get_collection(
            collection_name,
            codec_options=CodecOptions(tz_aware=True)
        )
        
        collection.insert_many(records)
        client.close()
        return True
    
    except Exception as e:
        logger.error(f"Error saving to MongoDB: {str(e)}")
        if 'client' in locals():
            client.close()
            logger.info("MongoDB connection closed")
        return False

def main(config_path):
    try:
        
        spark = create_spark_session({'spark_config': {'app_name': 'MyApp', 'master': 'local[*]'}})

        config = load_config(spark, config_path)
        global logger
        logger = setup_logging(config) 
        logger.info("Logger initialized")

        
        spark = create_spark_session(config)
        logger.info("Spark session created")

        file_layout = load_file_layout(spark, config['file_layout_path'])
        logger.info("File layout loaded")
        
        schema = create_schema(file_layout)
        logger.info("Schema created")

        bronze_data = read_fixed_width_file(spark, config['input_file_path'], schema, file_layout)
        logger.info("Fixed-width file read successfully")

        bronze_data = bronze_data.withColumn('validation_status', lit(""))
        bronze_data = validate_data(bronze_data, file_layout)
        logger.info("Data validation completed")        
        
        # Save Bronze Layer
        bronze_data.write.mode('overwrite').parquet(config['output_paths']['bronze'])
        logger.info(f"Bronze layer saved to {config['output_paths']['bronze']}")
        
        # Silver Layer
        silver_data = bronze_data.filter(col('reaction_type') != 'Reject')
        silver_data = silver_data.drop('validation_status', 'reaction_type')
        logger.info("Silver layer data prepared")
        
        # Data type conversions
        for date_column in config['date_columns']:
            silver_data = silver_data.withColumn(date_column, to_date(date_column, 'yyyy-MM-dd'))
        logger.info("Date columns converted")
        
        # Save Silver Layer
        silver_data.write.mode('overwrite').parquet(config['output_paths']['silver'])
        logger.info(f"Silver layer saved to {config['output_paths']['silver']}")
        
        # # Gold Layer
        # gold_data = silver_data
        
        # # Save Gold Layer as JSON files
        # gold_data.write.mode('overwrite').parquet(config['output_paths']['gold'])
        # logger.info(f"Gold layer saved to {config['output_paths']['gold']}")
        

        try:
            gold_data = silver_data
    
            output_path = config['output_paths']['gold']
            gold_data.write.mode('overwrite').parquet(output_path)
            logger.info(f"Gold layer saved to {output_path}")
            
            mongodb_uri = "mongodb+srv://admin:admin@atlascluster.sb6xh.mongodb.net/?retryWrites=true&w=majority&appName=AtlasCluster"
            
            success = save_to_mongodb(
                gold_data,
                mongodb_uri,
                database_name="fanisko",
                collection_name="fanisko_gold_layer"
            )
    
            if success:
                logger.info("Gold layer successfully saved to MongoDB")
            else:
                logger.error("Failed to save gold layer to MongoDB")
        except Exception as e:
            logger.error(f"An error occurred: {str(e)}")
            raise
    except Exception as e:
        if 'logger' in locals():
            logger.error(f"An error occurred: {str(e)}")
        else:
            print(f"An error occurred: {str(e)}")
    finally:
        if 'spark' in locals():
            spark.stop()
            if 'logger' in locals():
                logger.info("Spark session stopped")
        
        if 'logger' in locals():
            logger.info("--------------------------------------------")
            logger.info("--------------------------------------------")
        
            # Flush logs to GCS
            for handler in logger.handlers:
                if isinstance(handler, GCSHandler):
                    handler.flush()

if __name__ == "__main__":
    main('gs://fanisko-bucket/code/config.json')
