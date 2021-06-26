from pyspark.sql import SparkSession
from pyspark.sql.functions import *
import utils.utilities as ut
import yaml
import os.path
from pyspark.sql.types import StructType, IntegerType, BooleanType,DoubleType

if __name__ == '__main__':
    current_dir = os.path.abspath(os.path.dirname(__file__))
    app_config_path = os.path.abspath(current_dir + "/../../" + "application.yml")
    app_secrets_path = os.path.abspath(current_dir + "/../../" + ".secrets")

    conf = open(app_config_path)
    app_conf = yaml.load(conf, Loader=yaml.FullLoader)
    secret = open(app_secrets_path)
    app_secret = yaml.load(secret, Loader=yaml.FullLoader)

    # Create the SparkSession
    spark = SparkSession \
        .builder \
        .appName("DataFrames examples") \
        .config("spark.mongodb.input.uri", app_secret["mongodb_config"]["uri"])\
        .getOrCreate()
    spark.sparkContext.setLogLevel('ERROR')

    # Setup spark to use s3
    hadoop_conf = spark.sparkContext._jsc.hadoopConfiguration()
    hadoop_conf.set("fs.s3a.access.key", app_secret["s3_conf"]["access_key"])
    hadoop_conf.set("fs.s3a.secret.key", app_secret["s3_conf"]["secret_access_key"])

    tgt_list = app_conf["target_list"]
    # Check if passed from cmd line arg then override the above (e.g. source_list=OL,SB)
    for tgt in tgt_list:
        staging_path = "s3a://" + app_conf["s3_conf"]["s3_bucket"] + "/" + app_conf["s3_conf"]["staging_dir"]
        tgt_conf = app_conf[tgt]

        if tgt == 'REGIS_DIM':
            print('Creating REGIS_DIM table data')
            cp_df = spark.read\
                .parquet(staging_path + '/' + tgt_conf['source_data'])
            cp_df.show()
            cp_df.createOrReplaceTempView(tgt_conf['source_data'])

            spark.sql(tgt_conf['loadingQuery']).show()


# spark-submit --packages "org.apache.hadoop:hadoop-aws:2.7.4" com/dsm/target_data_loading.py

