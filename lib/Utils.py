from pyspark.sql import SparkSession

from lib.LoadConfig import get_spark_conf


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .config(conf=get_spark_conf(env)) \
            .master("local[3]") \
            .enableHiveSupport() \
            .getOrCreate()
    else:
        return SparkSession.builder \
            .enableHiveSupport() \
            .getOrCreate()


def load_df_csv(spark, datafile):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .csv(datafile)
