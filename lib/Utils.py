from pyspark.sql import SparkSession


def get_spark_session(env):
    if env == "LOCAL":
        return SparkSession.builder \
            .master("local[2]") \
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
