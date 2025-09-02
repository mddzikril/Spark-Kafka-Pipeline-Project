from pyspark.sql.types import StructField, IntegerType, DateType, StructType, StringType, TimestampType, LongType


def accounts_schema():
    accounts_schema_struct = StructType([
        StructField("load_date", DateType()),
        StructField("active_ind", IntegerType()),
        StructField("account_id", LongType()),
        StructField("source_sys", StringType()),
        StructField("account_start_date", TimestampType()),
        StructField("legal_title_1", StringType()),
        StructField("legal_title_2", StringType()),
        StructField("tax_id_type", StringType()),
        StructField("tax_id", StringType()),
        StructField("branch_code", StringType()),
        StructField("country", StringType())
    ])

    return accounts_schema_struct


def party_schema():
    party_schema_struct = StructType([
        StructField("load_date", DateType()),
        StructField("account_id", LongType()),
        StructField("party_id", LongType()),
        StructField("relation_type", StringType()),
        StructField("relation_start_date", DateType())
    ])

    return party_schema_struct


def party_address_schema():
    party_address_schema_struct = StructType([
        StructField("load_date", DateType()),
        StructField("party_id", IntegerType()),
        StructField("address_line_1", StringType()),
        StructField("address_line_2", StringType()),
        StructField("city", StringType()),
        StructField("postal_code", IntegerType()),
        StructField("country_of_address", StringType()),
        StructField("address_start_date", DateType())
    ])

    return party_address_schema_struct


def read_accounts(spark, datafile):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .schema(accounts_schema()) \
        .csv(datafile)


def read_party(spark, datafile):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .schema(party_schema()) \
        .csv(datafile)


def read_party_address(spark, datafile):
    return spark.read \
        .option("header", "true") \
        .option("inferSchema", "true") \
        .schema(party_address_schema()) \
        .csv(datafile)