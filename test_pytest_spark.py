import pytest
import pyspark.sql.functions as f
from lib import LoadConfig, DataReader, DataTransformations
from lib.Utils import get_spark_session
from pyspark.sql.types import StructType, StructField, StringType, NullType, TimestampType, ArrayType, DateType, Row

@pytest.fixture(scope='session')
def spark():
    return get_spark_session("LOCAL")

@pytest.fixture(scope='session')
def expected_final_df(spark):
    schema = StructType(
        [StructField('keys',
                     ArrayType(StructType([StructField('keyField', StringType()),
                                           StructField('keyValue', StringType())]))),
         StructField('payload',
                     StructType([
                         StructField('contractIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('sourceSystemIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractStartDateTime',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', TimestampType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractTitle',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', ArrayType(
                                                     StructType([StructField('contractTitleLineType', StringType()),
                                                                 StructField('contractTitleLine', StringType())]))),
                                                 StructField('oldValue', NullType())])),
                         StructField('taxIdentifier',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue',
                                                             StructType([StructField('taxIdType', StringType()),
                                                                         StructField('taxId', StringType())])),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractBranchCode',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('contractCountry',
                                     StructType([StructField('operation', StringType()),
                                                 StructField('newValue', StringType()),
                                                 StructField('oldValue', NullType())])),
                         StructField('partyRelations',
                                     ArrayType(StructType([
                                         StructField('partyIdentifier',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationshipType',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', StringType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyRelationStartDateTime',
                                                     StructType([
                                                         StructField('operation', StringType()),
                                                         StructField('newValue', TimestampType()),
                                                         StructField('oldValue', NullType())])),
                                         StructField('partyAddress',
                                                     StructType([StructField('operation', StringType()),
                                                                 StructField(
                                                                     'newValue',
                                                                     StructType(
                                                                         [StructField('addressLine1', StringType()),
                                                                          StructField('addressLine2', StringType()),
                                                                          StructField('addressCity', StringType()),
                                                                          StructField('addressPostalCode',
                                                                                      StringType()),
                                                                          StructField('addressCountry', StringType()),
                                                                          StructField('addressStartDate', DateType())
                                                                          ])),
                                                                 StructField('oldValue', NullType())]))])))]))])
    df = spark.read.format("json").schema(schema).load("test_data/results/final_df.json") \
        .select("keys", "payload")
    df_with_key = df.withColumn("sortKey", f.col("keys")[0]["keyValue"])

    # Sort by keyValue
    sorted_df = df_with_key.orderBy("sortKey")

    return sorted_df.drop("sortKey")


def test_spark_version(spark):
    assert spark.version == "3.5.4"


def test_get_config():
    conf_local = LoadConfig.get_config("LOCAL")
    assert conf_local["kafka.topic"] == "spark-pipeline-dev"


def test_read_accounts(spark):
    accounts_df = DataReader.read_accounts(spark, "test_data/accounts/account_samples.csv")
    assert accounts_df.count() == 9


def test_read_parties(spark):
    parties_df = DataReader.read_party(spark, "test_data/parties/party_samples.csv")
    assert parties_df.count() == 11


def test_read_address(spark):
    address_df = DataReader.read_party_address(spark, "test_data/party_address/address_samples.csv")
    assert address_df.count() == 9


def test_final_df(spark, expected_final_df):
    accounts_datafile = "test_data/accounts/account_samples.csv"
    parties_datafile = "test_data/parties/party_samples.csv"
    party_address_datafile = "test_data/party_address/address_samples.csv"

    accounts_df = DataReader.read_accounts(spark, accounts_datafile)
    party_df = DataReader.read_party(spark, parties_datafile)
    party_address_df = DataReader.read_party_address(spark, party_address_datafile)
    events_header_accounts_df = DataTransformations.add_event_header(accounts_df)
    events_and_key_header_accounts_df = DataTransformations.add_key_header(events_header_accounts_df)
    joined_party_address = DataTransformations.join_party_and_address(party_df, party_address_df)
    struct = DataTransformations.add_party_address_struct(joined_party_address)
    struct_df = DataTransformations.join_party_struct(events_and_key_header_accounts_df, struct)
    payload_accounts_df = DataTransformations.add_payload_header(struct_df)
    final_df = DataTransformations.get_header_columns(payload_accounts_df)
    assert final_df.select("keys", "payload").collect() == expected_final_df.collect()


