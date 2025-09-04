import sys

from lib import Utils
from lib.DataReader import read_accounts, read_party_address, read_party
from lib.DataTransformations import join_accounts_with_party, join_accounts_party_address, add_event_header, \
    add_key_header
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: spark_project {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    accounts_datafile = "test_data/accounts/account_samples.csv"
    parties_datafile = "test_data/parties/party_samples.csv"
    party_address_datafile = "test_data/party_address/address_samples.csv"

    accounts_df = read_accounts(spark, accounts_datafile)
    party_df = read_party(spark, parties_datafile)
    party_address_df = read_party_address(spark, party_address_datafile)

    events_header_accounts_df = add_event_header(accounts_df)
    events_and_key_header_accounts_df = add_key_header(events_header_accounts_df)

    joined_accounts_party_df = join_accounts_with_party(events_and_key_header_accounts_df, party_df)

    events_and_key_header_accounts_df.show()

    logger.info("Num Partitions: " + str(accounts_df.rdd.getNumPartitions()))

    events_and_key_header_accounts_df.write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "final_output") \
        .save()

    joined_accounts_party_address_df = join_accounts_party_address(joined_accounts_party_df, party_address_df)

    #joined_accounts_party_address_df.sort("account_id").show()

    logger.info("Finished creating Spark Session")
