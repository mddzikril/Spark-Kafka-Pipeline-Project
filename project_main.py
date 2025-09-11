import sys

from lib import Utils, DataReader, DataTransformations, LoadConfig
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: spark_project {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Starting spark job in " + job_run_env + " mode")
    application_conf = LoadConfig.get_config(job_run_env)

    accounts_datafile = "test_data/accounts/account_samples.csv"
    parties_datafile = "test_data/parties/party_samples.csv"
    party_address_datafile = "test_data/party_address/address_samples.csv"

    logger.info("Reading data files")
    accounts_df = DataReader.read_accounts(spark, accounts_datafile)
    party_df = DataReader.read_party(spark, parties_datafile)
    party_address_df = DataReader.read_party_address(spark, party_address_datafile)

    logger.info("Adding event header and key header to accounts df")
    events_header_accounts_df = DataTransformations.add_event_header(accounts_df)
    events_and_key_header_accounts_df = DataTransformations.add_key_header(events_header_accounts_df)

    logger.info("Join party df with address df")
    joined_party_address = DataTransformations.join_party_and_address(party_df, party_address_df)

    logger.info("Generating party relations struct in joined party and address df")
    struct = DataTransformations.add_party_address_struct(joined_party_address)

    logger.info("Join party relations struct with accounts df")
    struct_df = DataTransformations.join_party_struct(events_and_key_header_accounts_df, struct)

    logger.info("Adding payload header in accounts df")
    final_df = DataTransformations.add_payload_header(struct_df)

    logger.info("Separating JSON header columns and saving datafile to JSON")
    DataTransformations.save_to_json(final_df)

    logger.info("Spark job end")
