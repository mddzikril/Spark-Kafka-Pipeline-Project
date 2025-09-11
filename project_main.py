import sys

from lib import Utils
from lib.DataReader import read_accounts, read_party_address, read_party
from lib.DataTransformations import join_accounts_with_party, join_accounts_party_address, add_event_header, \
    add_key_header, add_payload_header, join_party_and_address, add_party_address_struct, join_party_struct, \
    save_to_json
from lib.logger import Log4j

if __name__ == '__main__':

    if len(sys.argv) < 3:
        print("Usage: spark_project {local, qa, prod} {load_date} : Arguments are missing")
        sys.exit(-1)

    job_run_env = sys.argv[1].upper()
    load_date = sys.argv[2]

    spark = Utils.get_spark_session(job_run_env)
    logger = Log4j(spark)

    logger.info("Starting spark program")
    accounts_datafile = "test_data/accounts/account_samples.csv"
    parties_datafile = "test_data/parties/party_samples.csv"
    party_address_datafile = "test_data/party_address/address_samples.csv"

    logger.info("Reading data files")
    accounts_df = read_accounts(spark, accounts_datafile)
    party_df = read_party(spark, parties_datafile)
    party_address_df = read_party_address(spark, party_address_datafile)

    logger.info("Adding event header and key header")
    events_header_accounts_df = add_event_header(accounts_df)
    events_and_key_header_accounts_df = add_key_header(events_header_accounts_df)

    logger.info("Join party with address")
    joined_party_address = join_party_and_address(party_df, party_address_df)

    logger.info("Generating party relations struct")
    struct = add_party_address_struct(joined_party_address)

    logger.info("Join party relations struct with accounts df")
    struct_df = join_party_struct(events_and_key_header_accounts_df, struct)

    logger.info("Adding payload header")
    final_df = add_payload_header(struct_df)

    logger.info("Saving datafile to JSON")
    save_to_json(final_df)

    logger.info("Spark program end")
