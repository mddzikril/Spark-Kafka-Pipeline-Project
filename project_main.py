import sys

from lib import Utils
from lib.DataReader import read_accounts, read_party_address, read_party
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

    accounts_df.show(10)
    accounts_df.printSchema()

    party_df.show(10)
    party_df.printSchema()

    party_address_df.show(10)
    party_address_df.printSchema()

    logger.info("Finished creating Spark Session")
