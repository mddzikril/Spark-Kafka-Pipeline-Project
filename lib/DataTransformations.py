import pyspark.sql.functions as f


def join_df(df1, df2, join_expr, join_type):
    repartitioned_df1 = df1.repartition(3)
    repartitioned_df2 = df2.repartition(3)
    return repartitioned_df1.join(repartitioned_df2, join_expr, join_type)


def join_accounts_with_party(accounts_df, party_df):
    join_expr = accounts_df.account_id == party_df.account_id
    joined_df = join_df(accounts_df, party_df, join_expr, "left")
    return joined_df.drop(party_df.account_id)


def join_accounts_party_address(joined_accounts_party_df, address_df):
    join_expr = joined_accounts_party_df.party_id == address_df.party_id
    joined_df = join_df(joined_accounts_party_df, address_df, join_expr, "left")
    return joined_df.drop(address_df.party_id)


def add_event_header(df):
    eventIdentifier = f.expr("uuid()").alias("eventIdentifier")
    eventType = f.lit("MDM-Contract").alias("eventType")
    MajorSchemaVersion = f.lit(1).alias("MajorSchemaVersion")
    MinorSchemaVersion = f.lit(0).alias("MinorSchemaVersion")
    eventDateTime = f.current_timestamp().alias("eventDateTime")
    new_df = df.withColumn("eventHeader", f.struct(eventIdentifier, eventType, MajorSchemaVersion,
                                                   MinorSchemaVersion, eventDateTime))
    return new_df


def add_key_header(df):
    keyField = f.lit("contractIdentifier").alias("keyField")
    keyValue = df.account_id.cast("string").alias("keyField")
    key_array = f.array(f.struct(keyField, keyValue))
    new_df = df.withColumn("keys", key_array)
    return new_df



# def add_payload(operation)