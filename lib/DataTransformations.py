import pyspark.sql.functions as f
from pyspark.sql.types import StringType


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
    event_header_struct = f.struct(
        f.expr("uuid()").alias("eventIdentifier"),
        f.lit("MDM-Contract").alias("eventType"),
        f.lit(1).alias("MajorSchemaVersion"),
        f.lit(0).alias("MinorSchemaVersion"),
        f.current_timestamp().alias("eventDateTime")
    )
    new_df = df.withColumn("eventHeader", event_header_struct)
    return new_df


def add_key_header(df):
    key_struct = f.struct(
        f.lit("contractIdentifier").alias("keyField"),
        df.account_id.cast("string").alias("keyField")
    )
    key_array = f.array(key_struct)
    new_df = df.withColumn("keys", key_array)
    return new_df


def insert_operation(col, alias):
    return (f.struct(
        f.lit("INSERT").alias("operation"),
        col.alias("newValue"),
        f.lit(None).alias("oldValue")
    )).alias(alias)


def create_legal_title_line_struct(df):
    legal_title_1 = f.struct(
        f.lit("lgl_ttl_ln_1").alias("contractTitleLineType"),
        df.legal_title_1.alias("contractTitleLine")
    )
    legal_title_2 = f.struct(
        f.lit("lgl_ttl_ln_2").alias("contractTitleLineType"),
        df.legal_title_2.alias("contractTitleLine")
    )

    array = f.array(legal_title_1, legal_title_2)

    return f.filter(array, lambda x: x.contractTitleLine.isNotNull())


def join_party_and_address(party_df, address_df):
    join_expr = party_df.party_id == address_df.party_id
    joined_df = join_df(party_df, address_df, join_expr, "left")
    joined_df = joined_df.drop(address_df.party_id)
    joined_df = joined_df.drop(address_df.load_date)
    return joined_df


def add_party_address_struct(df):
    address_struct = f.struct(
                df.address_line_1.alias("addressLine1"),
                df.address_line_2.alias("addressLine2"),
                df.city.alias("addressCity"),
                df.postal_code.cast(StringType()).alias("addressPostalCode"),
                df.country_of_address.alias("addressCountry"),
                df.address_start_date.alias("addressStartDate")
            )

    has_address = df.address_line_1.isNotNull()

    address_insert = f.when(has_address, insert_operation(address_struct, "partyAddress"))

    new_df = df.withColumn(
        "party_address", f.struct(
            insert_operation(df.party_id.cast(StringType()), "partyIdentifier"),
            insert_operation(df.relation_type, "partyRelationshipType"),
            insert_operation(df.relation_start_date, "partyRelationStartDateTime"),
            address_insert,
        )
    )
    new_df = new_df.groupBy("account_id").agg(
        f.collect_list("party_address").alias("party_address_struct")
    )
    return new_df


def join_party_struct(df, struct):
    join_expr = df.account_id == struct.account_id
    joined_df = join_df(df, struct, join_expr, "left")
    joined_df = joined_df.drop(struct.account_id)
    return joined_df


def add_payload_header(df):
    payload_struct = f.struct(
        insert_operation(df.account_id.cast(StringType()), "contractIdentifier"),
        insert_operation(df.source_sys, "sourceSystemIdentifier"),
        insert_operation(df.account_start_date, "contractStartDateTime"),
        insert_operation(create_legal_title_line_struct(df), "contractTitle"),
        insert_operation(f.struct(df.tax_id_type.alias("taxIdType"), df.tax_id.alias("taxId")),
                         "taxIdentifier"),
        insert_operation(df.branch_code, "contractBranchCode"),
        insert_operation(df.country, "contractCountry"),
        df.party_address_struct.alias("partyRelations")
    )
    new_df = df.withColumn("payload", payload_struct)
    return new_df


def save_to_json(df):
    repartitioned_df = df.repartition(1)
    sorted_df = repartitioned_df.sort("account_id")
    sorted_df.select("eventHeader", "keys", "payload").write \
        .format("json") \
        .mode("overwrite") \
        .option("path", "final_output") \
        .save()
