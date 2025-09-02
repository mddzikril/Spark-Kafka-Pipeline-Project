def join_df(df1, df2, join_expr, join_type):
    repartitioned_df1 = df1.repartition(3)
    repartitioned_df2 = df2.repartition(3)
    return repartitioned_df1.join(repartitioned_df2, join_expr, join_type)


def join_accounts_with_party(accounts_df, party_df):
    join_expr = accounts_df.account_id == party_df.account_id
    joined_df = join_df(accounts_df, party_df, join_expr, "left")
    return joined_df.drop(party_df.account_id)