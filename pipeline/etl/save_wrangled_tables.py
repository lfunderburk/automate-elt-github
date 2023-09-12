# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None


# +
import duckdb
import pandas as pd 

# +
def save_result(conn, query, file_name, table_name, columns ):
    result = conn.execute(query).fetchall()
    df = pd.DataFrame(result, columns=columns)
    
    # Remove ? from the data
    df = df.replace('?', float('NaN'))
    
    # Explicitly cast columns based on table_name
    if table_name == 'account_trans_order':
        int_columns = ['account_id', 'account_creation_date', 'trans_id', 'transaction_amount', 'balance', 'order_id', 'account_to', 'order_amount','transaction_date']
        for col in int_columns:
            df[col] = df[col].astype('int32')
    
    elif table_name == 'client_account_district':
        int_columns = ['client_id', 'birth_number', 'account_id', 'account_creation_date', 'no_of_entrepreneurs_per_1000_inhabitants']
        for col in int_columns:
            df[col] = df[col].astype('int32')
    
    # Save to parquet
    df.to_parquet(file_name, index=False)


# -

if __name__ == "__main__":
    # Connect to the DuckDB database file
    conn = duckdb.connect(database='bank_data.duck.db', read_only=True)

    query_a = "SELECT * FROM account_trans_order"
    save_result(conn, query_a, "etl/expanded_data/account_trans_order.parquet", "account_trans_order", ['account_id',
                                                                    'frequency',
                                                                    'account_creation_date',
                                                                    'trans_id',
                                                                    'transaction_date',
                                                                    'transaction_type',
                                                                    'operation',
                                                                    'transaction_amount',
                                                                    'balance',
                                                                    'order_id',
                                                                    'bank_to',
                                                                    'account_to',
                                                                    'order_amount'] )

    query_a = "SELECT * FROM client_account_district"
    save_result(conn, query_a, "etl/expanded_data/client_account_district.parquet", "client_account_district", ['client_id',
                                                                        'birth_number',
                                                                        'account_id',
                                                                        'frequency',
                                                                        'account_creation_date',
                                                                        'district_name',
                                                                        'region',
                                                                        'no_of_inhabitants',
                                                                        'average_salary',
                                                                        'unemployment_rate_95',
                                                                        'unemployment_rate_96',
                                                                        'no_of_entrepreneurs_per_1000_inhabitants'] )





