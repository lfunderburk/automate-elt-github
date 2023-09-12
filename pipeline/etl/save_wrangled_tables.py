# + tags=["parameters"]
# declare a list tasks whose products you want to use as inputs
upstream = None


# +
import duckdb
import pandas as pd 

# +
def save_result(query, file_name, columns):
    result = conn.execute(query).fetchall()
    with open(file_name, 'w') as file:
        file.write(','.join(columns) + '\n')
        for row in result:
            file.write(','.join(map(str, row)) + '\n')


# -

if __name__ == "__main__":
    # Connect to the DuckDB database file
    conn = duckdb.connect(database='bank_data.duck.db', read_only=True)

    query_a = "SELECT * FROM account_trans_order"
    save_result(query_a, "etl/expanded_data/account_trans_order.csv",['account_id',
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
    save_result(query_a, "etl/expanded_data/client_account_district.csv",['client_id',
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

     # Read the data from the CSV file
    cad = pd.read_csv('etl/expanded_data/client_account_district.csv')
    
    # Remove ? from thedata
    cad = cad.replace('?', float('NaN'))

    # Save to csv 
    cad.to_csv('etl/expanded_data/client_account_district.csv', index=False)

    ato = pd.read_csv('etl/expanded_data/account_trans_order.csv')
    # Remove ? from thedata
    ato = ato.replace('?', float('NaN'))

    # Save to csv 
    ato.to_csv('etl/expanded_data/account_trans_order.csv', index=False)




