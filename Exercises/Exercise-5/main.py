import psycopg2
import csv




def create_table(table_name, columns,cur):
    create_table_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({','.join(columns)})"
    cur.execute(create_table_query)


def isconvertable(ele) :
    try :
        return int(ele)
    except:
        return str(ele)
        pass

def main():
    host = "localhost"
    database = "postgres"
    user = "postgres"
    pas = "Uttu@2213"
    conn = psycopg2.connect(host=host, database=database, user=user, password=pas)
    cursor = conn.cursor()

    with open('data/accounts.csv', 'r') as file:
        create_table('accounts', ['customer_id INT PRIMARY KEY', 'first_name VARCHAR(255) NOT NULL', 'last_name VARCHAR(255)', 'address_1 VARCHAR(255)','address_2 VARCHAR(255)','city VARCHAR(255)','state VARCHAR(255)','zip_code VARCHAR(255)','join_date VARCHAR(255)'],cursor)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_accounts ON accounts(customer_id)')
        next(file)
        data_csv = csv.reader(file)
        for i in enumerate(data_csv) :
            cursor.execute(f"INSERT INTO accounts VALUES {tuple([isconvertable(x) for x in i[1]])}")
        print('accounts data updated')

    with open('data/products.csv','r') as file :
        create_table('products',['product_id INT PRIMARY KEY','product_code INT','product_description varchar(255)'],cursor)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_products ON products(product_id)')
        next(file)
        data_csv = csv.reader(file)
        for i in enumerate(data_csv) :
            cursor.execute(f"INSERT INTO products VALUES {tuple([isconvertable(x) for x in i[1]])}")
        print("products data updated")
    
    with open('data/transactions.csv','r') as file :
        create_table('transactions',['transaction_id VARCHAR(255)','transaction_date varchar(255)','product_id INT','product_code INT','product_description varchar(255)','quantity INT','account_id INT','FOREIGN KEY (product_id) REFERENCES products(product_id)'],cursor)
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_transaction ON transactions(transaction_id)')
        next(file)
        data_csv = csv.reader(file)
        for i in enumerate(data_csv) :
            cursor.execute(f"INSERT INTO transactions VALUES {tuple([isconvertable(x) for x in i[1]])}")
        print("transaction data updated")


    conn.commit()
    conn.close()
    



if __name__ == "__main__":
    main()
