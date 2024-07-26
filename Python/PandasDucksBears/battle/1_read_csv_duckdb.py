import duckdb

def read_csv_duckdb(file_path):
    query = f'''
        select * from "{file_path}";
    '''
    return duckdb.sql(query).arrow()

if __name__ == '__main__':
    print(read_csv_duckdb('data/1_million.csv'))