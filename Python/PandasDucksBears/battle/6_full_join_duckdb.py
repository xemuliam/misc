import duckdb

def full_join_duckdb(file_path1, file_path2):
    query = f'''
        select *
        from "{file_path1}" base_data
        full join "{file_path2}" join_data
            using (email) 
        ;
    '''
    return duckdb.sql(query).arrow()

if __name__ == '__main__':
    print(full_join_duckdb('data/10_million.csv', 'data/1_million.csv'))