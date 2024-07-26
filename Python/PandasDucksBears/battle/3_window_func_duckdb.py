import duckdb

def window_func_duckdb(file_path):
    query = f'''
        select 
            avg(age) over(partition by state),
        from "{file_path}"
        ;
    '''
    return duckdb.sql(query).arrow()

if __name__ == '__main__':
    print(window_func_duckdb('data/1_million.csv'))