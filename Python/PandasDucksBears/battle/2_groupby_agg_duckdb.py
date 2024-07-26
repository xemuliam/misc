import duckdb

def groupby_agg_duckdb(file_path):
    query = f'''
        select 
            state,
            count(1),
            avg(age),
            min(age),
            max(age)
        from "{file_path}"
        group by state
        order by state
        ;
    '''
    return duckdb.sql(query).arrow()

if __name__ == '__main__':
    print(groupby_agg_duckdb('data/1_million.csv'))