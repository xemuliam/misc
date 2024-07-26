import polars as pl 

def window_func_polars(file_path):
    lf = pl.scan_csv(file_path)
    return (
        lf
        .select(
            [
                'email',
                'first',
                'last',
                'state',
                'age'
            ]
        )
        .with_columns([
            pl.col('age').mean().over('state').alias('avg_age_per_state'),
            # pl.col('age').rank(method='dense').over('state').alias('age_rank')
        ])
        .sort('state')
        .collect()
    )

if __name__ == '__main__':
    print(window_func_polars('data/1_million.csv'))