import polars as pl 

def groupby_agg_polars(file_path):
    lf = pl.scan_csv(file_path)
    return (
        lf
        .group_by('state')
        .agg(
            pl.count().alias('cnt'),
            pl.col("age").mean().alias('mean age'),
            pl.col("age").min().alias('min age'),
            pl.col("age").max().alias('max age')
        )
        .sort('state')
        .collect()
    )

if __name__ == '__main__':
    print(groupby_agg_polars('data/1_million.csv'))