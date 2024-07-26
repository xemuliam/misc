import polars as pl

def left_join_polars(file_path1, file_path2):
    base_lf = pl.scan_csv(file_path1).collect()
    join_lf = pl.scan_csv(file_path2).collect()

    output = base_lf.lazy().join(join_lf.lazy(), on='email', how='left').collect()
    return output

if __name__ == '__main__':
    print(left_join_polars('data/10_million.csv', 'data/1_million.csv'))