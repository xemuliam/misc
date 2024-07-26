import polars as pl

def read_csv_polars(file_path):
    lf = pl.scan_csv(file_path)
    return lf.collect()

if __name__ == '__main__':
    print(read_csv_polars('data/1_million.csv'))