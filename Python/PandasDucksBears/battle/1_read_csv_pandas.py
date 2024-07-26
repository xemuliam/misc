import pandas as pd

def read_csv_pandas(file_path):
    df = pd.read_csv(file_path, engine=None, dtype_backend='numpy_nullable')
    return df

if __name__ == '__main__':
    print(read_csv_pandas('data/1_million.csv'))