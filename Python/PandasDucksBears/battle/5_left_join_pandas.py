import pandas as pd

def left_join_pandas(file_path1, file_path2):
    base_df = pd.read_csv(file_path1, engine=None, dtype_backend='numpy_nullable')
    join_df = pd.read_csv(file_path2, engine=None, dtype_backend='numpy_nullable')

    output = base_df.merge(join_df, on='email', how='left')
    return output

if __name__ == '__main__':
    print(left_join_pandas('data/10_million.csv', 'data/1_million.csv'))