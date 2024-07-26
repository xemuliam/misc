import pandas as pd 

def window_func_pandas(file_path):
    df = pd.read_csv(file_path, engine=None, dtype_backend='numpy_nullable')
    return (
        df
        .loc[:, ['email', 'first', 'last', 'state', 'age']]
        .assign(avg_age_per_state=lambda df: df.groupby('state')['age'].transform('mean'))
        # .assign(age_rank=lambda df: df.groupby('state')['age'].rank(method='dense'))
        .sort_values(by=['state'])
    )

if __name__ == '__main__':
    print(window_func_pandas('data/1_million.csv'))