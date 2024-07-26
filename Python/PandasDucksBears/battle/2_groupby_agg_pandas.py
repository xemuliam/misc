import pandas as pd 

def groupby_agg_pandas(file_path):
    df = pd.read_csv(file_path, engine=None, dtype_backend='numpy_nullable')
    return (
        df
        .groupby('state')['age']
        .agg(
            ['count', 'mean', 'min', 'max'],
        )
        .sort_values(by=['state'])
    )

if __name__ == '__main__':
    print(groupby_agg_pandas('data/1_million.csv'))