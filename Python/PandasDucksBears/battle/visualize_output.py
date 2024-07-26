import polars as pl
import seaborn as sns
import matplotlib.pyplot as plt

def visualize_output(data):
    
    sns.set_style(style=None)
    df = pl.DataFrame(data, schema=['time in seconds', 'query type', 'library'], orient="row")
    plt.figure(figsize=(20, 8))
    ax = sns.barplot(
        df,
        x='query type', 
        y='time in seconds', 
        hue='library', 
        errorbar=None, 
        palette=['#7DCEA0', '#F1948A', '#F9E79F']
    )
    
    for container in ax.containers:
        ax.bar_label(container)

    ax.set(xlabel='', ylabel='Time in Seconds')
    plt.title('Pandas vs DuckDB vs Polars - Speed Comparison')
    plt.savefig('output.png')
    plt.show()