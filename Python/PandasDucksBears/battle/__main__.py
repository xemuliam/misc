import importlib
import pathlib
import time
from visualize_output import visualize_output

def main():
    file_path_1 = 'data/1_million.csv'
    file_path_10 = 'data/10_million.csv'
    outputs = []
    this_dir = pathlib.Path(__file__).parent
    for benchmark in sorted(this_dir.glob('*[pandas|polars|duckdb].py')):
        module_name = benchmark.with_suffix('').name
        func_name = benchmark.with_suffix('').name[2:]
        module = importlib.import_module(module_name)
        benchmark_function = getattr(module, func_name)
        query_type = '_'.join(func_name.split('_')[:-1])
        library = func_name.split('_')[-1]

        start = time.time()
        if module_name[0] < '4':
            benchmark_function(file_path_1)
        else:
            benchmark_function(file_path_10, file_path_1)
        end = time.time()
        seconds = round(end - start, 2)
        print(func_name, seconds)

        output = [seconds, query_type, library]
        outputs.append(output)

    visualize_output(outputs)

if __name__ == '__main__':
    main()