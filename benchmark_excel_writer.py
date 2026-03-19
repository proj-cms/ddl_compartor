import pandas as pd
import numpy as np
import time
import os
from src.excel.excel_writer import ExcelWriter

def generate_data(rows=10000, cols=20):
    data = np.random.randint(0, 100, size=(rows, cols))
    columns = [f'col_{i}' for i in range(cols)]
    # Add some columns that will be highlighted
    columns[0] = 'diff_db1'
    columns[1] = 'diff_db2'
    df = pd.DataFrame(data, columns=columns)
    return df

def run_benchmark():
    diff = generate_data()
    only_db1 = generate_data(rows=5000)
    only_db2 = generate_data(rows=5000)
    out_path = 'benchmark_result.xlsx'

    start_time = time.time()
    ExcelWriter.write(diff, only_db1, only_db2, out_path)
    end_time = time.time()

    execution_time = end_time - start_time
    print(f"Execution time: {execution_time:.4f} seconds")

    if os.path.exists(out_path):
        os.remove(out_path)
    return execution_time

if __name__ == "__main__":
    # Warm up
    run_benchmark()

    iterations = 5
    times = []
    for i in range(iterations):
        print(f"Iteration {i+1}...")
        times.append(run_benchmark())

    avg_time = sum(times) / iterations
    print(f"Average execution time over {iterations} iterations: {avg_time:.4f} seconds")
