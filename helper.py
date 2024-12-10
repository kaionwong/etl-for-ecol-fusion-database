import time
import logging
import pandas as pd

def time_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        print(f"Execution time of '{func.__name__}': {execution_time:.2f} seconds")
        return result
    return wrapper

def set_pandas_display_options(max_columns=None, max_rows=None, width=120, float_format='{:.6f}'):
    """
    Set pandas display options for better control over DataFrame display.

    Parameters:
    - max_columns (int or None): Maximum number of columns to display (None means display all).
    - max_rows (int): Maximum number of rows to display.
    - width (int): Width of the display to prevent columns from being cut off.
    - float_format (str): Format string for floating-point numbers.
    """
    pd.set_option('display.max_columns', max_columns)
    pd.set_option('display.max_rows', max_rows)
    pd.set_option('display.width', width)
    pd.set_option('display.float_format', float_format.format)