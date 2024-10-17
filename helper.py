import time
import logging

def time_execution(func):
    def wrapper(*args, **kwargs):
        start_time = time.time()
        result = func(*args, **kwargs)
        end_time = time.time()
        execution_time = end_time - start_time
        logging.critical(f"Execution time of '{func.__name__}': {execution_time:.2f} seconds")
        return result
    return wrapper