import time
import os
import contextlib
from datetime import datetime
import matplotlib.pyplot as plt
from pyspark.sql.functions import expr

def prepare_csv_compatible(df):
    cols = df.columns

    # Convert geometry if it exists
    if "geom" in cols:
        df = df.withColumn("wkt", expr("ST_AsText(geom)")).drop("geom")
    elif "geometry" in cols and dict(df.dtypes).get("geometry") == "binary":
        df = df.withColumn("wkt", expr("ST_AsText(geometry)")).drop("geometry")

    # Drop unsupported types
    unsupported_types = {"binary", "array", "map", "struct"}
    to_drop = [name for name, dtype in df.dtypes if dtype in unsupported_types]
    return df.drop(*to_drop) if to_drop else df


def log_to_file(log_dir="../logs", prefix="perf"):
    timestamp = datetime.now().strftime("%y_%m_%d_%H_%M_%S")
    filename = f"{prefix}_{timestamp}.log"
    log_path = os.path.join(log_dir, filename)
    os.makedirs(log_dir, exist_ok=True)
    return contextlib.redirect_stdout(open(log_path, "w"))

def performance_logged(label=None, save=False, show=True, save_path=None, log_dir="output"):
    def decorator(func):
        def wrapper(*args, **kwargs):
            timestamp = datetime.now().strftime("%y_%m_%d_%H_%M")
            newLogDir = os.path.join("outputs", "output-" + timestamp, log_dir)
            os.makedirs(newLogDir, exist_ok=True)
            name = label or func.__name__
            log_file = os.path.join(newLogDir, f"{name}_{timestamp}.log")

            with open(log_file, "w") as log_f, contextlib.redirect_stdout(log_f):
                print(f"=== {name} ===")
                start = time.time()
                result = func(*args, **kwargs)
                end = time.time()
                duration = end - start
                print(f"[{name}] ⏱️ Time taken: {duration:.3f} seconds")

                if hasattr(result, "show") and show:
                    result.show(truncate=False)

                # before writing
                if save and save_path and hasattr(result, "write"):
                    result = prepare_csv_compatible(result)
                    result.coalesce(1).write.option("header", True).mode("overwrite").csv(save_path)
                    print(f"[{name}] 💾 Saved to: {save_path}")

            # Save performance graph
            # graph_file = os.path.join(log_dir, f"{name}_{timestamp}_time.png")
            # plt.figure(figsize=(4, 2))
            # plt.bar([name], [duration], color="skyblue")
            # plt.ylabel("Seconds")
            # plt.title(f"Execution Time: {name}")
            # plt.tight_layout()
            # plt.savefig(graph_file)
            # plt.close()

            return result
        return wrapper
    return decorator