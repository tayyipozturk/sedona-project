import os

def filewriter(df, output_path: str):
    os.makedirs(os.path.dirname("output/" + output_path), exist_ok=True)

    df.coalesce(1).write \
        .option("header", True) \
        .mode("overwrite") \
        .csv("output/" + output_path)

    return output_path
