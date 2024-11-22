from pyspark.sql import DataFrame

def output(df_list: list,output_path: list) -> None:
    # Output of transformed data
    for i,df in enumerate(df_list):
        try:
            output_config = output_path[i]["file_path"]
            u_output_path = f"{output_config}/{i}/"
            df.write.mode("overwrite").csv(u_output_path)
            print("Successfully wrote to csv file")

        except Exception as e:
            print(f"Failed to write DataFrame {i} to parquet: {e}")
