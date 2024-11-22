import argparse
import json
import s3fs
from pyspark.sql import SparkSession
from transform import transform
from input import read_file, rename_columns
from output import output


def main(file_path: str):
    # Configure Spark with necessary AWS credentials and S3 settings
    spark = (
        SparkSession
        .builder
        .master('yarn')
        .appName("Spark Pipeline Example")
        .getOrCreate()
    )

    # Read JSON configuration file from S3 if it's an S3 path
    if file_path.startswith("s3a://"):
        fs = s3fs.S3FileSystem()
        with fs.open(file_path, 'r') as file:
            json_data = json.load(file)
    else:
        # For local file paths
        with open(file_path) as file:
            json_data = json.load(file)


    file_input = json_data["file_input"]

    temp_view = []

    for file in file_input:
        df = read_file(spark, file["file_path"])
        rename_columns(df, file["table_name"])
        temp_view.append(file["table_name"])

    #QUERY = "select * from " + file_inputs[0]["table_name"]
    #spark.sql(QUERY).show(5)

    #print(temp_view)
    transforms = json_data["transform"]
    #print(transforms)
    t_df = transform(spark, transforms,temp_view)

    #for table in t_df:
     #   table.show(5)

    #trans_df = transform(spark, df)

    Output_path = json_data["file_output"]
    output(t_df,Output_path)

    spark.stop()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--json_file_path", type=str,
                        help="json file path which containers the input path, sql query, output path")
    args = parser.parse_args()

    #data_source = "Input/Food_Establishment_Inspection_Data_20240717.csv"
    #output_path = "output/"
    main(args.json_file_path)

    #transform(args.data_source, args.output_uri)
