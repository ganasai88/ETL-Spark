from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col


def transform(spark: SparkSession, transforms: list, temp_view: list) -> list:


    #num_partitions = df.rdd.getNumPartitions()

    # Print the number of partitions
    #print(f"Number of partitions: {num_partitions}")
    # SQL Query
    """
    GROUP_BY_QUERY =
                SELECT Restaurant_Name, count(*) AS total_red_violations
                FROM Restaurant_Violations
                WHERE `Violation Level` = 'RED'
                GROUP BY Restaurant_Name

    """
    transforms_list = []

    # Transform data
    for i,transform in enumerate(transforms):  # zip both lists together
        query = transform["sql"]
        view = temp_view[i]
        # Replace "table_name" in the query with the corresponding temp_view name
        query = query.replace("table_name",view)

        # Execute the SQL query
        transform_data = spark.sql(query)
        #transform_data.show(5)

        # Append the resulting DataFrame to the list
        transforms_list.append(transform_data)

    #transform_data.show(10)

    # Logs into EMR std
    #print(f"Number of rows in SQL query with red violations: {transform_data.count()}")

    return transforms_list
