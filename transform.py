# Import Libraries
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, sum, row_number
from pyspark.sql import Window
from pyspark.sql.types import StringType, StructField, LongType, StructType, IntegerType
import os
import shutil
from pathlib import Path

def aggregate_data(df):
    df = df.groupBy("Country") \
            .agg(sum("Confirmed").alias("Total_Confirmed"),
                 sum("Recovered").alias("Total_Recovered"),
                 sum("Deaths").alias("Total_Deaths")) \
            .sort(col("Country").asc()) \
            .coalesce(1).withColumn("row_id", row_number().over(Window.orderBy(col("Country")))) \
            .select("row_id", "Country", "Total_Confirmed", "Total_Recovered", "Total_Deaths")
    
    return df

def merge_csv_files(files_list, file_header, save_loc):
    # Writing header to the final file
    combined_csv = open(save_loc, 'w+')
    combined_csv.write(file_header)
    combined_csv.write('\n')

    # Writing data from every file
    for file in files_list:
        csv_file = open(file)
        for line in csv_file:
            combined_csv.write(line)
        csv_file.close()
    combined_csv.close()

def write_processed_data(data, save_loc, n_partition=1):
    tmp_partition_dir = "file://" + os.getcwd() + "/tmp"  # Temp Partition Folder
    data.repartition(n_partition).write.option("sep", ",").csv(tmp_partition_dir)  # Writing data with partitions

    # Removing 'file://' from tmp_partition_dir
    tmp_partition_dir = tmp_partition_dir[7:]
    
    header_names = ["row_id", "Country", "Total_Confirmed", "Total_Recovered", "Total_Deaths"]
    header = ",".join(header_names)  # String with header names separated by comma(,)

    # List containing csv file partitions
    partitioned_files = [ str(file) for file in Path(tmp_partition_dir).glob("*.csv") ]
    merge_csv_files(partitioned_files, header, save_loc)  # Merging all csv partitions

    shutil.rmtree(tmp_partition_dir)

if __name__=='__main__':
    # Init Spark Session
    spark = SparkSession.builder.master("local[*]").getOrCreate()

    hdfs_file_name = "covid_data_countries.csv"
    hdfs_dir = "hdfs://localhost:9000/user/hadoopusr/covid_data/"
    hdfs_data_location = hdfs_dir + hdfs_file_name

    transformed_data_output_dir = os.getcwd() + "/data/transformed_data.csv"

    # Reading CSV into PySpark DataFrame
    schema = StructType([
        StructField("Date", StringType(), True),
        StructField("Country", StringType(), True),
        StructField("Confirmed", LongType(), True),
        StructField("Recovered", LongType(), True),
        StructField("Deaths", LongType(), True),
    ])

    df = spark.read.csv(hdfs_data_location, header=True, schema=schema)

    # Aggregate data
    df = aggregate_data(df)

    # Write Output data
    write_processed_data(df, transformed_data_output_dir)
