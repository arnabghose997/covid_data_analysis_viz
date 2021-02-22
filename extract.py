# Import Libraries
import requests
import csv
import os
import subprocess
from utils import exec_shell_commands

# Download from Github to local
def download_data(url, local_file_dir):    
    response = requests.get(url)
    lines = (line.decode("utf-8") for line in response.iter_lines())
    
    with open(file_download_dir, "w+") as f:
        while True:
            try:
                f.write(next(lines) + "\n")
            except StopIteration:
                break
    
    print("Data download successful, Location:", local_file_dir)

# Transfer file local to HDFS storage
def local_to_hadoop_transfer(local_data_dir, hdfs_data_dir):
    if not os.path.exists(local_data_dir):
        raise FileNotFoundError("File: '" + local_data_dir + "' not found.")
    
    file_transfer_command = "hdfs dfs -copyFromLocal -f " + local_data_dir + " " + hdfs_data_dir
    output, error = exec_shell_commands(file_transfer_command)
    
    if error is not None and len(error)>148:
        raise Exception("Failed to execute the shell command '"+ file_transfer_command +"':\n\nError Message:\n\n" + error)
    
    print("File transfered to HDFS location:", hdfs_data_dir)

if __name__=='__main__':
    data_download_url = "https://raw.githubusercontent.com/datasets/covid-19/main/data/countries-aggregated.csv"
    file_download_name = "covid_data_countries.csv"
    file_download_dir = "data/" + file_download_name
    hdfs_data_dir = "covid_data/" + file_download_name

    # Download data from github
    download_data(data_download_url, file_download_dir)


    # Transfer file from Local to HDFS
    local_to_hadoop_transfer(file_download_dir, hdfs_data_dir)
