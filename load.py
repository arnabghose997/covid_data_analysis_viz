import pymysql.cursors
import os
import argparse
import configparser
from tqdm import tqdm
from time import sleep
import sys
from utils import csv_reader, get_latitude_longitude

def db_check(connection):
    db_check_query = """
        show databases like '%covid%';
    """

    with connection.cursor() as cursor:
        cursor.execute(db_check_query)
        if cursor.fetchall():
            return 1
    
    return 0

def db_creation(connection):
    db_creation_query = """
        create database covid_db;
    """

    with connection.cursor() as cursor:
        cursor.execute(db_creation_query)

    connection.commit()


def table_check(connection, table_name):
    if table_name in ["covid_data", "country_geodata", "covid_master_data"]:
        table_check_query = """
            show tables from covid_db where Tables_in_covid_db like '%{0}%';
        """.format(table_name)
    else:
        raise Exception("Invalid table name: '{}'".format(table_name))
    with connection.cursor() as cursor:
        cursor.execute(table_check_query)
        if cursor.fetchall():
            return 1
    
    return 0

def table_creation(connection, table_name):
    if table_name=='covid_data':
        table_creation_query = """
            create table covid_data(row_id bigint, Country varchar(100), Total_Confirmed bigint, Total_Recovered bigint, Total_Deaths bigint);
        """
    elif table_name=='country_geodata':
        table_creation_query = """
            create table country_geodata(row_id bigint, Country varchar(100), Latitude double, Longitude double);
        """
    elif table_name=='covid_master_data':
        table_creation_query = """
            create table covid_master_data(row_id bigint, Country varchar(100), Latitude double, Longitude double, Total_Confirmed bigint, Total_Recovered bigint, Total_Deaths bigint);
        """
    else:
        raise Exception("Invalid table name: '{0}'".format(table_name))

    with connection.cursor() as cursor:
        cursor.execute("use covid_db;")
        cursor.execute(table_creation_query)
    
    connection.commit()
    print("Table {} created".format(table_name))

def data_insertion(connection, insert_values_tuple):
    data_insertion_query = """
        insert into covid_db.covid_data(row_id,Country,Total_Confirmed,Total_Recovered,Total_Deaths) values (%s, %s, %s, %s, %s);
    """

    with connection.cursor() as cursor:
        cursor.execute(data_insertion_query, insert_values_tuple)

    connection.commit()

def table_update(connection, data):
    confirmed, recovered, deaths, country = data["Total_Confirmed"], data["Total_Recovered"], data["Total_Deaths"], data["Country"]
    data_update_query = """update covid_db.covid_data set Total_Confirmed={0}, Total_Recovered={1}, Total_Deaths={2} where Country="{3}";""" \
                        .format(confirmed, recovered, deaths, country)
    
    with connection.cursor() as cursor:
        cursor.execute(data_update_query)

    connection.commit()

def insert_country_geodata(connection, filepath):
    # Open source data
    reader = csv_reader(filepath)
    query = "insert into covid_db.country_geodata(row_id, Country, Latitude, Longitude) values (%s, %s, %s, %s);"
    
    # Insert Records
    with connection.cursor() as cursor:
        for row in tqdm(reader, desc="Progress", total=192):
            row_id = row["row_id"]
            country = row["Country"]
            latitude, longitude = get_latitude_longitude(country)
            cursor.execute(query, (row_id, country, latitude, longitude))
    
    connection.commit()

def update_master_data(connection):
    # Check if the table exist
    if not table_check(connection, "covid_master_data"):
        print("Table 'covid_master_data' doesn't exist...Creating Table...")
        table_creation(connection, "covid_master_data")
        print("Table 'covid_master_data' created")
    
    # Update records
    print("Updating Master Data.....")
    query_start_str = "select a.row_id as row_id, a.Country as Country, b.Latitude as Latitude, b.longitude as Longitude, "
    query_mid_str = "a.Total_Confirmed as Total_Confirmed ,a.Total_Recovered as Total_Recovered, a.Total_Deaths as Total_Deaths "
    query_end_str = "from covid_data a, country_geodata b where a.Country=b.Country"
    update_query = query_start_str + query_mid_str + query_end_str

    insert_query = "insert into covid_db.covid_master_data({0});".format(update_query)

    with connection.cursor() as cursor:
        cursor.execute("use covid_db;")
        cursor.execute(insert_query)

    print("Master Data Updated")
    connection.commit()

if __name__=='__main__':
    # Parse CLI arguments for MySQL Cred
    parser = argparse.ArgumentParser(description="Parse MySQL creds")
    parser.add_argument("--username", type=str, help="Username for MySQL Creds")
    parser.add_argument("--password", type=str, help="Password for MySQL Creds")

    args = parser.parse_args()

    db_username, db_password = args.username, args.password

    # File processed after Transform Stage
    input_file_path = "data/transformed_data.csv"
    
    if not os.path.exists(input_file_path):
        raise FileNotFoundError("The file '" + input_file_path + "' is not found")

    connection = pymysql.connect(host='localhost',
                                 user=db_username,
                                 password=db_password)

    # Check if 'covid_db' database is present
    if not db_check(connection):
        print("Database covid_db doesn't exists")
        print("Creating Database covid_db.......")
        db_creation(connection)
        print("Database covid_db created")

    # Check if the 'country_geodata' table is present
    if not table_check(connection, "country_geodata"):
        print("Table country_geodata doesn't exist. Creating Table...")
        table_creation(connection, "country_geodata")

        #Insert Data
        print("Inserting Data in the table: country_geodata")
        insert_country_geodata(connection, input_file_path)
    
    # Check if the 'covid_data' table is present
    if not table_check(connection, "covid_data"):
        print("Table covid_data doesn't exist. Creating table covid_data.......")
        table_creation(connection, "covid_data")
    
        # Insert Data
        print("Inserting Data in the table: covid_data")
        for row in tqdm(csv_reader(input_file_path), desc="Progress", total=192):
            input_tuple = (
                row["row_id"],
                row["Country"],
                row["Total_Confirmed"],
                row["Total_Recovered"],
                row["Total_Deaths"]
            )

            data_insertion(connection, input_tuple)

    # Update latest data in covid_data table
    else:    
        print("Updating records in the covid_data table")
        for row in tqdm(csv_reader(input_file_path), desc="Progress", total=192):
            table_update(connection, row)
        print("Records Updated")


    # Execute Master Data update query
    update_master_data(connection)