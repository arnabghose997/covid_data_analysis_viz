import subprocess
from geopy.geocoders import Nominatim
import csv

def exec_shell_commands(cmd):
    cmd_list = cmd.split()
    proc = subprocess.Popen(cmd_list, 
                            stdout=subprocess.PIPE, 
                            stderr=subprocess.PIPE)

    output, error = proc.communicate()
    return output.decode("utf-8"), error.decode("utf-8")

def csv_reader(filepath):
    transformed_data = csv.DictReader(open(filepath, newline=''))
    return transformed_data

def get_latitude_longitude(country):
    geolocator = Nominatim(user_agent="ETL")

    location = geolocator.geocode(country)

    return location.latitude, location.longitude