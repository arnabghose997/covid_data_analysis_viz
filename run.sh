#!/bin/bash

# MySQL DB Credentials
DB_USERNAME=$1
DB_PASSWORD=$2

# Remove the intermediate files
function remove_intermediate_files {
    echo "Removing intermediate files present in 'data' folder"
    sleep 5
    rm data/transformed_data.csv data/covid_data_countries.csv
}

# Center Pad the terminal Output text
function pad_center() {
  termwidth="$(tput cols)"
  padding="$(printf '%0.1s' ={1..500})"
  printf '%*.*s %s %*.*s\n' 0 "$(((termwidth-2-${#1})/2))" "$padding" "$1" 0 "$(((termwidth-1-${#1})/2))" "$padding"
}


####### Run ETL Pipeline #######

# Extract Stage
pad_center "Extract Stage Started";echo ;echo ;
python3 extract.py

if [[ $? = 0 ]]; then
    echo ;echo ;pad_center "Extract Stage Completed";echo ;
    
    # Transform Stage
    sleep 5
    pad_center "Transform Stage Started";echo ;echo ;
    python3 transform.py
    
    if [[ $? = 0 ]]; then
        echo ;echo ;pad_center "Transform Stage Completed";echo ;
        
        # Load Stage
        pad_center "Load Stage Started";echo ;echo ;
        sleep 5
        python3 load.py --username=${DB_USERNAME} --password=${DB_PASSWORD}

        if [[ $? = 0 ]]; then
            echo ;echo ;pad_center "Load Stage Completed";echo ;
            remove_intermediate_files

            echo "$(date "+%d-%m-%Y %H:%M:%S"): ETL run successful"
            exit 0
        else
            remove_intermediate_files
            
            echo ;echo "$(date "+%d-%m-%Y %H:%M:%S"): ETL run failed at Load Stage"
            exit 1
        fi
    else
        "$(date "+%d-%m-%Y %H:%M:%S"): ETL run failed at Transform Stage"
        exit 1
    fi
else
    "$(date "+%d-%m-%Y %H:%M:%S"): ETL run failed at Extract Stage"
    exit 1
fi

