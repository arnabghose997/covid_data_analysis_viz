# COVID-19 Data Anaylsis and Visualisation

Analysis and visualisation of a year of COVID-19 data. (Work in Progress!)

## Getting Started

### Dependencies

Run the following in terminal to install the required libraries.

```pip install -r requierements.txt```

The project only supports MySQL Database for now.

## Usage

The bash script ```run.sh``` runs the complete ETL pipeline. The MySQL database credentials need to passed into the script:

```bash run.sh <DB_USERNAME> <DB_PASSWORD>```