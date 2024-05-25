from datetime import datetime
import pyspark
import requests

import pandas as pd
import glob
from pyspark.sql import SparkSession
from pathlib import Path
from pyspark.sql.functions import lit

from sqlalchemy import create_engine





def spark_session_init():
    '''
    Initialize spark session
    '''

    conf = pyspark.SparkConf().setAppName('Weather_Analysis').setMaster('local')

    sc = pyspark.SparkContext(conf=conf)

    spark = SparkSession(sc)

    return spark



def https_request_to_csv(url):
    '''
        Function to download the weather data from the API
    '''
    r = requests.get(url)
    with open('temp.csv', 'wb') as f:
        f.write(r.content)
    return 'temp.csv'



def create_directory(dir):
    '''
    Generic function to create a directory if it does not exist
    '''
    output_dir = Path(dir)
    output_dir.mkdir(parents=True, exist_ok=True)





def extract_dimension_datasets(city_name, province_name, data_lake, sql_engine):
    '''
        Create a dimension table for Weather Stations
            - Pandas to read the Stations and GeoNames CSV files from the web
            - SQL Alchemy to create the tables
            - Combine Stations and GeoNames to get the full list of stations
            - Export to partitioned CSV files

    '''

    print('\n\nProcessing Dimension Datasets...\n')


    city_name = city_name.lower()
    province_name = province_name.lower()

    output_dim = data_lake + "/dim"
    
    create_directory(output_dim)
    output_geonames = output_dim+'/geonames.csv'
    output_stations = '{}/{}_stations.csv'.format(output_dim, city_name)

    # Make API Call using https_request_to_csv function and load geonames dimension table into sql
    print('\t\tGeonames API Call Started...')
    try:
        

        url = 'http://geogratis.gc.ca/services/geoname/en/geonames.csv'
        df_geonames = pd.read_csv(https_request_to_csv(url))
        df_geonames.to_sql('dim_geonames', sql_engine, if_exists='replace')

        # Export geonames dimensional table to CSV    
        df_geonames.to_csv(output_geonames, index=False)
        print('\t\tGeonames API Call Finished!')

        # Based on user's Province Name input, find the corresponding Province Code
        table = 'province'
        query = '''
            SELECT DISTINCT
                dg.name AS ProvinceName,
                dg.'province.code' AS ProvinceCode

            FROM
                dim_geonames as dg
            WHERE
                dg.'concise.code' = 'PROV'
                AND dg.'name' LIKE '%{}%'
            ;    
        '''.format(province_name)
        df = pd.read_sql(query, sql_engine)
        df.to_sql(table, sql_engine, if_exists='replace')
        provinceCode = df.ProvinceCode[0]


        '''
        - Create an internal reference ID (IRID)* using latitude and longitute to help filter stations in selected city
        - Based on user's City Name input and the corresponding Province Code, return the corresponding IRIDs

        * In order to pull data for Toronto, we need a reference table of stations and their coordinates that we can cross-reference with geo-names dataset from NRCAN to see which stations geographically are within the city of Toronto. Using one (1) decimal point precision, we can use the latitude and longitude to find stations within Toronto.
        '''
        table = 'geonames'
        query = '''
            SELECT DISTINCT
                -- Create an Internal Reference ID using coordinates
                REPLACE(REPLACE(ROUND(dg.latitude, 1) || ROUND(dg.longitude, 1), '-', 'X'), '.', 'Z') AS 'IRID'
            FROM
                dim_geonames as dg
            WHERE
                dg.'province.code' = {}
                AND dg.'name' LIKE '%{}%'
            ;    
        '''.format(provinceCode, city_name)
        df = pd.read_sql(query, sql_engine)
        df.to_sql(table, sql_engine, if_exists='replace')
        print('\t\tGeonames Data Transformation Complete!')


        # Make API Call using https_request_to_csv function and load stations dimension table into sql
        try:
            url = 'https://drive.google.com/u/0/uc?id=1HDRnj41YBWpMioLPwAFiLlK4SK8NV72C&export=download'
            df_stations = pd.read_csv(https_request_to_csv(url), skiprows=2, delimiter=',')
            df_stations.to_sql('dim_stations', sql_engine, if_exists='replace')

            print('\t\tStations API Call Complete!')

            # Select the columns we need from stations dataset and create an IRID using latitude and longitude
            table = 'stations'
            query = '''
                SELECT
                    ds.'Name' AS 'StationName',
                    ds.'Province' AS 'Province',
                    ds.'Climate ID' AS 'ClimateID',
                    ds.'Station ID' AS 'StationID',
                    ds.'Latitude (Decimal Degrees)' AS 'Latitude',
                    ds.'Longitude (Decimal Degrees)' AS 'Longitude',

                    -- Create an Internal Reference ID using coordinates
                    REPLACE(
                        REPLACE(
                            ROUND(ds.'Latitude (Decimal Degrees)', 1) ||
                            ROUND(ds.'Longitude (Decimal Degrees)', 1),
                        '-', 'X'),
                    '.', 'Z') AS 'IRID',

                    ds.'Elevation (m)' AS 'Elevation',
                    ds.'First Year' AS 'FirstYear',
                    ds.'Last Year' AS 'LastYear'
                FROM
                    dim_stations as ds
                ;    
            '''
            df = pd.read_sql(query, sql_engine)
            df.to_sql(table, sql_engine, if_exists='replace')



            # Filter the stations dataset for the stations that meet the user's city/province criteria based on IRID
            query = '''
                SELECT
                    stations.*
                FROM
                    stations
                        LEFT JOIN geonames
                        USING (IRID)
                WHERE
                    geonames.IRID IS NOT NULL
                ;    
            '''
            df = pd.read_sql(query, sql_engine)

            # Export stations dimension table to CSV
            df.to_csv(output_stations, index=False)
            
            list_stations = df.StationID.drop_duplicates().tolist()
        
            print('\t\tStations Data Transformation Complete!')
            
            return list_stations, df

        except:
            print('Stations API Call Failed.')


    except:
        print('Geonames API Call Failed.')



def extract_weather_dataset(list_stations, list_years, data_lake):
    '''
        Create a weather dataset for the selected stations and years
            - Spark to read the weather dataset from the API
            - Convert to Pandas (see notes below)
            - Export to partitioned CSV files
    '''

    print('\n\nProcessing Fact Dataset...\n')

    spark = spark_session_init()

    # Create a conversion list for column names
    cols = {
        "Longitude (x)": "Longitude",
        "Latitude (y)": "Latitude",
        "Station Name": "StationName",
        "Climate ID": "ClimateID",
        "Date/Time": "DateTime",
        "Year": "Year",
        "Month": "Month",
        "Day": "Day",
        "Data Quality": "DataQuality",
        "Max Temp (°C)": "MaxTemp",
        "Max Temp Flag": "MaxTempFlag",
        "Min Temp (°C)": "MinTemp",
        "Min Temp Flag": "MinTempFlag",
        "Mean Temp (°C)": "MeanTemp",
        "Mean Temp Flag": "MeanTempFlag",
        "Heat Deg Days (°C)": "HeatDegDays",
        "Heat Deg Days Flag": "HeatDegDaysFlag",
        "Cool Deg Days (°C)": "CoolDegDays",
        "Cool Deg Days Flag": "CoolDegDaysFlag",
        "Total Rain (mm)": "TotalRainmm",
        "Total Rain Flag": "TotalRainFlag",
        "Total Snow (cm)": "TotalSnowcm",
        "Total Snow Flag": "TotalSnowFlag",
        "Total Precip (mm)": "TotalPrecipmm",
        "Total Precip Flag": "TotalPrecipFlag",
        "Snow on Grnd (cm)": "SnowonGrndcm",
        "Snow on Grnd Flag": "SnowonGrndFlag",
        "Dir of Max Gust (10s deg)": "DirofMaxGustsdeg",
        "Dir of Max Gust Flag": "DirofMaxGustFlag",
        "Spd of Max Gust (km/h)": "SpdofMaxGustkmh",
        "Spd of Max Gust Flag": "SpdofMaxGustFlag",
        "StationID": "StationID"
    }

    # Based on the list of stations and years, make API calls to get the weather dataset
    for year in list_years:
        print("Year {} Starting...".format(year))

        for station in list_stations:

            # Make API call to get the weather dataset as csv and read with spark
            url = 'https://climate.weather.gc.ca/climate_data/bulk_data_e.html?format=csv&stationID={}&Year={}&Month=1&Day=1&time=&timeframe=2&submit=Download+Data'.format(station, year)

            df = spark.read.csv(https_request_to_csv(url), header=True, inferSchema=True)

            # Insert Station ID as a new column
            df = df.withColumn("StationID", lit(station))

            # Set the output directory
            output_folder = '{}/fact/{}/'.format(data_lake, year)
            output_dir = Path(output_folder)
            output_dir.mkdir(parents=True, exist_ok=True)


            # Convert the dataframe to a pandas dataframe and write to csv
                # NOTE TO WAVE: unfortunately I had to switch to pandas after having trouble with Spark's CSV reading and writing on my local machine
            df = df.toPandas()

            # Rename the columns
            df = df.rename(columns=cols)

            # Write csv to the partition folder
            output = '{}{}.csv'.format(output_folder, station)
            df.to_csv(output, index=False)

            # Print completion message
            index = list_stations.index(station)
            print("\t{}) Station ID '{}' Processed: {} Records".format(index, station, len(df)))



def query_weather_excel(list_years, city_name, province_name, data_lake, sql_engine):
    '''
        Create a dataset that combines the weather data for all the stations and years based on input city and province
            - Pandas to read and concat partitioned CSV files
            - Clean the data
            - Export to Excel and create a sheet for each year
    '''
    city_name = city_name.lower()
    province_name = province_name.lower()

    # directory of weather fact data and dimension tables
    # input_fact = data_lake + "/2021/31688.csv"
    fact_weather = data_lake + "/fact/*/*.csv"
    dim_stations = data_lake + "/dim/{}_stations.csv".format(city_name)
    dim_geonames = data_lake + "/dim/geonames.csv"

    # list of all the files from fact_weather
    list_files = sorted(glob.glob(fact_weather))
    print('\tProcessing {} Files...'.format(len(list_files)))


    # Using Pandas to read the weather CSV files, since we will be exporting as Excel
    df_weather = pd.concat([pd.read_csv(f) for f in list_files])


    # Remove records with missing temperature data
    df_weather = df_weather.dropna(subset=['MaxTemp'])
    df_weather.to_sql('weather', sql_engine, if_exists='replace', index=False)

    # Set Excel writer
    writer = pd.ExcelWriter('{}_dly.xlsx'.format(city_name), engine='xlsxwriter')

    # Loop through each year and create a sheet for each year
    for year in list_years:
        dfx = df_weather[df_weather['Year'] == year]
        dfx.to_excel(writer, sheet_name=str(year), index=False, freeze_panes=(1, 0))
    
    # Close the Excel writer
    writer.save()


    # Load stations dimension table and insert into sqlite
    df_stations = pd.read_csv(dim_stations)
    df_stations.to_sql('stations', sql_engine, if_exists='replace', index=False)

    # Load stations dimension table and insert into sqlite
    df_geonames = pd.read_csv(dim_geonames)
    df_geonames.to_sql('geonames', sql_engine, if_exists='replace', index=False)
    

    return df_weather, df_stations, df_geonames



def query_check_data(df_weather, df_stations, df_geonames):
    '''
        Check the data quality of the weather dataset
    '''
    # Check the data quality of the weather dataset
    print('\n\n\n\tChecking Data Quality Starting...')

    # Check the data quality of the weather dataset
    print('\n\t\tChecking Weather Data...')
    print('\t\t\tChecking Weather Data has {} Duplicate Records.'.format(df_weather.duplicated().sum()))
    print('\t\t\tChecking Weather Data for Missing Values...')
    print('\n{}\n'.format(df_weather.isnull().sum()))

    # Check the data quality of the stations dataset
    print('\n\t\tChecking Stations Data...')
    print('\t\t\tChecking Stations Data has {} Duplicate Records.'.format(df_stations.duplicated().sum()))
    print('\t\t\tChecking Stations Data for Missing Values...')
    print('\n{}\n'.format(df_stations.isnull().sum()))

    # Check the data quality of the geonames dataset
    print('\n\t\tChecking Geonames Data...')
    print('\t\t\tChecking Geonames Data has {} Duplicate Records.'.format(df_geonames.duplicated().sum()))
    print('\t\t\tChecking Geonames Data for Missing Values...')
    print('\n{}\n'.format(df_geonames.isnull().sum()))


    print('\n\nChecking Data Quality Complete!\n\n')



def etl_process_details(start, end, list_years, city_name, province_name, df):
    '''
        Function to print the ETL process details
    '''

    print('\n\nETL Pipeline Started at {}'.format(start))

    x = len(df)
    print('\tFinal Dataset has {} rows'.format(x))

    print('\tProcessing Data for Years:', list_years)

    print('\tCity:', city_name)

    print('\tProvince:', province_name)

    print('\nETL Pipeline Finished at {}\n'.format(end))

    duration = end - start
    print('ETL Pipeline Duration: {}\n'.format(duration))



def main():
    '''
        Main function to run the ETL pipeline for the weather dataset for the selected city and province
    '''

    start = datetime.now()
    print('\n\nETL Pipeline Started at {}\n\n'.format(start))

    
    # directory of data lake
    data_lake = "../datalake/weather"

    # sqlite engine for temporary query results
    create_directory('{}/sqlite'.format(data_lake))
    sql_engine = create_engine('sqlite:///{}/sqlite/tempdb.db'.format(data_lake), echo=False)

    # input_year = 2021
    # input_city = "Toronto"
    # input_province = "Ontario"

    # Set user input values
    input_year = input('Enter a year (e.g. 2021): ')
    input_year = int(input_year)
    input_city = input('Enter a city (e.g. Toronto): ')
    input_province = input('Enter the province (e.g. Ontario): ')

    # based on user input, determine the years to process
    list_years = list(range(input_year, input_year - 3, -1))
    print('\tProcessing Data for Years:', list_years)


    # Run the function to get the list of stations and the dataframe
    results = extract_dimension_datasets(input_city, input_province, data_lake, sql_engine)
    list_stations = results[0]
    df_stations = results[1]

    # Run the function to extract the weather dataset
    extract_weather_dataset(list_stations, list_years, data_lake)

    # Run the function to query the weather dataset and export Excel and return dataframes
    results = query_weather_excel(list_years, input_city, input_province, data_lake, sql_engine)
    
    df_weather = results[0]
    df_stations= results[1]
    df_geonames = results[2]

    query_check_data(df_weather, df_stations, df_geonames)

    end = datetime.now()
    etl_process_details(start, end, list_years, input_city, input_province, df_weather)



if __name__ == "__main__":
    main()


