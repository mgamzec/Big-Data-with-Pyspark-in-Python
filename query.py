from datetime import datetime
import pyspark

import pandas as pd
import glob
from pyspark.sql import SparkSession
from pathlib import Path

from sqlalchemy import create_engine


# directory of weather data
data_lake = "../datalake/weather"

# sqlite engine for temporary query results
sql_engine = create_engine('sqlite:///../sqlite/querydb.db', echo=False)




def spark_session_init():
    '''
    Initialize spark session
    '''

    conf = pyspark.SparkConf().setAppName('Weather_Analysis').setMaster('local')

    sc = pyspark.SparkContext(conf=conf)

    spark = SparkSession(sc)

    return spark



def create_directory(dir):
    '''
    Generic function to create a directory if it does not exist
    '''
    output_dir = Path(dir)
    output_dir.mkdir(parents=True, exist_ok=True)




def load_data_models(city_name, province_name, data_lake, sql_engine):
    '''
        Create a dataset that combines the weather data for all the stations and years based on input city and province
            - Pandas to read and concat partitioned CSV files
            - Clean the data and load into sqlite for analysis
            - TODO add comments
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


    # Load stations dimension table and insert into sqlite
    df_stations = pd.read_csv(dim_stations)
    df_stations.to_sql('stations', sql_engine, if_exists='replace', index=False)

    # Load stations dimension table and insert into sqlite
    df_geonames = pd.read_csv(dim_geonames)
    df_geonames.to_sql('geonames', sql_engine, if_exists='replace', index=False)
    
    return df_weather, df_stations, df_geonames


def run_sql(sql_query, table_name, sql_engine):
    '''
        Function to query the sqlite database and return the results
    '''

    # query the database
    df = pd.read_sql_query(sql_query, sql_engine)
    df.to_sql(table_name, sql_engine, if_exists='replace', index=False)

    return df



def staging_queries(sql_engine):
    '''
    Function to stage the dimension and fact tables and create a single denormalized table for analysis
    '''

    print('\nStaging Started...\n')

    # Clean the geonames dataset and filter for cities
    name = 'cities'
    query = (
        '''
        SELECT
            REPLACE(REPLACE(ROUND(latitude, 1) || ROUND(longitude, 1), '-', 'X'), '.', 'Z') AS 'IRID',
            "name" AS CityName,
            "category" AS Category,
            "status.code" AS StatusCode,
            "concise.code" AS NameType,
            "province.code" AS ProvinceCode,
            "latitude" AS Latitude,
            "longitude" AS Longitude,
            "decision" AS StatusCodeDate
        FROM
            geonames
        WHERE
            "concise.code" != "PROV"
        ;
        '''
    )   
    df = run_sql(query, name, sql_engine)
    print('\tDimension table Cities staged.')



    # Join the stations dimension table with the geonames table to get the city information
    name = 'stations'
    query = (
        '''
        SELECT
            stations.Province,
            cities.CityName,
            cities.IRID,
            cities.Latitude,
            cities.Longitude,
            stations.Elevation,
            stations.StationName,
            stations.StationID,
            stations.ClimateID
        FROM
            stations JOIN cities USING (IRID)
        ;
        '''
    )
    df = run_sql(query, name, sql_engine)
    print('\tDimension table Stations staged.')



    # Clean and stage the weather fact table
    name = 'weather'
    query = (
        '''
        SELECT
            weather.StationID,
            weather.ClimateID,
            weather.Year,
            weather.Month,
            weather.Day,
            weather.DateTime,
            weather.MaxTemp,
            weather.MinTemp,
            weather.MeanTemp,
            weather.TotalPrecipmm AS TotalPrecip,
            weather.SnowonGrndcm AS TotalSnow
        FROM
            weather
        WHERE
            MaxTemp IS NOT NULL AND
            MinTemp IS NOT NULL AND
            MeanTemp IS NOT NULL
        ;
        '''
    )
    df = run_sql(query, name, sql_engine)
    print('\tFact table Weather staged.')



    # Join the new stations dimension table with the weather fact table
    name = 'weather'
    query = (
        '''
        SELECT
            stations.Province,
            stations.CityName,
            stations.IRID,
            stations.Latitude,
            stations.Longitude,
            stations.Elevation,
            stations.StationName,
            stations.StationID,
            stations.ClimateID,
            weather.Year,
            weather.Month,
            weather.Day,
            weather.DateTime,
            weather.MaxTemp,
            weather.MinTemp,
            weather.MeanTemp,
            weather.TotalPrecip,
            weather.TotalSnow
        FROM
            weather JOIN stations USING (ClimateID)
        ;
        '''
    )
    df = run_sql(query, name, sql_engine)
    print('\tFact and dimension tables joined.')

    
    print('\nStaging Finished.\n')

    return df



def analysis_queries(input_year, sql_engine):

    '''
    QUERY 1:
        Inquiry: Based on input year, number of days where temperature delta was within 1 degree of last year's mean
        Method: Use SQL Lag function to calculate the difference between the current year's mean temperature and the last year's mean temperature
        Result: Print the result to the console
    '''

    name = 'yoy_delta'
    query = (
        '''
        SELECT
            CityName,
            Year,
            Month,
            Day,
            DateTime,
            MeanTemp,

            LAG(AVG(MeanTemp), 1, 0) OVER (
                ORDER BY (Month || Day) ASC
            ) AS MeanTempLag,

            ABS(LAG(AVG(MeanTemp), 1, 0) OVER (
                ORDER BY (Month || Day) ASC
            ) - MeanTemp) AS MeanTempLagDelta

        FROM
            weather
        GROUP BY
            CityName,
            Year,
            Month,
            Day
        ;
        '''
    )
    run_sql(query, name, sql_engine)

    query = (
        '''
        SELECT
            COUNT(DISTINCT DateTime) AS Count
        FROM
            yoy_delta
        WHERE
            MeanTempLagDelta < 1 AND
            Year = {}
        ;
        '''.format(input_year)
    )
    df_yoy_delta_1 = run_sql(query, name, sql_engine)

    analysis = "day(s) in {} where mean temperature was the same as the year before.".format(input_year)
    x = df_yoy_delta_1.iloc[0][0]
    print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))



    '''
    QUERY 2:
        Inquiry: Based on input year, number of days where temperature delta equal to last year's mean
        Method: Use SQL Lag function to calculate the difference between the current year's mean temperature and the last year's mean temperature
        Result: Print the result to the console
    '''

    name = 'yoy_delta'
    query = (
        '''
        SELECT
            CityName,
            Year,
            Month,
            Day,
            DateTime,
            MeanTemp,

            LAG(AVG(MeanTemp), 1, 0) OVER (
                ORDER BY (Month || Day) ASC
            ) AS MeanTempLag,

            ABS(LAG(AVG(MeanTemp), 1, 0) OVER (
                ORDER BY (Month || Day) ASC
            ) - MeanTemp) AS MeanTempLagDelta

        FROM
            weather
        GROUP BY
            CityName,
            Year,
            Month,
            Day
        ;
        '''
    )
    run_sql(query, name, sql_engine)

    query = (
        '''
        SELECT
            COUNT(DISTINCT DateTime) AS Count
        FROM
            yoy_delta
        WHERE
            MeanTempLagDelta = 0 AND
            Year = {}
        ;
        '''.format(input_year)
    )
    df_yoy_delta_0 = run_sql(query, name, sql_engine)

    analysis = "day(s) in {} where mean temperature was the same as the year before.".format(input_year)
    x = df_yoy_delta_0.iloc[0][0]
    print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))




    '''
    QUERY 3:
        Inquiry: Based on input year, what was the year's mean temperature
        Method: Use SQL avg function to calculate the mean temperature
        Result: Print the result to the console
    '''
    name = 'year_mean'
    query = (
        '''
        SELECT
            ROUND(AVG(MeanTemp), 1) AS MeanTemp
        FROM
            weather
        WHERE
            Year = {}
        GROUP BY
            Year
        ;
        '''.format(input_year)
    )
    df = run_sql(query, name, sql_engine)

    analysis = "was the average temperature in year {}.".format(input_year)
    x = df.iloc[0][0]
    print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))





    '''
    QUERY 4:
        Inquiry: Based on input year, what was the year's highest temperature
        Method: Use SQL max function to calculate the highest temperature
        Result: Print the result to the console
    '''
    name = 'year_max'
    query = (
        '''
        SELECT
            MAX(MaxTemp) AS MaxTemp
        FROM
            weather
        WHERE
            Year = {}
        GROUP BY
            Year
        ;
        '''.format(input_year)
    )
    df = run_sql(query, name, sql_engine)

    analysis = "was the highest temperature in year {}.".format(input_year)
    x = df.iloc[0][0]
    print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))





    '''
    QUERY 4:
        Inquiry: Based on input year, what was the year's lowest temperature
        Method: Use SQL min function to calculate the lowest temperature
        Result: Print the result to the console
    '''
    name = 'year_min'
    query = (
        '''
        SELECT
            MIN(MinTemp) AS MinTemp
        FROM
            weather
        WHERE
            Year = {}
        GROUP BY
            Year
        ;
        '''.format(input_year)
    )
    df = run_sql(query, name, sql_engine)

    analysis = "was the highest temperature in year {}.".format(input_year)
    x = df.iloc[0][0]
    print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))





    '''
    QUERY 5:
        Inquiry: Based on input year, what was each month's average temperature
        Method: Use SQL min function to calculate the lowest temperature
        Result: Print the result to the console
    '''
    name = 'monthly_mean'
    query = (
        '''
        SELECT
            ROUND(AVG(MeanTemp), 1) AS MeanTemp,
            Month
        FROM
            weather
        WHERE
            Year = {}
        GROUP BY
            Year,
            Month
        ;
        '''.format(input_year)
    )
    df = run_sql(query, name, sql_engine)

    print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))

    list_months = df['Month'].tolist()

    for month in list_months:
        dfx = df.query('Month == {}'.format(month))
        x = dfx.iloc[0][0]
        print('\nQUERY RESULTS:\n\t {} {} \n'.format(x, analysis))
        analysis = "was the average temperature in year {} month {}.".format(input_year, month)



    return df_yoy_delta_0, df_yoy_delta_1




def query_process_details(start, end, input_year, city_name, province_name):
    '''
        Function to print the ETL process details
    '''

    print('\n\nETL Pipeline Started at {}'.format(start))

    print('\tProcessing Data for Year:', input_year)

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



    input_year = "2021"
    input_city = "Toronto"
    input_province = "Ontario"

    # Set user input values
    input_year = input('Enter a year (e.g. 2021): ')
    input_year = int(input_year)
    input_city = input('Enter a city (e.g. Toronto): ')
    input_province = input('Enter the province (e.g. Ontario): ')




    # Run the function to query the weather dataset and export Excel and return dataframes

    load_data_models(input_city, input_province, data_lake, sql_engine)
    staging_queries(sql_engine)


    analysis_queries(input_year, sql_engine)


    end = datetime.now()
    query_process_details(start, end, input_year, input_city, input_province)

    print("\n\n\n\nDONE")



if __name__ == "__main__":
    main()
    
