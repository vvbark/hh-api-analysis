import pandas as pd
import folium
import pandas as pd
from pyspark.sql import functions as F
from src.analysis.config import SparkConnection


SPARK_URI = 'spark://127.0.0.1:7077'
APP_NAME = "metro-viz"

# Make connection to local spark server
spark = SparkConnection(APP_NAME)

def vacancies_per_station():
    spb_metro_stations_dict = {}
    spb = 'Санкт-Петербург'

    df = spark.read()

    df = df.dropna()

    for index, row in df.iterrows():
        city = row['address_city']
        station = row['address_metro_station_name']
        lat = row['address_metro_lat']
        lng = row['address_metro_lng']

        if city != city or station != station or city != spb:
            continue

        if station in spb_metro_stations_dict:
            spb_metro_stations_dict[station][0] += 1
        else:
            spb_metro_stations_dict[station] = [1, lat, lng]

def main():

    vacancies_per_station()
    qw = spb_metro_stations_dict.values()

    spb_map = folium.Map(location=[60, 30])

    for qw in spb_metro_stations_dict.values():
        folium.Marker(location=[qw[1], qw[2]], popup=qw[0]).add_to(spb_map)

    spb_map

    spark.stop()

if __name__ == '__main__':
    main()