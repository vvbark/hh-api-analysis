import pandas as pd
from pyspark.sql import functions as F
from src.analysis.config import SparkConnection


SPARK_URI = 'spark://127.0.0.1:7077'
APP_NAME = "metro-bar-viz"

# Make connection to local spark server
spark = SparkConnection(APP_NAME)


def main():
    df = spark.read()

    saintpi = df.loc[df['address_city'] == 'Санкт-Петербург']
    metro = saintpi.address_metro_station_name.value_counts().rename_axis('metro_station').reset_index(name='amount')
    fig = metro[:70].plot.bar('metro_station', 'amount')
    
    spark.stop()

if __name__ == '__main__':
    main()