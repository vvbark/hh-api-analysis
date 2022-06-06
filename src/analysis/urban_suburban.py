import numpy as np
import pandas as pd
from pyspark.sql import functions as F
from plotly.subplots import make_subplots
from plotly import graph_objects as go
from src.analysis.config import SparkConnection


SPARK_URI = 'spark://127.0.0.1:7077'
APP_NAME = "urban-suburban-viz"

# Make connection to local spark server
spark = SparkConnection(APP_NAME)


def main():
    df = spark.read()
    city = df.select('area_id', 
                 'address_city', 
                 'salary_from', 
                 'salary_to',
                 'schedule_name')\
    .where((F.col('area_id') == 2)\
        & (F.col('address_city') == 'Санкт-Петербург')\
        & (F.col('address_lng') > 30.2)
        & (F.col('address_lat') > 59.5)
        & (F.col('address_lng') < 30.5)
        & (F.col('address_lat') < 60.3))

    suburb = df.select('area_id', 'address_city', 'salary_from', 'salary_to', 'schedule_name')\
    .where(
        (F.col('area_id') == 2)\
        & (F.col('address_city') != 'Санкт-Петербург')\
        & (
              (F.col('address_lng') < 30.5)\
            | (F.col('address_lat') < 59.5)\
            | (F.col('address_lng') > 30.5)\
            | (F.col('address_lat') > 60.3)))\
    .where(df['address_lng'] > 29) \
    .where(df['address_lat'] > 58) \
    .where(df['address_lng'] < 31) \
    .where(df['address_lat'] < 61)

    city_hist = city.select('salary_from').rdd.flatMap(lambda x: x).histogram(75)
    city_hist = pd.DataFrame(
        list(zip(*city_hist)), 
        columns=['bin', 'freq']
    ).set_index('bin')

    city_hist['freq'] = city_hist['freq'].apply(np.log10)

    suburb_hist = suburb.select('salary_from').rdd.flatMap(lambda x: x).histogram(35)
    suburb_hist = pd.DataFrame(
        list(zip(*suburb_hist)), 
        columns=['bin', 'freq']
    ).set_index('bin')

    suburb_hist['freq'] = suburb_hist['freq'].apply(np.log10)

    fig = make_subplots(2, 1, shared_xaxes=True)
    fig.add_trace(
        go.Bar(x=city_hist.index, y=city_hist['freq'], name='urban'), row=1, col=1)
    fig.add_trace(
        go.Bar(x=suburb_hist.index, y=suburb_hist['freq'], name='suburban'), row=2, col=1)
    fig.update_layout(title='Log histograms urban and suburban area salaries')
    fig.write_image('./images/urban_suburban_hist.png')

    spark.stop()


if __name__ == '__main__':
    main()
