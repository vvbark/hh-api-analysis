import os

import pyspark as ps
import findspark
findspark.init('/opt/spark')


class SparkConnection:

    SPARK_PARAMS = {
        'host': 'localhost',
        'user': os.getenv('HH_API_USERNAME'),
        'password': os.getenv('HH_API_PASSWORD'),
        'url': "jdbc:clickhouse://127.0.0.1:9000",
        'table': 'hh_api.vacancy',
        'db_driver': "com.github.housepower.jdbc.ClickHouseDriver",
    }

    def __init__(self, app_name: str):

        self.spark = ps.sql.SparkSession.builder \
            .master('local') \
            .appName(app_name) \
            .config("spark.driver.extraClassPath", "./clickhouse-native-jdbc-shaded-2.5.4.jar") \
            .config('spark.sql.debug.maxToStringFields', '100') \
            .getOrCreate()

    def read(self) -> ps.sql.dataframe.DataFrame:
        dataframe = self.spark.read.format('jdbc') \
            .option('driver', self.SPARK_PARAMS['db_driver']) \
            .option('url', self.SPARK_PARAMS['url']) \
            .option('user', self.SPARK_PARAMS['user']) \
            .option('password', self.SPARK_PARAMS['password']) \
            .option('dbtable', self.SPARK_PARAMS['table']).load()

        return dataframe

    def close(self):
        self.spark.close()

    def open(self, app_name):
        self.__init__(app_name)
