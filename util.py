from pyspark.sql import SparkSession


def create_spark_session(env, app_name):

    if env == 'DEV':
        spark = SparkSession. \
            builder. \
            master('local'). \
            appName(app_name). \
            getOrCreate()
    elif env == 'PROD':
        spark = SparkSession. \
            builder. \
            master('yarn'). \
            appName(app_name). \
            getOrCreate()

        return spark

