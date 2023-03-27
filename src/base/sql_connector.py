import os

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame


class SQLConnector:
    def __init__(self, spark: SparkSession):
        self.spark = spark

    def read_db(self, table_name: str) -> DataFrame:
        df = (
            self.spark.read.format("jdbc")
            .option("url", os.environ["DB_URL"])
            .option("dbtable", table_name)
            .option("user", os.environ["DB_USER"])
            .option("password", os.environ["DB_PASSWORD"])
            .option("driver", "org.postgresql.Driver")
            .load()
        )

        return df

    def write_to_db(self, df: DataFrame, table_name: str, mode="append"):
        df.write.format("jdbc").option("url", os.environ["DB_URL"]).option("dbtable", table_name).option(
            "user", os.environ["DB_USER"]
        ).option("password", os.environ["DB_PASSWORD"]).option("driver", "org.postgresql.Driver").mode(mode).save()
