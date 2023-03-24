import datetime
import os

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, lit
from pyspark.sql.window import Window
from sqlalchemy import create_engine
from sqlalchemy import text as sql_text

from base.sql_connector import SQLConnector
from constants import Constants
from models.base.table import Table


class CDC:
    def __init__(
        self,
        new_data_df: DataFrame,
        table_name: str,
        pk_columns: list,
        enr_columns: list,
        update_columns: dict,
        insert_timestamp_column: str,
        active_flag_column: str,
        spark: SparkSession,
    ):
        self.new_data_df = new_data_df
        self.table_name = table_name
        self.pk_columns = pk_columns
        self.enr_columns = enr_columns
        self.update_columns = update_columns
        self.insert_timestamp_column = insert_timestamp_column
        self.active_flag_column = active_flag_column
        self.spark = spark
        self.sql_connector = SQLConnector(spark)

    def __call__(self):
        old_data_df = self.sql_connector.read_db(self.table_name)
        old_data_df = old_data_df.withColumn(Constants.IS_NEW_RECORD.value, lit(False))
        self.new_data_df = self.new_data_df.withColumn(Constants.IS_NEW_RECORD.value, lit(True))

        merged_df = old_data_df.select(list(self.new_data_df.columns)).unionByName(self.new_data_df)
        search_columns = self.pk_columns + self.enr_columns
        windows_spec = Window.partitionBy(*(col(c) for c in search_columns))
        filtered_df = (
            merged_df.withColumn("count_rows", count(lit("1")).over(windows_spec))
            .filter("count_rows = 1 AND IS_NEW_RECORD = True")
            .drop("count_rows", "IS_NEW_RECORD")
        )

        target_table = self.table_name

        update_statement = ""
        join_filter = ""
        for key_column in self.pk_columns:
            join_filter = join_filter + f""" TARGET_TABLE.{key_column} = LAST_UPDATED.{key_column} AND"""

        # Remove last AND
        join_filter = join_filter[:-3]

        for key in self.update_columns:
            update_statement = update_statement + f""" {key} =  {self.update_columns[key]},"""

        # Remove last comma
        update_statement = update_statement[:-1]
        # Columns to select in source table
        select_keys = ",".join(search_columns)

        # Update condition used in update clause
        update_condition = f"""
                TARGET_TABLE.{self.insert_timestamp_column} < LAST_UPDATED.{self.insert_timestamp_column}
            """
        where_clause = f"WHERE {self.active_flag_column} = True"

        # Source Table to get data from
        last_updated_table = f"""
                SELECT  {select_keys}, {self.insert_timestamp_column}
                FROM {target_table}
                {where_clause}
            """

        # Merge Into query used for updating CDC flags
        update_existed_rows_stm = f"""MERGE INTO {target_table} TARGET_TABLE
                USING( {last_updated_table} ) LAST_UPDATED
                ON (
                    {join_filter}
                    AND TARGET_TABLE.{self.active_flag_column} = True
                )
                WHEN MATCHED AND
                    {update_condition}
                THEN UPDATE SET {update_statement}
                """

        # existing_table = old_data_df.filter(col(Table.active_flag.name) == True)
        filtered_df = filtered_df.withColumn(Table.published_to.name, lit(None))
        filtered_df = filtered_df.withColumn(
            Table.published_from.name, lit(datetime.datetime.now().strftime("%Y-%m-%d"))
        )
        filtered_df = filtered_df.withColumn(Table.active_flag.name, lit(True))
        self.sql_connector.write_to_db(filtered_df, self.table_name)
        username = os.environ["DB_USER"]
        password = os.environ["DB_PASSWORD"]
        endpoint = os.environ["DB_ENDPOINT"]
        port = "5432"
        database = "kpi"
        engine = create_engine(f"postgresql://{username}:{password}@{endpoint}:{port}/{database}")
        connection = engine.connect()
        # connection.execute(sql_text(update_existed_rows_stm))
        result = connection.execute(sql_text("SELECT * FROM ENR_IFA"))
        print(result.fetchone())
        connection.close()
