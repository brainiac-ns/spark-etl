import datetime

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, count, current_date, lit, when
from pyspark.sql.window import Window

from base.sql_connector import SQLConnector
from constants import Constants
from models.base.table import Table
from models.enrichment_data.ifa_invoices import IfaInvoices


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

    def __call__(
        self,
    ):
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

        existing_table = old_data_df.filter(col(Table.active_flag.name) == True)
        existing_table = existing_table.drop(Constants.IS_NEW_RECORD.value)

        left_columns = []
        for column in existing_table.columns:
            if column in filtered_df.columns:
                left_columns.append(f"left.{column}")
            else:
                left_columns.append(column)

        joined_df = existing_table.alias("left").join(
            filtered_df.alias("right"),
            how="left",
            on=[
                col(f"left.{IfaInvoices.logsys.name}") == col(f"right.{IfaInvoices.logsys.name}"),
                col(f"left.{IfaInvoices.budat.name}") == col(f"right.{IfaInvoices.budat.name}"),
            ],
        )
        joined_df = joined_df.withColumn(
            Table.published_to.name,
            when(joined_df[f"right.{IfaInvoices.logsys.name}"].isNotNull(), current_date()).otherwise(
                joined_df[Table.published_to.name]
            ),
        )
        joined_df = joined_df.withColumn(
            Table.active_flag.name,
            when(joined_df[f"right.{IfaInvoices.logsys.name}"].isNotNull(), False).otherwise(
                joined_df[Table.active_flag.name]
            ),
        )
        joined_df = joined_df.select(left_columns)

        filtered_df = filtered_df.withColumn(Table.published_to.name, lit(None))
        filtered_df = filtered_df.withColumn(
            Table.published_from.name, lit(datetime.datetime.now().strftime("%Y-%m-%d"))
        )
        filtered_df = filtered_df.withColumn(Table.active_flag.name, lit(True))
        final_df = joined_df.unionByName(filtered_df)
        self.sql_connector.write_to_db(final_df, self.table_name, "overwrite")
        # username = os.environ["DB_USER"]
        # password = os.environ["DB_PASSWORD"]
        # endpoint = os.environ["DB_ENDPOINT"]
        # port = "5432"
        # database = "kpi"
        # engine = create_engine(f"postgresql://{username}:{password}@{endpoint}:{port}/{database}")
        # connection = engine.connect()
        # connection.execute(sql_text(update_existed_rows_stm))
        # result = connection.execute(sql_text("SELECT * FROM ENR_IFA"))
        # print(result.fetchone())
        # connection.close()
