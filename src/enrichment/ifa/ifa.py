import datetime

from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col, desc, lit, row_number, when
from pyspark.sql.window import Window

from base.sql_connector import SQLConnector
from constants import Constants
from enrichment.base.cdc import CDC
from enrichment.base.enrichment import Enrichment
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.enrichment_data.ifa_target import IfaTarget
from models.input_data.ifa_master import IfaMaster
from models.input_data.supplier import Supplier


class Ifa(Enrichment):
    def __init__(self, spark):
        super(Ifa, self).__init__(spark)
        self.spark = spark
        self.df_invoice = self.load_invoice()
        self.df_supplier = self.load_supplier()
        self.df_ifa_master = self.load_ifa_master()
        self.sql_connector = SQLConnector(spark)

    def __call__(self):
        df_lifnr = self.calculate_kpi(IfaInvoices.lifnr.name, IfaTarget.ifacmd_lifnr.name)
        df_krenr = self.calculate_kpi(IfaInvoices.krenr.name, IfaTarget.ifacmd_krenr.name)
        df_filkd = self.calculate_kpi(IfaInvoices.filkd.name, IfaTarget.ifacmd_filkd.name)

        df_merge = df_lifnr.join(df_krenr, [IfaInvoices.logsys.name, IfaInvoices.budat.name], how="inner").join(
            df_filkd, [IfaInvoices.logsys.name, IfaInvoices.budat.name], how="inner"
        )
        final_df = df_merge.select(
            [
                IfaInvoices.logsys.name,
                IfaInvoices.budat.name,
                IfaTarget.ifacmd_lifnr.name,
                IfaTarget.ifacmd_krenr.name,
                IfaTarget.ifacmd_filkd.name,
            ]
        )

        table_name = Constants.ENR_IFA.value
        pk_columns = [IfaInvoices.logsys.name, IfaInvoices.budat.name]
        enr_columns = [IfaTarget.ifacmd_filkd.name, IfaTarget.ifacmd_krenr.name, IfaTarget.ifacmd_lifnr.name]
        update_column = {
            IfaTarget.active_flag.name: False,
            IfaTarget.published_to.name: "'" + datetime.datetime.now().strftime("%Y-%m-%d") + "'",
        }
        insert_timestamp_column = IfaInvoices.published_from.name
        active_flag_column = IfaInvoices.active_flag.name

        cdc = CDC(
            final_df,
            table_name,
            pk_columns,
            enr_columns,
            update_column,
            insert_timestamp_column,
            active_flag_column,
            self.spark,
        )

        cdc()

        # final_df = final_df.withColumn(IfaInvoices.active_flag.name, lit(True))
        # final_df = final_df.withColumn(IfaInvoices.published_from.name, lit(current_date()))
        # final_df = final_df.withColumn(IfaInvoices.published_to.name, lit(current_date()))

        # self.sql_connector.write_to_db(final_df, Constants.ENR_IFA.value)

    def calculate_kpi(self, column_name: str, target_column: str) -> DataFrame:
        selected_columns = [
            f"left.{IfaInvoices.logsys.name}",
            f"left.{IfaInvoices.budat.name}",
            f"left.{column_name}",
            Supplier.ifanr.name,
            f"right.{Supplier.published_from.name}",
        ]
        df_join = self.merge_invoice_supplier(column_name, selected_columns)

        window = Window.partitionBy([IfaInvoices.logsys.name, column_name]).orderBy(desc(Supplier.published_from.name))
        df_join = df_join.withColumn("row_number", row_number().over(window))
        df_join = df_join.filter(df_join.row_number == 1).drop("row_number")

        df_join = df_join.withColumn(Constants.IFASAP.value, col(Supplier.ifanr.name))
        df_join = df_join.drop(Supplier.ifanr.name)

        df_join_ifa = df_join.join(
            self.df_ifa_master, on=col(Constants.IFASAP.value) == col(IfaMaster.ifanr.name), how="left"
        )
        df_join_ifa = df_join_ifa.withColumn(target_column, lit("#"))

        condition = (df_join_ifa[IfaMaster.loekz.name].isNotNull()) & (
            ~(
                (df_join_ifa[IfaMaster.loekz.name] == True)
                & (df_join_ifa[IfaMaster.deletion_date.name] < df_join_ifa[IfaInvoices.budat.name])
            )
            | (df_join_ifa[IfaMaster.loekz.name] == False)
        )

        df_join_ifa = df_join_ifa.withColumn(
            target_column, when(condition, col(Constants.IFASAP.value)).otherwise(col(target_column))
        )
        df_join_ifa = df_join_ifa.select([IfaInvoices.logsys.name, IfaInvoices.budat.name, target_column])

        return df_join_ifa
