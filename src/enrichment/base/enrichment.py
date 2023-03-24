import logging

from pyspark.sql import SparkSession
from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import col

from models.enrichment_data.ifa_invoices import IfaInvoices
from models.input_data.supplier import Supplier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class Enrichment:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        self.df_invoice: DataFrame = None
        self.df_supplier: DataFrame = None
        self.df_ifa_master: DataFrame = None
        self.df_organization: DataFrame = None

    def load_invoice(self) -> DataFrame:
        logging.info("Loading invoices")
        return self.spark.read.parquet("s3a://brainiac-kpi/landing/invoice")

    def load_supplier(self) -> DataFrame:
        logging.info("Loading suppliers")
        return self.spark.read.parquet("s3a://brainiac-kpi/landing/supplier")

    def load_ifa_master(self) -> DataFrame:
        logging.info("Loading ifa master")
        return self.spark.read.parquet("s3a://brainiac-kpi/landing/ifa_master")

    def load_organization(self) -> DataFrame:
        logging.info("Loading organizations")
        return self.spark.read.parquet("s3a://brainiac-kpi/landing/organization")

    def merge_invoice_supplier(self, column_name: str, selected_columns: list) -> DataFrame:
        df_join = (
            self.df_invoice.alias("left")
            .join(
                self.df_supplier.alias("right"),
                on=[
                    col(f"left.{IfaInvoices.logsys.name}") == col(f"right.{IfaInvoices.logsys.name}"),
                    col(f"left.{column_name}") == col(f"right.{Supplier.lifnr.name}"),
                    col(f"left.{IfaInvoices.budat.name}").between(
                        col(f"right.{Supplier.valid_from.name}"), col(f"right.{Supplier.valid_to.name}")
                    ),
                ],
                how="left",
            )
            .select(selected_columns)
        )

        return df_join
