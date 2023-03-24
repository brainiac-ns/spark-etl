from pyspark.sql.functions import col, current_date, desc, lit, row_number, when
from pyspark.sql.window import Window

from base.spark_utils import SparkUtils
from base.sql_connector import SQLConnector
from constants import Constants
from enrichment.base.enrichment import Enrichment
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.input_data.organization import Organization
from models.input_data.supplier import Supplier


class Buo(Enrichment):
    def __init__(self, spark):
        super(Buo, self).__init__(spark)
        self.spark = spark
        self.df_invoice = self.load_invoice()
        self.df_supplier = self.load_supplier()
        self.df_organization = self.load_organization()
        self.sql_connector = SQLConnector(spark)

    def __call__(self):
        column_name = IfaInvoices.lifnr.name
        selected_columns = [f"left.{IfaInvoices.logsys.name}", f"left.{IfaInvoices.budat.name}", Supplier.buo_org.name]
        df_join = self.merge_invoice_supplier(column_name, selected_columns)

        window = Window.partitionBy(Organization.supplier_org.name).orderBy(desc(Organization.published_from.name))
        self.df_organization = self.df_organization.withColumn("row_number", row_number().over(window))
        self.df_organization = self.df_organization.filter(col("row_number") == 1).drop("row_number")

        df_join_org = df_join.join(
            self.df_organization, on=[col(Supplier.buo_org.name) == col(Organization.supplier_org.name)], how="left"
        )

        df_join_org = df_join_org.withColumn(
            Constants.BUO.value,
            when(col(Organization.organization_id.name).isNotNull(), col(Organization.organization_id.name)).otherwise(
                lit(Constants.DUMMY_BUO.value)
            ),
        )

        df_join_org = df_join_org.select(IfaInvoices.logsys.name, IfaInvoices.budat.name, Constants.BUO.value)
        df_join_org = df_join_org.withColumn(IfaInvoices.active_flag.name, lit(True))
        df_join_org = df_join_org.withColumn(IfaInvoices.published_from.name, lit(current_date()))
        df_join_org = df_join_org.withColumn(IfaInvoices.published_to.name, lit(current_date()))

        df_join_org.show()

        self.sql_connector.write_to_db(df_join_org, Constants.ENR_BUO.value)


spark = SparkUtils().get_or_create_spark_session()

a = Buo(spark)
a()
