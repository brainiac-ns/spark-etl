from pyspark.sql.dataframe import DataFrame
from pyspark.sql.functions import array_contains, col, current_date, lit, when

from base.spark_utils import SparkUtils
from base.sql_connector import SQLConnector
from constants import Constants
from enrichment.base.enrichment import Enrichment
from models.base.table import Table
from models.enrichment_data.buo_target import BuoTarget
from models.enrichment_data.esn_target import EsnTarget
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.enrichment_data.ifa_target import IfaTarget
from models.input_data.organization import Organization


class Esn(Enrichment):
    def __init__(self, spark, logsys_list="*", timeframe_start="*", timeframe_end="*"):
        super(Esn, self).__init__(spark)
        self.spark = spark
        self.sql_connector = SQLConnector(spark)
        df_inv = self.load_invoice()
        df_inv = df_inv.withColumn(
            Table.active_flag.name, when(col(Table.active_flag.name) == "active", lit(True)).otherwise(lit(False))
        )
        self.df_invoice = self.filter_dataframe(df_inv, logsys_list, timeframe_start, timeframe_end)
        self.df_ifa = self.load_ifa_filtered(logsys_list, timeframe_start, timeframe_end)
        self.df_buo = self.load_buo_filtered(logsys_list, timeframe_start, timeframe_end)
        self.df_organization = self.load_organization()

    def __call__(self):
        self.df_invoice.show()
        self.df_buo.show()
        df_invoice_buo = self.df_invoice.join(self.df_buo, on=[IfaTarget.logsys.name, IfaTarget.budat.name], how="left")
        df_invoice_buo = df_invoice_buo.withColumn(EsnTarget.esn.name, col(BuoTarget.buo.name))

        df_invoice_buo.show()

        df_join = df_invoice_buo.join(self.df_ifa, on=[IfaTarget.logsys.name, IfaTarget.budat.name], how="left")
        df_join = df_join.withColumn(
            EsnTarget.esn.name,
            when(col(IfaTarget.ifacmd_filkd.name) != "#", lit(3)).otherwise(
                when(col(IfaTarget.ifacmd_krenr.name) != "#", lit(2)).otherwise(
                    when(col(IfaTarget.ifacmd_lifnr.name) != "#", lit(1)).otherwise(col(EsnTarget.esn.name))
                )
            ),
        )
        df_join = df_join.select(
            [
                EsnTarget.logsys.name,
                EsnTarget.budat.name,
                EsnTarget.esn.name,
                BuoTarget.buo.name,
                IfaInvoices.valid_org.name,
            ]
        )

        df_join.show()

        df_join_org = df_join.join(
            self.df_organization.dropDuplicates([Organization.organization_id.name]),
            how="left",
            on=[col(BuoTarget.buo.name) == col(Organization.organization_id.name)],
        )
        df_join_org = df_join_org.filter(col(Organization.parent_hierarchy_ids.name).isNotNull())
        df_join_org = df_join_org.withColumn(
            EsnTarget.esn.name,
            when(
                (col(IfaInvoices.valid_org.name).isNull())
                | array_contains(Organization.parent_hierarchy_ids.name, col(IfaInvoices.valid_org.name)),
                lit(4),
            ).otherwise(col(EsnTarget.esn.name)),
        ).select([EsnTarget.logsys.name, EsnTarget.budat.name, EsnTarget.esn.name])

        df_join_org = df_join_org.withColumn(IfaInvoices.active_flag.name, lit(True))
        df_join_org = df_join_org.withColumn(IfaInvoices.published_from.name, lit(current_date()))
        df_join_org = df_join_org.withColumn(IfaInvoices.published_to.name, lit(current_date()))

        df_join_org.show()

        self.sql_connector.write_to_db(df_join_org, Constants.ENR_ESN.value)

    def load_ifa_filtered(self, logsys_list, timeframe_start, timeframe_end) -> DataFrame:
        df_ifa = self.sql_connector.read_db(Constants.ENR_IFA.value)
        return self.filter_dataframe(df_ifa, logsys_list, timeframe_start, timeframe_end)

    def load_buo_filtered(self, logsys_list, timeframe_start, timeframe_end) -> DataFrame:
        df_buo = self.sql_connector.read_db(Constants.ENR_BUO.value)
        return self.filter_dataframe(df_buo, logsys_list, timeframe_start, timeframe_end)

    def filter_dataframe(self, df: DataFrame, logsys_list, timeframe_start, timeframe_end) -> DataFrame:
        df = df.filter(col(Table.active_flag.name) == True)

        if logsys_list != "*":
            df = df.filter(col(IfaTarget.logsys.name) in logsys_list)

        if timeframe_start != "*":
            df = df.filter(col(IfaTarget.budat.name) >= timeframe_start)

        if timeframe_end != "*":
            df = df.filter(col(IfaTarget.budat.name) < timeframe_end)

        return df


spark = SparkUtils().get_or_create_spark_session()

a = Esn(spark)
a()
