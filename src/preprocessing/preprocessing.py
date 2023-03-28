import logging

import boto3
from pyspark.sql.functions import split
from pyspark.sql.types import DateType, StringType, StructField, StructType

from base.spark_utils import SparkUtils
from models.enrichment_data.ifa_invoices import IfaInvoices
from models.input_data.ifa_master import IfaMaster
from models.input_data.organization import Organization
from models.input_data.supplier import Supplier

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s.%(msecs)03d %(levelname)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


class PreprocessingJob:
    def __init__(self):
        spark_utils = SparkUtils()
        self.spark = spark_utils.get_or_create_spark_session()
        self.s3 = boto3.client("s3", region_name="eu-north-1")

    def __call__(self):
        response = self.s3.list_objects_v2(Bucket="brainiac-kpi", Prefix="input-data")
        contents = response.get("Contents")

        for result in contents:
            key = result.get("Key")
            if "organization" not in key:
                continue
            if ".json" in key:
                continue
            if ".json" not in key and ".csv" not in key:
                continue
            logging.info(key)
            path_parts = key.split("/")
            input_file_path_csv = f"s3a://brainiac-kpi/{path_parts[0]}/{path_parts[1]}/*csv"
            input_file_path_json = f"s3a://brainiac-kpi/{path_parts[0]}/{path_parts[1]}/*json"
            schema = StructType()

            if "ifa_master" in key:
                schema = StructType(
                    [
                        StructField(IfaMaster.ifanr.name, StringType(), True),
                        StructField(IfaMaster.loekz.name, StringType(), True),
                        StructField(IfaMaster.deletion_date.name, DateType(), True),
                        StructField(IfaMaster.published_from.name, DateType(), True),
                        StructField(IfaMaster.published_to.name, DateType(), True),
                        StructField(IfaMaster.active_flag.name, StringType(), True),
                    ]
                )
            elif "invoice" in key:
                schema = StructType(
                    [
                        StructField(IfaInvoices.logsys.name, StringType(), True),
                        StructField(IfaInvoices.budat.name, StringType(), True),
                        StructField(IfaInvoices.active_flag.name, StringType(), True),
                        StructField(IfaInvoices.lifnr.name, StringType(), True),
                        StructField(IfaInvoices.krenr.name, StringType(), True),
                        StructField(IfaInvoices.filkd.name, StringType(), True),
                        StructField(IfaInvoices.valid_org.name, StringType(), True),
                        StructField(IfaInvoices.published_from.name, DateType(), True),
                        StructField(IfaInvoices.published_to.name, DateType(), True),
                    ]
                )
            elif "organization" in key:
                schema = StructType(
                    [
                        StructField(Organization.organization_id.name, StringType(), True),
                        StructField(Organization.supplier_org.name, StringType(), True),
                        StructField(Organization.organization_type.name, StringType(), True),
                        StructField(Organization.parent_hierarchy_ids.name, StringType(), True),
                        StructField(Organization.valid_from.name, DateType(), True),
                        StructField(Organization.valid_to.name, DateType(), True),
                        StructField(Organization.active_flag.name, StringType(), True),
                        StructField(Organization.published_from.name, DateType(), True),
                        StructField(Organization.published_to.name, DateType(), True),
                    ]
                )
            elif "supplier" in key:
                schema = StructType(
                    [
                        StructField(Supplier.logsys.name, StringType(), True),
                        StructField(Supplier.lifnr.name, StringType(), True),
                        StructField(Supplier.ifanr.name, StringType(), True),
                        StructField(Supplier.valid_from.name, DateType(), True),
                        StructField(Supplier.valid_to.name, DateType(), True),
                        StructField(Supplier.buo_org.name, StringType(), True),
                        StructField(Supplier.active_flag.name, StringType(), True),
                        StructField(Supplier.published_from.name, DateType(), True),
                        StructField(Supplier.published_to.name, DateType(), True),
                    ]
                )

            df_csv = self.spark.read.schema(schema).option("header", "true").csv(input_file_path_csv)
            df_json = self.spark.read.schema(schema).option("multiline", "true").json(input_file_path_json)

            if "organization" in key:
                df_csv = df_csv.withColumn(
                    Organization.parent_hierarchy_ids.name,
                    split(df_csv[Organization.parent_hierarchy_ids.name], ",").cast("array<string>"),
                )
                df_json = df_json.withColumn(
                    Organization.parent_hierarchy_ids.name,
                    split(df_json[Organization.parent_hierarchy_ids.name], ",").cast("array<string>"),
                )

            df_union = df_csv.unionByName(df_json)
            logging.info("Union Done")

            partition_by = ""
            if "ifa_master" in key:
                partition_by = IfaMaster.ifanr.name
            elif "invoice" in key:
                partition_by = IfaInvoices.budat.name
            elif "organization" in key:
                partition_by = Organization.organization_type.name
            elif "supplier" in key:
                partition_by = Supplier.logsys.name

            df_union.write.partitionBy(partition_by).mode("overwrite").parquet(
                "s3a://brainiac-kpi/landing/" + path_parts[1]
            )


def run_preprocessing():
    a = PreprocessingJob()
    a()
