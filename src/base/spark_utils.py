import os

from dotenv import load_dotenv
from pyspark.sql import SparkSession

load_dotenv()


class SparkUtils:
    # def get_or_create_spark_session(self):
    #     """
    #     Creates or acquires an already existing Spark Session
    #     :return: Spark Session
    #     """
    #     session_builder = SparkSession.builder.appName("SCM Processing Engine")

    #     # In local env use packages in local library
    #     (
    #         session_builder.config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0")
    #         # Make sure to set these aws credentials as env variables: access key, secret, and session token
    #         .config(
    #             "spark.hadoop.fs.s3a.aws.credentials.provider",
    #             "org.apache.hadoop.fs.s3a.TemporaryAWSCredentialsProvider",
    #         ).config("spark.jars", "https://jdbc.postgresql.org/download/postgresql-42.2.27.jre7.jar")
    #     )

    #     # Create Spark Session
    #     session = session_builder.getOrCreate()

    #     return session

    def get_or_create_spark_session(self):
        return (
            SparkSession.builder.appName("Spark Test")
            .config("spark.jars.packages", "org.apache.hadoop:hadoop-aws:3.0.0")
            .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
            .config("spark.hadoop.fs.s3a.endpoint", "s3.amazonaws.com")
            .config("spark.hadoop.fs.s3a.access.key", os.environ["AWS_ACCESS_KEY_ID"])
            .config("spark.hadoop.fs.s3a.secret.key", os.environ["AWS_SECRET_ACCESS_KEY"])
            .config(
                "spark.hadoop.fs.s3a.aws.credentials.provider",
                "org.apache.hadoop.fs.s3a.SimpleAWSCredentialsProvider",
            )
            .config("spark.jars", "https://jdbc.postgresql.org/download/postgresql-42.2.27.jre7.jar")
            .getOrCreate()
        )
