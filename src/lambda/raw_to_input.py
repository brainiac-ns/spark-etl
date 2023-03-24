import csv
import io
import json
import logging
import zipfile

import boto3

logger = logging.getLogger()
logger.setLevel(logging.INFO)


def raw_to_input(event, context):
    logger.info(event)
    s3_bucket = event["Records"][0]["s3"]["bucket"]["name"]
    s3_key = event["Records"][0]["s3"]["object"]["key"]

    s3 = boto3.client("s3")

    zip_file = s3.get_object(Bucket=s3_bucket, Key=s3_key)["Body"].read()

    with zipfile.ZipFile(io.BytesIO(zip_file)) as z:
        for filename in z.namelist():
            if filename.endswith(".json"):
                with z.open(filename) as f:
                    json_obj = json.loads(f.read())[0]
                    if "invoice" in filename:
                        dict1_invoice = {k: v[0] for k, v in json_obj.items()}
                        dict2_invoice = {k: v[1] for k, v in json_obj.items()}
                    elif "supplier" in filename:
                        dict1_supplier = {k: v[0] for k, v in json_obj.items()}
                        dict2_supplier = {k: v[1] for k, v in json_obj.items()}
                    elif "organization" in filename:
                        dict1_organization = {k: v[0] for k, v in json_obj.items()}
                        dict2_organization = {k: v[1] for k, v in json_obj.items()}
                    elif "ifa_master" in filename:
                        dict1_ifa_master = {k: v[0] for k, v in json_obj.items()}
                        dict2_ifa_master = {k: v[1] for k, v in json_obj.items()}

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(dict1_invoice.keys())
    csv_writer.writerow(dict1_invoice.values())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/invoice/invoice.csv", Body=csv_buffer.getvalue())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/invoice/invoice.json", Body=json.dumps(dict2_invoice))

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(dict1_supplier.keys())
    csv_writer.writerow(dict1_supplier.values())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/supplier/supplier.csv", Body=csv_buffer.getvalue())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/supplier/supplier.json", Body=json.dumps(dict2_supplier))

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(dict1_organization.keys())
    csv_writer.writerow(dict1_organization.values())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/organization/organization.csv", Body=csv_buffer.getvalue())
    s3.put_object(
        Bucket=s3_bucket, Key="input-data2/organization/organization.json", Body=json.dumps(dict2_organization)
    )

    csv_buffer = io.StringIO()
    csv_writer = csv.writer(csv_buffer)
    csv_writer.writerow(dict1_ifa_master.keys())
    csv_writer.writerow(dict1_ifa_master.values())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/ifa_master/ifa_master.csv", Body=csv_buffer.getvalue())
    s3.put_object(Bucket=s3_bucket, Key="input-data2/ifa_master/ifa_master.json", Body=json.dumps(dict2_ifa_master))
