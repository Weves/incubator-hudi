
import boto3

from bucket_info import BUCKET_NAME, TABLE_INFO

if __name__ == "__main__":

    s3_client = boto3.client("s3");
    insert_count = 40000

    for table_name, table in TABLE_INFO.items():

        if table_name == "sanity_check":
            continue

        prefix = table["s3_path"]
        print(prefix)

        for i in range(insert_count):
            response = s3_client.upload_file("dummy_file.txt", BUCKET_NAME, "{}/{}.txt".format(prefix, i))
