
import boto3
from datetime import datetime, timedelta


from cron_utils import get_all_keys
from bucket_info import BUCKET_NAME, TABLE_INFO


if __name__ == "__main__":

    current_time = datetime.now().timestamp()

    # get all objects from S3 buket
    s3 = boto3.resource('s3')
    s3_paginator = boto3.client('s3').get_paginator('list_objects_v2')

    # check each object to see if it should be cleaned
    for table in TABLE_INFO.values():

        # get the removal time for this table
        removal_point = timedelta(seconds=current_time) - timedelta(days=table["days_until_delete"])
        removal_point = removal_point.total_seconds()

        for key in get_all_keys(BUCKET_NAME, s3_paginator, prefix=table["s3_path"]):
            modification_time = key["LastModified"].timestamp()
            if modification_time < removal_point:
                s3.Object(BUCKET_NAME, key["Key"]).delete()
                print("Deleting {}".format(key["Key"]))
