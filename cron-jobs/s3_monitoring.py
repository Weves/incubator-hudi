
import boto3
from datetime import datetime

from cron_utils import get_all_keys
from bucket_info import BUCKET_NAME, TABLE_INFO

if __name__ == "__main__":

    current_time = datetime.now().timestamp()

    # get all objects from S3 buket
    s3 = boto3.resource('s3')
    s3_paginator = boto3.client('s3').get_paginator('list_objects_v2')

    for table in TABLE_INFO.values():
        # get the latest modification time in order to compare to current time
        latest_modification = -1
        for key in get_all_keys(BUCKET_NAME, s3_paginator, prefix=table["s3_path"]):
            latest_modification = max(latest_modification, key["LastModified"].timestamp())

        # get lag from last insert to send to prometheous
        insert_lag = current_time - latest_modification

        # TODO Send to prometheous
