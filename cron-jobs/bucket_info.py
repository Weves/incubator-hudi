
BUCKET_NAME = "robinhood-hudi"

TABLE_INFO = {
    "demo": {
        "s3_path": "demo/public/demo",
        "days_until_delete": 30
    },
    "test_bucket1": {
        "s3_path": "test_bucket1",
        "days_until_delete": 30
    },
    "test_bucket2": {
        "s3_path": "test_bucket2/",
        "days_until_delete": 0
    }
}
