To run file use the following  "python filename (s3 or dynamodb) source_bucket (dbname or destination_bucket) --prefix value"

- s3 or dynamodb: choose one, s3 indicates bucket to bucket transfer, dynamodb indicates bucket to db.
- dbname or destination_bucket: this matches with s3 or dynamodb. if you choose s3 then you use destination bucket and dbname if using dynamodb
- --prefix value: this is an optional parameter if you have a folder inside your bucket. you must add --prefix before your folder name for example --prefix sample-requests-folder
