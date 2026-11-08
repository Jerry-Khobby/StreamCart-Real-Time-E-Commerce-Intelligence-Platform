#!/bin/sh
# Creates the lakehouse's own MinIO login, so Spark and Airflow never hold the root account.
# Its policy reaches only the lakehouse and archive buckets, and only their objects: it
# can't create or delete buckets, change policies or manage users. Safe to re-run.
set -eu

export MC_CONFIG_DIR=/tmp/.mc
BUCKET="${MINIO_BUCKET}"
POLICY=streamcart-lakehouse

mc alias set local http://minio:9000 "$MINIO_ROOT_USER" "$MINIO_ROOT_PASSWORD" >/dev/null

cat > /tmp/lakehouse-policy.json <<EOF
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": ["s3:ListBucket", "s3:GetBucketLocation", "s3:ListBucketMultipartUploads"],
      "Resource": ["arn:aws:s3:::${BUCKET}", "arn:aws:s3:::${BUCKET}-archive"]
    },
    {
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject",
                 "s3:AbortMultipartUpload", "s3:ListMultipartUploadParts"],
      "Resource": ["arn:aws:s3:::${BUCKET}/*", "arn:aws:s3:::${BUCKET}-archive/*"]
    }
  ]
}
EOF

mc admin policy create local "$POLICY" /tmp/lakehouse-policy.json
mc admin user add local "$MINIO_APP_USER" "$MINIO_APP_PASSWORD"
# Match the PolicyName line: the user's own name could contain the policy's
if ! mc admin user info local "$MINIO_APP_USER" | grep -Eq "^PolicyName:.*\b${POLICY}\b"; then
    mc admin policy attach local "$POLICY" --user "$MINIO_APP_USER"
fi
echo "MinIO login $MINIO_APP_USER can reach ${BUCKET} and ${BUCKET}-archive only"
