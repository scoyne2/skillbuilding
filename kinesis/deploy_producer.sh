#build container and push ro ecr
docker build -t kinesis-twitter .
docker tag kinesis-twitter:latest 852056369035.dkr.ecr.us-west-2.amazonaws.com/kinesis-twitter:latest
$(aws ecr get-login --region us-west-2 --no-include-email --profile personal)
docker push 852056369035.dkr.ecr.us-west-2.amazonaws.com/kinesis-twitter:latest

#create kinesis stream
aws kinesis create-stream --stream-name twitter-stream --shard-count 1 --profile personal --region us-west-2

#deploy container on ecs
ecs-cli configure --cluster kinesis-project --default-launch-type FARGATE --config-name kinesis-project --region us-west-2
ecs-cli up --force --cluster-config kinesis-project --security-group sg-a6e79ce8 --vpc vpc-c955edb1 --subnets subnet-b36e7bca,subnet-b36e7bca
ecs-cli compose --project-name kinesis-project service up --cluster-config kinesis-project --create-log-groups

#create s3 bucket
aws s3api create-bucket --bucket kinesis-twitter-bucket --region us-east-1 --profile personal

#create firehose
aws firehose create-delivery-stream --profile personal --delivery-stream-name twitter-firehose --delivery-stream-type KinesisStreamAsSource --kinesis-stream-source-configuration '{"KinesisStreamARN": "arn:aws:kinesis:us-west-2:852056369035:stream/twitter-stream",  "RoleARN": "arn:aws:iam::852056369035:role/kinesis_firehose_delivery_role"}' --s3-destination-configuration '{"RoleARN": "arn:aws:iam::852056369035:role/kinesis_firehose_delivery_role", "BucketARN": "arn:aws:s3:::kinesis-twitter-bucket"}' --region us-west-2