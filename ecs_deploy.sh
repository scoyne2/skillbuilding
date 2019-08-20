#ecs-cli down --force --cluster airflow-project

ecs-cli configure --cluster airflow-project --default-launch-type FARGATE --config-name airflow-project --region us-west-1

ecs-cli up --force --cluster-config airflow-project --security-group sg-08d79347eb5180b05 --vpc vpc-0dcf325813702429d --subnets subnet-05e4552ae8ad6263e,subnet-0c6c2fdd59d944ab5 

ecs-cli compose --project-name airflow-project service up --cluster-config airflow-project --create-log-groups --target-group-arn arn:aws:elasticloadbalancing:us-west-1:852056369035:targetgroup/airflow-target-group-2/b94ad9849e1d43f8 --container-name webserver --container-port 8080