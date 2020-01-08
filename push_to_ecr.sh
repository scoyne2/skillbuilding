docker build -t airflow .

docker tag airflow:latest 852056369035.dkr.ecr.us-west-1.amazonaws.com/airflow:latest

$(aws ecr get-login --region us-west-1 --no-include-email --profile personal)

docker push 852056369035.dkr.ecr.us-west-1.amazonaws.com/airflow:latest




