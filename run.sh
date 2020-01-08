AWS_ACCESS_KEY_ID=$(aws --profile personal configure get aws_access_key_id)
AWS_SECRET_ACCESS_KEY=$(aws --profile personal configure get aws_secret_access_key)

docker stop $(docker ps -a -q)
docker build -t airflow:latest .
docker run -e AWS_ACCESS_KEY_ID=$AWS_ACCESS_KEY_ID -e AWS_SECRET_ACCESS_KEY=$AWS_SECRET_ACCESS_KEY --user root -p 8080:8080 airflow 
