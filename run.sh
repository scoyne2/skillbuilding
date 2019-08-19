docker stop $(docker ps -a -q)

docker run -d -p 8080:8080 airflow webserver
