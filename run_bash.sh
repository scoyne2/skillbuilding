docker stop $(docker ps -a -q)

docker run -it --user root -p 8888:8888  -v /Users:/Users airflow  /bin/bash
