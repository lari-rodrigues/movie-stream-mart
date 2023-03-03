FROM python:3.7

# Configure Airflow: connect to backend
WORKDIR /root/airflow/
COPY airflow-engine/airflow.sh airflow.sh
COPY src/ src/
RUN chmod +x airflow.sh
COPY airflow-engine/fernet.py fernet.py
COPY airflow-engine/mysqlconnect.py mysqlconnect.py

RUN pip3 install 'apache-airflow[mysql,crypto]' mysql-connector-python mysqlclient==2.1.1
RUN pip3 install -r src/requirements.txt

CMD ./airflow.sh 