INIT_FILE=.airflowinitialized
if [ ! -f «$INIT_FILE» ]; then
    # Create all Airflow configuration files
    airflow db init
    rm /root/airflow/airflow.db

    # Secure the storage of connections’ passwords
    python fernet.py

    # Wait until the DB is ready
    apt update && apt install -y netcat
    while ! nc -z airflow-backend 3306; do  
        sleep 1
    done
    apt remove -y netcat

    # Setup the DB
    python mysqlconnect.py
    airflow db init

    # This configuration is done only the first time
    touch «$INIT_FILE»
fi

# Run the Airflow webserver and scheduler
airflow scheduler &
airflow webserver &
wait 