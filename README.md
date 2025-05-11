# Youtube_Analytics

Run the ETL Pipeline : python run_etl.py

Run inspect output : python inspect_output.py











Start Airflow :
    file path of airflow file : /home/admin1/airflow/dags
    run this command : export AIRFLOW_HOME=~/airflow

    In two separate terminals:
    Terminal 1: Webserver    
    airflow webserver --port 8080

    Terminal 2: Scheduler    
    airflow scheduler

    then go to  http://localhost:8080 
    

Run the Streamlit : streamlit run dashboard.py

To run the pytest test case run this command : pytest

