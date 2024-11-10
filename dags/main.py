# We'll start by importing the DAG object
from datetime import timedelta

from airflow import DAG
# We need to import the operators used in our tasks
from airflow.operators.python_operator import PythonOperator
# We then import the days_ago function
from airflow.utils.dates import days_ago

import pandas as pd
import psycopg2
import os
import boto3
from datetime import date


# get dag directory path
dag_path = os.getcwd()


def tranform_data():
    #establishing the connection
    conn = psycopg2.connect(
    database="dwhDB", user='dwh', password='dwh', host='postgres-dwh', port= '5432'
    )
    #Creating a cursor object using the cursor() method
    cursor = conn.cursor()

    #Executing a Posgres function using the execute() method
    cursor.execute("select version()")

    # Fetch a single row using fetchone() method.
    data = cursor.fetchone()
    print("Connection established to: ",data)


    command = (
        """

CREATE TABLE IF NOT EXISTS citi_bike_trips_report (
  bikeid bigint,
  start_station_name varchar,
  end_station_name varchar,
  starttime timestamp,
  stoptime timestamp,
  tripduration bigint,
  trip_date timestamp,
  longest_trip_this_day bigint,
  rank_longest_trip_this_day smallint,
  prev_trip_stoptime timestamp,
  idle_time interval
);
""")

    try:
        print(command)
        cursor.execute(command)
        conn.commit()
        print("Table citibike_trips_report created if not exist successfully....")
    except (Exception, psycopg2.DatabaseError) as err:
        #os.remove(dag_path+'/processed_data/'+last_added)
            # pass exception to function
        print(psycopg2.DatabaseError)
        print(Exception)
        # show_psycopg2_exception(err)
        cursor.close()
    conn.commit()


    command = ("""

        INSERT INTO citi_bike_trips_report
      (bikeid, start_station_name, end_station_name, starttime, stoptime, tripduration, trip_date,
       longest_trip_this_day, rank_longest_trip_this_day, prev_trip_stoptime, idle_time)
        SELECT DISTINCT bikeid, start_station_name, end_station_name, my_start_time, my_stop_time, tripduration
               ,DATE(my_start_time)
               ,MAX(r.tripduration) OVER(PARTITION BY bikeid, CAST(r.starttime AS DATE)) AS longest_trip_this_day
              ,RANK() OVER(PARTITION BY bikeid, CAST(r.starttime AS DATE) ORDER BY r.tripduration DESC) AS rank_longest_trip_this_day
              ,LAG(r.stoptime) OVER(PARTITION BY bikeid, CAST(r.starttime AS DATE) ORDER BY r.starttime) AS prev_trip_stoptime
              ,r.starttime  - LAG(r.stoptime) OVER(PARTITION BY bikeid, DATE(r.starttime) ORDER BY r.starttime) AS idle_time
        FROM (
            SELECT DISTINCT bikeid
            ,starttime as my_start_time
            ,stoptime as my_stop_time
            ,start_station_name
            ,end_station_name
            ,starttime
            ,stoptime
            ,tripduration
                FROM "public"."citibike_trips"
                WHERE bikeid = 17054 -- and "start station name" = 'Broadway & W 24 St'
        ) as r
        ORDER BY my_start_time;""")


    cursor.execute(command)
    conn.commit()
    conn.close()



def generate_report():
    print('Generate Report')

# initializing the default arguments that we'll pass to our DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1)
}

report_dag = DAG(
    'citybike-trips_dag',
    default_args=default_args,
    description='Aggregates city bike trips for data analysis',
    schedule_interval=timedelta(days=1),
    catchup=False
)


task_1 = PythonOperator(
    task_id='load_data',
    python_callable=tranform_data,
    dag=report_dag,
)

task_2 = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=report_dag,
)

task_1 >> task_2