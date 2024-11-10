CREATE TABLE citibike_trips(
  tripduration              bigint, 
  starttime                 timestamp, 
  stoptime                  timestamp, 
  start_station_id          bigint, 
  start_station_name        varchar, 
  start_station_latitude    decimal, 
  start_station_longitude   decimal, 
  end_station_id            bigint, 
  end_station_name          varchar, 
  end_station_latitude      decimal, 
  end_station_longitude     decimal, 
  bikeid                    bigint, 
  usertype                  varchar, 
  birth_year                real, 
  gender                    varchar
  );


COPY citibike_trips
FROM '/docker-entrypoint-initdb.d/201601-citibike-tripdata_1.csv'
  DELIMITER ','
  CSV HEADER;

COPY citibike_trips
FROM '/docker-entrypoint-initdb.d/201602-citibike-tripdata_1.csv'
  DELIMITER ','
  CSV HEADER;  

COPY citibike_trips
FROM '/docker-entrypoint-initdb.d/201603-citibike-tripdata_01.csv'
  DELIMITER ','
  CSV HEADER;  

COPY citibike_trips
FROM '/docker-entrypoint-initdb.d/201603-citibike-tripdata_02.csv'
  DELIMITER ','
  CSV HEADER; 

  COPY citibike_trips
FROM '/docker-entrypoint-initdb.d/201604-citibike-tripdata_01.csv'
  DELIMITER ','
  CSV HEADER;  

COPY citibike_trips
FROM '/docker-entrypoint-initdb.d/201604-citibike-tripdata_02.csv'
  DELIMITER ','
  CSV HEADER;    
