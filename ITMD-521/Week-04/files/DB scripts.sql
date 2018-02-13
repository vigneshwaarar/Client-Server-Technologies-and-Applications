-- 1.create database using following command

create database itmd521_1; 

-- 2.create tables using following commands

-- 9091_Data table

create table 9091_Data (
usa_wsi_id VARCHAR(10),
wban_wsi_id INTEGER,
observation_date INTEGER,
latitude_degree VARCHAR(6),
longitude_degree VARCHAR(7),
elevation_meters VARCHAR(5),
wind_direction INTEGER,
sky_ch_meters INTEGER,
visibility_dist INTEGER,
air_temperature VARCHAR(5),
dew_pt_temperature VARCHAR(5),
atm_pressure INTEGER);

-- 9293_Data table

create table 9293_Data (
usa_wsi_id VARCHAR(10),
wban_wsi_id INTEGER,
observation_date INTEGER,
latitude_degree VARCHAR(6),
longitude_degree VARCHAR(7),
elevation_meters VARCHAR(5),
wind_direction INTEGER,
sky_ch_meters INTEGER,
visibility_dist INTEGER,
air_temperature VARCHAR(5),
dew_pt_temperature VARCHAR(5),
atm_pressure INTEGER);
