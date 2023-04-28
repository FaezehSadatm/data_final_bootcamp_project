SHOW GLOBAL VARIABLES LIKE 'local_infile';

SET GLOBAL local_infile = 1;
use rides;
drop table if exists `weather` ;
CREATE TABLE `weather` (
   `temp` text DEFAULT NULL,
   `location` text,
   `clouds` double DEFAULT NULL,
   `pressure` double DEFAULT NULL,
   `rain` double DEFAULT NULL,
   `time_stamp` int DEFAULT NULL,
   `humidity` double DEFAULT NULL,
   `wind` double DEFAULT NULL
 ) ;
 CREATE TABLE `cab_rides` (
   `distance` float DEFAULT NULL,
   `cab_type` text,
   `time_stamp` bigint DEFAULT NULL,
   `destination` text,
   `source` text,
   `price` float DEFAULT NULL,
   `surge_multiplier` float DEFAULT NULL,
   `id` text,
   `product_id` text,
   `name` text
 );
 
LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/cab_rides.csv' INTO TABLE cab_rides
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\''
LINES TERMINATED BY '\r\n' -- Windows right line terminator
(@vdistance,cab_type,time_stamp,destination,source,@vprice,@vsurge_multiplier,id,product_id,name)
SET
distance = NULLIF(@vdistance,''), 
price = NULLIF(@vprice,''), 
surge_multiplier = NULLIF(@vsurge_multiplier,'');

ALTER TABLE `rides`.`cab_rides` 
DROP COLUMN `product_id`,
DROP COLUMN `id`;
ALTER TABLE rides.cab_rides ADD COLUMN `id` INT AUTO_INCREMENT UNIQUE FIRST;

LOAD DATA INFILE 'C:/ProgramData/MySQL/MySQL Server 8.0/Uploads/weather.csv' INTO TABLE weather
FIELDS TERMINATED BY ','
OPTIONALLY ENCLOSED BY '\''
LINES TERMINATED BY '\r\n' -- Windows right line terminator
(temp,location,clouds,pressure,@vrain,time_stamp,humidity,wind)
set rain = NULLIF(@vrain,'');


create table weather_enhanced as
SELECT *,  lead(time_stamp) over (ORDER BY location, time_stamp) as next_time_stamp
    FROM weather;

update cab_rides set time_stamp = CAST(time_stamp / 1000 AS UNSIGNED);
update weather_enhanced set  next_time_stamp = ( select max(time_stamp) from cab_rides) +1 
where next_time_stamp is null;

ALTER TABLE `rides`.`weather_enhanced` 
CHANGE COLUMN `next_time_stamp` `next_time_stamp` BIGINT(20) NOT NULL DEFAULT 0 ;

ALTER TABLE `rides`.`weather_enhanced` 
CHANGE COLUMN `time_stamp` `time_stamp` BIGINT(20) NOT NULL DEFAULT 0 ;

ALTER TABLE `rides`.`weather_enhanced` ADD INDEX `weather_enhanced_ts1` (`time_stamp` ASC);
ALTER TABLE `rides`.`weather_enhanced` ADD INDEX `weather_enhanced_ts2` (`next_time_stamp` ASC);
ALTER TABLE `rides`.`weather_enhanced` ADD INDEX `weather_enhanced_location` (`location`(20) ASC;

ALTER TABLE `rides`.`cab_rides` 
ADD INDEX `cab_rides_source` (`source`(20) ASC) ,
ADD INDEX `cab_rides_dest` (`destination`(20) ASC) ,
ADD INDEX `cab_rides_timestamp` (`time_stamp` ASC) ;

delete from cab_rides where `name` = 'Taxi';

create table cab_rides_dest_weather as 
select c.*, temp as destination_temp, clouds as destination_clouds, pressure as destination_pressure,
 rain as destination_rain, humidity as destination_humidity, wind as destination_wind
 from  cab_rides c 
	left join weather_enhanced w
    on c.destination = w.location and c.time_stamp < w.next_time_stamp and c.time_stamp >w.time_stamp;
    
ALTER TABLE `rides`.`cab_rides_dest_weather` ADD INDEX `cab_rides_dest_weather_src` (`source`(20) ASC) ;
ALTER TABLE `rides`.`cab_rides_dest_weather` ADD INDEX `cab_rides_dest_weather_ts` (`time_stamp` ASC) ;
        
create table cab_rides_weather as 
select c.*, temp as source_temp, clouds as source_clouds, pressure as source_pressure, rain as source_rain, humidity as source_humidity, wind as source_wind
 from  cab_rides_dest_weather c 
	left join weather_enhanced w
    on c.`source` = w.location and c.time_stamp < w.next_time_stamp and c.time_stamp >w.time_stamp;

ALTER TABLE rides.cab_rides_weather ADD COLUMN `id` INT AUTO_INCREMENT UNIQUE FIRST;

create table rides.cab_rides_weather_refined as 
select 
  cast(distance as float) as distance, 
  cab_type, 
  cast(time_stamp as unsigned) as time_stamp, 
  destination, 
  `source` as `source`, 
  cast(price as float) as price, 
  cast(surge_multiplier as float) as surge_multiplier, 
  cast(destination_temp as float) as destination_temp, 
  cast(destination_clouds as float) as destination_clouds, 
  cast(destination_pressure as float) as destination_pressure, 
  cast(destination_rain as float) as destination_rain, 
  cast(destination_humidity as float) as destination_humidity, 
  cast(destination_wind as float) as destination_wind, 
  cast(source_temp as float) as source_temp, 
  cast(source_clouds as float) as source_clouds, 
  cast(source_pressure as float) as source_pressure, 
  cast(source_rain as float) as source_rain, 
  cast(source_humidity as float) as source_humidity, 
  cast(source_wind as float) as source_wind 
from 
  rides.cab_rides_weather;


ALTER TABLE `rides`.`cab_rides_weather_refined` 
CHANGE COLUMN `destination` `destination` TEXT NOT NULL ,
CHANGE COLUMN `source` `source` TEXT NOT NULL ,
CHANGE COLUMN `price` `price` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `surge_multiplier` `surge_multiplier` FLOAT NOT NULL DEFAULT 1 ,
CHANGE COLUMN `destination_temp` `destination_temp` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `destination_clouds` `destination_clouds` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `destination_pressure` `destination_pressure` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `destination_rain` `destination_rain` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `destination_humidity` `destination_humidity` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `destination_wind` `destination_wind` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `source_temp` `source_temp` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `source_clouds` `source_clouds` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `source_pressure` `source_pressure` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `source_rain` `source_rain` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `source_humidity` `source_humidity` FLOAT NOT NULL DEFAULT 0 ,
CHANGE COLUMN `source_wind` `source_wind` FLOAT NOT NULL DEFAULT 0 ;
;
