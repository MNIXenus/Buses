DROP TABLE IF EXISTS avg_speed;
DROP TABLE IF EXISTS window_stops_all;
DROP PROCEDURE IF EXISTS add_one_pair;
DROP PROCEDURE IF EXISTS fill_window_stops;
DROP PROCEDURE IF EXISTS get_distance; 
CREATE TEMPORARY TABLE avg_speed (id INT PRIMARY KEY AUTO_INCREMENT, driver VARCHAR(45), speed FLOAT);
CREATE TABLE window_stops_all (
				id INT PRIMARY KEY AUTO_INCREMENT,
				stop_1 INT,
				stop_2 INT,
				distance FLOAT,
				time_departure DATETIME,
				time_arrival DATETIME);

DELIMITER //
CREATE PROCEDURE add_one_pair(i INT, j INT)
	BEGIN
		SET @stop_1 = (SELECT stop FROM stops_by_drivers WHERE id = i);
        SET @stop_2 = (SELECT stop FROM stops_by_drivers WHERE id = j);
		SET @qry = CONCAT('
		INSERT INTO window_stops (stop_1, time_departure, distance, stop_2, time_arrival)
		WITH
			stop_1_data (col_1, col_2) AS (SELECT stop, time_departure FROM stops_by_drivers WHERE id = ', i, '),
			stop_2_data (col_1, col_2) AS (SELECT stop, time_arrival FROM stops_by_drivers WHERE id = ', j, '),
			distance_data AS (SELECT stop_', @stop_1,' FROM distances WHERE iddistances = ', @stop_2, ')
		SELECT * FROM stop_1_data JOIN distance_data JOIN stop_2_data
		');
		PREPARE add_pair FROM @qry;
		EXECUTE add_pair;
		DEALLOCATE PREPARE add_pair;
	END
//

CREATE PROCEDURE fill_window_stops()
	BEGIN
		SET @counter = 0;
		WHILE @counter <= (SELECT COUNT(*) FROM drivers) DO
			DROP TABLE IF EXISTS window_stops;
            DROP TABLE IF EXISTS stops_by_drivers;
            CREATE TABLE stops_by_drivers (id INT PRIMARY KEY AUTO_INCREMENT) AS (SELECT stop, time_arrival, time_departure FROM sensor_data WHERE driver = @counter);
			CREATE TEMPORARY TABLE window_stops (
				id INT PRIMARY KEY AUTO_INCREMENT,
				stop_1 INT,
				stop_2 INT,
				distance FLOAT,
				time_departure DATETIME,
				time_arrival DATETIME);
			SET @stop = 1;
			WHILE @stop < (SELECT COUNT(*) FROM stops_by_drivers) DO
				CALL add_one_pair(@stop, @stop+1);
				SET @stop = @stop + 1;
			END WHILE;
			INSERT INTO avg_speed (driver, speed) SELECT CONCAT(last_name, ' ', first_name), SUM(distance)/SUM(time_arrival - time_departure)*3.6 FROM window_stops, drivers WHERE iddrivers = @counter;
            INSERT INTO window_stops_all (stop_1, stop_2, distance, time_departure, time_arrival) SELECT stop_1, stop_2, distance, time_departure, time_arrival FROM window_stops;
			SET @counter = @counter + 1;
		END WHILE;
		DROP TABLE IF EXISTS window_stops;
		DROP TABLE IF EXISTS stops_by_drivers;
	END
//
DELIMITER ;

CALL fill_window_stops();
SELECT * FROM avg_speed ORDER BY -speed LIMIT 5; #1st task

DROP TABLE IF EXISTS road_sections;
CREATE TEMPORARY TABLE road_sections AS SELECT * FROM window_stops_all GROUP BY stop_1, stop_2 ORDER BY avg(distance/(time_arrival-time_departure));
SELECT stops.name, stops_2.name, distance/(time_arrival-time_departure) AS speed
FROM stops
INNER JOIN road_sections ON stops.idstops = road_sections.stop_1
INNER JOIN stops AS stops_2 ON stops_2.idstops = road_sections.stop_2
ORDER BY -speed LIMIT 5; #2nd task

SELECT *, COUNT(last_name) AS count_missed
FROM sensor_data 
INNER JOIN drivers ON drivers.iddrivers = sensor_data.driver
WHERE time_departure-time_arrival <10
GROUP BY last_name
ORDER BY count_missed; #task_3