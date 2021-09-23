-- Databricks notebook source
USE f1_processed;

-- COMMAND ----------

-- recalculate points by assigninig rank 1 10 points, rank 2 points 9, etc
-- add database name so easier to track where the data is coming from (rather than using USE <database>)

CREATE TABLE f1_presentation.calculated_race_results
USING parquet
AS
SELECT races.race_year,
       constructors.name AS team_name,
       drivers.name AS driver_name,
       results.position,
       results.points,
       11 - results.position AS calculated_points
  FROM results
  JOIN f1_processed.drivers ON (results.driver_id = drivers.driver_id)
  JOIN f1_processed.constructors ON (results.constructor_id = constructors.constructor_id)
  JOIN f1_processed.races ON (results.race_id = races.race_id)
WHERE results.position <= 10

-- COMMAND ----------

SELECT * FROM f1_presentation.calculated_race_results