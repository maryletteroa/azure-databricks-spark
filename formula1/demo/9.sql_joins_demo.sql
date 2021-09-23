-- Databricks notebook source
USE f1_presentation;

-- COMMAND ----------

DESC driver_standings

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2018
AS
SELECT race_year, driver_name, team, total_points, wins, rank
  FROM driver_standings
WHERE race_year = 2018;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2018

-- COMMAND ----------

CREATE OR REPLACE TEMP VIEW v_driver_standings_2020
AS
SELECT race_year, driver_name, team, total_points, wins, rank
  FROM driver_standings
WHERE race_year = 2020;

-- COMMAND ----------

SELECT * FROM v_driver_standings_2020

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Inner join

-- COMMAND ----------

SELECT * 
  FROM v_driver_standings_2018 d_2018
  JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Left join

-- COMMAND ----------

-- drivers who raced in 2018 but not in 2020

SELECT * 
  FROM v_driver_standings_2018 d_2018
  LEFT JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- drivers who raced in 2020 but not in 2018

SELECT * 
  FROM v_driver_standings_2018 d_2018
  RIGHT JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Full join

-- COMMAND ----------

-- all drivers who raced on either 2018, 2020; superset of these two views

SELECT * 
  FROM v_driver_standings_2018 d_2018
  FULL JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Semi join

-- COMMAND ----------

-- inner join with data only from 2018 i.e. exclude drivers who did not race in 2020

SELECT * 
  FROM v_driver_standings_2018 d_2018
  SEMI JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Anti-join

-- COMMAND ----------

-- the drivers who raced in 2018 but not in 2020

SELECT * 
  FROM v_driver_standings_2018 d_2018
  ANTI JOIN v_driver_standings_2020 d_2020
    ON (d_2018.driver_name = d_2020.driver_name)

-- COMMAND ----------

-- MAGIC %md
-- MAGIC Cross-join

-- COMMAND ----------

-- gives cartesian product of the views

SELECT * 
  FROM v_driver_standings_2018 d_2018
  CROSS JOIN v_driver_standings_2020 d_2020
