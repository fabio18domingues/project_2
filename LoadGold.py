# Databricks notebook source
# MAGIC %md
# MAGIC **Create Dimensions**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_leagues AS
# MAGIC SELECT DISTINCT
# MAGIC     ROW_NUMBER() OVER (ORDER BY league_name) AS ID_LEAGUE,
# MAGIC     league_name AS LEAGUE_NAME,
# MAGIC     country AS COUNTRY,
# MAGIC     division AS DIVISION
# MAGIC FROM (
# MAGIC     SELECT 'PremierLeague' AS league_name, 'England' AS country, 1 AS division
# MAGIC     UNION
# MAGIC     SELECT 'LaLiga' AS league_name, 'Spain' AS country, 1 AS division
# MAGIC     UNION
# MAGIC     SELECT 'Ligue1' AS league_name, 'France' AS country, 1 AS division
# MAGIC     UNION
# MAGIC     SELECT 'Bundesliga' AS league_name, 'Germany' AS country, 1 AS division
# MAGIC     UNION
# MAGIC     SELECT 'Liga1' AS league_name, 'Portugal' AS country, 1 AS division
# MAGIC     UNION
# MAGIC     SELECT 'SerieA' AS league_name, 'Italy' AS country, 1 AS division
# MAGIC ) AS leagues;
# MAGIC

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_teams AS
# MAGIC SELECT DISTINCT
# MAGIC     ROW_NUMBER() OVER (ORDER BY team) AS ID_TEAM,
# MAGIC     team AS TEAM_NAME,
# MAGIC     B.ID_LEAGUE AS ID_LEAGUE
# MAGIC FROM (
# MAGIC     SELECT Home_Team AS team, LEAGUE_NAME AS league_name FROM silver.tbl_premierleague
# MAGIC     UNION
# MAGIC     SELECT Away_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_premierleague
# MAGIC     UNION
# MAGIC     SELECT Home_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_laliga
# MAGIC     UNION
# MAGIC     SELECT Away_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_laliga
# MAGIC     UNION
# MAGIC     SELECT Home_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_ligue1
# MAGIC     UNION
# MAGIC     SELECT Away_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_ligue1
# MAGIC     UNION 
# MAGIC     SELECT Home_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_bundesliga
# MAGIC     UNION 
# MAGIC     SELECT Away_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_bundesliga
# MAGIC     UNION
# MAGIC     SELECT Home_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_liga1
# MAGIC     UNION 
# MAGIC     SELECT Away_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_liga1
# MAGIC     UNION
# MAGIC     SELECT Home_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_seriea
# MAGIC     UNION
# MAGIC     SELECT Away_Team AS team, LEAGUE_NAME AS league_name  FROM silver.tbl_seriea
# MAGIC ) AS A
# MAGIC LEFT JOIN gold.dim_leagues AS B ON A.league_name = B.LEAGUE_NAME;

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.dim_date AS
# MAGIC WITH bounds AS (
# MAGIC     SELECT
# MAGIC         DATE('1993-08-01') AS start_date,
# MAGIC         current_date() AS end_date
# MAGIC ),
# MAGIC dates AS (
# MAGIC     SELECT explode(sequence(start_date, end_date, interval 1 day)) AS DateValue
# MAGIC     FROM bounds
# MAGIC )
# MAGIC SELECT
# MAGIC     -- Surrogate key (YYYYMMDD)
# MAGIC     CAST(date_format(DateValue, 'yyyyMMdd') AS INT) AS ID_DATE,
# MAGIC
# MAGIC     -- Raw date
# MAGIC     DateValue,
# MAGIC
# MAGIC     -- Calendar breakdown
# MAGIC     year(DateValue) AS Year,
# MAGIC     month(DateValue) AS Month,
# MAGIC     day(DateValue) AS Day,
# MAGIC     quarter(DateValue) AS Quarter,
# MAGIC     weekofyear(DateValue) AS WeekOfYear,
# MAGIC     weekday(DateValue) AS WeekDayNumber,
# MAGIC     date_format(DateValue, 'EEEE') AS WeekDayName,
# MAGIC     date_format(DateValue, 'MMMM') AS MonthName,
# MAGIC
# MAGIC     -- Weekend flag
# MAGIC     CASE WHEN weekday(DateValue) IN (5, 6) THEN 'Weekend' ELSE 'Weekday' END AS DayType,
# MAGIC
# MAGIC     -- Season (época desportiva)
# MAGIC     CASE 
# MAGIC         WHEN month(DateValue) >= 8
# MAGIC             THEN concat(year(DateValue), '/', year(DateValue) + 1)
# MAGIC         ELSE concat(year(DateValue) - 1, '/', year(DateValue))
# MAGIC     END AS Season
# MAGIC
# MAGIC FROM dates
# MAGIC ORDER BY DateValue;
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC **Create Facts**

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TEMPORARY VIEW VW_GOLD_FACT AS
# MAGIC SELECT *
# MAGIC FROM silver.tbl_premierleague
# MAGIC UNION 
# MAGIC SELECT *
# MAGIC FROM silver.tbl_laliga
# MAGIC UNION
# MAGIC SELECT *
# MAGIC FROM silver.tbl_ligue1
# MAGIC UNION
# MAGIC SELECT *
# MAGIC FROM silver.tbl_bundesliga
# MAGIC UNION
# MAGIC SELECT *
# MAGIC FROM silver.tbl_liga1
# MAGIC UNION
# MAGIC SELECT *
# MAGIC FROM silver.tbl_seriea

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE OR REPLACE TABLE gold.fact_matches AS
# MAGIC SELECT
# MAGIC     ROW_NUMBER() OVER (ORDER BY FULL_DATE,B.ID_TEAM) AS ID_MATCH,
# MAGIC     DATE AS MATCH_DATE,
# MAGIC     CAST(date_format(DATE,'yyyyMMdd') AS INT) AS ID_DATE,
# MAGIC     TIME AS MATCH_TIME,
# MAGIC     CAST(date_format(TIME, 'HHmm') AS INT) AS ID_TIME,
# MAGIC     FULL_DATE AS MATCH_DATETIME,
# MAGIC     CAST(date_format(FULL_DATE, 'yyyyMMddHHmmss') AS BIGINT) AS ID_DATETIME,
# MAGIC     B.ID_TEAM AS ID_HOME_TEAM,
# MAGIC     C.ID_TEAM AS ID_AWAY_TEAM,
# MAGIC     D.ID_LEAGUE AS ID_LEAGUE,
# MAGIC     FTHG AS FULL_TIME_HOME_GOAL,
# MAGIC     FTAG AS FULL_TIME_AWAY_GOAL,
# MAGIC     FTR AS FULL_TIME_RESULT,
# MAGIC     HTHG AS HALFTIME_HOME_GOAL,
# MAGIC     HTAG AS HALFTIME_AWAY_GOAL,
# MAGIC     HTR AS HALFTIME_RESULT,
# MAGIC     HS AS HOME_SHOTS,
# MAGIC     AS AS AWAY_SHOTS,
# MAGIC     HST AS HOME_SHOTS_ON_TARGET,
# MAGIC     AST AS AWAY_SHOTS_ON_TARGET,
# MAGIC     HF AS HOME_FOULS,
# MAGIC     AF AS AWAY_FOULS,
# MAGIC     HC AS HOME_CORNERS,
# MAGIC     AC AS AWAY_CORNERS,
# MAGIC     HY AS HOME_YELLOW,
# MAGIC     AY AS AWAY_YELLOW,
# MAGIC     HR AS HOME_RED,
# MAGIC     AR AS AWAY_RED
# MAGIC FROM VW_GOLD_FACT A
# MAGIC LEFT JOIN gold.dim_teams B ON A.HOME_TEAM = B.TEAM_NAME
# MAGIC LEFT JOIN gold.dim_teams C ON A.AWAY_TEAM = C.TEAM_NAME
# MAGIC LEFT JOIN gold.dim_leagues D ON A.LEAGUE_NAME = D.LEAGUE_NAME;