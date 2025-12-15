# Databricks notebook source
# MAGIC %md
# MAGIC **Receive Table Name**

# COMMAND ----------

# Widget para o nome da pasta final
dbutils.widgets.text("final_path", "")
final_path = dbutils.widgets.get("final_path")

# COMMAND ----------

# MAGIC %md
# MAGIC **Extract from bronze**

# COMMAND ----------

df = spark.read.format("parquet").load("/mnt/bronze/" + final_path)


# COMMAND ----------

query = f"""
CREATE OR REPLACE TEMPORARY VIEW VW_TRF_{final_path}_PART_1
USING parquet
OPTIONS (
  path "/mnt/bronze/{final_path}"
)
"""

spark.sql(query)


# COMMAND ----------

query = f"""
CREATE OR REPLACE TEMPORARY VIEW VW_TRF_{final_path}_PART_2 AS
SELECT 
    to_date(`Date`, 'dd/MM/yyyy') AS DATE,
    `Time` AS TIME,
    to_timestamp(concat(`Date`, ' ', `Time`), 'dd/MM/yyyy HH:mm') AS FULL_DATE,
    `HomeTeam` AS HOME_TEAM,
    `AwayTeam` AS AWAY_TEAM,
    '{final_path}' as LEAGUE_NAME,
    try_cast(FTHG as int),
    try_cast(FTAG as int),
    FTR,
    try_cast(HTHG as int),
    try_cast(HTAG as int),
    HTR,
    try_cast(HS as int),
    try_cast(AS as int),
    try_cast(HST as int),
    try_cast(AST as int),
    try_cast(HF as int),
    try_cast(AF as int),
    try_cast(HC as int),
    try_cast(AC as int),
    try_cast(HY as int),
    try_cast(AY as int),
    try_cast(HR as int),
    try_cast(AR as int)
FROM VW_TRF_{final_path}_PART_1
"""

spark.sql(query)

# COMMAND ----------

query = f"""
CREATE OR REPLACE TEMPORARY VIEW VW_TRF_{final_path}_PART_3 AS
SELECT 
    DATE,
    TIME,
    FULL_DATE,
    HOME_TEAM,
    AWAY_TEAM,
    LEAGUE_NAME,
    FTHG,
    FTAG,
    CASE WHEN FTR IS NULL AND FTHG > FTAG THEN 'H' WHEN FTR IS NULL AND FTHG < FTAG THEN 'A' WHEN FTR IS NULL AND FTHG = FTAG THEN 'D' ELSE FTR END AS FTR,
    HTHG,
    HTAG,
    CASE WHEN HTR IS NULL AND HTHG > HTAG THEN 'H' WHEN HTR IS NULL AND HTHG < HTAG THEN 'A' WHEN HTR IS NULL AND HTHG = HTAG THEN 'D' ELSE HTR END AS HTR,
    HS,
    AS,
    HST,
    AST,
    HF,
    AF,
    HC,
    AC,
    HY,
    AY,
    HR,
    AR
FROM VW_TRF_{final_path}_PART_2
"""

spark.sql(query)

# COMMAND ----------

# MAGIC %md
# MAGIC **Transform & Load Silver**

# COMMAND ----------

database_name = "silver"
table_name = final_path

full_table_name = f"{database_name}.tbl_{table_name}"

query = f"""
CREATE OR REPLACE TABLE {full_table_name}
USING DELTA AS
SELECT 
    DATE,
    TIME,
    FULL_DATE,
    HOME_TEAM,
    AWAY_TEAM,
    LEAGUE_NAME,
    FTHG,
    FTAG,
    FTR,
    HTHG,
    HTAG,
    HTR,
    HS,
    AS,
    HST,
    AST,
    HF,
    AF,
    HC,
    AC,
    HY,
    AY,
    HR,
    AR
FROM VW_TRF_{final_path}_PART_3
"""

spark.sql(query)