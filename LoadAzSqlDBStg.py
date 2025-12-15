# Databricks notebook source
jdbc_hostname = "futtopleagues.database.windows.net"
jdbc_port = 1433
jdbc_database = "futtopleagues"
jdbc_url = f"jdbc:sqlserver://{jdbc_hostname}:{jdbc_port};databaseName={jdbc_database}"

jdbc_user = "fabs_user"
jdbc_password = "****"

# COMMAND ----------

df_fact = spark.table("gold.fact_matches")
df_dim_date = spark.table("gold.dim_date")
df_dim_teams = spark.table("gold.dim_teams")
df_dim_leagues = spark.table("gold.dim_leagues")

# COMMAND ----------

(
    df_fact.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "stg_fact_matches")
      .option("user", jdbc_user)
      .option("password", jdbc_password)
      .save()
)

# COMMAND ----------

(
    df_dim_date.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "stg_dim_date")
      .option("user", jdbc_user)
      .option("password", jdbc_password)
      .save()
)

# COMMAND ----------

(
    df_dim_teams.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "stg_dim_teams")
      .option("user", jdbc_user)
      .option("password", jdbc_password)
      .save()
)

# COMMAND ----------

(
    df_dim_leagues.write
      .mode("overwrite")
      .format("jdbc")
      .option("url", jdbc_url)
      .option("dbtable", "stg_dim_leagues")
      .option("user", jdbc_user)
      .option("password", jdbc_password)
      .save()
)