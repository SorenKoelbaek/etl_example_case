# Databricks notebook source
# COMMAND ----------
from pyspark.sql import SparkSession
from dependencies.api import APIClient
from pyspark.sql import Row

# COMMAND ----------
# Initialize Spark and helper functions
spark = SparkSession.builder.appName("PokemonIngestion").getOrCreate()

def save_to_table(df, table_name: str):
    try:
        existing_df = spark.read.table(table_name) #check if table exists
        print(f"Table '{table_name}' exists")
        new_rows = df

        if len(new_rows.collect()) > 0:
            print(f"Found {new_rows.count()} new Pokémon → appending")
            new_rows.write.mode("append").saveAsTable(table_name) # This seems to always be writing all pokemon? maybe a bug in the API client or the way we fetch data?
        else:
            print("No new Pokémon found → nothing to append")

    except Exception:
        #missing table, overwrite
        print(f"Overwriting table '{table_name}' (table missing or schema mismatch)")
        df.write.mode("overwrite").saveAsTable(table_name)


# COMMAND ----------
# Define target table
TARGET_TABLE_NAME = "pokemonApi.pokemon"  # Change as needed

# COMMAND ----------
# Create API client for PokéAPI
api = APIClient("https://pokeapi.co/api/v2") #for more information about the schema and response please see https://pokeapi.co/docs/v2

# Extract: fetch first 2000 Pokémon from API

data = api.get("pokemon", params={"limit": 2000})["results"]
rows = [Row(**d) for d in data]
pokemon_df = spark.createDataFrame(rows)

# COMMAND ----------
# Call the function to save our data
save_to_table(pokemon_df, TARGET_TABLE_NAME)

print("Daily ingestion complete. Pokémon data ingested into table.")
