import streamlit as st
import pandas as pd
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

# Initialize Spark session
spark = SparkSession.builder.appName("Stock Data Analysis").getOrCreate()

# Function to read data
@st.cache_data
def read_data(file_path):
    # Define schema
    schema = "Date DATE, Fermeture FLOAT, Ouverture FLOAT, Max FLOAT, Min FLOAT"
    # Load data
    df = spark.read.csv(file_path, header=True, schema=schema, dateFormat="MM/dd/yyyy")
    return df

# Function to calculate average opening and closing prices
def calculate_averages(df, period):
    df = df.withColumn("Year", F.year("Date"))
    if period == "Monthly":
        df = df.withColumn("Month", F.month("Date"))
        grouped = df.groupBy("Year", "Month").agg(
            F.avg("Ouverture").alias("Avg_Ouverture"),
            F.avg("Fermeture").alias("Avg_Fermeture")
        )
    elif period == "Weekly":
        df = df.withColumn("Week", F.weekofyear("Date"))
        grouped = df.groupBy("Year", "Week").agg(
            F.avg("Ouverture").alias("Avg_Ouverture"),
            F.avg("Fermeture").alias("Avg_Fermeture")
        )
    else:  # Yearly
        grouped = df.groupBy("Year").agg(
            F.avg("Ouverture").alias("Avg_Ouverture"),
            F.avg("Fermeture").alias("Avg_Fermeture")
        )

    # Convert to Pandas for easy display in Streamlit
    return grouped.toPandas()

# Streamlit UI
st.title("Average Opening and Closing Prices by Period")

# File upload
file_path = st.file_uploader("Upload a CSV file with stock data", type=["csv"])

if file_path:
    # Read data
    df = read_data(file_path)

    # Choose period
    period = st.selectbox("Choose a period to calculate averages:", ["Weekly", "Monthly", "Yearly"])

    # Calculate and display averages
    avg_df = calculate_averages(df, period)
    st.write(f"Average Opening and Closing Prices ({period})")
    st.dataframe(avg_df)
