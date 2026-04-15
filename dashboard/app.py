# modules.
import pandas as pd
import numpy as np
import time
import logging
from google.cloud import bigquery
from datetime import datetime
import streamlit as st
from dotenv import load_dotenv
import os

# load the env file.
load_dotenv()

# bigquery client.
client = bigquery.Client()

# project details.
PROJECT_ID = os.getenv('GCP_PROJECT_ID')
DATASET_ID = os.getenv('GCP_DATASET_ID')
FACT_TABLE_NAME = os.getenv('GCP_FACT_TABLE_NAME')
DIM_TABLE_NAME = os.getenv('GCP_DIM_TABLE_NAME')

FACT_TABLE_ID = f"`{PROJECT_ID}.{DATASET_ID}.{FACT_TABLE_NAME}`"
DIM_TABLE_ID = f"`{PROJECT_ID}.{DATASET_ID}.{DIM_TABLE_NAME}`"

# title on the streamlit page.
st.title("Real Estate trend Dashboard (2001 - 2024) 📊")
st.write("Real Estate Dashboard (2001-2024)")

# Fetch data from the TABLE_ID
@st.cache_data
def load_data():
    # define query.
    query = f"""
        select
            extract(year from recorded_year) as recorded_year,
            count(*) as listing_freq,
            round(avg(assessed_value), 2) as avg_assessed_value,
            round(avg(sale_amount), 2) as avg_sale_amount,
            round(avg(sales_ratio), 2) as avg_sales_ratio
        from {FACT_TABLE_ID}
        group by extract(year from recorded_year)
    """
    # return the data from bigquery to streamlit dashboard.
    return client.query(query).to_dataframe()

# take the dataframe.
df = load_data()

# visuals.
st.subheader("Yearwise distribution of average assessed value vs sale amount (2001-2024) 📈")
st.line_chart(df, x = "recorded_year", y = ["avg_assessed_value", "avg_sale_amount"])

st.subheader("yearwise sale_ratio growth (2001-2024)")
st.bar_chart(df, x = 'recorded_year', y='avg_sales_ratio', color="#ffaa00")

st.subheader("yearly listing frequency trend")
st.area_chart(df, x='recorded_year', y= 'listing_freq')

st.header("Property and residential type wise value distribution (2001 - 2024)")
# town wise sale frequency, avg assessed_value, avg sale_amount and avg sales_ratio
@st.cache_data
def property_type():
    query = f"""
        select
            d.property_type,
            d.residential_type,
            count(*) as sales_frequency,
            round(avg(f.assessed_value), 2) as avg_assessed_value,
            round(avg(f.sale_amount), 2) as avg_sale_amount,
            round(avg(sales_ratio), 2) as avg_sales_ratio
        from {FACT_TABLE_ID} as f
        join {DIM_TABLE_ID} as d on d.property_id = f.property_id
        where d.property_type != 'unspecified' and 
        d.residential_type != 'unspecified'
        group by d.property_type, d.residential_type
    """

    return client.query(query).to_dataframe()

# dataframe.
df1 = property_type()

# visual of property wise average assessed value vs. sale amount.
st.subheader("average assessed value vs. sale amount by property type")
st.bar_chart(df1, x = 'property_type', y = ['avg_assessed_value', 'avg_sale_amount'], stack = False)

# visual of residential_wise average assessed value vs. sale amount.
st.subheader("average assessed value vs. sale amount by property type")
st.bar_chart(df1, x = 'residential_type', y = ['avg_assessed_value', 'avg_sale_amount'], stack = False)

# location wise dashboard.
@st.cache_data
def town_wise_data():
    query = f"""
        select
            town,
            count(*) as listing_freq,
            round(avg(assessed_value), 2) as avg_assessed_value,
            round(avg(sale_amount), 2) as avg_sale_amount,
            round(avg(sales_ratio), 2) as avg_sales_ratio
        from {FACT_TABLE_ID}
        group by town
        order by avg_assessed_value desc, avg_sale_amount desc
        limit 10
    """

    return client.query(query).to_dataframe()

df2 = town_wise_data()

# average assessed value and sale amount trend by top 10 town.
st.subheader("average assessed value and sale amount trend by top 10 town")
st.bar_chart(df2, x = 'town', y = ['avg_assessed_value', 'avg_sale_amount'], stack=False, color= ['#fa22ab', '#ffaa00'])