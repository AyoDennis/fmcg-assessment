import os
from io import StringIO

import boto3
import pandas as pd
import streamlit as st
from dotenv import load_dotenv

# --- Streamlit App ---
st.set_page_config(layout="wide", page_title="Supplier Raw Material Procurement Dashboard")
st.title("Supplier Raw Material Procurement Dashboard")
st.markdown("---")

load_dotenv()

try:
    aws_access_key_id = os.getenv("AWS_ACCESS_KEY_ID")
    aws_secret_access_key = os.getenv("AWS_SECRET_ACCESS_KEY")
    aws_region = os.getenv("AWS_DEFAULT_REGION")
except KeyError:
    st.error("AWS credentials not found in Streamlit secrets.")
    st.stop()

s3_bucket_name = "fmcg-de-assessment"
s3_csv_key = "supplier_data/merged_supplier_data.csv"


@st.cache_data
def load_data_from_s3(
    bucket_name, key, aws_access_key_id, aws_secret_access_key, aws_region
    ):
    """This function initializes boto3"""
    try:
        s3 = boto3.client(
            's3',
            aws_access_key_id=aws_access_key_id,
            aws_secret_access_key=aws_secret_access_key,
            region_name=aws_region
        )
        obj = s3.get_object(Bucket=bucket_name, Key=key)
        data = obj['Body'].read()
        
        df = pd.read_csv(StringIO(data.decode('utf-8')))

        
        date_cols = ['last_audit_date', 'last_inspection', 'last_order_date']
        for col in date_cols:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col], errors='coerce') 

        
        if 'supplier_uuid.1' in df.columns and 'supplier_uuid.2' in df.columns:
            
            df.rename(columns={
                'supplier_uuid.1': 'status_uuid', 
                'supplier_uuid.2': 'order_uuid' 
            }, inplace=True)
            
        return df
    except Exception as e:
        st.error(f"Error loading data from S3. Please check your bucket name, key, and AWS credentials. Details: {e}")
        return None


df = load_data_from_s3(
    s3_bucket_name, s3_csv_key, aws_access_key_id, \
    aws_secret_access_key, aws_region
    )

if df is not None:
    st.subheader(f"Data Loaded from s3://{s3_bucket_name}/{s3_csv_key}")
    st.dataframe(df)

    st.markdown("---")
    st.subheader("Data Overview")
    st.write(f"Number of rows: **{len(df)}**")
    st.write(f"Number of columns: **{len(df.columns)}**")


    st.write("#### Descriptive Statistics for Numeric Columns")
    st.write(df.describe().T)

    
    st.write("#### Value Counts for Key Categorical Columns")
    col1, col2, col3 = st.columns(3)
    with col1:
        st.write("##### Country Distribution")
        st.dataframe(df['country'].value_counts())
    with col2:
        st.write("##### Status Distribution")
        st.dataframe(df['status'].value_counts())
    with col3:
        st.write("##### Category Distribution")
        st.dataframe(df['category'].value_counts())


    st.markdown("---")
    st.subheader("Interactive Visualizations")

    
    st.sidebar.header("Filter Data")
    selected_country = st.sidebar.multiselect(
        "Select Country(s)", options=df['country'].unique(), \
            default=df['country'].unique())
    selected_status = st.sidebar.multiselect(
        "Select Status(es)", options=df['status'].unique(), \
              default=df['status'].unique())
    selected_category = st.sidebar.multiselect(
        "Select Category(s)", options=df['category'].unique(), \
            default=df['category'].unique())

    filtered_df = df[
        (df['country'].isin(selected_country)) &
        (df['status'].isin(selected_status)) &
        (df['category'].isin(selected_category))
    ]

    st.write(f"Displaying **{len(filtered_df)}** rows after filter.")

    # Get numeric and date columns for plotting
    numeric_cols = filtered_df.select_dtypes(
        include=['number']).columns.tolist()
    date_cols = filtered_df.select_dtypes(
        include=['datetime64[ns]']).columns.tolist()
    all_cols = filtered_df.columns.tolist()

    if not filtered_df.empty:
        # --- Scatter Plot ---
        st.write("#### Scatter Plot")
        col_x, col_y, col_color = st.columns(3)
        with col_x:
            x_axis = st.selectbox("X-axis:", numeric_cols, index=numeric_cols.index('performance_score') if 'performance_score' in numeric_cols else 0)
        with col_y:
            y_axis = st.selectbox("Y-axis:", numeric_cols, index=numeric_cols.index('compliance_score') if 'compliance_score' in numeric_cols else (1 if len(numeric_cols) > 1 else 0))
        with col_color:
            color_by = st.selectbox("Color by:", ['None'] + [col for col in all_cols if filtered_df[col].nunique() < 20]) # Limit color by to columns with fewer unique values for readability

        if x_axis and y_axis:
            if color_by != 'None':
                st.scatter_chart(filtered_df, x=x_axis, y=y_axis, color=color_by)
            else:
                st.scatter_chart(filtered_df, x=x_axis, y=y_axis)
        else:
            st.info("Please select X and Y axes for the scatter plot.")

        # --- Bar Chart for Categorical Data ---
        st.write("#### Bar Chart (Count by Category)")
        bar_chart_col = st.selectbox("Select column for Bar Chart", [col for col in all_cols if filtered_df[col].nunique() < 50 and filtered_df[col].dtype == 'object'])
        if bar_chart_col:
            st.bar_chart(filtered_df[bar_chart_col].value_counts())

        # --- Line Chart for Time Series Data ---
        if date_cols:
            st.write("#### Line Chart (Over Time)")
            time_col = st.selectbox("Select Date Column for Time Series", date_cols)
            numeric_for_time_series = st.selectbox("Select Metric for Time Series", numeric_cols)

            if time_col and numeric_for_time_series:
                # Group by the date column and sum the numeric column
                
                time_series_data = filtered_df.set_index(time_col)[numeric_for_time_series].resample('M').mean().reset_index()
                st.line_chart(time_series_data, x=time_col, y=numeric_for_time_series)
            else:
                st.info("Select date column and numeric metric for time series chart.")
        else:
            st.info("No date columns found for time series charting.")

    else:
        st.warning("No data matches the selected filters.")

else:
    st.warning("Could not load data. Check configuration")
