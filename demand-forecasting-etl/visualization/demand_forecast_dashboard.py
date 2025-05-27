import streamlit as st
import pandas as pd
import plotly.express as px
from datetime import datetime, timedelta
import os
import time
from sqlalchemy import create_engine
from sqlalchemy.exc import SQLAlchemyError
from dotenv import load_dotenv


# Configure Streamlit page
st.set_page_config(
    page_title="Demand Forecasting Dashboard for Sales Planning",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

load_dotenv()

# Cache the database connection
@st.cache_resource
def get_redshift_connection():
    """Create connection to Redshift with proper error handling"""
    try:
        host = os.getenv("REDSHIFT_HOST")
        port = 5439
        user = os.getenv("REDSHIFT_USER")
        password = os.getenv("REDSHIFT_PASSWORD")
        database = os.getenv("REDSHIFT_DB")
        
        if not all([host, user, password, database]):
            try:
                host = st.secrets["REDSHIFT_HOST"]
                user = st.secrets["REDSHIFT_USER"]
                password = st.secrets["REDSHIFT_PASSWORD"]
                database = st.secrets["REDSHIFT_DB"]
            except Exception:
                pass
        
        if not all([host, user, password, database]):
            st.error("❌ Missing required Redshift connection parameters")
            st.info("Please set the following environment variables or add them to Streamlit secrets:")
            st.code("""
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_DB=your_database_name
            """)
            return None
            
        connection_string = f"redshift+psycopg2://{user}:{password}@{host}:{port}/{database}"
        engine = create_engine(
            connection_string,
            pool_size=5,
            max_overflow=10,
            pool_timeout=30,
            pool_recycle=3600
        )
        return engine
    except Exception as e:
        st.error(f"Failed to create database connection: {str(e)}")
        return None

# Cache data with TTL
@st.cache_data(ttl=30)
def load_sales_data(hours_back=24):
    engine = get_redshift_connection()
    if engine is None:
        return pd.DataFrame()

    query = f"""
        SELECT 
            sale_id,
            channel,
            customer_name,
            customer_email,
            product_category,
            quantity,
            unit_price,
            total_price,
            payment_method,
            event_datetime,
            country,
            city,
            created_at
        FROM demand_forecast 
        WHERE event_datetime >= CURRENT_TIMESTAMP - INTERVAL '{hours_back} hours'
        ORDER BY event_datetime DESC
        LIMIT 50
    """

    try:
        with engine.connect() as conn:
            # Get the underlying raw DBAPI connection from the SQLAlchemy connection
            # This raw_connection object will have a .cursor() method
            raw_connection = conn.connection 
            df = pd.read_sql(query, raw_connection) 
        return df
    except Exception as e:
        st.error(f"Unexpected error loading sales data: {str(e)}")
        return pd.DataFrame()


@st.cache_data(ttl=30)
def get_real_time_metrics():
    """Get real-time metrics with optimized query execution"""
    engine = get_redshift_connection()
    if engine is None:
        return {
            'total_sales_today': 0,
            'total_orders_today': 0,
            'avg_order_value': 0,
            'top_channel': 'N/A'
        }
    
    metrics = {}
    queries = {
        'total_sales_today': (
            "SELECT COALESCE(SUM(total_price), 0) as value"
            " FROM demand_forecast "
            "WHERE DATE(event_datetime) = CURRENT_DATE"
        ),
        'total_orders_today': (
            "SELECT COUNT(*) as value"
            " FROM demand_forecast "
            "WHERE DATE(event_datetime) = CURRENT_DATE"
        ),
        'avg_order_value': (
            "SELECT COALESCE(AVG(total_price), 0) as value"
            " FROM demand_forecast "
            "WHERE DATE(event_datetime) = CURRENT_DATE"
        ),
        'top_channel': (
            "SELECT channel as value"
            " FROM demand_forecast "
            "WHERE DATE(event_datetime) = CURRENT_DATE"
            " GROUP BY channel"
            " ORDER BY SUM(total_price) DESC"
            " LIMIT 1"
        )
    }
    
    try:
        with engine.connect() as conn:
            for key, query in queries.items():
                result = conn.execute(query)
                metrics[key] = result.scalar() or 0
                
                # Special handling for top_channel
                if key == 'top_channel' and metrics[key] == 0:
                    metrics[key] = 'N/A'
                    
    except SQLAlchemyError as e:
        st.error(f"Database error fetching metrics: {str(e)}")
        return {
            'total_sales_today': 0,
            'total_orders_today': 0,
            'avg_order_value': 0,
            'top_channel': 'N/A'
        }
    
    return metrics

def format_currency(value):
    """Helper function to format currency values"""
    return f"${value:,.2f}" if isinstance(value, (int, float)) else str(value)

def main():
    st.title("🚀 Demand Forecasting Dashboard for Sales Planning")
    st.markdown("Live streaming data from Kafka → Redshift")
    
    # Check if connection parameters are available
    # We check environment variables, as st.secrets are loaded by get_redshift_connection
    if not (os.getenv("REDSHIFT_HOST") and os.getenv("REDSHIFT_USER") and 
            os.getenv("REDSHIFT_PASSWORD") and os.getenv("REDSHIFT_DB")):
        
        # Show configuration section
        st.error("⚠️ Database Connection Required")
        with st.expander("🔧 Database Configuration", expanded=True):
            st.markdown("**Set these environment variables:**")
            st.markdown("**Or create a `.env` file:**")
            st.code("""
REDSHIFT_HOST=your-cluster.region.redshift.amazonaws.com
REDSHIFT_USER=your_username
REDSHIFT_PASSWORD=your_password
REDSHIFT_DB=your_database_name
            """ )
            st.warning("If deploying to Streamlit Cloud, use `secrets.toml` in your `.streamlit` directory instead of `.env`.")
        return
    
    # Sidebar controls
    st.sidebar.header("Dashboard Controls")
    auto_refresh = st.sidebar.checkbox("Auto Refresh (30s)", value=True)
    hours_filter = st.sidebar.slider("Hours to show", 1, 168, 24)
    timezone = st.sidebar.selectbox("Timezone", ["UTC", "Local"], index=0) # Currently not implemented for conversion
    
    # Load data with progress indicator
    with st.spinner("Loading real-time data..."):
        df = load_sales_data(hours_filter)
        metrics = get_real_time_metrics()
    
    if df.empty:
        st.warning("No data available. Please check:")
        st.markdown("- Database connection settings (host, user, password, database)")
        st.markdown("- Redshift security group rules (inbound access)")
        st.markdown("- Streaming pipeline status (is data flowing to `demand_forecast`?)")
        st.markdown("- Table permissions (does your user have `SELECT` on `demand_forecast`?)")
        st.markdown("- The `hours_to_show` filter (try increasing it)")
        return
    
    # Real-time metrics
    st.header("📈 Real-time Metrics")
    col1, col2, col3, col4 = st.columns(4)
    
    with col1:
        st.metric(
            label="💰 Total Sales Today",
            value=format_currency(metrics['total_sales_today'])
        )
    
    with col2:
        st.metric(
            label="📦 Orders Today",
            value=f"{int(metrics['total_orders_today']):,}"
        )
    
    with col3:
        st.metric(
            label="🛒 Avg Order Value",
            value=format_currency(metrics['avg_order_value'])
        )
    
    with col4:
        st.metric(
            label="🏆 Top Channel",
            value=str(metrics['top_channel'])
        )
    
    # Charts Section
    st.header("📊 Analytics")
    
    # Time-based charts
    if not df.empty:
        # Convert to datetime and localize if needed (though current timezone select doesn't apply it)
        df['event_datetime'] = pd.to_datetime(df['event_datetime'])
        if timezone == "Local":
             # This requires pytz or dateutil for proper timezone conversion
             # For simplicity, keeping it UTC as per Redshift default, but good to note
             pass 
        df['hour'] = df['event_datetime'].dt.floor('H')
        
        hourly_sales = df.groupby('hour')['total_price'].sum().reset_index()
        
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Sales Over Time")
            fig_time = px.line(
                hourly_sales, 
                x='hour', 
                y='total_price',
                title="Hourly Sales Trend",
                labels={'total_price': 'Sales ($)', 'hour': 'Time'},
                template="streamlit" # Use Streamlit's default theme for plots
            )
            fig_time.update_traces(line_color='#1f77b4', line_width=3)
            fig_time.update_layout(height=400)
            st.plotly_chart(fig_time, use_container_width=True)
        
        with col2:
            st.subheader("Sales by Channel")
            channel_sales = df.groupby('channel')['total_price'].sum().reset_index()
            fig_channel = px.pie(
                channel_sales,
                values='total_price',
                names='channel',
                title="Revenue by Channel",
                template="streamlit"
            )
            st.plotly_chart(fig_channel, use_container_width=True)
    
    # Product and Payment charts
    if not df.empty:
        col1, col2 = st.columns(2)
        with col1:
            st.subheader("Product Categories")
            # Sort categories by total sales for better visualization
            category_sales = df.groupby('product_category')['total_price'].sum().sort_values(ascending=True).reset_index()
            fig_category = px.bar(
                category_sales,
                x='total_price',
                y='product_category',
                orientation='h',
                title="Sales by Product Category",
                labels={'total_price': 'Sales ($)', 'product_category': 'Category'},
                template="streamlit"
            )
            st.plotly_chart(fig_category, use_container_width=True)
        
        with col2:
            st.subheader("Payment Methods")
            payment_dist = df['payment_method'].value_counts().reset_index()
            payment_dist.columns = ['payment_method', 'count'] # Rename columns for clarity
            fig_payment = px.bar(
                payment_dist,
                x='payment_method',
                y='count',
                title="Payment Method Distribution",
                labels={'payment_method': 'Payment Method', 'count': 'Number of Transactions'},
                template="streamlit"
            )
            st.plotly_chart(fig_payment, use_container_width=True)
    
    # Geographic distribution
    if not df.empty:
        st.subheader("🌍 Geographic Distribution")
        geo_sales = df.groupby('country')['total_price'].sum().sort_values(ascending=False).head(10).reset_index()
        fig_geo = px.bar(
            geo_sales,
            x='total_price',
            y='country',
            orientation='h',
            title="Top 10 Countries by Sales",
            labels={'total_price': 'Sales ($)', 'country': 'Country'},
            template="streamlit"
        )
        st.plotly_chart(fig_geo, use_container_width=True)
    
    # Recent transactions
    st.header("🔄 Recent Transactions")
    if not df.empty:
        recent_df = df.head(20)[['event_datetime', 'channel', 'customer_name', 
                                'quantity', 'total_price', 'country']].copy()
        recent_df['total_price'] = recent_df['total_price'].apply(format_currency)
        
        # Format datetime for display
        recent_df['event_datetime'] = recent_df['event_datetime'].dt.strftime('%Y-%m-%d %H:%M:%S')

        st.dataframe(
            recent_df,
            use_container_width=True,
            hide_index=True,
            column_config={
                "event_datetime": "Time",
                "channel": "Channel", 
                "customer_name": "Customer",
                "quantity": "Qty",
                "total_price": "Total",
                "country": "Country"
            }
        )
    
    # Footer
    st.sidebar.markdown("---")
    st.sidebar.markdown(f"**Last Updated:** {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    st.sidebar.markdown(f"**Records Shown:** {len(df):,}")
    
    # Auto-refresh mechanism
    if auto_refresh:
        time.sleep(30)
        st.rerun()

if __name__ == "__main__":
    main()