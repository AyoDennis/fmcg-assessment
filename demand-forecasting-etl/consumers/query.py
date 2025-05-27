import os
import psycopg2
from dotenv import load_dotenv
from tabulate import tabulate  # For pretty printing

# Load environment variables
load_dotenv()

def get_redshift_connection():
    """Create and return a Redshift connection"""
    return psycopg2.connect(
        host=os.getenv('REDSHIFT_HOST'),
        port=int(os.getenv('REDSHIFT_PORT')),
        database=os.getenv('REDSHIFT_DB'),
        user=os.getenv('REDSHIFT_USER'),
        password=os.getenv('REDSHIFT_PASSWORD')
    )

def query_and_print(query, max_rows=20):
    """Execute query and print formatted results"""
    try:
        with get_redshift_connection() as conn:
            with conn.cursor() as cursor:
                cursor.execute(query)
                
                # Get column names
                columns = [desc[0] for desc in cursor.description]
                
                # Fetch limited rows
                rows = cursor.fetchmany(max_rows)
                
                # Print results
                print("\nQuery Results:")
                print(tabulate(rows, headers=columns, tablefmt="grid"))
                
                # Show row count if limited
                if len(rows) == max_rows:
                    print(f"\nShowing first {max_rows} rows. More rows available.")
                
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    # Example query - modify as needed
    sample_query = """
    DROP TABLE sales_events
    """
    
    query_and_print(sample_query)