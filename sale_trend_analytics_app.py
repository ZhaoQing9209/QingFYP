import streamlit as st
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import plotly.express as px
import plotly.graph_objects as go
from reportlab.lib.pagesizes import letter
from reportlab.lib.styles import getSampleStyleSheet, ParagraphStyle
from reportlab.lib.units import inch
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Table, TableStyle
from reportlab.lib import colors
from reportlab.lib.enums import TA_CENTER, TA_LEFT
from io import BytesIO
import random
from sqlalchemy import create_engine, text

# --- 1. App Configuration ---
st.set_page_config(
    page_title="Retail Sales Trend Analysis",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# Define required columns aligned with database schema
REQUIRED_FIELDS = {
    'SalesDate': 'SalesDate',
    'ProductID': 'ProductID',
    'ProductName': 'ProductName',
    'CategoryID': 'CategoryID', 
    'CategoryName': 'CategoryName',
    'UnitSold': 'UnitSold',
    'Price': 'Price',
    'Revenue': 'Revenue',
    'CustomerID': 'CustomerID',
    'CustomerName': 'CustomerName',
    'Email': 'Email',
    'PhoneNumber': 'PhoneNumber',
    'Age': 'Age'
}

# --- 2. Database Helpers ---
def get_db_connection_str():
    """
    Constructs DB connection string from secrets.
    Expected secrets format in secrets.toml:
    [mysql]
    host = "localhost"
    port = 3306
    database = "qingfyp"
    username = "root"
    password = "Qing3465$11"
    """
    try:
        if "mysql" in st.secrets:
            creds = st.secrets["mysql"]
            return f"mysql+pymysql://{creds['username']}:{creds['password']}@{creds['host']}:{creds['port']}/{creds['database']}"
        return None
    except Exception:
        return None

# --- 3. Data Processing Class ---
class DataProcessor:
    def __init__(self):
        # Use database schema aligned internal names
        self.required_internal_columns = [
            'SalesDate', 'ProductID', 'ProductName', 'CategoryID', 'CategoryName',
            'UnitSold', 'Price', 'Revenue', 'CustomerID', 'CustomerName', 'Email', 
            'PhoneNumber', 'Age'
        ]
    
    @st.cache_data(ttl=3600)
    def load_data(_self, file):
        """Loads data from the uploaded file, handling various encodings."""
        try:
            if file.name.endswith('.csv'):
                # Try reading with default UTF-8 first
                try:
                    df = pd.read_csv(file)
                except UnicodeDecodeError:
                    # If UTF-8 fails, try resetting file pointer and using common fallback encodings
                    file.seek(0)
                    try:
                        df = pd.read_csv(file, encoding='ISO-8859-1')
                    except UnicodeDecodeError:
                        file.seek(0)
                        df = pd.read_csv(file, encoding='cp1252')
                        
            elif file.name.endswith(('.xlsx', '.xls')):
                df = pd.read_excel(file)
            else:
                raise ValueError("Unsupported file format. Please upload CSV or Excel file.")
            
            # Convert all column names to strings and strip whitespace
            df.columns = df.columns.astype(str).str.strip()
            
            return df
            
        except Exception as e:
            raise Exception(f"Error loading file: {str(e)}")

    @st.cache_data(ttl=3600)
    def clean_data(_self, df: pd.DataFrame, column_mapping: dict):
        """Cleans and processes data based on user-provided column mapping."""
        df_clean = df.copy()
        
        # 1. Rename columns to standard internal names
        reverse_mapping = {user_col: internal_col for internal_col, user_col in column_mapping.items()}
        df_clean = df_clean.rename(columns=reverse_mapping)
        
        # Ensure all required internal columns are now present
        for col in _self.required_internal_columns:
            if col not in df_clean.columns:
                 # Set default values for missing columns
                 if col in ['CustomerID', 'Age', 'CategoryID', 'ProductID']:
                     df_clean[col] = 0
                 elif col in ['Email', 'PhoneNumber', 'CustomerName']:
                     df_clean[col] = 'N/A'
                 elif col == 'Revenue':
                     # Calculate revenue if not provided
                     if 'UnitSold' in df_clean.columns and 'Price' in df_clean.columns:
                         df_clean[col] = df_clean['UnitSold'] * df_clean['Price']
        
        # 2. Type Conversion and Validation
        date_col = 'SalesDate'
        # Coerce dates, handling multiple formats
        df_clean[date_col] = pd.to_datetime(df_clean[date_col], errors='coerce')
        df_clean = df_clean.dropna(subset=[date_col])
        
        numeric_cols = ['UnitSold', 'Price', 'Revenue', 'CustomerID', 'Age', 'CategoryID', 'ProductID']
        for col in numeric_cols:
            if col in df_clean.columns:
                df_clean[col] = pd.to_numeric(df_clean[col], errors='coerce')
        
        df_clean = df_clean.dropna(subset=['UnitSold', 'Price'])
        
        # Filter valid numeric data
        df_clean = df_clean[df_clean['UnitSold'] > 0]
        df_clean = df_clean[df_clean['Price'] > 0]
        
        # Clean text columns
        text_cols = ['ProductName', 'CategoryName', 'CustomerName', 'Email', 'PhoneNumber']
        for col in text_cols:
            if col in df_clean.columns:
                df_clean[col] = df_clean[col].astype(str).str.strip()
        
        # 3. Feature Engineering
        # Ensure Revenue is calculated if not provided
        if 'Revenue' not in df_clean.columns or df_clean['Revenue'].isna().any():
            df_clean['Revenue'] = df_clean['UnitSold'] * df_clean['Price']
        
        # Generate missing IDs if needed
        if 'ProductID' not in df_clean.columns or df_clean['ProductID'].isna().any():
            df_clean['ProductID'] = range(1, len(df_clean) + 1)
        
        if 'CategoryID' not in df_clean.columns and 'CategoryName' in df_clean.columns:
            # Create CategoryID from CategoryName
            category_mapping = {name: idx for idx, name in enumerate(df_clean['CategoryName'].unique(), 1)}
            df_clean['CategoryID'] = df_clean['CategoryName'].map(category_mapping)
        
        if 'CustomerID' not in df_clean.columns or df_clean['CustomerID'].isna().any():
            df_clean['CustomerID'] = range(1, len(df_clean) + 1)
        
        # Add date components
        df_clean['Year'] = df_clean[date_col].dt.year
        df_clean['Month'] = df_clean[date_col].dt.month
        df_clean['Month Name'] = df_clean[date_col].dt.strftime('%B')
        df_clean['Day'] = df_clean[date_col].dt.day
        df_clean['Day of Week'] = df_clean[date_col].dt.day_name()
        df_clean['Quarter'] = df_clean[date_col].dt.quarter
        
        df_clean = df_clean.sort_values(date_col).reset_index(drop=True)
        return df_clean
    
    def save_to_mysql(self, df, table_name="product_sales"):
        """Saves the processed DataFrame to MySQL following the database schema."""
        db_str = get_db_connection_str()
        if not db_str:
            return False, "⚠️ Database credentials not configured in .streamlit/secrets.toml"
        
        try:
            engine = create_engine(db_str)
            
            # Prepare data for different tables according to schema
            # Category table
            if 'CategoryID' in df.columns and 'CategoryName' in df.columns:
                categories_df = df[['CategoryID', 'CategoryName']].drop_duplicates()
                categories_df = categories_df.rename(columns={
                    'CategoryID': 'category_id',
                    'CategoryName': 'category_name'
                })
            else:
                categories_df = pd.DataFrame()
            
            # Product table
            if all(col in df.columns for col in ['ProductID', 'ProductName', 'CategoryID', 'Price']):
                products_df = df[['ProductID', 'ProductName', 'CategoryID', 'Price']].drop_duplicates()
                products_df = products_df.rename(columns={
                    'ProductID': 'product_id',
                    'ProductName': 'product_name', 
                    'CategoryID': 'category_id',
                    'Price': 'price'
                })
                products_df['stock_quantity'] = 100  # Default value
            else:
                products_df = pd.DataFrame()
            
            # Customer table
            if all(col in df.columns for col in ['CustomerID', 'CustomerName']):
                customers_df = df[['CustomerID', 'CustomerName', 'Email', 'PhoneNumber', 'Age']].drop_duplicates()
                customers_df = customers_df.rename(columns={
                    'CustomerID': 'customer_id',
                    'CustomerName': 'customer_name',
                    'Email': 'email',
                    'PhoneNumber': 'phone_number',
                    'Age': 'age'
                })
            else:
                customers_df = pd.DataFrame()
            
            # Product_Sales table (main sales data)
            if all(col in df.columns for col in ['ProductID', 'SalesDate', 'UnitSold', 'Revenue']):
                product_sales_df = df[['ProductID', 'SalesDate', 'UnitSold', 'Revenue']].copy()
                product_sales_df = product_sales_df.rename(columns={
                    'ProductID': 'product_id',
                    'SalesDate': 'sales_date', 
                    'UnitSold': 'unit_sold',
                    'Revenue': 'revenue'
                })
                product_sales_df['product_sales_id'] = range(1, len(product_sales_df) + 1)
            else:
                product_sales_df = pd.DataFrame()
            
            # Sales table (aggregated sales)
            if 'CustomerID' in df.columns and 'SalesDate' in df.columns and 'Revenue' in df.columns:
                sales_df = df.groupby(['SalesDate', 'CustomerID']).agg({
                    'Revenue': 'sum'
                }).reset_index()
                sales_df = sales_df.rename(columns={
                    'SalesDate': 'sales_date',
                    'CustomerID': 'customer_id', 
                    'Revenue': 'total_amount'
                })
                sales_df['sales_id'] = range(1, len(sales_df) + 1)
            else:
                sales_df = pd.DataFrame()
            
            # Save to respective tables
            try:
                if not categories_df.empty:
                    categories_df.to_sql('category', con=engine, if_exists='append', index=False)
                
                if not products_df.empty:
                    products_df.to_sql('product', con=engine, if_exists='append', index=False)
                
                if not customers_df.empty:
                    customers_df.to_sql('customer', con=engine, if_exists='append', index=False)
                
                if not product_sales_df.empty:
                    product_sales_df.to_sql('product_sales', con=engine, if_exists='append', index=False)
                
                if not sales_df.empty:
                    sales_df.to_sql('sales', con=engine, if_exists='append', index=False)
                
                return True, f"Successfully saved data to normalized tables"
            except Exception as e:
                return False, f"Database Error: {str(e)}"
                
        except Exception as e:
            return False, f"Database Connection Error: {str(e)}"

    def fetch_latest_records(self, limit=5, table_name="product_sales"):
        """Fetches latest records to verify insertion."""
        db_str = get_db_connection_str()
        if not db_str:
            return None, "Database not configured."
        
        try:
            engine = create_engine(db_str)
            # Join query to get comprehensive data
            query = text(f"""
                SELECT 
                    ps.product_sales_id,
                    p.product_name,
                    c.category_name,
                    ps.sales_date,
                    ps.unit_sold,
                    ps.revenue,
                    cust.customer_name
                FROM {table_name} ps
                JOIN product p ON ps.product_id = p.product_id
                JOIN category c ON p.category_id = c.category_id
                LEFT JOIN sales s ON ps.sales_date = s.sales_date
                LEFT JOIN customer cust ON s.customer_id = cust.customer_id
                ORDER BY ps.sales_date DESC 
                LIMIT :limit
            """)
            
            with engine.connect() as conn:
                try:
                    df = pd.read_sql(query, conn, params={"limit": limit})
                    return df, None
                except Exception as e:
                    return None, f"Query failed (Table might not exist yet): {str(e)}"
        except Exception as e:
            return None, str(e)

@st.cache_data(ttl=3600)
def generate_sample_data(num_records=1000, num_days=180):
    """Generates sample data following the database schema."""
    categories = [
        {'CategoryID': 1, 'CategoryName': 'Electronics'},
        {'CategoryID': 2, 'CategoryName': 'Clothing'},
        {'CategoryID': 3, 'CategoryName': 'Home & Kitchen'},
        {'CategoryID': 4, 'CategoryName': 'Sports & Outdoors'},
        {'CategoryID': 5, 'CategoryName': 'Books'},
        {'CategoryID': 6, 'CategoryName': 'Toys & Games'},
        {'CategoryID': 7, 'CategoryName': 'Health & Beauty'},
        {'CategoryID': 8, 'CategoryName': 'Automotive'},
        {'CategoryID': 9, 'CategoryName': 'Garden & Tools'},
        {'CategoryID': 10, 'CategoryName': 'Office Products'}
    ]
    
    products = {
        1: [
            {'ProductID': 101, 'ProductName': 'Wireless Headphones', 'Price': 89.99},
            {'ProductID': 102, 'ProductName': 'Smart Watch', 'Price': 199.99},
            {'ProductID': 103, 'ProductName': 'USB Cable', 'Price': 15.99},
            {'ProductID': 104, 'ProductName': 'Bluetooth Speaker', 'Price': 59.99},
            {'ProductID': 105, 'ProductName': 'Phone Case', 'Price': 24.99},
            {'ProductID': 106, 'ProductName': 'Laptop Stand', 'Price': 45.99}
        ],
        2: [
            {'ProductID': 201, 'ProductName': 'T-Shirt', 'Price': 19.99},
            {'ProductID': 202, 'ProductName': 'Jeans', 'Price': 49.99},
            {'ProductID': 203, 'ProductName': 'Hoodie', 'Price': 39.99},
            {'ProductID': 204, 'ProductName': 'Sneakers', 'Price': 79.99},
            {'ProductID': 205, 'ProductName': 'Dress', 'Price': 59.99},
            {'ProductID': 206, 'ProductName': 'Jacket', 'Price': 89.99}
        ],
        3: [
            {'ProductID': 301, 'ProductName': 'Coffee Maker', 'Price': 89.99},
            {'ProductID': 302, 'ProductName': 'Blender', 'Price': 49.99},
            {'ProductID': 303, 'ProductName': 'Cookware Set', 'Price': 129.99},
            {'ProductID': 304, 'ProductName': 'Dinnerware', 'Price': 69.99},
            {'ProductID': 305, 'ProductName': 'Kitchen Knife', 'Price': 29.99},
            {'ProductID': 306, 'ProductName': 'Cutting Board', 'Price': 19.99}
        ],
        4: [
            {'ProductID': 401, 'ProductName': 'Yoga Mat', 'Price': 29.99},
            {'ProductID': 402, 'ProductName': 'Dumbbells', 'Price': 49.99},
            {'ProductID': 403, 'ProductName': 'Water Bottle', 'Price': 24.99},
            {'ProductID': 404, 'ProductName': 'Running Shoes', 'Price': 89.99},
            {'ProductID': 405, 'ProductName': 'Fitness Tracker', 'Price': 79.99},
            {'ProductID': 406, 'ProductName': 'Bicycle', 'Price': 299.99}
        ],
        5: [
            {'ProductID': 501, 'ProductName': 'Fiction Novel', 'Price': 14.99},
            {'ProductID': 502, 'ProductName': 'Non-Fiction Book', 'Price': 19.99},
            {'ProductID': 503, 'ProductName': 'Cookbook', 'Price': 24.99},
            {'ProductID': 504, 'ProductName': 'Self-Help Book', 'Price': 16.99},
            {'ProductID': 505, 'ProductName': 'Biography', 'Price': 21.99},
            {'ProductID': 506, 'ProductName': 'Textbook', 'Price': 89.99}
        ]
    }
    
    customers = [
        {'CustomerID': 1, 'CustomerName': 'John Smith', 'Email': 'john@email.com', 'PhoneNumber': '555-0101', 'Age': 35},
        {'CustomerID': 2, 'CustomerName': 'Jane Doe', 'Email': 'jane@email.com', 'PhoneNumber': '555-0102', 'Age': 28},
        {'CustomerID': 3, 'CustomerName': 'Bob Johnson', 'Email': 'bob@email.com', 'PhoneNumber': '555-0103', 'Age': 42},
        {'CustomerID': 4, 'CustomerName': 'Alice Brown', 'Email': 'alice@email.com', 'PhoneNumber': '555-0104', 'Age': 31},
        {'CustomerID': 5, 'CustomerName': 'Charlie Wilson', 'Email': 'charlie@email.com', 'PhoneNumber': '555-0105', 'Age': 45},
        {'CustomerID': 6, 'CustomerName': 'Diana Lee', 'Email': 'diana@email.com', 'PhoneNumber': '555-0106', 'Age': 29},
        {'CustomerID': 7, 'CustomerName': 'Edward Garcia', 'Email': 'edward@email.com', 'PhoneNumber': '555-0107', 'Age': 38},
        {'CustomerID': 8, 'CustomerName': 'Fiona Chen', 'Email': 'fiona@email.com', 'PhoneNumber': '555-0108', 'Age': 33}
    ]
    
    end_date = datetime.now()
    start_date = end_date - timedelta(days=num_days)
    data = []
    
    for _ in range(num_records):
        category = random.choice(categories)
        category_id = category['CategoryID']
        category_name = category['CategoryName']
        
        product = random.choice(products[category_id])
        product_id = product['ProductID']
        product_name = product['ProductName']
        base_price = product['Price']
        
        customer = random.choice(customers)
        customer_id = customer['CustomerID']
        customer_name = customer['CustomerName']
        email = customer['Email']
        phone = customer['PhoneNumber']
        age = customer['Age']
        
        random_days = random.randint(0, num_days - 1)
        sales_date = start_date + timedelta(days=random_days)
        
        # Seasonal adjustments
        day_of_week = sales_date.weekday()
        quantity_multiplier = 1.3 if day_of_week >= 5 else 1.0
        
        month = sales_date.month
        if month in [11, 12]:  # Holiday season
            quantity_multiplier *= 1.5
        elif month in [6, 7]:  # Summer
            quantity_multiplier *= 1.2
        
        # Price variation
        price_variation = random.uniform(0.8, 1.2)
        price = round(base_price * price_variation, 2)
        
        base_quantity = random.choices([1, 2, 3, 4, 5], weights=[50, 25, 15, 7, 3], k=1)[0]
        unit_sold = max(1, int(base_quantity * quantity_multiplier))
        
        revenue = round(unit_sold * price, 2)
        
        data.append({
            'SalesDate': sales_date.strftime('%Y-%m-%d'),
            'ProductID': product_id,
            'ProductName': product_name,
            'CategoryID': category_id,
            'CategoryName': category_name,
            'UnitSold': unit_sold,
            'Price': price,
            'Revenue': revenue,
            'CustomerID': customer_id,
            'CustomerName': customer_name,
            'Email': email,
            'PhoneNumber': phone,
            'Age': age
        })
    
    df = pd.DataFrame(data)
    
    # Store the mapping used for sample data generation
    st.session_state['sample_mapping'] = {
        'SalesDate': 'SalesDate',
        'ProductID': 'ProductID',
        'ProductName': 'ProductName',
        'CategoryID': 'CategoryID',
        'CategoryName': 'CategoryName',
        'UnitSold': 'UnitSold',
        'Price': 'Price',
        'Revenue': 'Revenue',
        'CustomerID': 'CustomerID',
        'CustomerName': 'CustomerName',
        'Email': 'Email',
        'PhoneNumber': 'PhoneNumber',
        'Age': 'Age'
    }
    
    return df.sort_values('SalesDate').reset_index(drop=True)

# --- 4. Analytics Class ---
class SalesAnalytics:
    def __init__(self, df):
        self.df = df
    
    @st.cache_data(ttl=3600)
    def get_top_products(_self, n=10):
        top_products = _self.df.groupby('ProductName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum'
        }).sort_values('Revenue', ascending=False).head(n)
        
        top_products['Revenue'] = top_products['Revenue'].apply(lambda x: f"${x:,.2f}")
        top_products = top_products.reset_index()
        return top_products
    
    @st.cache_data(ttl=3600)
    def get_bottom_products(_self, n=10):
        """Get bottom performing products by revenue"""
        bottom_products = _self.df.groupby('ProductName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum'
        }).sort_values('Revenue', ascending=True).head(n)
        
        bottom_products['Revenue'] = bottom_products['Revenue'].apply(lambda x: f"${x:,.2f}")
        bottom_products = bottom_products.reset_index()
        return bottom_products
    
    @st.cache_data(ttl=3600)
    def get_top_products_by_quantity(_self, n=10):
        top_products = _self.df.groupby('ProductName').agg({
            'UnitSold': 'sum',
            'Revenue': 'sum'
        }).sort_values('UnitSold', ascending=False).head(n)
        
        top_products['Revenue'] = top_products['Revenue'].apply(lambda x: f"${x:,.2f}")
        top_products = top_products.reset_index()
        return top_products
    
    @st.cache_data(ttl=3600)
    def get_bottom_products_by_quantity(_self, n=10):
        """Get bottom performing products by quantity sold"""
        bottom_products = _self.df.groupby('ProductName').agg({
            'UnitSold': 'sum',
            'Revenue': 'sum'
        }).sort_values('UnitSold', ascending=True).head(n)
        
        bottom_products['Revenue'] = bottom_products['Revenue'].apply(lambda x: f"${x:,.2f}")
        bottom_products = bottom_products.reset_index()
        return bottom_products
    
    @st.cache_data(ttl=3600)
    def get_category_statistics(_self):
        category_stats = _self.df.groupby('CategoryName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'ProductName': 'nunique'
        }).sort_values('Revenue', ascending=False)
        
        category_stats.columns = ['Total Revenue', 'Units Sold', 'Unique Products']
        category_stats['Total Revenue'] = category_stats['Total Revenue'].apply(lambda x: f"${x:,.2f}")
        category_stats = category_stats.reset_index()
        return category_stats
    
    @st.cache_data(ttl=3600)
    def get_customer_statistics(_self):
        if 'CustomerName' in _self.df.columns:
            customer_stats = _self.df.groupby('CustomerName').agg({
                'Revenue': 'sum',
                'UnitSold': 'sum',
                'SalesDate': 'count'
            }).sort_values('Revenue', ascending=False)
            
            customer_stats.columns = ['Total Revenue', 'Units Purchased', 'Number of Orders']
            customer_stats['Total Revenue'] = customer_stats['Total Revenue'].apply(lambda x: f"${x:,.2f}")
            customer_stats = customer_stats.reset_index()
            return customer_stats.head(10)
        return pd.DataFrame()
    
    @st.cache_data(ttl=3600)
    def get_daily_statistics(_self, df=None):
        if df is None:
            df = _self.df
        
        daily_stats = df.groupby('SalesDate').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'ProductName': 'count'
        }).sort_values('SalesDate', ascending=False)
        
        daily_stats.columns = ['Revenue', 'Units Sold', 'Orders']
        daily_stats['Revenue'] = daily_stats['Revenue'].apply(lambda x: f"${x:,.2f}")
        daily_stats = daily_stats.reset_index()
        daily_stats['SalesDate'] = daily_stats['SalesDate'].dt.strftime('%Y-%m-%d')
        return daily_stats.head(30)
    
    @st.cache_data(ttl=3600)
    def get_monthly_statistics(_self, df=None):
        if df is None:
            df = _self.df
        
        monthly_stats = df.groupby(['Year', 'Month', 'Month Name']).agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'ProductName': 'count'
        }).sort_values(['Year', 'Month'], ascending=False)
        
        monthly_stats.columns = ['Revenue', 'Units Sold', 'Orders']
        monthly_stats['Revenue'] = monthly_stats['Revenue'].apply(lambda x: f"${x:,.2f}")
        monthly_stats = monthly_stats.reset_index()
        monthly_stats['Period'] = monthly_stats['Month Name'] + ' ' + monthly_stats['Year'].astype(str)
        return monthly_stats[['Period', 'Revenue', 'Units Sold', 'Orders']].head(12)
    
    @st.cache_data(ttl=3600)
    def calculate_growth_rate(_self, df=None):
        if df is None:
            df = _self.df
        
        if len(df) < 2:
            return 0.0
        
        try:
            df_sorted = df.sort_values('SalesDate')
            min_date = df_sorted['SalesDate'].min()
            max_date = df_sorted['SalesDate'].max()
            mid_point = min_date + (max_date - min_date) / 2
            
            first_half = df_sorted[df_sorted['SalesDate'] <= mid_point]['Revenue'].sum()
            second_half = df_sorted[df_sorted['SalesDate'] > mid_point]['Revenue'].sum()
            
            if first_half == 0:
                return 100.0 if second_half > 0 else 0.0
            
            growth_rate = ((second_half - first_half) / first_half) * 100
            return growth_rate
        except Exception:
            return 0.0
    
    @st.cache_data(ttl=3600)
    def get_summary_statistics(_self):
        summary_data = {
            'Metric': [
                'Total Revenue',
                'Total Orders',
                'Total Units Sold',
                'Average Order Value',
                'Average Units per Order',
                'Unique Products',
                'Unique Categories',
                'Date Range (Days)',
                'Daily Average Revenue',
                'Monthly Average Revenue'
            ],
            'Value': [
                f"${_self.df['Revenue'].sum():,.2f}",
                f"{len(_self.df):,}",
                f"{_self.df['UnitSold'].sum():,}",
                f"${_self.df['Revenue'].mean():.2f}",
                f"{_self.df['UnitSold'].mean():.2f}",
                f"{_self.df['ProductName'].nunique()}",
                f"{_self.df['CategoryName'].nunique()}",
                f"{(_self.df['SalesDate'].max() - _self.df['SalesDate'].min()).days}",
                f"${_self.df.groupby('SalesDate')['Revenue'].sum().mean():,.2f}",
                f"${_self.df.groupby(['Year', 'Month'])['Revenue'].sum().mean():,.2f}"
            ]
        }
        
        # Add customer metrics if available
        if 'CustomerName' in _self.df.columns:
            summary_data['Metric'].extend(['Unique Customers', 'Average Customer Age'])
            summary_data['Value'].extend([
                f"{_self.df['CustomerName'].nunique()}",
                f"{_self.df['Age'].mean():.1f}" if 'Age' in _self.df.columns else "N/A"
            ])
        
        return pd.DataFrame(summary_data)
    
    @st.cache_data(ttl=3600)
    def get_product_performance_comparison(_self):
        """Get comprehensive product performance metrics for comparison"""
        product_performance = _self.df.groupby(['ProductName', 'CategoryName']).agg({
            'Revenue': ['sum', 'mean', 'count'],
            'UnitSold': ['sum', 'mean'],
            'Price': 'mean'
        }).round(2)
        
        # Flatten column names
        product_performance.columns = [
            'Total Revenue', 'Average Revenue', 'Number of Sales',
            'Total Units Sold', 'Average Units per Sale', 'Average Price'
        ]
        
        product_performance = product_performance.sort_values('Total Revenue', ascending=False)
        return product_performance.reset_index()
    
    @st.cache_data(ttl=3600)
    def get_seasonal_comparison(_self):
        """Compare sales performance across seasons/quarters"""
        seasonal_data = _self.df.groupby(['Year', 'Quarter']).agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'ProductName': 'nunique'
        }).reset_index()
        
        seasonal_data['Period'] = 'Q' + seasonal_data['Quarter'].astype(str) + ' ' + seasonal_data['Year'].astype(str)
        seasonal_data = seasonal_data[['Period', 'Revenue', 'UnitSold', 'ProductName']]
        seasonal_data.columns = ['Period', 'Total Revenue', 'Total Units', 'Unique Products']
        seasonal_data['Total Revenue'] = seasonal_data['Total Revenue'].apply(lambda x: f"${x:,.2f}")
        
        return seasonal_data
    
    @st.cache_data(ttl=3600)
    def get_category_comparison_details(_self):
        """Get detailed category comparison metrics"""
        category_comparison = _self.df.groupby('CategoryName').agg({
            'Revenue': ['sum', 'mean', 'count'],
            'UnitSold': ['sum', 'mean'],
            'ProductName': 'nunique',
            'Price': 'mean'
        }).round(2)
        
        category_comparison.columns = [
            'Total Revenue', 'Average Revenue per Sale', 'Number of Sales',
            'Total Units Sold', 'Average Units per Sale', 'Unique Products', 'Average Price'
        ]
        
        category_comparison = category_comparison.sort_values('Total Revenue', ascending=False)
        return category_comparison.reset_index()

# --- 5. Visualization Class ---
class SalesVisualizer:
    def __init__(self, df):
        self.df = df
    
    @st.cache_data(ttl=3600)
    def plot_sales_trend(_self):
        daily_sales = _self.df.groupby('SalesDate')['Revenue'].sum().reset_index()
        fig = px.line(
            daily_sales,
            x='SalesDate',
            y='Revenue',
            title='Sales Trend Over Time',
            labels={'Revenue': 'Revenue ($)', 'SalesDate': 'Date'}
        )
        fig.update_traces(line_color='#1f77b4', line_width=2)
        fig.update_layout(hovermode='x unified', height=400)
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_category_revenue(_self):
        category_revenue = _self.df.groupby('CategoryName')['Revenue'].sum().sort_values(ascending=True)
        fig = px.bar(
            x=category_revenue.values,
            y=category_revenue.index,
            orientation='h',
            title='Revenue by Category',
            labels={'x': 'Revenue ($)', 'y': 'Category'},
            color=category_revenue.values,
            color_continuous_scale='Blues'
        )
        fig.update_layout(showlegend=False, height=400)
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_category_pie_chart(_self):
        category_revenue = _self.df.groupby('CategoryName')['Revenue'].sum().reset_index()
        fig = px.pie(
            category_revenue,
            values='Revenue',
            names='CategoryName',
            title='Revenue Distribution by Category',
            hole=0.4
        )
        fig.update_traces(textposition='inside', textinfo='percent+label')
        fig.update_layout(height=500)
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_category_comparison(_self):
        category_stats = _self.df.groupby('CategoryName')['Revenue'].sum().sort_values(ascending=False)
        fig = px.bar(
            x=category_stats.index,
            y=category_stats.values,
            title='Category Revenue Comparison',
            labels={'x': 'Category', 'y': 'Revenue ($)'},
            color=category_stats.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(showlegend=False, height=400, xaxis_tickangle=-45)
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_category_quantity(_self):
        category_quantity = _self.df.groupby('CategoryName')['UnitSold'].sum().sort_values(ascending=False)
        fig = px.bar(
            x=category_quantity.index,
            y=category_quantity.values,
            title='Units Sold by Category',
            labels={'x': 'Category', 'y': 'Units Sold'},
            color=category_quantity.values,
            color_continuous_scale='Oranges'
        )
        fig.update_layout(showlegend=False, height=400, xaxis_tickangle=-45)
        return fig
    
    def plot_customer_analysis(_self):
        if 'CustomerName' not in _self.df.columns:
            return go.Figure().add_annotation(text="No customer data available", showarrow=False)
        
        customer_revenue = _self.df.groupby('CustomerName')['Revenue'].sum().sort_values(ascending=False).head(10)
        fig = px.bar(
            x=customer_revenue.index,
            y=customer_revenue.values,
            title='Top 10 Customers by Revenue',
            labels={'x': 'Customer', 'y': 'Revenue ($)'},
            color=customer_revenue.values,
            color_continuous_scale='Viridis'
        )
        fig.update_layout(showlegend=False, height=400, xaxis_tickangle=-45)
        return fig

    def plot_daily_sales(_self, df=None):
        if df is None:
            df = _self.df
        
        if len(df) == 0:
            return go.Figure().add_annotation(text="No data", showarrow=False)
        
        daily_sales = df.groupby('SalesDate')['Revenue'].sum().reset_index()
        
        if len(daily_sales) == 0:
             return go.Figure().add_annotation(text="No data", showarrow=False)
        
        fig = go.Figure()
        mode = 'lines+markers' if len(daily_sales) > 1 else 'markers'
        
        fig.add_trace(go.Scatter(
            x=daily_sales['SalesDate'],
            y=daily_sales['Revenue'],
            mode=mode,
            name='Daily Sales',
            line=dict(color='#2ecc71', width=2) if len(daily_sales) > 1 else None,
            marker=dict(size=8)
        ))
        
        fig.update_layout(
            title='Daily Sales Performance',
            xaxis_title='Date',
            yaxis_title='Revenue ($)',
            hovermode='x unified',
            height=400
        )
        return fig
    
    def plot_monthly_sales(_self, df=None):
        if df is None:
            df = _self.df
        
        if len(df) == 0:
             return go.Figure().add_annotation(text="No data", showarrow=False)
        
        monthly_sales = df.groupby(['Year', 'Month', 'Month Name'])['Revenue'].sum().reset_index()
        
        if len(monthly_sales) == 0:
             return go.Figure().add_annotation(text="No data", showarrow=False)
        
        monthly_sales['Period'] = monthly_sales['Month Name'] + ' ' + monthly_sales['Year'].astype(str)
        monthly_sales = monthly_sales.sort_values(['Year', 'Month'])
        
        fig = px.bar(
            monthly_sales,
            x='Period',
            y='Revenue',
            title='Monthly Sales Performance',
            labels={'Revenue': 'Revenue ($)', 'Period': 'Month'},
            color='Revenue',
            color_continuous_scale='Blues'
        )
        fig.update_layout(showlegend=False, height=400, xaxis_tickangle=-45)
        return fig
    
    def plot_seasonal_pattern(_self, df=None):
        if df is None:
            df = _self.df
        
        if len(df) == 0:
            return go.Figure().add_annotation(text="No data", showarrow=False)
        
        seasonal = df.groupby('Quarter')['Revenue'].sum().reset_index()
        
        if len(seasonal) == 0:
            return go.Figure().add_annotation(text="No data", showarrow=False)
        
        seasonal['Quarter'] = seasonal['Quarter'].apply(lambda x: f"Q{x}")
        
        fig = px.bar(
            seasonal,
            x='Quarter',
            y='Revenue',
            title='Seasonal Sales Pattern',
            labels={'Revenue': 'Revenue ($)', 'Quarter': 'Quarter'},
            color='Revenue',
            color_continuous_scale='Sunset'
        )
        fig.update_layout(showlegend=False, height=400)
        return fig
    
    def plot_category_trend(_self, category):
        category_df = _self.df[_self.df['CategoryName'] == category]
        trend = category_df.groupby('SalesDate')['Revenue'].sum().reset_index()
        
        fig = px.area(
            trend,
            x='SalesDate',
            y='Revenue',
            title=f'Sales Trend for {category}',
            labels={'Revenue': 'Revenue ($)', 'SalesDate': 'Date'}
        )
        fig.update_traces(fill='tozeroy', line_color='#9b59b6')
        fig.update_layout(hovermode='x unified', height=400)
        return fig
    
    # NEW VISUALIZATION METHODS FOR PRODUCT PERFORMANCE COMPARISON
    @st.cache_data(ttl=3600)
    def plot_product_performance_comparison(_self, top_n=15):
        """Plot comprehensive product performance comparison"""
        product_performance = _self.df.groupby('ProductName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum'
        }).sort_values('Revenue', ascending=False).head(top_n)
        
        fig = go.Figure()
        
        # Add revenue bars
        fig.add_trace(go.Bar(
            x=product_performance.index,
            y=product_performance['Revenue'],
            name='Revenue',
            marker_color='#3498db',
            yaxis='y'
        ))
        
        # Add units sold line
        fig.add_trace(go.Scatter(
            x=product_performance.index,
            y=product_performance['UnitSold'],
            name='Units Sold',
            line=dict(color='#e74c3c', width=3),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title=f'Top {top_n} Products Performance Comparison',
            xaxis=dict(title='Product', tickangle=45),
            yaxis=dict(title='Revenue ($)', side='left'),
            yaxis2=dict(title='Units Sold', side='right', overlaying='y'),
            hovermode='x unified',
            height=500,
            showlegend=True
        )
        
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_underperforming_products(_self, bottom_n=10):
        """Plot underperforming products analysis"""
        product_performance = _self.df.groupby('ProductName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'Price': 'mean'
        }).sort_values('Revenue', ascending=True).head(bottom_n)
        
        fig = px.bar(
            product_performance,
            x=product_performance.index,
            y='Revenue',
            title=f'Bottom {bottom_n} Underperforming Products by Revenue',
            labels={'Revenue': 'Revenue ($)', 'x': 'Product'},
            color='Revenue',
            color_continuous_scale='Reds'
        )
        fig.update_layout(showlegend=False, height=400, xaxis_tickangle=45)
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_category_performance_radar(_self):
        """Plot category performance comparison using radar chart"""
        category_stats = _self.df.groupby('CategoryName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'ProductName': 'nunique'
        }).reset_index()
        
        # Normalize values for radar chart
        category_stats['Revenue Normalized'] = category_stats['Revenue'] / category_stats['Revenue'].max()
        category_stats['Units Normalized'] = category_stats['UnitSold'] / category_stats['UnitSold'].max()
        category_stats['Products Normalized'] = category_stats['ProductName'] / category_stats['ProductName'].max()
        
        fig = go.Figure()
        
        for _, row in category_stats.iterrows():
            fig.add_trace(go.Scatterpolar(
                r=[row['Revenue Normalized'], row['Units Normalized'], row['Products Normalized']],
                theta=['Revenue', 'Units Sold', 'Unique Products'],
                fill='toself',
                name=row['CategoryName']
            ))
        
        fig.update_layout(
            polar=dict(
                radialaxis=dict(visible=True, range=[0, 1])
            ),
            title='Category Performance Radar Chart',
            height=500
        )
        
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_seasonal_comparison(_self):
        """Plot seasonal comparison across quarters"""
        seasonal_data = _self.df.groupby(['Year', 'Quarter']).agg({
            'Revenue': 'sum',
            'UnitSold': 'sum'
        }).reset_index()
        
        seasonal_data['Period'] = 'Q' + seasonal_data['Quarter'].astype(str) + ' ' + seasonal_data['Year'].astype(str)
        
        fig = go.Figure()
        
        fig.add_trace(go.Bar(
            x=seasonal_data['Period'],
            y=seasonal_data['Revenue'],
            name='Revenue',
            marker_color='#27ae60'
        ))
        
        fig.add_trace(go.Scatter(
            x=seasonal_data['Period'],
            y=seasonal_data['UnitSold'],
            name='Units Sold',
            line=dict(color='#e67e22', width=3),
            yaxis='y2'
        ))
        
        fig.update_layout(
            title='Seasonal Performance Comparison',
            xaxis=dict(title='Quarter', tickangle=45),
            yaxis=dict(title='Revenue ($)', side='left'),
            yaxis2=dict(title='Units Sold', side='right', overlaying='y'),
            hovermode='x unified',
            height=400,
            showlegend=True
        )
        
        return fig
    
    @st.cache_data(ttl=3600)
    def plot_product_trend_comparison(_self, product_names):
        """Compare trends for multiple products over time"""
        if not product_names:
            return go.Figure().add_annotation(text="Please select products to compare", showarrow=False)
        
        product_trends = _self.df[_self.df['ProductName'].isin(product_names)]
        
        if product_trends.empty:
            return go.Figure().add_annotation(text="No data for selected products", showarrow=False)
        
        # Aggregate by date and product
        trend_data = product_trends.groupby(['SalesDate', 'ProductName'])['Revenue'].sum().reset_index()
        
        fig = px.line(
            trend_data,
            x='SalesDate',
            y='Revenue',
            color='ProductName',
            title='Product Trend Comparison Over Time',
            labels={'Revenue': 'Revenue ($)', 'SalesDate': 'Date'}
        )
        
        fig.update_layout(hovermode='x unified', height=400)
        return fig

# --- 6. Report Generator Class ---
class ReportGenerator:
    def __init__(self, df, analytics):
        self.df = df
        self.analytics = analytics
        self.styles = getSampleStyleSheet()
        self.setup_custom_styles()
    
    def setup_custom_styles(self):
        self.title_style = ParagraphStyle(
            'CustomTitle',
            parent=self.styles['Heading1'],
            fontSize=24,
            textColor=colors.HexColor('#2c3e50'),
            spaceAfter=30,
            alignment=TA_CENTER
        )
        
        self.heading_style = ParagraphStyle(
            'CustomHeading',
            parent=self.styles['Heading2'],
            fontSize=16,
            textColor=colors.HexColor('#34495e'),
            spaceAfter=12,
            spaceBefore=12
        )
        
        self.normal_style = ParagraphStyle(
            'CustomNormal',
            parent=self.styles['Normal'],
            fontSize=11,
            spaceAfter=6
        )
    
    def generate_report(self, title="Sales Performance Report", period="", include_charts=True, include_top_products=True):
        buffer = BytesIO()
        
        doc = SimpleDocTemplate(
            buffer,
            pagesize=letter,
            rightMargin=72,
            leftMargin=72,
            topMargin=72,
            bottomMargin=72
        )
        
        story = []
        
        # Title and header
        story.append(Paragraph(title, self.title_style))
        story.append(Spacer(1, 12))
        
        if period:
            story.append(Paragraph(f"<b>Report Period:</b> {period}", self.normal_style))
        
        story.append(Paragraph(f"<b>Generated:</b> {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}", self.normal_style))
        story.append(Spacer(1, 20))
        
        # Executive Summary
        story.append(Paragraph("Executive Summary", self.heading_style))
        
        summary_data = [
            ['Metric', 'Value'],
            ['Total Revenue', f"${self.df['Revenue'].sum():,.2f}"],
            ['Total Orders', f"{len(self.df):,}"],
            ['Total Units Sold', f"{self.df['UnitSold'].sum():,}"],
            ['Average Order Value', f"${self.df['Revenue'].mean():.2f}"],
            ['Unique Products', f"{self.df['ProductName'].nunique()}"],
            ['Unique Categories', f"{self.df['CategoryName'].nunique()}"],
            ['Date Range', f"{self.df['SalesDate'].min().strftime('%Y-%m-%d')} to {self.df['SalesDate'].max().strftime('%Y-%m-%d')}"]
        ]
        
        # Add customer metrics if available
        if 'CustomerName' in self.df.columns:
            summary_data.append(['Unique Customers', f"{self.df['CustomerName'].nunique()}"])
        
        summary_table = Table(summary_data, colWidths=[3*inch, 3*inch])
        summary_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#3498db')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 12),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.beige),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 10),
        ]))
        
        story.append(summary_table)
        story.append(Spacer(1, 20))
        
        # Top Products
        if include_top_products:
            story.append(Paragraph("Top 10 Products by Revenue", self.heading_style))
            
            top_products = self.df.groupby('ProductName').agg({
                'Revenue': 'sum',
                'UnitSold': 'sum'
            }).sort_values('Revenue', ascending=False).head(10).reset_index()
            
            products_data = [['Rank', 'Product Name', 'Revenue', 'Units Sold']]
            
            for idx, row in top_products.iterrows():
                products_data.append([
                    str(idx + 1),
                    row['ProductName'][:40],
                    f"${row['Revenue']:,.2f}",
                    f"{int(row['UnitSold']):,}"
                ])
            
            products_table = Table(products_data, colWidths=[0.6*inch, 2.8*inch, 1.5*inch, 1.5*inch])
            products_table.setStyle(TableStyle([
                ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#2ecc71')),
                ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
                ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
                ('ALIGN', (2, 0), (3, -1), 'RIGHT'),
                ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
                ('FONTSIZE', (0, 0), (-1, 0), 11),
                ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
                ('BACKGROUND', (0, 1), (-1, -1), colors.white),
                ('GRID', (0, 0), (-1, -1), 1, colors.black),
                ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
                ('FONTSIZE', (0, 1), (-1, -1), 9),
            ]))
            
            story.append(products_table)
            story.append(Spacer(1, 20))
        
        # Category Performance
        story.append(Paragraph("Category Performance", self.heading_style))
        
        category_stats = self.df.groupby('CategoryName').agg({
            'Revenue': 'sum',
            'UnitSold': 'sum',
            'ProductName': 'nunique'
        }).sort_values('Revenue', ascending=False).reset_index()
        
        category_data = [['Category', 'Revenue', 'Units Sold', 'Products']]
        
        for idx, row in category_stats.iterrows():
            category_data.append([
                row['CategoryName'][:30],
                f"${row['Revenue']:,.2f}",
                f"{int(row['UnitSold']):,}",
                str(int(row['ProductName']))
            ])
        
        category_table = Table(category_data, colWidths=[2*inch, 1.8*inch, 1.5*inch, 1.2*inch])
        category_table.setStyle(TableStyle([
            ('BACKGROUND', (0, 0), (-1, 0), colors.HexColor('#e74c3c')),
            ('TEXTCOLOR', (0, 0), (-1, 0), colors.whitesmoke),
            ('ALIGN', (0, 0), (-1, -1), 'LEFT'),
            ('ALIGN', (1, 0), (-1, -1), 'RIGHT'),
            ('FONTNAME', (0, 0), (-1, 0), 'Helvetica-Bold'),
            ('FONTSIZE', (0, 0), (-1, 0), 11),
            ('BOTTOMPADDING', (0, 0), (-1, 0), 12),
            ('BACKGROUND', (0, 1), (-1, -1), colors.white),
            ('GRID', (0, 0), (-1, -1), 1, colors.black),
            ('FONTNAME', (0, 1), (-1, -1), 'Helvetica'),
            ('FONTSIZE', (0, 1), (-1, -1), 9),
        ]))
        
        story.append(category_table)
        story.append(Spacer(1, 20))
        
        # Key Insights
        story.append(Paragraph("Key Insights", self.heading_style))
        
        growth_rate = self.analytics.calculate_growth_rate()
        best_category = self.df.groupby('CategoryName')['Revenue'].sum().idxmax()
        best_product = self.df.groupby('ProductName')['Revenue'].sum().idxmax()
        avg_daily_revenue = self.df.groupby('SalesDate')['Revenue'].sum().mean()
        
        story.append(Paragraph(f"• Overall growth rate: <b>{growth_rate:.1f}%</b>", self.normal_style))
        story.append(Paragraph(f"• Best performing category: <b>{best_category}</b>", self.normal_style))
        story.append(Paragraph(f"• Top selling product: <b>{best_product}</b>", self.normal_style))
        story.append(Paragraph(f"• Average daily revenue: <b>${avg_daily_revenue:,.2f}</b>", self.normal_style))
        story.append(Paragraph(f"• Total unique products sold: <b>{self.df['ProductName'].nunique()}</b>", self.normal_style))
        
        if 'CustomerName' in self.df.columns:
            best_customer = self.df.groupby('CustomerName')['Revenue'].sum().idxmax()
            story.append(Paragraph(f"• Best customer: <b>{best_customer}</b>", self.normal_style))
        
        story.append(Spacer(1, 20))
        
        story.append(Paragraph("Report End", self.heading_style))
        story.append(Paragraph("This report was automatically generated by the Retail Sales Trend Analysis System.", self.normal_style))
        
        doc.build(story)
        buffer.seek(0)
        return buffer.getvalue()

# --- 7. Initialization ---
def initialize_session_state():
    if 'data' not in st.session_state:
        st.session_state.data = None
    if 'processed_data' not in st.session_state:
        st.session_state.processed_data = None
    if 'analytics' not in st.session_state:
        st.session_state.analytics = None
    if 'column_mapping' not in st.session_state:
        st.session_state.column_mapping = {}
    if 'uploaded_data_preview' not in st.session_state:
        st.session_state.uploaded_data_preview = None
    if 'sample_mapping' not in st.session_state:
        st.session_state.sample_mapping = {}

# --- 8. Pages ---
def show_home_page():
    st.title("🛒 Retail Sales Trend Analysis & Visualization")
    st.markdown("### Sales Intelligence for Physical Products in Digital Retail")
    
    st.markdown("""
    Welcome to the **Retail Sales Trend Analysis System**! This platform helps e-commerce businesses 
    make data-driven decisions by transforming raw sales data into actionable insights.
    
    #### 🎯 Key Features:
    - **📁 Data Upload**: Import sales data from CSV or Excel files with flexible column names.
    - **📊 Interactive Dashboard**: View key metrics and performance indicators.
    - **📈 Trend Analysis**: Identify seasonal patterns and sales trends.
    - **💾 Save to Database**: Persist processed data to MySQL for long-term storage.
    - **📄 Report Generation**: Generate comprehensive PDF reports.
    - **🔍 Product Performance**: Compare product performance and identify opportunities.
    
    #### 🚀 Getting Started:
    1. Navigate to **Data Upload** to import your sales data. You will be prompted to map your column names to the required fields.
    2. Verify data and **Save to MySQL** if desired.
    3. Explore the **Dashboard** for an overview of your sales performance.
    4. Use **Product Performance** for detailed product comparisons and insights.
    """)
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.info("**Flexible Data Input**\n\nUpload any CSV/Excel and map columns on the fly.")
    
    with col2:
        st.success("**Interactive Visualizations**\n\nDynamic charts powered by Plotly.")
    
    with col3:
        st.warning("**Database Integration**\n\nSave processed insights directly to MySQL.")

def show_data_upload_page():
    st.title("📁 Data Upload & Management")
    
    tab1, tab2 = st.tabs(["Upload Data", "Generate Sample Data"])
    processor = DataProcessor()
    
    # --- Tab 1: Upload Data ---
    with tab1:
        st.markdown("### 1. Upload Your Sales Data")
        st.info("Upload a CSV or Excel file. We will guide you to map your columns to the required internal fields.")
        
        uploaded_file = st.file_uploader(
            "Choose a file",
            type=['csv', 'xlsx', 'xls'],
            help="Upload CSV or Excel file with sales data"
        )
        
        if uploaded_file is not None:
            # Clear previous session data if a new file is uploaded
            if st.session_state.uploaded_data_preview is None or st.session_state.uploaded_data_preview.name != uploaded_file.name:
                st.session_state.processed_data = None
                st.session_state.column_mapping = {}
                
            try:
                # Use cached load_data method
                df = processor.load_data(uploaded_file)
                # Hack to attach name to dataframe for session state tracking
                df.name = uploaded_file.name
                
                st.session_state.uploaded_data_preview = df
                
                st.success(f"✅ Data loaded successfully! {len(df)} records found.")
                st.markdown("#### Data Preview")
                st.dataframe(df.head(5), use_container_width=True)
                
                all_cols = df.columns.tolist()
                
                st.markdown("### 2. Map Columns to Required Fields")
                st.warning("Map your columns to the required fields. Strings for Product/Category, Numeric for Quantity/Price.")
                
                # --- Column Mapping Interface ---
                current_mapping = {}
                required_fields = [
                    'SalesDate', 'ProductID', 'ProductName', 'CategoryID', 'CategoryName',
                    'UnitSold', 'Price', 'Revenue', 'CustomerID', 'CustomerName'
                ]
                
                with st.form("column_mapping_form"):
                    col_map_c1, col_map_c2 = st.columns(2)
                    
                    for i, internal_field in enumerate(required_fields):
                        default_index = 0
                        # Try to guess the column based on name similarity
                        for idx, col_name in enumerate(all_cols):
                            if internal_field.replace(' ', '_').lower() in col_name.replace(' ', '_').lower():
                                default_index = idx
                                break

                        if i % 2 == 0:
                            with col_map_c1:
                                st.markdown(f"**{internal_field}**")
                                current_mapping[internal_field] = st.selectbox(
                                    f"Select column for {internal_field}",
                                    options=[''] + all_cols,
                                    index=default_index + 1 if default_index < len(all_cols) else 0,
                                    key=f"map_{internal_field}"
                                )
                        else:
                            with col_map_c2:
                                st.markdown(f"**{internal_field}**")
                                current_mapping[internal_field] = st.selectbox(
                                    f"Select column for {internal_field}",
                                    options=[''] + all_cols,
                                    index=default_index + 1 if default_index < len(all_cols) else 0,
                                    key=f"map_{internal_field}"
                                )
                            
                    map_submitted = st.form_submit_button("Process & Analyze Data", type="primary")

                if map_submitted:
                    # Check required fields
                    required_missing = [field for field in ['SalesDate', 'ProductName', 'UnitSold', 'Price'] 
                                      if not current_mapping[field]]
                    if required_missing:
                        st.error(f"❌ Required fields missing: {', '.join(required_missing)}")
                    else:
                        with st.spinner("Processing data..."):
                            try:
                                processed_df = processor.clean_data(df, current_mapping)
                                
                                st.session_state.data = df
                                st.session_state.column_mapping = current_mapping
                                st.session_state.processed_data = processed_df
                                st.session_state.analytics = SalesAnalytics(processed_df)
                                
                                st.success("✅ Data successfully processed! Navigate to Dashboard.")
                                
                            except ValueError as ve:
                                st.error(f"❌ Data Validation Error: {str(ve)}")
                            except Exception as e:
                                st.error(f"❌ An unexpected error occurred: {str(e)}")

            except Exception as e:
                st.error(f"Error loading data: {str(e)}")

    # --- Tab 2: Generate Sample Data ---
    with tab2:
        st.markdown("### Generate Sample Data")
        st.info("Generate sample data that follows the database schema with customers, products, and categories.")
        
        col1, col2 = st.columns(2)
        with col1:
            num_records = st.slider("Number of records", 100, 5000, 1000, step=100)
            num_days = st.slider("Number of days", 30, 365, 180, step=30)
        
        if st.button("Generate Sample Data", key="generate_btn", type="primary"):
            with st.spinner("Generating and processing sample data..."):
                df_raw = generate_sample_data(num_records, num_days)
                sample_mapping = st.session_state['sample_mapping']
                
                processed_df = processor.clean_data(df_raw, sample_mapping)
                
                st.session_state.data = df_raw
                st.session_state.column_mapping = sample_mapping
                st.session_state.processed_data = processed_df
                st.session_state.analytics = SalesAnalytics(processed_df)
                
                st.success(f"✅ Generated {len(df_raw)} sample records!")
                st.markdown("#### Sample Data Preview")
                st.dataframe(df_raw.head(5), use_container_width=True)
                
    # --- Save to MySQL Section (Common) ---
    if st.session_state.processed_data is not None:
        st.markdown("---")
        st.markdown("### 💾 Save to Database")
        
        col1, col2 = st.columns([1, 2])
        
        with col1:
            st.info("Store processed data into MySQL for long-term analysis.")
            if st.button("Save to MySQL Tables", type="primary"):
                with st.spinner("Saving records to database..."):
                    success, msg = processor.save_to_mysql(st.session_state.processed_data)
                    if success:
                        st.success(msg)
                        st.balloons()
                    else:
                        st.error(msg)
        
        # --- Verification Section ---
        with col2:
            with st.expander("🔍 Verify Database Records"):
                st.markdown("Click below to fetch the latest 5 records directly from your MySQL database.")
                if st.button("Refresh/View Last 5 Saved Records"):
                    df_db, err = processor.fetch_latest_records()
                    if err:
                        st.warning(f"Could not fetch records: {err}")
                    elif df_db is not None and not df_db.empty:
                        st.dataframe(df_db)
                        st.caption("Showing last 5 records from MySQL tables joined together.")
                    else:
                        st.info("Tables are empty or do not exist yet.")

def show_dashboard_page():
    st.title("📊 Sales Dashboard")
    
    if st.session_state.processed_data is None or st.session_state.analytics is None:
        st.warning("⚠️ Please upload or generate data first in the Data Upload page.")
        return
    
    try:
        df = st.session_state.processed_data
        analytics = st.session_state.analytics
        visualizer = SalesVisualizer(df)
    except Exception as e:
        st.error(f"Error initializing dashboard: {str(e)}")
        return
    
    st.markdown("### Key Performance Indicators")
    
    summary = analytics.get_summary_statistics()
    col1, col2, col3, col4 = st.columns(4)
    
    # Helper to safely get value
    def get_metric(name):
        row = summary[summary['Metric'] == name]
        return row['Value'].iloc[0] if not row.empty else "N/A"

    with col1: st.metric("Total Revenue", get_metric("Total Revenue"))
    with col2: st.metric("Total Orders", get_metric("Total Orders"))
    with col3: st.metric("Avg Order Value", get_metric("Average Order Value"))
    with col4: st.metric("Unique Products", get_metric("Unique Products"))
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 📈 Sales Trend Over Time")
        st.plotly_chart(visualizer.plot_sales_trend(), use_container_width=True)
    with col2:
        st.markdown("### 📊 Revenue by Category")
        st.plotly_chart(visualizer.plot_category_revenue(), use_container_width=True)
    
    st.markdown("---")
    
    col1, col2 = st.columns(2)
    with col1:
        st.markdown("### 🏆 Top 10 Products by Revenue")
        st.dataframe(analytics.get_top_products(10), use_container_width=True, hide_index=True)
    with col2:
        st.markdown("### 📦 Top 10 Products by Quantity Sold")
        st.dataframe(analytics.get_top_products_by_quantity(10), use_container_width=True, hide_index=True)
    
    # Customer analysis if data available
    if 'CustomerName' in df.columns:
        st.markdown("---")
        st.markdown("### 👥 Customer Analysis")
        st.plotly_chart(visualizer.plot_customer_analysis(), use_container_width=True)
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown("### 🏆 Top Customers by Revenue")
            customer_stats = analytics.get_customer_statistics()
            if not customer_stats.empty:
                st.dataframe(customer_stats, use_container_width=True, hide_index=True)

def show_trend_analysis_page():
    st.title("📈 Sales Trend Analysis")
    
    if st.session_state.processed_data is None:
        st.warning("⚠️ Please upload data first.")
        return
    
    df = st.session_state.processed_data
    analytics = st.session_state.analytics
    visualizer = SalesVisualizer(df)
    
    st.sidebar.markdown("### Filters")
    date_range = st.sidebar.date_input("Select Date Range", value=(df['SalesDate'].min(), df['SalesDate'].max()))
    categories = st.sidebar.multiselect("Filter by Category", options=df['CategoryName'].unique())
    
    filtered_df = df.copy()
    if len(date_range) == 2:
        filtered_df = filtered_df[(filtered_df['SalesDate'] >= pd.Timestamp(date_range[0])) & (filtered_df['SalesDate'] <= pd.Timestamp(date_range[1]))]
    if categories:
        filtered_df = filtered_df[filtered_df['CategoryName'].isin(categories)]
        
    if filtered_df.empty:
        st.warning("No data found for selected filters.")
        return

    filtered_analytics = SalesAnalytics(filtered_df)
    
    st.markdown("### 📊 Filtered Data Summary")
    c1, c2, c3, c4 = st.columns(4)
    c1.metric("Revenue", f"${filtered_df['Revenue'].sum():,.2f}")
    c2.metric("Orders", len(filtered_df))
    
    daily_rev = filtered_df.groupby('SalesDate')['Revenue'].sum()
    c3.metric("Avg Daily Sales", f"${daily_rev.mean():.2f}" if not daily_rev.empty else "$0.00")
    c4.metric("Growth Rate", f"{filtered_analytics.calculate_growth_rate():.1f}%")
    
    st.markdown("---")
    
    t1, t2, t3 = st.tabs(["Daily", "Monthly", "Seasonal"])
    with t1:
        st.plotly_chart(visualizer.plot_daily_sales(filtered_df), use_container_width=True)
    with t2:
        st.plotly_chart(visualizer.plot_monthly_sales(filtered_df), use_container_width=True)
    with t3:
        st.plotly_chart(visualizer.plot_seasonal_pattern(filtered_df), use_container_width=True)

def show_category_comparison_page():
    st.title("🔍 Category Comparison Analysis")
    
    if st.session_state.processed_data is None:
        st.warning("⚠️ Please upload data first.")
        return

    df = st.session_state.processed_data
    analytics = st.session_state.analytics
    visualizer = SalesVisualizer(df)
    
    c1, c2 = st.columns([2, 1])
    with c1:
        st.plotly_chart(visualizer.plot_category_pie_chart(), use_container_width=True)
    with c2:
        st.dataframe(analytics.get_category_statistics(), use_container_width=True, hide_index=True)
        
    st.markdown("---")
    c1, c2 = st.columns(2)
    with c1: st.plotly_chart(visualizer.plot_category_comparison(), use_container_width=True)
    with c2: st.plotly_chart(visualizer.plot_category_quantity(), use_container_width=True)

def show_product_performance_page():
    st.title("📊 Product Performance Comparison")
    
    if st.session_state.processed_data is None:
        st.warning("⚠️ Please upload data first.")
        return
    
    df = st.session_state.processed_data
    analytics = st.session_state.analytics
    visualizer = SalesVisualizer(df)
    
    st.markdown("""
    ### Comprehensive Product Performance Analysis
    
    Compare product performance across multiple dimensions to identify top performers,
    underperformers, and seasonal trends.
    """)
    
    # Top vs Bottom Products
    st.markdown("### 🏆 Top vs Bottom Performing Products")
    
    col1, col2 = st.columns(2)
    
    with col1:
        st.markdown("#### Top Products by Revenue")
        top_products = analytics.get_top_products(10)
        st.dataframe(top_products, use_container_width=True, hide_index=True)
    
    with col2:
        st.markdown("#### Underperforming Products by Revenue")
        bottom_products = analytics.get_bottom_products(10)
        st.dataframe(bottom_products, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Product Performance Comparison Chart
    st.markdown("### 📈 Product Performance Comparison")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        num_products = st.slider("Number of products to display", 5, 25, 15, key="product_perf_slider")
        st.plotly_chart(visualizer.plot_product_performance_comparison(num_products), use_container_width=True)
    
    with col2:
        st.markdown("#### Performance Metrics")
        st.metric("Total Products", df['ProductName'].nunique())
        st.metric("Avg Revenue per Product", f"${df.groupby('ProductName')['Revenue'].sum().mean():.2f}")
        st.metric("Avg Units per Product", f"{df.groupby('ProductName')['UnitSold'].sum().mean():.1f}")
    
    st.markdown("---")
    
    # Underperforming Products Analysis
    st.markdown("### 📉 Underperforming Products Analysis")
    
    col1, col2 = st.columns([2, 1])
    
    with col1:
        bottom_n = st.slider("Number of underperforming products", 5, 20, 10, key="underperf_slider")
        st.plotly_chart(visualizer.plot_underperforming_products(bottom_n), use_container_width=True)
    
    with col2:
        st.markdown("#### Underperformance Insights")
        
        # Calculate underperformance metrics
        product_revenue = df.groupby('ProductName')['Revenue'].sum()
        threshold = product_revenue.quantile(0.2)  # Bottom 20%
        underperforming = product_revenue[product_revenue <= threshold]
        
        st.metric("Underperforming Products", len(underperforming))
        st.metric("Revenue Threshold", f"${threshold:.2f}")
        st.metric("Avg Underperformer Revenue", f"${underperforming.mean():.2f}")
    
    st.markdown("---")
    
    # Category Performance Radar
    st.markdown("### 🎯 Category Performance Radar")
    st.plotly_chart(visualizer.plot_category_performance_radar(), use_container_width=True)
    
    st.markdown("---")
    
    # Seasonal Comparison
    st.markdown("### 🌸 Seasonal Performance Comparison")
    
    col1, col2 = st.columns([3, 1])
    
    with col1:
        st.plotly_chart(visualizer.plot_seasonal_comparison(), use_container_width=True)
    
    with col2:
        st.markdown("#### Seasonal Insights")
        seasonal_data = analytics.get_seasonal_comparison()
        st.dataframe(seasonal_data, use_container_width=True, hide_index=True)
    
    st.markdown("---")
    
    # Product Trend Comparison
    st.markdown("### 🔄 Product Trend Comparison")
    
    # Product selection for trend comparison
    available_products = df['ProductName'].unique().tolist()
    selected_products = st.multiselect(
        "Select products to compare trends",
        options=available_products,
        default=available_products[:3] if len(available_products) >= 3 else available_products,
        help="Select multiple products to compare their sales trends over time"
    )
    
    if selected_products:
        st.plotly_chart(visualizer.plot_product_trend_comparison(selected_products), use_container_width=True)
    else:
        st.info("Please select products to compare their trends")
    
    # Detailed Product Performance Table
    st.markdown("### 📋 Detailed Product Performance")
    
    with st.expander("View Detailed Product Performance Metrics"):
        product_performance = analytics.get_product_performance_comparison()
        st.dataframe(product_performance, use_container_width=True)
        
        # Download option
        csv = product_performance.to_csv(index=False)
        st.download_button(
            "📥 Download Product Performance Data",
            data=csv,
            file_name="product_performance_comparison.csv",
            mime="text/csv"
        )

def show_reports_page():
    st.title("📄 Report Generation")
    
    if st.session_state.processed_data is None:
        st.warning("⚠️ Please upload data first.")
        return
        
    df = st.session_state.processed_data
    analytics = st.session_state.analytics
    
    col1, col2 = st.columns(2)
    with col1:
        report_title = st.text_input("Report Title", "Sales Performance Report")
    with col2:
        include_top = st.checkbox("Include Top Products", value=True)
        
    if st.button("Generate PDF Report", type="primary"):
        with st.spinner("Generating..."):
            gen = ReportGenerator(df, analytics)
            pdf = gen.generate_report(title=report_title, include_top_products=include_top)
            st.download_button("📥 Download PDF", data=pdf, file_name="sales_report.pdf", mime="application/pdf")
    
    st.markdown("---")
    st.markdown("### Export Data")
    c1, c2, c3 = st.columns(3)
    with c1:
        st.download_button("📊 Download Processed CSV", df.to_csv(index=False), "processed_sales.csv", "text/csv")
    with c2:
        stats = analytics.get_summary_statistics()
        st.download_button("📈 Download Stats CSV", stats.to_csv(), "summary_stats.csv", "text/csv")
    with c3:
        product_perf = analytics.get_product_performance_comparison()
        st.download_button("📋 Download Product Performance", product_perf.to_csv(), "product_performance.csv", "text/csv")

def main():
    initialize_session_state()
    
    st.sidebar.title("📊 Sales Analytics")
    st.sidebar.markdown("### Navigation")
    
    page = st.sidebar.radio(
        "Select Page",
        ["Home", "Data Upload", "Dashboard", "Product Performance", "Trend Analysis", "Category Comparison", "Reports"]
    )
    
    if page == "Home":
        show_home_page()
    elif page == "Data Upload":
        show_data_upload_page()
    elif page == "Dashboard":
        show_dashboard_page()
    elif page == "Product Performance":
        show_product_performance_page()
    elif page == "Trend Analysis":
        show_trend_analysis_page()
    elif page == "Category Comparison":
        show_category_comparison_page()
    elif page == "Reports":
        show_reports_page()

if __name__ == "__main__":

    main()
