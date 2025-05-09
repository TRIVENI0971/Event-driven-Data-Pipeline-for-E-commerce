import streamlit as st
from statements import logger
import Connection as BQ
from google.cloud import bigquery
import pandas as pd
import time
import plotly.express as px
import uuid

try:
    client = bigquery.Client()
    
except Exception as e:
    st.error(f"Error initializing BigQuery client: {e}")

EVENT_TYPES = [
    "page_view", "search_product", "view_product", "add_to_cart",
    "remove_from_cart", "wishlist_add", "wishlist_remove", "apply_coupon",
    "checkout", "purchase", "login", "logout"
]

BQ_ORDERS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.Orders"
BQ_ORDERS_ITEMS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_items"
BQ_ORDERS_PAYMENTS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_payments"
BQ_CUSTOMERS_TABLE = f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.customers"
PARTITIONED_ORDER_ITEMS= f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.partitioned_order_items"
CLUSTERING_ORDERS=f"{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.ClusteredOrders"

QUERY_ORDER_STATUS = f"""
SELECT OrderStatus, COUNT(*) as Count
FROM `{CLUSTERING_ORDERS}` 
GROUP BY OrderStatus
ORDER BY Count DESC;
"""

QUERY_DAILY_ORDERS = f"""
SELECT OrderDate, COUNT(*) as Orders
FROM `{CLUSTERING_ORDERS}`
GROUP BY OrderDate
ORDER BY OrderDate;
"""

TOP_PRODUCTS = f"""
SELECT REPLACE(INITCAP(p.product_category_name), '_', '') as ProductCategoryName ,SUM(oi.price) AS Revenue
 FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_items` oi
 INNER JOIN  `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.products` p ON
 oi.product_id=p.product_id
 group by p.product_category_name
 order by revenue desc LIMIT 10
 """

WORST_PRODUCTS = f"""
SELECT  REPLACE(INITCAP(p.product_category_name), '_', '') as ProductCategoryName, SUM(oi.price) AS Revenue
 FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_items` oi
 INNER JOIN  `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.products` p ON
 oi.product_id=p.product_id
 group by p.product_category_name
 order by revenue LIMIT 10
 """

ALLSTATS = f"""
  SELECT COUNT(DISTINCT order_id) AS total_orders,
    SUM(price + freight_value) AS total_revenue,
    (SELECT COUNT(DISTINCT customer_id) FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.customers` ) AS active_customers,
    COUNT(DISTINCT product_id) AS total_products_sold,
    AVG(price + freight_value) AS avg_order_value
FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.order_items`
"""


@st.cache_data
def fetch_batch_data(query):
    logger.info("Fetching Query")
    try:
        return client.query(query).to_dataframe()
    except Exception as e:
        st.error(f"Error fetching real-time events: {e}")
        logger.warning(e)
        return pd.DataFrame()

def fetch_realtime_events():
    logger.info("Fetching real-time event data...")
    query = f"""
    SELECT event_id as EventId, user_id as UserId, REPLACE(INITCAP(event_type), '_', '') as EventType, product_id as ProductId, price as Price, timestamp 
FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.events`
WHERE PARSE_TIMESTAMP('%Y-%m-%dT%H:%M:%E*S', timestamp) 
      >= TIMESTAMP_SUB(TIMESTAMP(DATETIME(CURRENT_TIMESTAMP(), "Asia/Kolkata")), INTERVAL 10 MINUTE)
ORDER BY timestamp DESC;
    """
    return client.query(query).to_dataframe()
def fetch_all_events():
    logger.info("Fetching all events data...")
    query = f"""
    SELECT event_id as EventId, user_id as UserId, REPLACE(INITCAP(event_type), '_', '') as EventType, product_id as ProductId, price as Price, timestamp
    FROM `{BQ.BQ_PROJECT}.{BQ.BQ_DATASET}.events`
    ORDER BY timestamp DESC;
    """
    return client.query(query).to_dataframe()


def animate_metric(label, final_value, key, is_currency=False):
    placeholder = st.empty()
    value = 0 
    step = max(1, final_value // 50)  
    while value < final_value:
        value += step
        if is_currency:
            placeholder.metric(label, f"${value:,.2f}") 
        else:
            placeholder.metric(label, f"{value:,}")  
        time.sleep(0.02)  
    
    
    if is_currency:
        placeholder.metric(label, f"${final_value:,.2f}")  
    else:
        placeholder.metric(label, f"{final_value:,}")  



def main():
    st.set_page_config(layout="wide")
    st.markdown(
    """
    <style>
        .title-container {
            width: 100%;
            display: flex;
            justify-content: center;
            align-items: center;
            padding: 10px;
            background: linear-gradient(90deg, #11998E, #1c5786);
            border-radius: 8px;
            box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.2);
        }
        .title {
            font-size: 45px;
            font-weight: bold;
            color: white;
            text-shadow: 3px 3px 5px rgba(0, 0, 0, 0.3);
            margin: 0;
        }
    </style>
    <div class="title-container">
        <h1 class="title">🚀 E-commerce Analytics Dashboard</h1>
    </div>
    """,
    unsafe_allow_html=True
)
    

    
    st.sidebar.title("Dashboard Menu")
    st.markdown(
        """
        <style>
        [data-testid="stSidebar"] {
            background-color: #1c5786;
            color: white;
        }
        [data-testid="stSidebar"] h1, [data-testid="stSidebar"] h2, [data-testid="stSidebar"] h3, [data-testid="stSidebar"] label {
            color: white;
        }
        /* Sidebar text color and bold */
        [data-testid="stSidebar"] * {
            color: white !important;
            font-weight: bold !important;
        }
        
        /* Page Background Color */
        /*
        .stApp {
            background-color: #dfddd6; 
            color: white;
        }
        */
        </style>
        """,
        unsafe_allow_html=True
    )



    
    st.session_state.view_option = st.sidebar.radio(
        "🗂️ View Sections:", 
        ["Performance Overview", "Order Summary", "Event Metrics ", "Live Data Stream","Tables"]
    )



    if "view_option" in st.session_state and st.session_state.view_option == "Performance Overview":
        st.header("Performance Overview")
        kpi_data = fetch_batch_data(ALLSTATS).iloc[0]
        st.markdown(
        """
        <style>
            .metric-container {
                background-color: white;
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
                margin-bottom: 10px;
            }
            .metric-label {
                font-size: 16px;
                font-weight: bold;
                color: #1C5786;
            }
            .metric-value {
                font-size: 28px;
                font-weight: bold;
                color: #007bff;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
        
        
        col1, col2, col3, col4 = st.columns(4)

        with col1:
            with st.container():
                
                st.markdown('<div class="metric-label">📦 Total Orders</div>', unsafe_allow_html=True)
                animate_metric("Total Orders", int(kpi_data["total_orders"]), "orders")
                st.markdown('</div>', unsafe_allow_html=True)

            
        with col2:
            with st.container():
                
                st.markdown('<div class="metric-label">💰 Total Revenue</div>', unsafe_allow_html=True)
                animate_metric("Total Revenue", round(kpi_data['total_revenue'], 1), "revenue", is_currency=True)
                st.markdown('</div>', unsafe_allow_html=True)
        with col3:
            with st.container():
                
                st.markdown('<div class="metric-label">🧑‍💼 Active Customers</div>', unsafe_allow_html=True)
                animate_metric("Active Customers", int(kpi_data["active_customers"]), "customers")
                st.markdown('</div>', unsafe_allow_html=True)
        with col4:
            with st.container():
                
                st.markdown('<div class="metric-label">📊 Total Products Sold</div>', unsafe_allow_html=True)
                animate_metric("Total Products Sold", int(kpi_data["total_products_sold"]), "products")
                st.markdown('</div>', unsafe_allow_html=True)
        
        

        top_products_data = fetch_batch_data(TOP_PRODUCTS)
        worst_products_data = fetch_batch_data(WORST_PRODUCTS)

        col1, col2 = st.columns(2)

        with col1:
            fig_top_products = px.treemap(
            top_products_data, 
            path=["ProductCategoryName"], 
            values="Revenue", 
            title="Top 10 Product Categories - Treemap",
            color="Revenue",
            
        )

        st.plotly_chart(fig_top_products, use_container_width=True)

        with col2:
            fig_worst_products = px.treemap(
            worst_products_data, 
            path=["ProductCategoryName"], 
            values="Revenue", 
            title="Worst 10 Product Categories - Treemap",
            color="Revenue",
            color_continuous_scale="Turbo"
        )
        st.plotly_chart(fig_worst_products, use_container_width=True)

        
    elif st.session_state.view_option == "Order Summary":
        st.header("Order Summary")
        order_status_data = fetch_batch_data(QUERY_ORDER_STATUS)
        daily_orders_data = fetch_batch_data(QUERY_DAILY_ORDERS)
        
        st.subheader("Order Status Distribution")
        fig1 = px.bar(order_status_data, x="OrderStatus", y="Count", title="Order Status Breakdown", text='Count')
        fig1.update_traces(textposition="outside")
        st.plotly_chart(fig1, use_container_width=True)
        
        st.subheader("Daily Orders Trend")
        daily_orders_data["OrderDate"] = pd.to_datetime(daily_orders_data["OrderDate"])
        available_years = sorted(daily_orders_data["OrderDate"].dt.year.unique())
        available_years.insert(0, "All")
        
        selected_year = st.selectbox("Select Year", available_years)
        filtered_data = daily_orders_data if selected_year == "All" else daily_orders_data[daily_orders_data["OrderDate"].dt.year == selected_year]
        
        fig2 = px.line(filtered_data, x="OrderDate", y="Orders", title="Daily Orders Trend")
        st.plotly_chart(fig2, use_container_width=True)

    elif st.session_state.view_option == "Event Metrics ":
        st.header("Event Metrics ")
        df = fetch_all_events()
        st.markdown(
        """
        <style>
            .metric-card {
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 10px;
                text-align: center;
                box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
            }
            .metric-label {
                font-size: 16px;
                font-weight: bold;
                color: #1C5786;
            }
            .metric-value {
                font-size: 28px;
                font-weight: bold;
                color: #007bff;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
       
        col1, col2, col3 = st.columns(3)
        with col1:
            with st.container():
    
                st.markdown('<div class="metric-label">📦 Total Events</div>', unsafe_allow_html=True)
                animate_metric("Total Events",len(df),"events")
                st.markdown('</div>', unsafe_allow_html=True)
        with col2:
            with st.container():
                
                st.markdown('<div class="metric-label">📦 Unique Users</div>', unsafe_allow_html=True)
                animate_metric("Unique Users",df['UserId'].nunique(),"events")
                st.markdown('</div>', unsafe_allow_html=True)
       
        with col3:
            with st.container():
                
                st.markdown('<div class="metric-label">📦Total Revenue</div>', unsafe_allow_html=True)
                animate_metric("Event Revenues", df['Price'].sum(), "events", is_currency=True)

                st.markdown('</div>', unsafe_allow_html=True)
       
        fig1 = px.bar(df.groupby('EventType').size().reset_index(name='Count'), x='EventType', y='Count', title='Conversion Funnel Breakdown', color='EventType')
        fig2 = px.pie(df, names='EventType', title='Customer Engagement Flow')

        st.plotly_chart(fig1, use_container_width=True)
        st.plotly_chart(fig2, use_container_width=True)
        
       
        col1, col2, col3 = st.columns([1, 1, 1])

        event_type_filter = col1.multiselect("Event Type", df["EventType"].unique())
        user_id_filter = col2.text_input("User ID")
        product_id_filter = col3.text_input("Product ID")

        filter_checkbox = st.checkbox("Apply Filters")

        if filter_checkbox:
            
            if event_type_filter:
                df = df[df['EventType'].isin(event_type_filter)]
            if user_id_filter:
                df = df[df['UserId'].str.contains(user_id_filter, na=False)]
            if product_id_filter:
                df = df[df['ProductId'].str.contains(product_id_filter, na=False)]

        rows_per_page = 25
        total_pages = (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0)

        if "current_page" not in st.session_state:
            st.session_state.current_page = 1

        def next_page():
            if st.session_state.current_page < total_pages:
                st.session_state.current_page += 1

        def prev_page():
            if st.session_state.current_page > 1:
                st.session_state.current_page -= 1

        
        start_idx = (st.session_state.current_page - 1) * rows_per_page
        end_idx = start_idx + rows_per_page

       
        st.data_editor(df.iloc[start_idx:end_idx], height=500, use_container_width=True)

        
        col1, col2, col3,col4,col5,col6 = st.columns([1, 1, 1,1,1,1])
        with col1:
            if st.button("Previous", disabled=st.session_state.current_page == 1):
                prev_page()
        with col3:
            st.markdown(f"<p style='text-align:center; font-weight:bold; font-size:16px;'>Page {st.session_state.current_page} of {total_pages}</p>", unsafe_allow_html=True)

        
        with col6: 
            if st.button("Next", disabled=st.session_state.current_page == total_pages):
                next_page()

        
    elif st.session_state.view_option == "Live Data Stream":
            st.header("Live Data Stream")
            st.success("✅ Live Data Generated Successfully!")
            df = fetch_realtime_events()
            st.markdown(
        """
        <style>
            .metric-card {
                background-color: #f8f9fa;
                padding: 15px;
                border-radius: 10px;
                text-align: center;
                box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
            }
            .metric-container {
                background-color: white;
                padding: 20px;
                border-radius: 12px;
                text-align: center;
                box-shadow: 2px 2px 10px rgba(0, 0, 0, 0.1);
                margin-bottom: 10px;
            }
            .metric-label {
                font-size: 16px;
                font-weight: bold;
                color: #1C5786;
            }
            .metric-value {
                font-size: 28px;
                font-weight: bold;
                color: #007bff;
            }
        </style>
        """,
        unsafe_allow_html=True
    )
            col1, col2, col3 = st.columns(3)
            with col1:
                with st.container():
        
                    st.markdown('<div class="metric-label">📦 Total Events</div>', unsafe_allow_html=True)
                    animate_metric("Total Events",len(df),"events")
                    st.markdown('</div>', unsafe_allow_html=True)
            with col2:
                with st.container():
                    
                    st.markdown('<div class="metric-label">📦 Unique Users</div>', unsafe_allow_html=True)
                    animate_metric("Unique Users",df['UserId'].nunique(),"events")
                    st.markdown('</div>', unsafe_allow_html=True)
        
            with col3:
                with st.container():
                    
                    st.markdown('<div class="metric-label">📦Total Revenue</div>', unsafe_allow_html=True)
                    animate_metric("Event Revenues", df['Price'].sum(), "events", is_currency=True)

                    st.markdown('</div>', unsafe_allow_html=True)
       
            
            fig1 = px.bar(df.groupby('EventType').size().reset_index(name='Count'), x='EventType', y='Count', title='Event Type Distribution', color='EventType')
            fig2 = px.pie(df, names='EventType', title='Event Type Proportions') 

            st.plotly_chart(fig1, use_container_width=True)
            st.plotly_chart(fig2, use_container_width=True)
            
            col1, col2, col3 = st.columns([1, 1, 1])

            event_type_filter = col1.multiselect("Event Type", df["EventType"].unique())
            user_id_filter = col2.text_input("Event ID")
            product_id_filter = col3.text_input("Product ID")

            filter_checkbox = st.checkbox("Apply Filters")

            if filter_checkbox:
                if event_type_filter:
                    df = df[df['EventType'].isin(event_type_filter)]
                    
                if user_id_filter:
                    df = df[df['EventId'].str.contains(user_id_filter, na=False)]
                if product_id_filter:
                    df = df[df['ProductId'].str.contains(product_id_filter, na=False)]
                
            rows_per_page = 25
            total_pages = (len(df) // rows_per_page) + (1 if len(df) % rows_per_page > 0 else 0)

            if "current_page" not in st.session_state:
                st.session_state.current_page = 1

            def next_page():
                if st.session_state.current_page < total_pages:
                    st.session_state.current_page += 1

            def prev_page():
                if st.session_state.current_page > 1:
                    st.session_state.current_page -= 1

            start_idx = (st.session_state.current_page - 1) * rows_per_page
            end_idx = start_idx + rows_per_page

            st.dataframe(df.iloc[start_idx:end_idx][['EventType', 'EventId', 'ProductId']], height=500)
            col1, col2, col3,col4 = st.columns([1, 1, 1,1])
            with col1:
                if st.button("Previous", disabled=st.session_state.current_page == 1):
                    prev_page()
            with col3:
                if st.button("Next", disabled=st.session_state.current_page == total_pages):
                    next_page()
            with col2:
               st.write(f"Page {st.session_state.current_page} of {total_pages}")   
            time.sleep(10) 
            st.rerun()






    elif st.session_state.view_option == "Tables":
        st.header("Table Data")
        if "page_orders" not in st.session_state:
            st.session_state.page_orders = 0 

        if "page_summary" not in st.session_state:
            st.session_state.page_summary = 0 

        
        
        LIMIT = 50 

        
        def fetch_batc_data(query, offset, limit):
            client = bigquery.Client()
            query_with_pagination = f"{query} LIMIT {limit} OFFSET {offset}"
            query_job = client.query(query_with_pagination)
            return query_job.result().to_dataframe()

        @st.fragment
        def pagination_controls(page_key, query_key, total_rows):
            col1, col2, col3 = st.columns([1, 1, 1]) 

            
            if page_key not in st.session_state:
                st.session_state[page_key] = 0

            total_pages = (total_rows + LIMIT - 1) // LIMIT  
            
            prev_key = f"prev_{query_key}_{uuid.uuid4().hex}"
            next_key = f"next_{query_key}_{uuid.uuid4().hex}"


            with col1:
                if st.button("Previous", key=f"prev_{prev_key}", disabled=st.session_state[page_key] == 0):
                    st.session_state[page_key] -= 1
                    st.rerun()  
            
            with col2:
                if st.button("Next", key=f"next_{next_key}", disabled=st.session_state[page_key] >= (total_rows // LIMIT)):
                    st.session_state[page_key] += 1
                    st.rerun()  
            
            with col3:
    
                start_row = st.session_state[page_key] * LIMIT + 1
                end_row = min((st.session_state[page_key] + 1) * LIMIT, total_rows)
                st.write(f"Showing rows {start_row}–{end_row} of {total_rows}")
                
       

        st.subheader("Orders Table")
    
    
        total_rows_orders = bigquery.Client().query(f"SELECT COUNT(*) AS total FROM `{CLUSTERING_ORDERS}`").result().to_dataframe().iloc[0]['total']
        
        
        offset_orders = st.session_state.page_orders * LIMIT
        query_orders = """
            SELECT 
                OrderId,
                CustomerId,
                OrderStatus,
                OrderDate,
                ApprovedAt,
                DeliveredCarrierDate,
                DeliveredCustomerDate,
                EstimatedDeliveryDate
            FROM `{CLUSTERING_ORDERS}`
        """.format(CLUSTERING_ORDERS=CLUSTERING_ORDERS)
        
        orders_table = fetch_batc_data(query_orders, offset_orders, LIMIT)
        st.dataframe(orders_table, height=500)
        
       
        pagination_controls("page_orders", "orders", total_rows_orders)

        st.markdown("---")
        st.subheader("Order Items Table")
        
        
        total_rows_summary = bigquery.Client().query(f"SELECT COUNT(*) AS total FROM `{PARTITIONED_ORDER_ITEMS}`").result().to_dataframe().iloc[0]['total']
        
        
        offset_summary = st.session_state.page_summary * LIMIT
        query_summary = """
            SELECT
            OrderId,
            OrderItemId,
            ProductId,
            SellerId,
            ShippingLimitDate, 
            price,
            FreightValue
        FROM `{PARTITIONED_ORDER_ITEMS}`
        """.format(PARTITIONED_ORDER_ITEMS=PARTITIONED_ORDER_ITEMS)
        
        summary_table = fetch_batc_data(query_summary, offset_summary, LIMIT)
        st.dataframe(summary_table, height=500)
        
        
        pagination_controls("page_summary", "summary", total_rows_summary)

        st.markdown("---")
        
        st.subheader("Customers Table")
    
    
        total_rows_orders = bigquery.Client().query(f"SELECT COUNT(*) AS total FROM `{BQ_CUSTOMERS_TABLE}`").result().to_dataframe().iloc[0]['total']
        
        offset_orders = st.session_state.page_orders * LIMIT
        query_orders = """
            SELECT
            customer_id AS CustomerId,
            customer_unique_id AS CustomerUniqueId,
            customer_zip_code_prefix AS CustomerZipCodePrefix,
            customer_city AS CustomerCity,
            customer_state AS CustomerState
            FROM `{BQ_CUSTOMERS_TABLE}`
        """.format(BQ_CUSTOMERS_TABLE=BQ_CUSTOMERS_TABLE)
        
        orders_table = fetch_batc_data(query_orders, offset_orders, LIMIT)
        st.dataframe(orders_table, height=500)
        
        pagination_controls("page_orders", "orders", total_rows_orders)



           
        
if __name__ == "__main__":
    main()

