from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import random
import json
import numpy as np
import pandas as pd
import sqlite3
from contextlib import contextmanager
from threading import Thread
import time
from collections import defaultdict
from typing import Dict, List, Optional

app = Flask(__name__)

# Database Configuration
DATABASE_FILE = 'techflow.db'

class DatabaseManager:
    """Handles all database operations for TechFlow API"""
    
    def __init__(self, db_file):
        self.db_file = db_file
        self.init_database()
    
    @contextmanager
    def get_connection(self):
        """Context manager for database connections"""
        conn = None
        try:
            conn = sqlite3.connect(self.db_file)
            conn.row_factory = sqlite3.Row  # Enable column access by name
            yield conn
        finally:
            if conn:
                conn.close()
    
    def init_database(self):
        """Initialize database tables if they don't exist"""
        
        schemas = {
            'products': '''
                CREATE TABLE IF NOT EXISTS products (
                    product_sku TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    price DECIMAL(10,2) NOT NULL,
                    category TEXT,
                    brand TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'stores': '''
                CREATE TABLE IF NOT EXISTS stores (
                    store_id TEXT PRIMARY KEY,
                    name TEXT NOT NULL,
                    location TEXT,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'sales': '''
                CREATE TABLE IF NOT EXISTS sales (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    store_id TEXT,
                    product_sku TEXT,
                    quantity_sold INTEGER,
                    unit_price DECIMAL(10,2),
                    total_revenue DECIMAL(10,2),
                    promotion_code TEXT,
                    customer_segment TEXT,
                    payment_method TEXT,
                    FOREIGN KEY (store_id) REFERENCES stores (store_id),
                    FOREIGN KEY (product_sku) REFERENCES products (product_sku)
                )
            ''',
            'inventory': '''
                CREATE TABLE IF NOT EXISTS inventory (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    store_id TEXT,
                    product_sku TEXT,
                    current_stock INTEGER DEFAULT 0,
                    reorder_point INTEGER DEFAULT 10,
                    daily_sales_velocity DECIMAL(5,2) DEFAULT 0,
                    last_updated TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    UNIQUE(store_id, product_sku),
                    FOREIGN KEY (store_id) REFERENCES stores (store_id),
                    FOREIGN KEY (product_sku) REFERENCES products (product_sku)
                )
            ''',
            'competitor_prices': '''
                CREATE TABLE IF NOT EXISTS competitor_prices (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_sku TEXT,
                    competitor_name TEXT,
                    price DECIMAL(10,2),
                    stock_status TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_sku) REFERENCES products (product_sku)
                )
            ''',
            'social_mentions': '''
                CREATE TABLE IF NOT EXISTS social_mentions (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    product_sku TEXT,
                    platform TEXT DEFAULT 'general',
                    mention_count INTEGER DEFAULT 0,
                    sentiment_score DECIMAL(3,2),
                    sentiment_category TEXT,
                    trending_topics TEXT,
                    sample_mentions TEXT,
                    timeframe TEXT DEFAULT '24h',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_sku) REFERENCES products (product_sku)
                )
            ''',
            'economic_indicators': '''
                CREATE TABLE IF NOT EXISTS economic_indicators (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    indicator_name TEXT NOT NULL,
                    indicator_key TEXT NOT NULL,
                    value DECIMAL(10,2),
                    trend TEXT,
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'supply_chain_events': '''
                CREATE TABLE IF NOT EXISTS supply_chain_events (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    event_id TEXT UNIQUE,
                    event_type TEXT NOT NULL,
                    description TEXT,
                    severity TEXT,
                    affected_categories TEXT,
                    estimated_duration TEXT,
                    status TEXT DEFAULT 'active',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'demand_forecasts': '''
                CREATE TABLE IF NOT EXISTS demand_forecasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    forecast_id TEXT UNIQUE,
                    product_sku TEXT,
                    store_id TEXT,
                    forecast_horizon TEXT,
                    forecast_value DECIMAL(10,2),
                    confidence_level DECIMAL(3,2),
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (product_sku) REFERENCES products (product_sku),
                    FOREIGN KEY (store_id) REFERENCES stores (store_id)
                )
            ''',
            'webhook_logs': '''
                CREATE TABLE IF NOT EXISTS webhook_logs (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    webhook_type TEXT NOT NULL,
                    payload TEXT,
                    status TEXT DEFAULT 'received',
                    timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            ''',
            'anomaly_log': '''
                CREATE TABLE IF NOT EXISTS anomaly_log (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    anomaly_id TEXT UNIQUE,
                    type TEXT NOT NULL,
                    severity TEXT NOT NULL,
                    product_sku TEXT,
                    store_id TEXT,
                    actual_value DECIMAL(10,2),
                    expected_value DECIMAL(10,2),
                    confidence_score DECIMAL(3,2),
                    z_score DECIMAL(5,2),
                    description TEXT,
                    status TEXT DEFAULT 'active',
                    detected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    resolved_at TIMESTAMP,
                    metadata TEXT
                )
            ''',
            'anomaly_rules': '''
                CREATE TABLE IF NOT EXISTS anomaly_rules (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    rule_name TEXT UNIQUE NOT NULL,
                    rule_type TEXT NOT NULL,
                    parameters TEXT NOT NULL,
                    is_active BOOLEAN DEFAULT 1,
                    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                )
            '''
        }
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            for table_name, schema in schemas.items():
                cursor.execute(schema)
            
            conn.commit()
            print(f"Database initialized with tables: {list(schemas.keys())}")
    
    def insert_sample_data(self):
        """Insert comprehensive sample data for all endpoints"""
        
        sample_products = [
            ("APPLE_IPHONE15_128GB", "iPhone 15 128GB", 899.99, "smartphones", "Apple"),
            ("SAMSUNG_GALAXY_S24", "Galaxy S24", 799.99, "smartphones", "Samsung"),
            ("SONY_PS5_CONSOLE", "PlayStation 5", 499.99, "gaming", "Sony"),
            ("APPLE_AIRPODS_PRO2", "AirPods Pro 2", 249.99, "accessories", "Apple")
        ]
        
        sample_stores = [
            ("DE_BERLIN_001", "TechFlow Berlin Mitte", "Berlin, Germany"),
            ("DE_MUNICH_002", "TechFlow Munich Center", "Munich, Germany"),
            ("FR_PARIS_001", "TechFlow Paris Chatelet", "Paris, France")
        ]
        
        sample_rules = [
            ("sales_zscore_threshold", "statistical", '{"zscore_threshold": 2.5, "window_days": 7}'),
            ("inventory_critical_stock", "inventory", '{"critical_days": 3, "reorder_multiplier": 1.5}'),
            ("price_competitiveness", "pricing", '{"max_difference_percent": 15, "min_competitors": 2}'),
            ("sentiment_decline", "sentiment", '{"negative_threshold": -0.5, "change_threshold": 0.3}'),
            ("demand_spike", "sales", '{"spike_multiplier": 3.0, "comparison_days": 14}')
        ]
        
        competitors = ["amazon_de", "mediamarkt", "saturn", "otto", "cyberport"]
        
        economic_indicators_data = [
            ("consumer_confidence_germany", "Consumer Confidence Germany"),
            ("unemployment_rate_eu", "Unemployment Rate EU"),
            ("retail_sales_index", "Retail Sales Index"),
            ("consumer_price_index", "Consumer Price Index")
        ]
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            # Insert products
            cursor.executemany(
                "INSERT OR REPLACE INTO products (product_sku, name, price, category, brand) VALUES (?, ?, ?, ?, ?)",
                sample_products
            )
            
            # Insert stores
            cursor.executemany(
                "INSERT OR REPLACE INTO stores (store_id, name, location) VALUES (?, ?, ?)",
                sample_stores
            )
            
            # Insert inventory data
            for store_id, _, _ in sample_stores:
                for product_sku, _, price, _, _ in sample_products:
                    cursor.execute('''
                        INSERT OR REPLACE INTO inventory 
                        (store_id, product_sku, current_stock, reorder_point, daily_sales_velocity, last_updated)
                        VALUES (?, ?, ?, ?, ?, ?)
                    ''', (
                        store_id, product_sku, 
                        random.randint(10, 100),  # random stock
                        random.randint(5, 20),    # random reorder point
                        round(random.uniform(2, 8), 2),     # random velocity
                        datetime.now()
                    ))
            
            # Insert competitor pricing data
            for product_sku, _, our_price, _, _ in sample_products:
                for competitor in competitors:
                    # Competitor prices vary ±20% from our price
                    variation = random.uniform(-0.2, 0.2)
                    competitor_price = our_price * (1 + variation)
                    
                    cursor.execute('''
                        INSERT OR REPLACE INTO competitor_prices 
                        (product_sku, competitor_name, price, stock_status, timestamp)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        product_sku,
                        competitor,
                        round(competitor_price, 2),
                        random.choice(["in_stock", "low_stock", "out_of_stock"]),
                        datetime.now()
                    ))
            
            # Insert social media data
            for product_sku, _, _, _, brand in sample_products:
                base_mentions = {"Apple": 150, "Samsung": 100, "Sony": 75}.get(brand, 50)
                mentions_count = max(0, int(np.random.poisson(base_mentions)))
                sentiment_score = round(np.random.beta(2, 2) * 2 - 1, 3)
                sentiment_category = "positive" if sentiment_score > 0.1 else "negative" if sentiment_score < -0.1 else "neutral"
                
                trending_topics = json.dumps(random.sample(["price", "quality", "features", "availability", "design", "performance"], 2))
                sample_mentions_data = json.dumps([
                    f"Just got the new {brand} device, loving it!",
                    f"Thinking about upgrading to {product_sku}...",
                    f"Has anyone tried the {brand} latest model?",
                    f"Great deal on {product_sku} at TechFlow!"
                ])
                
                cursor.execute('''
                    INSERT OR REPLACE INTO social_mentions 
                    (product_sku, mention_count, sentiment_score, sentiment_category, 
                     trending_topics, sample_mentions, timeframe, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product_sku,
                    mentions_count,
                    sentiment_score,
                    sentiment_category,
                    trending_topics,
                    sample_mentions_data,
                    '24h',
                    datetime.now()
                ))
            
            # Insert economic indicators
            for indicator_key, indicator_name in economic_indicators_data:
                if indicator_key == "consumer_confidence_germany":
                    value = round(random.uniform(95, 110), 1)
                elif indicator_key == "unemployment_rate_eu":
                    value = round(random.uniform(6.5, 8.5), 1)
                elif indicator_key == "retail_sales_index":
                    value = round(random.uniform(98, 115), 1)
                else:  # consumer_price_index
                    value = round(random.uniform(102, 108), 1)
                
                cursor.execute('''
                    INSERT OR REPLACE INTO economic_indicators 
                    (indicator_name, indicator_key, value, trend, timestamp)
                    VALUES (?, ?, ?, ?, ?)
                ''', (
                    indicator_name,
                    indicator_key,
                    value,
                    random.choice(["rising", "falling", "stable"]),
                    datetime.now()
                ))
            
            # Insert supply chain events (30% chance of active events)
            if random.random() < 0.3:
                event_types = [
                    ("shipping_delay", "Port congestion in Hamburg causing 3-5 day delays", "medium", '["smartphones", "laptops"]', "5-7 days"),
                    ("supplier_disruption", "Foxconn facility operating at reduced capacity", "high", '["smartphones"]', "2-3 weeks"),
                    ("raw_material_shortage", "Semiconductor shortage affecting production", "high", '["all_electronics"]', "4-6 weeks"),
                    ("customs_delay", "Brexit-related customs delays at Dover", "medium", '["all_products"]', "3-5 days"),
                    ("weather_disruption", "Storm affecting logistics in Northern Europe", "low", '["all_products"]', "1-2 days")
                ]
                
                selected_events = random.sample(event_types, random.randint(1, 2))
                
                for event_type, description, severity, affected_categories, duration in selected_events:
                    event_id = f"EVENT_{random.randint(1000, 9999)}"
                    
                    cursor.execute('''
                        INSERT INTO supply_chain_events 
                        (event_id, event_type, description, severity, affected_categories, estimated_duration, timestamp)
                        VALUES (?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        event_id,
                        event_type,
                        description,
                        severity,
                        affected_categories,
                        duration,
                        datetime.now()
                    ))
            
            # Insert sample anomaly rules
            cursor.executemany(
                "INSERT OR REPLACE INTO anomaly_rules (rule_name, rule_type, parameters) VALUES (?, ?, ?)",
                sample_rules
            )
            
            # Insert historical sales data for anomaly detection
            self._insert_historical_sales_data(cursor, sample_products, sample_stores)
            
            conn.commit()
            print("Comprehensive sample data inserted successfully")
    
    def _insert_historical_sales_data(self, cursor, products, stores):
        """Insert historical sales data for anomaly detection"""
        
        for days_ago in range(30, 0, -1):
            date = datetime.now() - timedelta(days=days_ago)
            
            for store_id, _, _ in stores:
                for product_sku, _, price, _, brand in products:
                    # Generate normal sales with some variation
                    base_sales = {"Apple": 15, "Samsung": 12, "Sony": 8}.get(brand, 5)
                    
                    # Add day-of-week effect
                    if date.weekday() in [5, 6]:  # Weekend
                        base_sales = int(base_sales * 1.3)
                    
                    quantity = max(0, int(np.random.poisson(base_sales)))
                    
                    # Inject some anomalies in the past week
                    if days_ago <= 7 and random.random() < 0.05:  # 5% chance of anomaly in past week
                        quantity = int(quantity * random.choice([0.2, 3.5]))  # Either very low or very high
                    
                    total_revenue = quantity * price
                    
                    # Apply random promotions
                    promotion = random.choice([None, "SUMMER10", "STUDENT15", None, None, None])
                    if promotion == "SUMMER10":
                        total_revenue *= 0.9
                    elif promotion == "STUDENT15":
                        total_revenue *= 0.85
                    
                    cursor.execute('''
                        INSERT INTO sales 
                        (timestamp, store_id, product_sku, quantity_sold, unit_price, total_revenue,
                         promotion_code, customer_segment, payment_method)
                        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ''', (
                        date.isoformat(),
                        store_id,
                        product_sku,
                        quantity,
                        price,
                        total_revenue,
                        promotion,
                        random.choice(["consumer", "business", "student"]),
                        random.choice(["credit_card", "debit_card", "paypal", "cash"])
                    ))
    
    # ========== DATA RETRIEVAL METHODS ==========
    
    def get_sales_data(self, days_back=30, store_id=None, product_sku=None):
        """Fetch sales data from database"""
        
        query = '''
            SELECT 
                DATE(timestamp) as sales_date,
                product_sku,
                store_id,
                SUM(quantity_sold) as sales_quantity,
                AVG(unit_price) as unit_price,
                SUM(total_revenue) as total_revenue
            FROM sales 
            WHERE timestamp >= datetime('now', '-{} days')
        '''.format(days_back)
        
        params = []
        if store_id:
            query += " AND store_id = ?"
            params.append(store_id)
        if product_sku:
            query += " AND product_sku = ?"
            params.append(product_sku)
            
        query += '''
            GROUP BY DATE(timestamp), product_sku, store_id
            ORDER BY timestamp DESC
        '''
        
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def get_recent_sales_for_api(self, hours_back=1):
        """Get recent sales for the realtime API"""
        
        query = '''
            SELECT *
            FROM sales 
            WHERE timestamp >= datetime('now', '-{} hours')
            ORDER BY timestamp DESC
            LIMIT 50
        '''.format(hours_back)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_inventory_data(self, store_id=None):
        """Fetch inventory data from database"""
        
        if store_id:
            query = '''
                SELECT i.*, p.name as product_name, p.category, p.brand, p.price
                FROM inventory i
                JOIN products p ON i.product_sku = p.product_sku
                WHERE i.store_id = ?
            '''
            params = (store_id,)
        else:
            query = '''
                SELECT i.*, p.name as product_name, p.category, p.brand, p.price
                FROM inventory i
                JOIN products p ON i.product_sku = p.product_sku
            '''
            params = ()
        
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def get_competitor_prices(self):
        """Fetch competitor pricing data from database"""
        
        query = '''
            SELECT 
                cp.product_sku,
                p.price as our_price,
                cp.competitor_name,
                cp.price as competitor_price,
                cp.stock_status,
                cp.timestamp
            FROM competitor_prices cp
            JOIN products p ON cp.product_sku = p.product_sku
            WHERE cp.timestamp >= datetime('now', '-24 hours')
            ORDER BY cp.product_sku, cp.competitor_name
        '''
        
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn)
    
    def get_social_mentions(self, timeframe='24h'):
        """Fetch social media data from database"""
        
        hours_back = {'1h': 1, '24h': 24, '7d': 168}.get(timeframe, 24)
        
        query = '''
            SELECT *
            FROM social_mentions
            WHERE timestamp >= datetime('now', '-{} hours')
            ORDER BY product_sku
        '''.format(hours_back)
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_economic_indicators(self):
        """Fetch economic indicators from database"""
        
        query = '''
            SELECT *
            FROM economic_indicators
            WHERE timestamp >= datetime('now', '-24 hours')
            ORDER BY indicator_key
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    def get_active_supply_chain_events(self):
        """Fetch active supply chain events from database"""
        
        query = '''
            SELECT *
            FROM supply_chain_events
            WHERE status = 'active'
            ORDER BY timestamp DESC
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query)
            return [dict(row) for row in cursor.fetchall()]
    
    # ========== DATA INSERTION METHODS ==========
    
    def insert_sales_transaction(self, transaction_data):
        """Insert a new sales transaction"""
        
        query = '''
            INSERT INTO sales 
            (timestamp, store_id, product_sku, quantity_sold, unit_price, total_revenue, 
             promotion_code, customer_segment, payment_method)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                transaction_data['timestamp'],
                transaction_data['store_id'],
                transaction_data['product_sku'],
                transaction_data['quantity_sold'],
                transaction_data['unit_price'],
                transaction_data['total_revenue'],
                transaction_data.get('promotion_code'),
                transaction_data.get('customer_segment'),
                transaction_data.get('payment_method')
            ))
            conn.commit()
            return cursor.lastrowid
    
    def update_inventory(self, store_id, product_sku, quantity_change):
        """Update inventory levels"""
        
        query = '''
            UPDATE inventory 
            SET current_stock = current_stock + ?,
                last_updated = ?
            WHERE store_id = ? AND product_sku = ?
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (quantity_change, datetime.now(), store_id, product_sku))
            conn.commit()
    
    def insert_demand_forecast(self, forecast_data):
        """Insert demand forecast data"""
        
        # Generate a base forecast ID for the batch
        base_forecast_id = forecast_data.get('batch_id', f"BATCH_{random.randint(1000000, 9999999)}")
        
        query = '''
            INSERT INTO demand_forecasts 
            (forecast_id, product_sku, store_id, forecast_horizon, forecast_value, confidence_level)
            VALUES (?, ?, ?, ?, ?, ?)
        '''
        
        inserted_forecasts = []
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            
            if 'forecasts' in forecast_data:
                for i, forecast in enumerate(forecast_data['forecasts']):
                    # Generate unique forecast_id for each forecast
                    unique_forecast_id = f"{base_forecast_id}_{i+1}"
                    
                    # Handle both 'predicted_demand' and 'forecast_value' field names
                    forecast_value = forecast.get('forecast_value') or forecast.get('predicted_demand', 0)
                    
                    cursor.execute(query, (
                        unique_forecast_id,
                        forecast.get('product_sku'),
                        forecast.get('store_id'),
                        forecast_data.get('forecast_horizon', 'unknown'),
                        forecast_value,
                        forecast.get('confidence_level', 0.5)
                    ))
                    
                    inserted_forecasts.append({
                        'forecast_id': unique_forecast_id,
                        'product_sku': forecast.get('product_sku'),
                        'forecast_value': forecast_value
                    })
            
            conn.commit()
            
        return {
            'base_forecast_id': base_forecast_id,
            'inserted_forecasts': inserted_forecasts,
            'count': len(inserted_forecasts)
        }
    
    def log_webhook(self, webhook_type, payload):
        """Log webhook data"""
        
        query = '''
            INSERT INTO webhook_logs (webhook_type, payload)
            VALUES (?, ?)
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (webhook_type, json.dumps(payload)))
            conn.commit()
            return cursor.lastrowid
    
    def update_competitor_prices(self):
        """Update competitor prices with fresh data"""
        
        # Delete old data (older than 24 hours)
        delete_query = '''
            DELETE FROM competitor_prices 
            WHERE timestamp < datetime('now', '-24 hours')
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(delete_query)
            
            # Get all products
            cursor.execute("SELECT product_sku, price FROM products")
            products = cursor.fetchall()
            
            competitors = ["amazon_de", "mediamarkt", "saturn", "otto", "cyberport"]
            
            # Insert fresh competitor data
            for product in products:
                product_sku = product['product_sku']
                our_price = product['price']
                
                for competitor in competitors:
                    # Prices vary ±20% from our price
                    variation = random.uniform(-0.2, 0.2)
                    competitor_price = our_price * (1 + variation)
                    
                    cursor.execute('''
                        INSERT INTO competitor_prices 
                        (product_sku, competitor_name, price, stock_status, timestamp)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        product_sku,
                        competitor,
                        round(competitor_price, 2),
                        random.choice(["in_stock", "low_stock", "out_of_stock"]),
                        datetime.now()
                    ))
            
            conn.commit()
    
    def update_social_mentions(self):
        """Update social media mentions with fresh data"""
        
        # Delete old data
        delete_query = '''
            DELETE FROM social_mentions 
            WHERE timestamp < datetime('now', '-24 hours')
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(delete_query)
            
            # Get all products
            cursor.execute("SELECT product_sku, brand FROM products")
            products = cursor.fetchall()
            
            for product in products:
                product_sku = product['product_sku']
                brand = product['brand']
                
                base_mentions = {"Apple": 150, "Samsung": 100, "Sony": 75}.get(brand, 50)
                mentions_count = max(0, int(np.random.poisson(base_mentions)))
                sentiment_score = round(np.random.beta(2, 2) * 2 - 1, 3)
                sentiment_category = "positive" if sentiment_score > 0.1 else "negative" if sentiment_score < -0.1 else "neutral"
                
                trending_topics = json.dumps(random.sample(["price", "quality", "features", "availability", "design", "performance"], 2))
                sample_mentions_data = json.dumps([
                    f"Just got the new {brand} device, loving it!",
                    f"Thinking about upgrading to {product_sku}...",
                    f"Has anyone tried the {brand} latest model?",
                    f"Great deal on {product_sku} at TechFlow!"
                ])
                
                cursor.execute('''
                    INSERT INTO social_mentions 
                    (product_sku, mention_count, sentiment_score, sentiment_category, 
                     trending_topics, sample_mentions, timeframe, timestamp)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?)
                ''', (
                    product_sku,
                    mentions_count,
                    sentiment_score,
                    sentiment_category,
                    trending_topics,
                    sample_mentions_data,
                    '24h',
                    datetime.now()
                ))
            
            conn.commit()
    
    def update_economic_indicators(self):
        """Update economic indicators with fresh data"""
        
        try:
            indicators_data = [
                ("consumer_confidence_germany", "Consumer Confidence Germany", round(random.uniform(95, 110), 1)),
                ("unemployment_rate_eu", "Unemployment Rate EU", round(random.uniform(6.5, 8.5), 1)),
                ("retail_sales_index", "Retail Sales Index", round(random.uniform(98, 115), 1)),
                ("consumer_price_index", "Consumer Price Index", round(random.uniform(102, 108), 1))
            ]
            
            with self.get_connection() as conn:
                cursor = conn.cursor()
                
                # Delete old data
                cursor.execute("DELETE FROM economic_indicators WHERE timestamp < datetime('now', '-24 hours')")
                
                for indicator_key, indicator_name, value in indicators_data:
                    cursor.execute('''
                        INSERT INTO economic_indicators 
                        (indicator_name, indicator_key, value, trend, timestamp)
                        VALUES (?, ?, ?, ?, ?)
                    ''', (
                        indicator_name,
                        indicator_key,
                        value,
                        random.choice(["rising", "falling", "stable"]),
                        datetime.now().isoformat()
                    ))
                
                conn.commit()
                print("Economic indicators updated successfully")
                
        except Exception as e:
            print(f"Error updating economic indicators: {str(e)}")
            raise e
    
    # ========== ANOMALY DETECTION METHODS ==========
    
    def log_anomaly(self, anomaly_data):
        """Log detected anomaly to database"""
        
        anomaly_id = f"{anomaly_data['type']}_{anomaly_data.get('product_sku', 'unknown')}_{int(datetime.now().timestamp())}"
        
        query = '''
            INSERT OR REPLACE INTO anomaly_log 
            (anomaly_id, type, severity, product_sku, store_id, actual_value, expected_value,
             confidence_score, z_score, description, metadata)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (
                anomaly_id,
                anomaly_data['type'],
                anomaly_data['severity'],
                anomaly_data.get('product_sku'),
                anomaly_data.get('store_id'),
                anomaly_data.get('actual_value'),
                anomaly_data.get('expected_value'),
                anomaly_data.get('confidence_score', 0.5),
                anomaly_data.get('z_score'),
                anomaly_data.get('description'),
                json.dumps(anomaly_data.get('metadata', {}))
            ))
            conn.commit()
            return anomaly_id
    
    def get_anomaly_history(self, days_back=7, status='all'):
        """Fetch anomaly history from database"""
        
        if status == 'all':
            query = '''
                SELECT * FROM anomaly_log 
                WHERE detected_at >= datetime('now', '-{} days')
                ORDER BY detected_at DESC
            '''.format(days_back)
            params = ()
        else:
            query = '''
                SELECT * FROM anomaly_log 
                WHERE detected_at >= datetime('now', '-{} days') AND status = ?
                ORDER BY detected_at DESC
            '''.format(days_back)
            params = (status,)
        
        with self.get_connection() as conn:
            return pd.read_sql_query(query, conn, params=params)
    
    def resolve_anomaly(self, anomaly_id, resolution_notes=None):
        """Mark an anomaly as resolved"""
        
        query = '''
            UPDATE anomaly_log 
            SET status = 'resolved', resolved_at = ?
            WHERE anomaly_id = ?
        '''
        
        with self.get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute(query, (datetime.now(), anomaly_id))
            conn.commit()
            return cursor.rowcount > 0


class TechFlowAnomalyDetector:
    """Enhanced anomaly detection system"""
    
    def __init__(self, db_manager):
        self.db_manager = db_manager
        self.default_thresholds = {
            'statistical_zscore': 2.5,
            'inventory_critical_days': 3,
            'demand_spike_multiplier': 2.5,
            'price_difference_percent': 15,
            'sentiment_change_threshold': 0.3
        }
    
    def _calculate_zscore(self, values):
        """Calculate Z-scores manually"""
        if len(values) < 2:
            return [0] * len(values)
        
        mean_val = np.mean(values)
        std_val = np.std(values, ddof=1)
        
        if std_val == 0:
            return [0] * len(values)
        
        return [(x - mean_val) / std_val for x in values]
    
    def _calculate_confidence_score(self, anomaly_type, severity, data_quality=1.0):
        """Calculate confidence score for anomaly detection"""
        base_confidence = {
            'high': 0.8,
            'medium': 0.6,
            'low': 0.4
        }.get(severity, 0.5)
        
        type_multiplier = {
            'statistical_outlier': 0.9,
            'inventory_critical': 0.95,
            'demand_spike': 0.8,
            'pricing_anomaly': 0.85,
            'sentiment_anomaly': 0.7
        }.get(anomaly_type, 0.6)
        
        return min(1.0, base_confidence * type_multiplier * data_quality)
    
    def run_full_anomaly_detection(self, persist=True):
        """Run comprehensive anomaly detection across all data sources"""
        
        anomalies = []
        detection_summary = {
            'total_anomalies': 0,
            'high_severity': 0,
            'medium_severity': 0,
            'low_severity': 0,
            'categories': {},
            'confidence_scores': []
        }
        
        try:
            # 1. Sales anomalies
            sales_data = self.db_manager.get_sales_data(days_back=30)
            if not sales_data.empty:
                sales_anomalies = self._detect_sales_anomalies(sales_data)
                anomalies.extend(sales_anomalies)
            
            # 2. Inventory anomalies
            inventory_data = self.db_manager.get_inventory_data()
            if not inventory_data.empty:
                inventory_anomalies = self._detect_inventory_anomalies(inventory_data)
                anomalies.extend(inventory_anomalies)
            
            # Calculate summary statistics
            detection_summary['total_anomalies'] = len(anomalies)
            for anomaly in anomalies:
                severity = anomaly['severity']
                detection_summary[f'{severity}_severity'] += 1
                
                category = anomaly['type']
                detection_summary['categories'][category] = detection_summary['categories'].get(category, 0) + 1
                
                if 'confidence_score' in anomaly:
                    detection_summary['confidence_scores'].append(anomaly['confidence_score'])
            
            # Calculate average confidence
            if detection_summary['confidence_scores']:
                detection_summary['avg_confidence'] = np.mean(detection_summary['confidence_scores'])
            
            # Persist anomalies to database if requested
            if persist:
                for anomaly in anomalies:
                    try:
                        self.db_manager.log_anomaly(anomaly)
                    except Exception as e:
                        print(f"Error persisting anomaly: {e}")
                        
        except Exception as e:
            print(f"Error in anomaly detection: {str(e)}")
            detection_summary['error'] = str(e)
            
        return {
            'anomalies': anomalies,
            'summary': detection_summary,
            'timestamp': datetime.now().isoformat()
        }
    
    def _detect_sales_anomalies(self, sales_data):
        """Detect anomalies in sales data"""
        
        anomalies = []
        zscore_threshold = self.default_thresholds['statistical_zscore']
        spike_multiplier = self.default_thresholds['demand_spike_multiplier']
        
        for (product_sku, store_id), group in sales_data.groupby(['product_sku', 'store_id']):
            if len(group) < 7:
                continue
                
            group = group.sort_values('sales_date')
            sales_values = group['sales_quantity'].values.astype(float)
            
            if len(sales_values) > 3:
                z_scores = self._calculate_zscore(sales_values)
                
                for idx, z_score in enumerate(z_scores):
                    if abs(z_score) > zscore_threshold:
                        row = group.iloc[idx]
                        confidence = self._calculate_confidence_score('statistical_outlier', 
                                                                   'high' if abs(z_score) > 3 else 'medium')
                        
                        anomalies.append({
                            'type': 'statistical_outlier',
                            'severity': 'high' if abs(z_score) > 3 else 'medium',
                            'product_sku': product_sku,
                            'store_id': store_id,
                            'actual_value': float(row['sales_quantity']),
                            'expected_value': float(np.mean(sales_values)),
                            'z_score': float(z_score),
                            'confidence_score': confidence,
                            'date': row['sales_date'],
                            'description': f"Sales quantity {row['sales_quantity']} is {abs(z_score):.1f} standard deviations from normal",
                            'metadata': {
                                'mean': float(np.mean(sales_values)),
                                'std': float(np.std(sales_values)),
                                'data_points': len(sales_values)
                            }
                        })
                
                # Demand spike detection
                if len(sales_values) >= 7:
                    recent_avg = np.mean(sales_values[-3:])
                    historical_avg = np.mean(sales_values[:-3]) if len(sales_values) > 3 else recent_avg
                    
                    if historical_avg > 0 and recent_avg > historical_avg * spike_multiplier:
                        confidence = self._calculate_confidence_score('demand_spike', 'high')
                        
                        anomalies.append({
                            'type': 'demand_spike',
                            'severity': 'high',
                            'product_sku': product_sku,
                            'store_id': store_id,
                            'actual_value': float(recent_avg),
                            'expected_value': float(historical_avg),
                            'spike_ratio': float(recent_avg / historical_avg),
                            'confidence_score': confidence,
                            'description': f"Demand spike detected: {recent_avg:.1f} vs baseline {historical_avg:.1f}"
                        })
        
        return anomalies
    
    def _detect_inventory_anomalies(self, inventory_data):
        """Detect inventory-related anomalies"""
        
        anomalies = []
        critical_days = self.default_thresholds['inventory_critical_days']
        
        for _, row in inventory_data.iterrows():
            current_stock = float(row['current_stock'])
            daily_velocity = float(row['daily_sales_velocity'])
            
            if daily_velocity > 0:
                days_remaining = current_stock / daily_velocity
                
                if days_remaining <= critical_days:
                    confidence = self._calculate_confidence_score('inventory_critical', 'high')
                    
                    anomalies.append({
                        'type': 'low_stock_critical',
                        'severity': 'high',
                        'product_sku': row['product_sku'],
                        'store_id': row['store_id'],
                        'actual_value': float(current_stock),
                        'expected_value': float(critical_days * daily_velocity),
                        'days_remaining': float(days_remaining),
                        'confidence_score': confidence,
                        'description': f"Critical low stock: {days_remaining:.1f} days remaining"
                    })
                
                elif days_remaining > 60:
                    confidence = self._calculate_confidence_score('inventory_critical', 'medium')
                    
                    anomalies.append({
                        'type': 'overstock',
                        'severity': 'medium',
                        'product_sku': row['product_sku'],
                        'store_id': row['store_id'],
                        'actual_value': float(current_stock),
                        'expected_value': float(30 * daily_velocity),
                        'days_remaining': float(days_remaining),
                        'confidence_score': confidence,
                        'description': f"Overstock detected: {days_remaining:.0f} days of inventory"
                    })
        
        return anomalies


# Initialize database and components
db_manager = DatabaseManager(DATABASE_FILE)
anomaly_detector = TechFlowAnomalyDetector(db_manager)

# Background data update service
def background_data_updater():
    """Background service to periodically update simulated data"""
    while True:
        try:
            # Update competitor prices every 30 minutes
            time.sleep(1800)  # 30 minutes
            db_manager.update_competitor_prices()
            
            # Update social mentions every hour
            time.sleep(1800)  # Another 30 minutes = 1 hour total
            db_manager.update_social_mentions()
            
            # Update economic indicators every 6 hours
            if random.random() < 0.1:  # 10% chance every hour = roughly every 6 hours
                db_manager.update_economic_indicators()
                
        except Exception as e:
            print(f"Background update error: {e}")
            time.sleep(300)  # Wait 5 minutes on error

# Start background updater in a separate thread
background_thread = Thread(target=background_data_updater, daemon=True)
background_thread.start()

# ============= ENHANCED API ENDPOINTS WITH FULL DATABASE INTEGRATION =============

@app.route('/api/v1/init/sample-data', methods=['POST'])
def initialize_sample_data():
    """Initialize database with comprehensive sample data"""
    try:
        db_manager.insert_sample_data()
        return jsonify({
            "status": "success",
            "message": "Comprehensive sample data inserted successfully",
            "timestamp": datetime.now().isoformat(),
            "note": "Database now contains sample data for all endpoints"
        })
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/sales/realtime', methods=['GET'])
def get_realtime_sales():
    """Get real-time sales data from database"""
    try:
        # Get recent sales from database
        recent_sales = db_manager.get_recent_sales_for_api(hours_back=1)
        
        # If no recent sales, generate some new ones
        if len(recent_sales) < 5:
            # Generate a few random sales and add to database
            with db_manager.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT store_id FROM stores")
                stores = [row['store_id'] for row in cursor.fetchall()]
                cursor.execute("SELECT product_sku, price FROM products")
                products = {row['product_sku']: row['price'] for row in cursor.fetchall()}
            
            for _ in range(random.randint(5, 10)):
                sku = random.choice(list(products.keys()))
                store_id = random.choice(stores)
                quantity = random.randint(1, 3)
                price = products[sku]
                total_revenue = quantity * price
                
                # Apply promotions
                promotion = random.choice([None, "SUMMER10", "STUDENT15", None, None])
                if promotion == "SUMMER10":
                    total_revenue *= 0.9
                elif promotion == "STUDENT15":
                    total_revenue *= 0.85
                
                transaction_data = {
                    "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 60))).isoformat(),
                    "store_id": store_id,
                    "product_sku": sku,
                    "quantity_sold": quantity,
                    "unit_price": price,
                    "total_revenue": total_revenue,
                    "promotion_code": promotion,
                    "customer_segment": random.choice(["consumer", "business", "student"]),
                    "payment_method": random.choice(["credit_card", "debit_card", "paypal", "cash"])
                }
                
                db_manager.insert_sales_transaction(transaction_data)
                # Update inventory
                db_manager.update_inventory(store_id, sku, -quantity)
            
            # Get updated recent sales
            recent_sales = db_manager.get_recent_sales_for_api(hours_back=1)
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "count": len(recent_sales),
            "sales": recent_sales
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/inventory/levels', methods=['GET'])
def get_inventory_levels():
    """Get current inventory levels from database"""
    try:
        store_id = request.args.get('store_id')
        inventory_df = db_manager.get_inventory_data(store_id)
        
        if store_id == 'all' or store_id is None:
            # Convert to nested dictionary structure like original
            inventory_dict = {}
            for _, row in inventory_df.iterrows():
                store = row['store_id']
                if store not in inventory_dict:
                    inventory_dict[store] = {}
                
                inventory_dict[store][row['product_sku']] = {
                    "current_stock": int(row['current_stock']),
                    "reorder_point": int(row['reorder_point']),
                    "daily_sales_velocity": float(row['daily_sales_velocity']),
                    "last_updated": str(row['last_updated'])
                }
            
            return jsonify({
                "status": "success",
                "timestamp": datetime.now().isoformat(),
                "inventory": inventory_dict
            })
        else:
            # Single store response
            store_inventory = {}
            store_data = inventory_df[inventory_df['store_id'] == store_id]
            
            for _, row in store_data.iterrows():
                store_inventory[row['product_sku']] = {
                    "current_stock": int(row['current_stock']),
                    "reorder_point": int(row['reorder_point']),
                    "daily_sales_velocity": float(row['daily_sales_velocity']),
                    "last_updated": str(row['last_updated'])
                }
            
            if store_inventory:
                return jsonify({
                    "status": "success",
                    "store_id": store_id,
                    "inventory": store_inventory
                })
            else:
                return jsonify({"status": "error", "message": "Store not found"}), 404
                
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/competitors/prices', methods=['GET'])
def get_competitor_prices():
    """Get competitor pricing data from database"""
    try:
        # Update prices if data is stale
        competitor_df = db_manager.get_competitor_prices()
        
        if competitor_df.empty:
            # Generate fresh data if none exists
            db_manager.update_competitor_prices()
            competitor_df = db_manager.get_competitor_prices()
        
        # Group by product and structure the response
        competitor_data = {}
        
        for product_sku, group in competitor_df.groupby('product_sku'):
            our_price = group.iloc[0]['our_price']
            
            competitors = {}
            for _, row in group.iterrows():
                competitors[row['competitor_name']] = {
                    "price": float(row['competitor_price']),
                    "stock_status": row['stock_status'],
                    "last_updated": row['timestamp']
                }
            
            competitor_data[product_sku] = {
                "our_price": float(our_price),
                "competitors": competitors
            }
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "competitor_prices": competitor_data
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/social/mentions', methods=['GET'])
def get_social_mentions():
    """Get social media mentions and sentiment from database"""
    try:
        timeframe = request.args.get('timeframe', '24h')
        
        # Get social data from database
        social_data_list = db_manager.get_social_mentions(timeframe)
        
        if not social_data_list:
            # Generate fresh data if none exists
            db_manager.update_social_mentions()
            social_data_list = db_manager.get_social_mentions(timeframe)
        
        # Structure the response
        social_data = {}
        
        for row in social_data_list:
            product_sku = row['product_sku']
            
            social_data[product_sku] = {
                "mentions_count": int(row['mention_count']),
                "sentiment_score": float(row['sentiment_score']),
                "sentiment_category": row['sentiment_category'],
                "trending_topics": json.loads(row['trending_topics']) if row['trending_topics'] else [],
                "sample_mentions": json.loads(row['sample_mentions']) if row['sample_mentions'] else [],
                "timeframe": row['timeframe'],
                "last_updated": row['timestamp']
            }
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "social_data": social_data
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/economic/indicators', methods=['GET'])
def get_economic_indicators():
    """Get economic indicators from database"""
    try:
        # Get indicators from database
        indicators_list = db_manager.get_economic_indicators()
        
        if not indicators_list:
            # Generate fresh data if none exists
            db_manager.update_economic_indicators()
            indicators_list = db_manager.get_economic_indicators()
        
        # Structure the response
        indicators = {}
        
        for row in indicators_list:
            indicators[row['indicator_key']] = {
                "value": float(row['value']),
                "trend": row['trend'],
                "last_updated": row['timestamp']
            }
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "indicators": indicators
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/events/supply_chain', methods=['GET'])
def get_supply_chain_events():
    """Get supply chain disruption alerts from database"""
    try:
        # Get active events from database
        events_list = db_manager.get_active_supply_chain_events()
        
        # Structure the response
        active_events = []
        
        for row in events_list:
            event_data = {
                "event_id": row['event_id'],
                "type": row['event_type'],
                "description": row['description'],
                "severity": row['severity'],
                "affected_categories": json.loads(row['affected_categories']) if row['affected_categories'] else [],
                "estimated_duration": row['estimated_duration'],
                "timestamp": row['timestamp']
            }
            active_events.append(event_data)
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "active_events": active_events
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/forecasts/demand', methods=['POST'])
def submit_demand_forecast():
    """Endpoint for submitting forecast results - stores in database"""
    try:
        forecast_data = request.json
        
        if not forecast_data or 'forecasts' not in forecast_data:
            return jsonify({
                "status": "error",
                "message": "Missing 'forecasts' field in request body"
            }), 400
        
        # Store forecast in database
        result = db_manager.insert_demand_forecast(forecast_data)
        
        response = {
            "status": "success",
            "message": "Forecasts received and stored in database",
            "base_forecast_id": result['base_forecast_id'],
            "timestamp": datetime.now().isoformat(),
            "inserted_forecasts": result['inserted_forecasts'],
            "total_forecasts": result['count'],
            "forecast_horizon": forecast_data.get("forecast_horizon", "unknown")
        }
        
        return jsonify(response)
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/webhooks/demand_alert', methods=['POST'])
def demand_alert_webhook():
    """Webhook for demand alerts - logs to database"""
    try:
        alert_data = request.json
        
        # Log webhook to database
        log_id = db_manager.log_webhook("demand_alert", alert_data)
        
        print(f"WEBHOOK RECEIVED: Demand Alert - {alert_data}")
        
        return jsonify({
            "status": "received",
            "webhook_log_id": log_id,
            "timestamp": datetime.now().isoformat()
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

# ============= ANOMALY DETECTION ENDPOINTS =============

@app.route('/api/v1/anomalies/detect', methods=['GET'])
def detect_anomalies():
    """Main anomaly detection endpoint using real database data"""
    try:
        persist = request.args.get('persist', 'true').lower() == 'true'
        
        # Run anomaly detection
        detection_results = anomaly_detector.run_full_anomaly_detection(persist=persist)
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "detection_results": detection_results
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e),
            "timestamp": datetime.now().isoformat()
        }), 500

@app.route('/api/v1/anomalies/history', methods=['GET'])
def get_anomaly_history():
    """Get historical anomaly data from database"""
    try:
        days_back = int(request.args.get('days_back', 7))
        status = request.args.get('status', 'all')
        anomaly_type = request.args.get('type')
        severity = request.args.get('severity')
        
        # Get anomaly history
        history_df = db_manager.get_anomaly_history(days_back, status)
        
        # Convert to list and apply filters
        anomalies = []
        for _, row in history_df.iterrows():
            anomaly_dict = {
                "anomaly_id": row['anomaly_id'],
                "type": row['type'],
                "severity": row['severity'],
                "product_sku": row['product_sku'],
                "store_id": row['store_id'],
                "actual_value": float(row['actual_value']) if row['actual_value'] else None,
                "expected_value": float(row['expected_value']) if row['expected_value'] else None,
                "confidence_score": float(row['confidence_score']) if row['confidence_score'] else None,
                "z_score": float(row['z_score']) if row['z_score'] else None,
                "description": row['description'],
                "status": row['status'],
                "detected_at": row['detected_at'],
                "resolved_at": row['resolved_at'],
                "metadata": json.loads(row['metadata']) if row['metadata'] else {}
            }
            
            # Apply filters
            if anomaly_type and anomaly_dict['type'] != anomaly_type:
                continue
            if severity and anomaly_dict['severity'] != severity:
                continue
                
            anomalies.append(anomaly_dict)
        
        # Calculate summary
        summary = {
            "total_anomalies": len(anomalies),
            "active_anomalies": len([a for a in anomalies if a['status'] == 'active']),
            "resolved_anomalies": len([a for a in anomalies if a['status'] == 'resolved']),
            "severity_breakdown": {
                "high": len([a for a in anomalies if a['severity'] == 'high']),
                "medium": len([a for a in anomalies if a['severity'] == 'medium']),
                "low": len([a for a in anomalies if a['severity'] == 'low'])
            }
        }
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "filters_applied": {
                "days_back": days_back,
                "status": status,
                "type": anomaly_type,
                "severity": severity
            },
            "summary": summary,
            "anomalies": anomalies
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/anomalies/<anomaly_id>/resolve', methods=['POST'])
def resolve_anomaly(anomaly_id):
    """Mark an anomaly as resolved in database"""
    try:
        success = db_manager.resolve_anomaly(anomaly_id)
        
        if success:
            return jsonify({
                "status": "success",
                "message": f"Anomaly {anomaly_id} marked as resolved",
                "timestamp": datetime.now().isoformat()
            })
        else:
            return jsonify({
                "status": "error",
                "message": "Anomaly not found or already resolved"
            }), 404
            
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/anomalies/summary', methods=['GET'])
def get_anomaly_summary():
    """Get real-time anomaly summary dashboard from database"""
    try:
        recent_anomalies = db_manager.get_anomaly_history(days_back=1, status='active')
        
        total_active = len(recent_anomalies)
        high_severity = len(recent_anomalies[recent_anomalies['severity'] == 'high'])
        medium_severity = len(recent_anomalies[recent_anomalies['severity'] == 'medium'])
        low_severity = len(recent_anomalies[recent_anomalies['severity'] == 'low'])
        
        # Type breakdown
        type_counts = recent_anomalies['type'].value_counts().to_dict() if not recent_anomalies.empty else {}
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "summary": {
                "total_active_anomalies": total_active,
                "severity_breakdown": {
                    "high": high_severity,
                    "medium": medium_severity,
                    "low": low_severity
                },
                "type_breakdown": type_counts
            }
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/anomalies/sales', methods=['GET'])
def detect_sales_anomalies():
    """Detect only sales anomalies from database"""
    
    try:
        days_back = int(request.args.get('days_back', 30))
        sales_df = db_manager.get_sales_data(days_back=days_back)
        
        sales_anomalies = anomaly_detector._detect_sales_anomalies(sales_df)
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "anomaly_type": "sales",
            "records_processed": len(sales_df),
            "anomalies_found": len(sales_anomalies),
            "anomalies": sales_anomalies
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

@app.route('/api/v1/anomalies/inventory', methods=['GET'])
def detect_inventory_anomalies():
    """Detect only inventory anomalies from database"""
    
    try:
        inventory_df = db_manager.get_inventory_data()
        
        inventory_anomalies = anomaly_detector._detect_inventory_anomalies(inventory_df)
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "anomaly_type": "inventory",
            "records_processed": len(inventory_df),
            "anomalies_found": len(inventory_anomalies),
            "anomalies": inventory_anomalies
        })
        
    except Exception as e:
        return jsonify({"status": "error", "message": str(e)}), 500

# ============= ADDITIONAL DATABASE ENDPOINTS =============

@app.route('/api/v1/database/status', methods=['GET'])
def get_database_status():
    """Get database status and table information"""
    try:
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Get table information
            cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
            tables = [row['name'] for row in cursor.fetchall()]
            
            table_info = {}
            for table in tables:
                cursor.execute(f"SELECT COUNT(*) as count FROM {table}")
                count = cursor.fetchone()['count']
                table_info[table] = {"record_count": count}
        
        return jsonify({
            "status": "success",
            "database_file": DATABASE_FILE,
            "timestamp": datetime.now().isoformat(),
            "tables": table_info,
            "total_tables": len(tables)
        })
        
    except Exception as e:
        return jsonify({
            "status": "error",
            "message": str(e)
        }), 500

@app.route('/api/v1/database/refresh', methods=['POST'])
def refresh_database_data():
    """Refresh all database data with new simulated data"""
    try:
        print("Starting database refresh...")
        
        # Check if tables exist and have the right schema
        with db_manager.get_connection() as conn:
            cursor = conn.cursor()
            
            # Check if economic_indicators table has the right columns
            try:
                cursor.execute("PRAGMA table_info(economic_indicators)")
                columns = [row[1] for row in cursor.fetchall()]
                if 'indicator_key' not in columns:
                    print("Economic indicators table needs schema fix...")
                    # Recreate the table
                    cursor.execute("DROP TABLE IF EXISTS economic_indicators")
                    cursor.execute('''
                        CREATE TABLE economic_indicators (
                            id INTEGER PRIMARY KEY AUTOINCREMENT,
                            indicator_name TEXT NOT NULL,
                            indicator_key TEXT NOT NULL,
                            value DECIMAL(10,2),
                            trend TEXT,
                            timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                        )
                    ''')
                    conn.commit()
                    print("✓ Fixed economic_indicators schema")
            except Exception as e:
                print(f"Schema check error: {e}")
        
        # Update all dynamic data
        print("Updating competitor prices...")
        db_manager.update_competitor_prices()
        
        print("Updating social mentions...")
        db_manager.update_social_mentions()
        
        print("Updating economic indicators...")
        db_manager.update_economic_indicators()
        
        return jsonify({
            "status": "success",
            "message": "Database data refreshed successfully",
            "timestamp": datetime.now().isoformat(),
            "updated": ["competitor_prices", "social_mentions", "economic_indicators"]
        })
        
    except Exception as e:
        error_msg = str(e)
        print(f"Error in refresh_database_data: {error_msg}")
        return jsonify({
            "status": "error",
            "message": error_msg,
            "timestamp": datetime.now().isoformat(),
            "suggestion": "Try database reset: POST /api/v1/database/reset"
        }), 500

if __name__ == '__main__':
    print("Starting Enhanced TechFlow API with Complete SQLite3 Database Integration...")
    print(f"Database: {DATABASE_FILE}")
    print("\nRequired libraries: pip install flask pandas numpy")
    
    print("\n=== AVAILABLE API ENDPOINTS ===")
    print("\n📊 Standard Data Endpoints (All Database-Backed):")
    print("- POST /api/v1/init/sample-data (initialize database)")
    print("- GET  /api/v1/sales/realtime (real sales data)")
    print("- GET  /api/v1/inventory/levels (real inventory data)")
    print("- GET  /api/v1/competitors/prices (database competitor prices)")
    print("- GET  /api/v1/social/mentions (database social media data)")
    print("- GET  /api/v1/economic/indicators (database economic data)")
    print("- GET  /api/v1/events/supply_chain (database supply chain events)")
    print("- POST /api/v1/forecasts/demand (stores forecasts in database)")
    print("- POST /webhooks/demand_alert (logs webhooks in database)")
    
    print("\n🚨 Anomaly Detection Endpoints:")
    print("- GET  /api/v1/anomalies/detect (comprehensive detection)")
    print("- GET  /api/v1/anomalies/history (historical anomalies)")
    print("- POST /api/v1/anomalies/<id>/resolve (resolve anomaly)")
    print("- GET  /api/v1/anomalies/summary (dashboard summary)")
    print("- GET  /api/v1/anomalies/sales (sales-specific)")
    print("- GET  /api/v1/anomalies/inventory (inventory-specific)")
    
    print("\n🗄️ Database Management Endpoints:")
    print("- GET  /api/v1/database/status (database info)")
    print("- POST /api/v1/database/refresh (refresh all data)")
    
    print("\n🔧 First time setup:")
    print("1. Run: curl -X POST http://localhost:5000/api/v1/init/sample-data")
    print("2. Test: curl http://localhost:5000/api/v1/database/status")
    print("3. Test: curl http://localhost:5000/api/v1/anomalies/detect")
    
    print("\n📈 All endpoints now use real SQLite3 database!")
    print("Background data updater running to simulate fresh data...")
    
    if __name__ == '__main__':
        import os
        port = int(os.environ.get('PORT', 5000))
        app.run(debug=False, host='0.0.0.0', port=port)