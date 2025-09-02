from flask import Flask, jsonify, request
from datetime import datetime, timedelta
import random
import json
import numpy as np
from threading import Thread
import time

app = Flask(__name__)

# Simulated database
class MockDatabase:
    def __init__(self):
        self.sales_data = []
        self.inventory_data = {}
        self.competitor_prices = {}
        self.social_mentions = {}
        self.economic_data = {}
        
        # Initialize with some data
        self._initialize_data()
    
    def _initialize_data(self):
        """Initialize with baseline data"""
        
        # Products
        self.products = {
            "APPLE_IPHONE15_128GB": {"price": 899.99, "category": "smartphones", "brand": "Apple"},
            "SAMSUNG_GALAXY_S24": {"price": 799.99, "category": "smartphones", "brand": "Samsung"},
            "SONY_PS5_CONSOLE": {"price": 499.99, "category": "gaming", "brand": "Sony"},
            "APPLE_AIRPODS_PRO2": {"price": 249.99, "category": "accessories", "brand": "Apple"}
        }
        
        # Initialize inventory levels
        stores = ["DE_BERLIN_001", "DE_MUNICH_002", "FR_PARIS_001"]
        for store in stores:
            self.inventory_data[store] = {}
            for sku in self.products.keys():
                self.inventory_data[store][sku] = {
                    "current_stock": random.randint(10, 100),
                    "reorder_point": random.randint(5, 20),
                    "last_updated": datetime.now().isoformat()
                }

db = MockDatabase()

@app.route('/api/v1/sales/realtime', methods=['GET'])
def get_realtime_sales():
    """Simulate real-time sales data"""
    
    # Generate random sales for the last hour
    sales_events = []
    
    for _ in range(random.randint(5, 20)):  # 5-20 sales in the last hour
        sku = random.choice(list(db.products.keys()))
        product = db.products[sku]
        
        sale = {
            "timestamp": (datetime.now() - timedelta(minutes=random.randint(0, 60))).isoformat(),
            "store_id": random.choice(["DE_BERLIN_001", "DE_MUNICH_002", "FR_PARIS_001"]),
            "product_sku": sku,
            "quantity_sold": random.randint(1, 3),
            "unit_price": product["price"],
            "promotion_code": random.choice([None, "SUMMER10", "STUDENT15", None, None]),
            "customer_segment": random.choice(["consumer", "business", "student"]),
            "payment_method": random.choice(["credit_card", "debit_card", "paypal", "cash"])
        }
        
        sale["total_revenue"] = sale["quantity_sold"] * sale["unit_price"]
        if sale["promotion_code"] == "SUMMER10":
            sale["total_revenue"] *= 0.9
        elif sale["promotion_code"] == "STUDENT15":
            sale["total_revenue"] *= 0.85
        
        sales_events.append(sale)
    
    return jsonify({
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "count": len(sales_events),
        "sales": sales_events
    })

@app.route('/api/v1/inventory/levels', methods=['GET'])
def get_inventory_levels():
    """Get current inventory levels"""
    
    store_id = request.args.get('store_id', 'all')
    
    if store_id == 'all':
        # Simulate small random changes in inventory
        for store in db.inventory_data:
            for sku in db.inventory_data[store]:
                # Random small inventory changes
                change = random.randint(-2, 0)  # Sales reduce inventory
                db.inventory_data[store][sku]["current_stock"] = max(0, 
                    db.inventory_data[store][sku]["current_stock"] + change)
                db.inventory_data[store][sku]["last_updated"] = datetime.now().isoformat()
        
        return jsonify({
            "status": "success",
            "timestamp": datetime.now().isoformat(),
            "inventory": db.inventory_data
        })
    
    else:
        if store_id in db.inventory_data:
            return jsonify({
                "status": "success",
                "store_id": store_id,
                "inventory": db.inventory_data[store_id]
            })
        else:
            return jsonify({"status": "error", "message": "Store not found"}), 404

@app.route('/api/v1/competitors/prices', methods=['GET'])
def get_competitor_prices():
    """Get competitor pricing data"""
    
    competitors = ["amazon_de", "mediamarkt", "saturn"]
    competitor_data = {}
    
    for sku, product in db.products.items():
        competitor_data[sku] = {
            "our_price": product["price"],
            "competitors": {}
        }
        
        for competitor in competitors:
            # Competitor prices vary ±15% from our price
            variation = random.uniform(-0.15, 0.15)
            competitor_price = product["price"] * (1 + variation)
            
            competitor_data[sku]["competitors"][competitor] = {
                "price": round(competitor_price, 2),
                "stock_status": random.choice(["in_stock", "low_stock", "out_of_stock"]),
                "last_updated": datetime.now().isoformat()
            }
    
    return jsonify({
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "competitor_prices": competitor_data
    })

@app.route('/api/v1/social/mentions', methods=['GET'])
def get_social_mentions():
    """Get social media mentions and sentiment"""
    
    timeframe = request.args.get('timeframe', '24h')
    
    social_data = {}
    
    for sku, product in db.products.items():
        
        # Base mentions based on brand popularity
        base_mentions = {"Apple": 150, "Samsung": 100, "Sony": 75}.get(product["brand"], 50)
        
        # Add some randomness
        mentions_count = max(0, int(np.random.poisson(base_mentions)))
        
        # Sentiment score (-1 to 1, slightly positive bias)
        sentiment_score = np.random.beta(2, 2) * 2 - 1
        
        # Generate sample mentions
        sample_mentions = [
            f"Just got the new {product['brand']} device, loving it!",
            f"Thinking about upgrading to {sku}...",
            f"Has anyone tried the {product['brand']} latest model?",
            f"Great deal on {sku} at TechFlow!"
        ]
        
        social_data[sku] = {
            "mentions_count": mentions_count,
            "sentiment_score": round(sentiment_score, 3),
            "sentiment_category": "positive" if sentiment_score > 0.1 else "negative" if sentiment_score < -0.1 else "neutral",
            "trending_topics": random.sample(["price", "quality", "features", "availability"], 2),
            "sample_mentions": random.sample(sample_mentions, min(3, len(sample_mentions))),
            "timeframe": timeframe,
            "last_updated": datetime.now().isoformat()
        }
    
    return jsonify({
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "social_data": social_data
    })

@app.route('/api/v1/economic/indicators', methods=['GET'])
def get_economic_indicators():
    """Get economic indicators"""
    
    # Simulate current economic conditions
    indicators = {
        "consumer_confidence_germany": {
            "value": round(random.uniform(95, 110), 1),
            "trend": random.choice(["rising", "falling", "stable"]),
            "last_updated": datetime.now().isoformat()
        },
        "unemployment_rate_eu": {
            "value": round(random.uniform(6.5, 8.5), 1),
            "trend": random.choice(["rising", "falling", "stable"]),
            "last_updated": datetime.now().isoformat()
        },
        "retail_sales_index": {
            "value": round(random.uniform(98, 115), 1),
            "trend": random.choice(["rising", "falling", "stable"]),
            "last_updated": datetime.now().isoformat()
        },
        "consumer_price_index": {
            "value": round(random.uniform(102, 108), 1),
            "trend": random.choice(["rising", "falling", "stable"]),
            "last_updated": datetime.now().isoformat()
        }
    }
    
    return jsonify({
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "indicators": indicators
    })

@app.route('/api/v1/events/supply_chain', methods=['GET'])
def get_supply_chain_events():
    """Get supply chain disruption alerts"""
    
    # Simulate random supply chain events
    events = []
    
    if random.random() < 0.3:  # 30% chance of an event
        event_types = [
            {
                "type": "shipping_delay",
                "description": "Port congestion in Hamburg causing 3-5 day delays",
                "severity": "medium",
                "affected_categories": ["smartphones", "laptops"],
                "estimated_duration": "5-7 days"
            },
            {
                "type": "supplier_disruption", 
                "description": "Foxconn facility operating at reduced capacity",
                "severity": "high",
                "affected_categories": ["smartphones"],
                "estimated_duration": "2-3 weeks"
            },
            {
                "type": "raw_material_shortage",
                "description": "Semiconductor shortage affecting production",
                "severity": "high", 
                "affected_categories": ["all_electronics"],
                "estimated_duration": "4-6 weeks"
            }
        ]
        
        event = random.choice(event_types)
        event["timestamp"] = datetime.now().isoformat()
        event["event_id"] = f"EVENT_{random.randint(1000, 9999)}"
        events.append(event)
    
    return jsonify({
        "status": "success",
        "timestamp": datetime.now().isoformat(),
        "active_events": events
    })

@app.route('/api/v1/forecasts/demand', methods=['POST'])
def submit_demand_forecast():
    """Endpoint for submitting forecast results (for testing automation output)"""
    
    forecast_data = request.json
    
    # Simulate storing the forecast
    response = {
        "status": "success",
        "message": "Forecast received and stored",
        "forecast_id": f"FORECAST_{random.randint(10000, 99999)}",
        "timestamp": datetime.now().isoformat(),
        "received_products": len(forecast_data.get("forecasts", [])),
        "forecast_horizon": forecast_data.get("forecast_horizon", "unknown")
    }
    
    return jsonify(response)

# Webhook simulation endpoint
@app.route('/webhooks/demand_alert', methods=['POST'])
def demand_alert_webhook():
    """Simulate webhook for demand alerts"""
    
    alert_data = request.json
    
    print(f"WEBHOOK RECEIVED: Demand Alert - {alert_data}")
    
    return jsonify({"status": "received", "timestamp": datetime.now().isoformat()})

# Background data simulation
def simulate_data_changes():
    """Background thread to simulate realistic data changes"""
    
    while True:
        time.sleep