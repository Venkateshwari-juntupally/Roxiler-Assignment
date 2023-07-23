from flask import Flask, jsonify, request
import requests
import mysql.connector

app = Flask(__name__)

# MySQL database connection settings
db_config = {
    'host': 'localhost',
    'user': 'root',
    'password': 'Venky@123',
    'database': 'djangocrmdb',
}

# Initialize database table structure
def initialize_database():
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS product_transaction (
            id INT AUTO_INCREMENT PRIMARY KEY,
            dateOfSale DATE,
            category VARCHAR(100),
            price FLOAT,
            sold BOOLEAN
        )
    """)
    connection.commit()
    connection.close()

# Fetch JSON from the third-party API and initialize the database with seed data
def fetch_and_initialize_data():
    url = "https://s3.amazonaws.com/roxiler.com/product_transaction.json"
    response = requests.get(url)
    if response.status_code == 200:
        data = response.json()
        connection = mysql.connector.connect(**db_config)
        cursor = connection.cursor()
        for item in data:
            cursor.execute("""
                INSERT INTO product_transaction (dateOfSale, category, price, sold)
                VALUES (%s, %s, %s, %s)
            """, (item['dateOfSale'], item['category'], item['price'], item['sold']))
        connection.commit()
        connection.close()

# API for statistics
@app.route('/api/statistics', methods=['GET'])
def get_statistics():
    month = request.args.get('month')
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    # Total sale amount of selected month
    cursor.execute("""
        SELECT SUM(price) FROM product_transaction
        WHERE MONTH(dateOfSale) = %s
    """, (month,))
    total_sale_amount = cursor.fetchone()[0] or 0

    # Total number of sold items of selected month
    cursor.execute("""
        SELECT COUNT(*) FROM product_transaction
        WHERE MONTH(dateOfSale) = %s AND sold = 1
    """, (month,))
    total_sold_items = cursor.fetchone()[0] or 0

    # Total number of not sold items of selected month
    cursor.execute("""
        SELECT COUNT(*) FROM product_transaction
        WHERE MONTH(dateOfSale) = %s AND sold = 0
    """, (month,))
    total_not_sold_items = cursor.fetchone()[0] or 0

    connection.close()

    return jsonify({
        'total_sale_amount': total_sale_amount,
        'total_sold_items': total_sold_items,
        'total_not_sold_items': total_not_sold_items
    })

# API for bar chart
@app.route('/api/bar_chart', methods=['GET'])
def get_bar_chart():
    month = request.args.get('month')
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    price_ranges = {
        '0 - 100': (0, 100),
        '101 - 200': (101, 200),
        '201 - 300': (201, 300),
        '301 - 400': (301, 400),
        '401 - 500': (401, 500),
        '501 - 600': (501, 600),
        '601 - 700': (601, 700),
        '701 - 800': (701, 800),
        '801 - 900': (801, 900),
        '901 - above': (901, float('inf'))
    }

    result = {}
    for label, (lower, upper) in price_ranges.items():
        cursor.execute("""
            SELECT COUNT(*) FROM product_transaction
            WHERE MONTH(dateOfSale) = %s AND price >= %s AND price <= %s
        """, (month, lower, upper))
        count = cursor.fetchone()[0] or 0
        result[label] = count

    connection.close()

    return jsonify(result)

# API for pie chart
@app.route('/api/pie_chart', methods=['GET'])
def get_pie_chart():
    month = request.args.get('month')
    connection = mysql.connector.connect(**db_config)
    cursor = connection.cursor()

    cursor.execute("""
        SELECT category, COUNT(*) FROM product_transaction
        WHERE MONTH(dateOfSale) = %s
        GROUP BY category
    """, (month,))
    data = cursor.fetchall()
    result = {category: count for category, count in data}

    connection.close()

    return jsonify(result)

# API to fetch combined data from all APIs
@app.route('/api/combined_data', methods=['GET'])
def get_combined_data():
    month = request.args.get('month')

    # Fetching statistics
    statistics_data = get_statistics().json

    # Fetching bar chart data
    bar_chart_data = get_bar_chart().json

    # Fetching pie chart data
    pie_chart_data = get_pie_chart().json

    combined_data = {
        'statistics': statistics_data,
        'bar_chart': bar_chart_data,
        'pie_chart': pie_chart_data
    }

    return jsonify(combined_data)

if __name__ == 'main':
    initialize_database()
    fetch_and_initialize_data()
    app.run()
