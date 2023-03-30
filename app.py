from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import json
import time
from datetime import datetime, timedelta
import pandas as pd
from google.cloud import bigquery
import pandas_gbq

app = Flask(__name__)
CORS(app)

### KUCOIN

@app.route('/api/kucoin/margin/currencies')
def get_currencies():
    response = requests.get('https://api.kucoin.com/api/v1/currencies')
    data=response.json()
    currencies = []
    for datapoint in data['data']:
        if datapoint['isMarginEnabled']:
            currencies.append(datapoint['currency'])

    currencies.sort()
    return currencies

@app.route('/api/kucoin/margin/current')
def test():

    project_id='data-warehouse-course-ps'

    sql = """
    SELECT *
    FROM test_set.kcs_loans
    WHERE timestamp = (
    SELECT MAX(timestamp)
    FROM test_set.kcs_loans)
    ORDER BY coin;
    """

    sql2 = """
    SELECT *
    FROM test_set.kcs_prices
    WHERE timestamp = (
    SELECT MAX(timestamp)
    FROM test_set.kcs_prices)
    ORDER BY coin;
    """

    NO_LOANS = -1

    threshold = request.args.get('threshold', 2000)
    df_loans = pandas_gbq.read_gbq(sql, project_id=project_id)
    df_prices = pandas_gbq.read_gbq(sql2, project_id=project_id)
    marginal_rates = []
    for index, row in df_prices.iterrows():
        df = df_loans[df_loans['coin']==row['coin']]
        df = df.sort_values(by=['dailyIntRate'])
        price = float(row['price'])
        total=0
        success=False

        for subindex, subrow in df.iterrows():
            total+=float(subrow['size'])*price
            if total>threshold:
                marginal_rates.append([row['coin'],subrow['dailyIntRate']])
                success=True
                break
        if not success:
            marginal_rates.append([row['coin'], NO_LOANS])
    
    return jsonify(marginal_rates)

@app.route('/api/kucoin/margin/historical', methods=['GET'])
def get_kcs_margin_historical():
    coin = request.args.get("coin", default="BTC")
    threshold = float(request.args.get("threshold", default="1"))

    project_id = 'data-warehouse-course-ps'

    sql = f"""
    DECLARE threshold FLOAT64 DEFAULT {threshold};

    WITH ranked_data AS (
        SELECT *,
            SUM(CAST(size AS FLOAT64)) OVER (PARTITION BY coin, timestamp ORDER BY CAST(dailyIntRate AS FLOAT64)) AS cumulative_size
        FROM `test_set.kcs_loans`
        WHERE coin = '{coin}'
        AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 3 DAY)
    )
    SELECT timestamp, CAST(dailyIntRate AS FLOAT64) as dailyIntRate
    FROM ranked_data
    WHERE cumulative_size >= threshold
    QUALIFY ROW_NUMBER() OVER (PARTITION BY timestamp ORDER BY CAST(dailyIntRate AS FLOAT64)) = 1
    ORDER BY timestamp;
    """

    results = pandas_gbq.read_gbq(sql, project_id=project_id)
    print(results)

    return "great success"

### TEST DATA

@app.route('/api/interest-rates', methods=['GET'])
def get_interest_rates():
    # Load the JSON data from the file when the route is called
    with open('interest_rate_data.json', 'r') as f:
        interest_rate_data = json.load(f)
    return jsonify(interest_rate_data)

@app.route('/api/data')
def get_data():
    data = {'name': 'John', 'age': 30, 'city': 'New York'}
    return jsonify(data)


def kcs_marginal_rates_coin(currency, threshold, showprogress=False):
        
        response = requests.get(BASE_KUCOIN+'/api/v1/prices?currencies='+coin)
        data=response.json()
        price=float(data['data'][coin])
        response = requests.get(BASE_KUCOIN+'/api/v1/margin/market?currency='+coin)
        data=response.json()

        borrowable=False
        total=0
        
        for datapoint in data['data']:
            total+=float(datapoint['size'])
            if total*price > USD_threshold:
                APR = float(datapoint['dailyIntRate'])*365
                borrowable=True
                break
        if borrowable:
            return [coin, APR]
        else:
            return [coin, -1]


if __name__ == '__main__':
    app.run(debug=True)
