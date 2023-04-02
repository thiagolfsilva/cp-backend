from flask import Flask, jsonify, request
from flask_cors import CORS
import requests
import json
import time
import datetime
import pandas as pd
from google.cloud import bigquery
import pandas_gbq
from google.oauth2 import service_account
import os
from dotenv import load_dotenv
import base64

app = Flask(__name__)
CORS(app)
load_dotenv()

#Add google credentials, to use google bigquery
credentials_64 = (os.environ['GOOGLE_APPLICATION_CREDENTIALS_BASE64'])
credentials_json_string = base64.b64decode(credentials_64).decode('utf-8')
credentials_json = json.loads(credentials_json_string)
credentials = service_account.Credentials.from_service_account_info(credentials_json)
pandas_gbq.context.credentials = credentials

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
    WITH data_over_last_7_days AS (
        SELECT 
            coin,
            size,
            timestamp,
            hrtimestamp,
            CAST(dailyIntRate AS FLOAT64) AS dailyIntRate
        FROM `test_set.kcs_loans`
        WHERE coin = '{coin}'
            AND timestamp >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    )
    SELECT *
    FROM data_over_last_7_days
    ORDER BY timestamp;
    """

    df = pandas_gbq.read_gbq(sql, project_id=project_id)
    df['hrtimestamp'] = df['hrtimestamp'].dt.tz_localize(None)  # Convert 'hrtimestamp' to timezone-naive object
    timestamp = datetime.datetime.now().timestamp()
    current_hrtimestamp = pd.to_datetime(timestamp*10**9).floor('H')
    hrtimestamps = [current_hrtimestamp - datetime.timedelta(hours=i) for i in reversed(range(7*24-1))]
    hrtimestamps = [ts.tz_localize(None) for ts in hrtimestamps]  # Convert list elements to timezone-naive objects

    results=[]
    print(df)
    print(hrtimestamps)

    for hrtimestamp in hrtimestamps:
        current_df =  df[df['hrtimestamp'] == hrtimestamp]
        print(current_df)
        current_df_sorted = current_df.sort_values(by='dailyIntRate')
        total=0
        #print(current_df_sorted)
        for index, row in current_df_sorted.iterrows():
            total+=float(row['size'])
            if total>threshold:
                results.append({"timestamp":hrtimestamp, "dailyIntRate":row['dailyIntRate']})
                break
        if total<threshold:
            results.append({"timestamp":hrtimestamp, "dailyIntRate":None})

    return jsonify(results)

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
