from flask import jsonify
from flask import Flask
from flask import render_template
from flask import request
import csv
import json
import os
import sys
import uuid
import redis
import time
import math

from azure.core.exceptions import AzureError
from azure.cosmos import CosmosClient, PartitionKey
from azure.cosmos.cosmos_client import CosmosClient
# from azure.cosmos.partition_key import PartitionKey
from collections import Counter
from azure.identity import ClientSecretCredential, DefaultAzureCredential
from azure.cosmos import exceptions

app = Flask(__name__)

@app.route('/index')
# def index():
#     data_list = fetch_data("Parma")
#     return render_template('index.html', data_list=data_list)
def index():
    return render_template('index.html')

@app.route('/')
def hello_world():  # put application's code here
    return render_template('/stat/closest_cities.html')


DB_CONN_STR = "AccountEndpoint=https://tutorial-uta-cse6332.documents.azure.com:443/;AccountKey=fSDt8pk5P1EH0NlvfiolgZF332ILOkKhMdLY6iMS2yjVqdpWx4XtnVgBoJBCBaHA8PIHnAbFY4N9ACDbMdwaEw==;"
db_client = CosmosClient.from_connection_string(conn_str = DB_CONN_STR)
database = db_client.get_database_client("tutorial")
container_us_cities = database.get_container_client("us_cities")
container_reviews = database.get_container_client("reviews")

@app.route('/data/closest_cities', methods=['GET'])
def closest_cities():
    start_time = time.time() * 1000  # Record start time for response time calculation

    # Get request parameters
    city_name = request.args.get('city', default="Burton")
    page = int(request.args.get('page', type=int, default=0))
    page_size = int(request.args.get('page_size', type=int, default=10))

    # Find the given city
    query = "SELECT us_cities.city, us_cities.lat, us_cities.lng FROM us_cities"
    # params = [dict(name="@city_name", value='city_name')]
    query_list = list(container_us_cities.query_items(
        query=query,
        enable_cross_partition_query=True
    ))
    for city_info in query_list:
        if city_info['lat'] is not None:
            city_info['lat'] = float(city_info['lat'])
        if city_info['lng'] is not None:
            city_info['lng'] = float(city_info['lng'])
    for item in query_list:
        print(f"city: {item['city']}, lat: {item['lat']}, lng: {item['lng']}")
    
    # result = []
    # row_id = 0
    # headers = ["city", "lat", "lng"]

    # for item in container_us_cities.query_items(
    #     query=query, parameters=params,
    #     enable_cross_partition_query=True,
    # ):
    #     row_id += 1
    #     line = [str(row_id)]
    #     for col in headers:
    #         line.append(item[col])
    #     result.append(line)
    # # given_city = next((city for city in us_cities if city['city'] == city_name), None)

    # # if not given_city:
    # #     return jsonify({'error': 'City not found'}), 404

    # Calculate distances to the given city
    current_city_lat, current_city_lng = get_lat_lng(city_name, query_list)
    print(f"lat = {current_city_lat}, lng = {current_city_lng}")
    filtered_cities = [city for city in query_list if city['lat'] is not None and city['lng'] is not None]

    sorted_cities = sorted(filtered_cities, key=lambda x: math.sqrt((x['lat'] - current_city_lat) ** 2 + (x['lng'] - current_city_lng) ** 2))

    # # Pagination
    start_index = page * page_size
    end_index = start_index + page_size
    cities_on_page = sorted_cities[start_index:end_index]

    # # Calculate response time
    end_time = time.time() * 1000
    response_time = end_time - start_time

    return jsonify({
        'cities': cities_on_page,
        'response_time_ms': response_time
    })

def get_lat_lng(city_name, query_list) -> tuple:
    for city_info in query_list:
        if city_info['city'] == city_name:
            return city_info['lat'], city_info['lng']
    return None, None

if __name__ == "__main__":
    app.run(host="127.0.0.1", port=8084, debug=True)