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
import random

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

redis_passwd = "IoTA8TUm3pQADVPF4TiSPh0JFMAx58rwrAzCaKPoZn8="
redis_host = "wyc.redis.cache.windows.net"
redis_port = 6380

cache = redis.StrictRedis(
    host=redis_host,
    port=redis_port,
    db=0,
    password=redis_passwd,
    ssl=True,
    ssl_cert_reqs=u'none',
)

if cache.ping():
    print("pong")



def euclidean_distance(city1, city2):
    return math.sqrt((city1['lat'] - city2['lat'])**2 + (city1['lng'] - city2['lng'])**2)

# KNN分类函数
def knn_classify(city, k, seed_cities):
    distances = [(data, euclidean_distance(city, data)) for data in seed_cities]
    distances.sort(key=lambda x: x[1])
    neighbors = [neighbor[0] for neighbor in distances[:k]]
    print(f"neighbors = {neighbors}")
    return neighbors

def calculate_center_city(class_cities):
    # 计算类中所有城市的经度和纬度的平均值
    avg_lat = sum(city['lat'] for city in class_cities) / len(class_cities)
    avg_lng = sum(city['lng'] for city in class_cities) / len(class_cities)

    # 找出离平均经纬度最近的城市作为中心城市
    center_city = min(class_cities, key=lambda city: (city['lat'] - avg_lat)**2 + (city['lng'] - avg_lng)**2)
    
    return center_city

@app.route('/data/knn_reviews', methods=['GET'])
def knn_reviews():
    start_time = time.time() * 1000  # Record start time for response time calculation

    # Get request parameters
    classes = int(request.args.get('classes', type=int, default=6))
    k = int(request.args.get('k', type=int, default=3))
    words = int(request.args.get('words', type=int, default=100))

    query = "SELECT us_cities.city, us_cities.lat, us_cities.lng, us_cities.population FROM us_cities"
    # quickly fetch the result if it already in the cache
    if cache.exists(query):
        query_list = json.loads(cache.get(query).decode())
        print("cache hit: [{}]".format(query))
    else:
        # params = [dict(name="@city_name", value='city_name')]
        query_list = list(container_us_cities.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        cache.set(query, json.dumps(query_list))
        print("cache miss: [{}]".format(query))
    for city_info in query_list:
        if city_info['lat'] is not None:
            city_info['lat'] = float(city_info['lat'])
        if city_info['lng'] is not None:
            city_info['lng'] = float(city_info['lng'])
        if city_info['population'] is not None:
            city_info['population'] = float(city_info['population'])

    # for item in query_list:
    #     print(f"city: {item['city']}, lat: {item['lat']}, lng: {item['lng']}")
    
    # 选一些城市作为初始类别的中心点        
    N = classes
    seed_cities = random.sample(query_list, N)
    # print(f"seed_cities = {seed_cities}")
    classified_cities = [[] for _ in range(N)]
    classified_seeds = {}
    classNum = 0
    for index, city in enumerate(seed_cities):
        # classified_cities[seed_cities[0]['city']] = classNum
        # classified_cities[classNum].append(seed_cities[classNum])
        classified_cities[classNum].append(city)
        classified_seeds[city['city']] = classNum
        classNum += 1
    # print(f"classified_cities: {classified_cities}")
    # print(f"classified_seeds: {classified_seeds}")

    for incoming_city in query_list:
        # print(f"incoming_city = {incoming_city}")
        if incoming_city not in seed_cities:
             # 计算当前城市与所有种子城市的欧氏距离
            # distances = [(seed_city, math.sqrt((query_list['lat'] - seed_city['lat'])**2 + (query_list['lng'] - seed_city['lng'])**2)) for seed_city in seed_cities]
            distances = [(seed_city, math.sqrt((query_city['lat'] - seed_city['lat'])**2 + (query_city['lng'] - seed_city['lng'])**2)) for seed_city in seed_cities for query_city in query_list]
            # 按照距离排序并选择K个最近邻
            distances.sort(key=lambda x: x[1])
            nearest_neighbors = [neighbor[0] for neighbor in distances[:k]]
            # print(f"nearest_neighbors: {nearest_neighbors}")
            # 根据最近邻的类别进行投票，选择最多的类别作为当前城市的类别
            neighbor_classes = [neighbor['city'] for neighbor in nearest_neighbors]
            # print(f"neighbor_classes = {neighbor_classes}")
            # print(f"neighbor_classes[0] = {neighbor_classes[0]}")
            # nearest_city = Counter(neighbor_classes).most_common(1)[0][0]
            # city_class = classified_seeds[neighbor_classes[0]]
            city_class = random.randint(0, N-1)
            # print(f"city_class = {city_class}")
            # # 把确定的类别city_class赋给city当前城市，并存入一个字典中
            # classified_cities[incoming_city['city']] = city_class
            classified_cities[city_class].append(incoming_city)
            # print(f"classified_cities = {classified_cities}")
            # print()
            # city_class = class
    print(f"classified_cities = {classified_cities}")
    # for index, cities_in_class in enumerate(classified_cities):
    #     print(f"Class {index}:")
    #     for city in cities_in_class:
    #         print(city['city'])
    cities_by_class = {}
    for index, cities_in_class in enumerate(classified_cities):
        cities_by_class[f"Class {index}"] = [city['city'] for city in cities_in_class]


    centers_by_class = {}
    for index, cities_in_class in enumerate(classified_cities):
        center_city = calculate_center_city(cities_in_class)
        centers_by_class[f"Class {index}"] = center_city


    # 构建SQL查询语句，筛选出包含指定城市名的评论
    query2 = "SELECT reviews.city, reviews.score, reviews.review FROM reviews"
    if cache.exists(query2):
        query_list_2 = json.loads(cache.get(query2).decode())
        print("cache hit: [{}]".format(query2))
    else:
        # params = [dict(name="@city_name", value='city_name')]
        query_list_2 = list(container_reviews.query_items(
            query=query2,
            enable_cross_partition_query=True
        ))
        cache.set(query2, json.dumps(query_list_2))
        print("cache miss: [{}]".format(query2))
    print(f"query2 = {query2}")
    for city_info in query_list_2:
        if city_info['score'] is not None:
            city_info['score'] = int(city_info['score'])

    # weighted_avg_scores = {}

    # for class_index, cities in cities_by_class.items():
    #     total_weighted_score = 0
    #     total_weight = 0
    
    # for city_name in cities:
    #     # Find population and score for the city from query_list and query_list_2
    #     population = next((item['population'] for item in query_list if item['city'] == city_name), None)
    #     score = next((item['score'] for item in query_list_2 if item['city'] == city_name), None)
        
    #     if population is not None and score is not None:
    #         # Calculate weighted score
    #         weighted_score = population * score
    #         total_weighted_score += weighted_score
    #         total_weight += population
    
    # # Calculate weighted average score for the class
    # if total_weight > 0:
    #     weighted_avg_scores[class_index] = total_weighted_score / total_weight
    # else:
    #     weighted_avg_scores[class_index] = 0  # or handle the case when total_weight is zero

# Now you have a dictionary weighted_avg_scores containing weighted average scores for each class

    # 使用列表中的城市名构建查询条件
    # conditions = " OR ".join([f"city = '{city}'" for city in ])
    # query += f"({' OR '.join(conditions)})"

    # # Calculate response time
    end_time = time.time() * 1000
    response_time = end_time - start_time
    return jsonify({
        'cities_by_class': cities_by_class,
        'center_city': centers_by_class,
        'weighted_avg_score': weighted_avg_scores,
        'response_time_ms': response_time
    })

@app.route('/data/closest_cities', methods=['GET'])
def closest_cities():
    start_time = time.time() * 1000  # Record start time for response time calculation

    # Get request parameters
    city_name = request.args.get('city', default="Burton")
    page = int(request.args.get('page', type=int, default=0))
    page_size = int(request.args.get('page_size', type=int, default=10))

    query = "SELECT us_cities.city, us_cities.lat, us_cities.lng FROM us_cities"
    if cache.exists(query):
        result = json.loads(cache.get(query).decode())
        print("cache hit: [{}]".format(query))
    else:
        # params = [dict(name="@city_name", value='city_name')]
        query_list = list(container_us_cities.query_items(
            query=query,
            enable_cross_partition_query=True
        ))
        cache.set(query, json.dumps(query_list))
        print("cache miss: [{}]".format(query))
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