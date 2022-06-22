import os
from typing import Optional
import sys
import csv

import pandas as pd
import requests

def read_df(file_path: str) -> pd.DataFrame:
    """
    读取csv文件

    @param file_path: 文件路径
    @return: 数据框
    """
    file_path = os.path.join(os.path.dirname(__file__), file_path)

    return pd.read_csv(file_path)

class GaodeMap:
    """
    使用该类处理所有有高德地图API的操作
    """
    def __init__(self):
        self.directionV1_url = 'https://restapi.amap.com/v3/direction/driving'
        self.directionV2_url = 'http://restapi.amap.com/v5/direction/driving'

        self.session = requests.Session()

    def login(self, key: str) -> None:
        """
        使用高德地图API需要提供key

        @param key: 从高德地图API的控制台处获得的key
        """
        self.key = key


    def get_cities_lng_lat(self, file_path_cities: str) -> None:
        """
        使用高德地图API需要提供key
        @param file_path_cities: 城市名称csv存储路径
        @param file_path_cities_lng_lat: 城市经纬度csv存储路径
        @param key: 从高德地图API的控制台处获得的key
        """
        cities_df = read_df(file_path_cities)
        cities_lng_lat_dict = {
            'city':[],
            'longitude':[],
            'latitude':[]
        }

        for index in range(len(cities_df)):
            params = {
                'key': self.key,
                # 读取城市表格数据中city栏
                'address': cities_df.at[index, 'city']
            }

            resp = self.session.get(url = 'http://restapi.amap.com/v3/geocode/geo', params = params).json()
            if resp['status'] == '1':
                cities_lng_lat_dict['city'].append(cities_df.at[index, 'city'])
                cities_lng_lat_dict['longitude'].append(resp['geocodes'][0]['location'].split(',')[0])
                cities_lng_lat_dict['latitude'].append(resp['geocodes'][0]['location'].split(',')[1])
            else:
                print(params)
                raise Exception('调用出错！')

        cities_lng_lat_df = pd.DataFrame(cities_lng_lat_dict)
        cities_lng_lat_df.to_csv('cities_lng_lat_gaode.csv', index=False)

    def init_result_csv(self, file_path: str) -> None:
        """
        使用高德地图API需要提供key
        @param file_path: 城市名称csv存储路径
        """

        cities_df = read_df(file_path)
        cities_list = cities_df.values
        init_result_dict = {
            'origin': [],
            'destination': [],
            'gaode_map_distance': [],
            'duration': []
        }

        for i in range(len(cities_list)):
            for city in cities_list:
                if cities_list[i] != city :
                    init_result_dict['origin'].append(cities_list[i][0])
                    init_result_dict['destination'].append(city[0])
                    init_result_dict['gaode_map_distance'].append(0)
                    init_result_dict['duration'].append(0)
                else:
                    print(f"{i}目的地起始地相同")

        cities_lng_lat_df = pd.DataFrame(init_result_dict)
        cities_lng_lat_df.to_csv('distance_gaode.csv', index=False)        


    def read_lng_lat(self, file_path: str) -> None:
        """
        读取城市经纬度

        @param file_path: 经纬度数据存储路径
        """
        lng_lat_df = read_df(file_path)
        self.lng_lat = {}
        for index in range(len(lng_lat_df)):
            self.lng_lat[lng_lat_df.at[index, 'city']] = (round(lng_lat_df.at[index, 'longitude'], 5), round(lng_lat_df.at[index, 'latitude'], 5))

    def get_lng_lat(self, city_name: str) -> tuple:
        """
        获取城市经纬度

        @param city_name: 城市名字
        @return: 城市的经纬度
        """
        return self.lng_lat[city_name]

    def fetch_route(self, origin: list, destination: list, method: str = 'directionlite') -> dict:
        """
        使用高德地图API获取所需要的信息（里程数，驾车时间，过路费）

        @param origin: 起点的经纬度，经度在前，纬度在后
        @param destination: 终点的经纬度，经度在前，纬度在后
        @param method: 使用轻量路线规划或路线规划API，取值为directionlite或direction

        @return: 返回想要写入数据框中的信息
        """
        if method == 'directionV1_url':
            url = self.directionV1_url
        elif method == 'directionV2_url':
            url = self.directionV2_url
        
        params = {
            'origin': f'{origin[0]},{origin[1]}',
            'destination': f'{destination[0]},{destination[1]}',
            'key': self.key,
            'extensions': 'all'
        }

        resp = self.session.get(url = url, params = params).json()

        if resp['status'] == '1':
            routes = resp['route']['paths'][0]
            return {
                'distance': routes['distance'],
                'duration': routes['duration']
            }
        else:
            print(params)
            raise Exception('调用出错！')

if __name__ == "__main__":
    import time
    map = GaodeMap()
    map.login('673f63b7ea3b20ff3bf73cbf2c9db60f')
    # 刷新列表城市的经纬度
    map.get_cities_lng_lat('cities.csv')
    map.init_result_csv('cities.csv')
    map.read_lng_lat('cities_lng_lat_gaode.csv')
    
    result_df = read_df('distance_gaode.csv')
    # 更新全量数据需要先将数据置为0
    # result_df['gaode_map_distance'] = 0 
    print("=======\n", result_df)

    print('使用高德地图API读取数据...')
    for index in range(len(result_df)):
        if result_df.at[index, 'gaode_map_distance'] == 0.0:
            origin_city = result_df.at[index, 'origin']
            destination_city = result_df.at[index, 'destination']

            origin_position = map.get_lng_lat(origin_city)
            destination_position = map.get_lng_lat(destination_city)

            try:
                resp = map.fetch_route(origin_position, destination_position, method='directionV1_url')
            except:
                print('运行出错了，速速存盘保平安！')
                result_df.to_csv(os.path.join(os.path.dirname(__file__), 'distance_gaode.csv'), index=False, encoding="UTF-8-sig")
                sys.exit()
            
            result_df.at[index, 'gaode_map_distance'] = resp['distance']
            result_df.at[index, 'duration'] = resp['duration']

            if index % 20 == 19:
                print(f'处理到第 {index + 1} 条数据，准备存盘...')
                result_df.to_csv(os.path.join(os.path.dirname(__file__), 'distance_gaode.csv'), index=False, encoding="UTF-8-sig")