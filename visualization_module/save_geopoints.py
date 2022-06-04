import pyodbc
import pandas as pd
import json

geopoint_path = 'D:\\magistrValya\\address_search_system\\visualization_module\\geopoints.json'

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-2ELPTI6;DATABASE=prom')
cursor = cnxn.cursor()
addresses = pd.read_sql("SELECT a.[URL_ID], l.url_addr, a.[ADDRESS_RES], a.[GEOPOINT].Lat as geo_lat, a.[GEOPOINT].Long as geo_lon FROM [OARB].[ADDRESS] (nolock) a inner join [OARB].[LINKS] (nolock) l on a.URL_ID = l.ID where a.[PROCESSED] = 3",cnxn)
geopoints_res = []
for el in addresses.itertuples():
    geodata = {}
    url_address = el.url_addr
    addr = el.ADDRESS_RES
    lat = el.geo_lat
    lon = el.geo_lon
    geodata['lat'] = lat
    geodata['lon'] = lon
    geodata['address'] = addr
    geodata['url_address'] = url_address
    geopoints_res.append(geodata)

geopoint_json = {}
geopoint_json['points'] = geopoints_res
with open(geopoint_path, "w") as write_file:
    json.dump(geopoint_json, write_file)  
