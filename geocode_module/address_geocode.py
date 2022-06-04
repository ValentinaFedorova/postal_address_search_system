import pyodbc
import pandas as pd
import requests
from bs4 import BeautifulSoup



# def check_coords(coords):
#     global geocode_API_key
#     geocode_query = 'https://geocode-maps.yandex.ru/1.x/?apikey='+ geocode_API_key +'&geocode=' + coords.get('center_x') + ',' + coords.get('center_y')
#     response = requests.get(geocode_query, timeout=1, verify=False)
#     #добавить проверку

def get_coords(addr_text):
    global geocode_API_key
    geocode_query = 'https://geocode-maps.yandex.ru/1.x/?apikey='+ geocode_API_key +'&geocode=' + addr_text
    response = requests.get(geocode_query)
    soup = BeautifulSoup(response.content, 'lxml')
    try:
        g_point = soup.findAll("point")[0].text
    except Exception:
        return '-1 -1'
    return g_point
 


geocode_API_key = '7d4fd90a-cf16-44aa-b6a1-3c51855a47d5'

kw_with_dot = ['тер','мкр','х','пл','пер','п','с','стр','д','наб','м','ул','ш','обл','ст','Респ','г']

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-2ELPTI6;DATABASE=prom')
cursor = cnxn.cursor()
addresses = pd.read_sql("SELECT [URL_ID],[SENT],[REGION],[REGION_SOCR],[DISTRICT],[DISTRICT_SOCR],[CITY],[CITY_SOCR],[STREET], [STREET_SOCR], [HOUSE] FROM [OARB].[ADDRESS] (nolock) where [PROCESSED] = 2",cnxn)
for addr in addresses.itertuples():
    url_id = addr.URL_ID
    sent = addr.SENT
    region = addr.REGION
    region_socr = addr.REGION_SOCR
    district = addr.DISTRICT
    district_socr = addr.DISTRICT_SOCR
    city = addr.CITY
    city_socr = addr.CITY_SOCR
    street = addr.STREET
    street_socr = addr.STREET_SOCR
    house = addr.HOUSE
    address = ''
    if region is not None and region!='':
        if region_socr in kw_with_dot:
            address += region_socr + '. ' + region + ', '
        else: 
            if region_socr is not None:
                address += region_socr + ' ' + region + ', '
            else:
                address += region + ', '
    if district is not None and district!='':
        if district_socr in kw_with_dot:
            address += district_socr + '. ' + district + ', '
        else: 
            if district_socr is not None:
                address += district_socr + ' ' + district + ', '
            else:
                address += district + ', '
    if city is not None and city!='':
        if city_socr in kw_with_dot:
            address += city_socr + '. ' + city + ', '
        else: 
            if city_socr is not None:
                address += city_socr + ' ' + city + ', '
            else:
                address += city + ', '
    if street is not None and street!='':
        if street_socr in kw_with_dot:
            address += street_socr + '. ' + street + ', '
        else: 
            if street_socr is not None:
                address += street_socr + ' ' + street + ', '
            else:
                address += street + ', '
    if house is not None and house!='':
        address += 'д. ' + house
    else:
        continue
    longitude, latitude = get_coords(address).split()
    if longitude == '-1' and latitude == '-1':
        cursor.execute("update [OARB].[ADDRESS] set [PROCESSED] = -1 where [PROCESSED] = 2 and [URL_ID] = ? and [SENT]=?", url_id, sent)
        continue
    longitude, latitude = float(longitude), float(latitude)
    cursor.execute("update [OARB].[ADDRESS] set geopoint = geography::Point(?,? , 4326), [PROCESSED] = 3, [ADDRESS_RES] = ? where [PROCESSED] = 2 and [URL_ID] = ? and [SENT]=?", latitude, longitude, address, url_id, sent)

cnxn.commit()
cursor.close()
cnxn.close()    