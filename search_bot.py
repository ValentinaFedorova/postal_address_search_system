import requests
from bs4 import BeautifulSoup
import pyodbc
import datetime
import re
import pathlib




def save_url_content(path_addr, url_id, content):
    file_path = str(url_id) + '_' + re.sub(r'[^0-9]','_',str(datetime.datetime.now())) + '.txt'
    f = open(path_addr + '\\' + file_path,'w',encoding='utf-8')
    f.write(content)
    f.close()
    return file_path

def search_html(url_data):
    new_links = set()
    update_hrefs = []
    cnt = 0
    for line in url_data:
        url_addr = line.url_addr
        url_id = line.ID
        try:
            response = requests.get(url_addr, timeout=1, verify=False)
        except Exception:
            cursor.execute("update [OARB].[LINKS] set [processed] = 1 where [ID] = ?", url_id)
            return -1
        cnt += 1
        if response.status_code == 200:
            try:
                soup = BeautifulSoup(response.content, 'html.parser')
                url_path = save_url_content(url_data_path,url_id,response.content.decode('utf-8')) 
            except Exception:
                cursor.execute("update [OARB].[LINKS] set [processed] = 1 where [ID] = ?", url_id)
                return -1
            cursor.execute("update [OARB].[LINKS] set [processed] = 1,[path_to_content] = ?  where [ID] = ?", url_path, url_id)
            for link in soup.find_all('a'):
                cur_link = link.get('href')
                if cur_link is not None:
                    possible_ext = cur_link.rsplit('.',1)
                    if len(possible_ext) > 1:
                        if possible_ext[1] in drop_extentions:
                            continue
                    if cur_link[:1] == '/':
                        new_links.add(url_addr+cur_link[1:])
                    elif cur_link[:4] == 'http':
                        new_links.add(cur_link)
                    else:
                        continue
        else:
            cursor.execute("update [OARB].[LINKS] set [processed] = 1 where [ID] = ?", url_id)     
    insert_new_html(new_links)
    if cnt == 100:
        cursor.commit()

def insert_new_html(new_links):
    for link in list(new_links):
        cursor.execute("select * from [OARB].[LINKS] where [url_addr] = ?", link)
        req_answer = cursor.fetchone() 
        if req_answer is None:
            cursor.execute("insert into [OARB].[LINKS]([url_addr],processed) values (?,0)", link)


c_url = 0
url_data_path = str(pathlib.Path().resolve()) + '\\search_bot_module\\url_content_data'
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-2ELPTI6;DATABASE=prom')
cursor = cnxn.cursor()
cursor.fast_executemany = True
drop_extentions = ['pdf', 'js','css']

while c_url < 1000:
    cursor.execute("select top 1 ID, url_addr from [OARB].[LINKS] where processed = 0")
    url_data = cursor.fetchall()
    search_html(url_data)
    print('c_url: ',c_url)
    c_url += 1
    

cursor.commit()
cursor.close()
cnxn.close()






    
    