import os
from bs4 import BeautifulSoup
import json

def read_file(file_path):
    f = open(file_path,'r',encoding='utf-8')
    file_content = f.read()
    f.close()
    return file_content

def make_string(content_list):
    result = ''
    for el in content_list:
        result += el + '\n'
    return result.strip()

def save_file(file_path, file_name, file_content):
    global prefix
    f = open(file_path + '\\' + prefix + file_name,'w',encoding='utf-8')
    f.write(file_content)
    f.close()


def find_header(header_tag, parser_data):
    header_text = []
    global url_headers
    for el in soup.find_all(header_tag):
        header_text.append(el.text.strip())
    url_headers[header_tag] = header_text


url_dataset_path = 'D:\\magistrValya\\address_search_system\\search_bot_module\\url_content_data'
prefix = 'clean_'
files = os.listdir(path=url_dataset_path)
files.remove('headers')
files.remove('processed')
header_tags = ['h' + str(num) for num in range(1,5)]
for file in files:
    if file[:6] != prefix and prefix+file not in files:
        file_path = url_dataset_path + '\\' + file
        file_parts = file.split('_')
        url_id = file_parts[0]
        file_content = read_file(file_path)
        soup = BeautifulSoup(file_content, 'html.parser')
        url_headers = {}
        url_headers['ID'] = url_id
        for h in header_tags:
            find_header(h,soup)
        content_text = []
        for el in soup.find_all('p'):
            if el != '':
                content_text.append(el.text)
        content_text = make_string(content_text)
        save_file(url_dataset_path, file, content_text)
        url_headers_path = 'D:\\magistrValya\\address_search_system\\search_bot_module\\url_content_data\\headers\\header_' + url_id + '.json'
        with open(url_headers_path, "w") as write_file:
            json.dump(url_headers, write_file)  
