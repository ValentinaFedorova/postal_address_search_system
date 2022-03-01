import requests
from bs4 import BeautifulSoup


response = requests.get('https://www.rbc.ru/')

if response.status_code == 200:
    soup = BeautifulSoup(response.content, 'html.parser')
    print('ok')
    for link in soup.find_all('a'):
        print(link.get('href'))
    
    