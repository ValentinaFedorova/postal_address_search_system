import pathlib
import pyodbc
import pandas as pd
import json
from bs4 import BeautifulSoup
from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsNERTagger,
    Doc
)


segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
ner_tagger = NewsNERTagger(emb)


# def read_json_data(path_to_file):
#     f = open(path_to_file, 'r')
#     data = f.read()
#     f.close()
#     return json.loads(data)

def read_data(path_to_file):
    f = open(path_to_file, 'r', encoding='utf-8')
    data = f.read()
    f.close()
    return data



url_processed_data_path = str(pathlib.Path().resolve()) + '\\search_bot_module\\url_content_data\\processed\\'
url_data_path = str(pathlib.Path().resolve()) + '\\search_bot_module\\url_content_data\\headers\\'

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-2ELPTI6;DATABASE=prom')


# url_id_list = pd.read_sql("select URL_ID from OARB.ADDRESS where processed = 3 group by URL_ID having count(*) > 3",cnxn)['URL_ID'].values.tolist()

# header_data = {}

# for u_id in url_id_list:
#     header_data[u_id] = read_json_data(url_data_path + 'header_' + str(u_id) + '.json')
    
# pos_set = set()

# for u_id, u_data in header_data.items():
#     header_levels = list(u_data.keys())
#     header_levels.remove('ID')
#     new_data = {}
#     for h_level in header_levels:
#         tokens = []
#         for h in u_data[h_level]:
#             doc = Doc(h)
#             doc.segment(segmenter)
#             doc.tag_morph(morph_tagger)
#             h_tokens = [el.text for el in doc.tokens if el.pos in ['NOUN','ADJ','PROPN'] and len(el.text)>1]
#             if len(h_tokens)>0 and h_tokens not in tokens:    
#                 tokens.append(h_tokens)
#         if len(tokens)>0:
#             new_data[h_level] = tokens
#     header_data[u_id] = new_data    
            

url_id_tags = pd.read_sql("select distinct a.URL_ID, a.p_tag_text, l.path_to_content from OARB.ADDRESS a inner join OARB.LINKS l on a.URL_ID = l.ID where a.URL_ID in (select URL_ID from OARB.ADDRESS where processed = 3 group by URL_ID having count(*) > 3) and a.processed = 3",cnxn)
cnxn.close()
header_levels = [i for i in range(4,0,-1)]

url_id_headers = {}
processed_id = []
for row in url_id_tags.itertuples():
    url_id = row.URL_ID
    if url_id in processed_id:
        continue
    path_to_content = row.path_to_content
    p_tag_text = row.p_tag_text
    html_text = read_data(url_processed_data_path + path_to_content)
    soup = BeautifulSoup(html_text, 'html.parser')
    current_header = ''
    for el in soup.find_all('p'):
        if current_header == '':
            if el.text == p_tag_text:
                for level in header_levels:
                    current_header = el.find_previous('h'+str(level))
                    if current_header is not None:
                        current_header = current_header.text
                        break
        else:
            url_id_headers[url_id] = current_header.strip()
            processed_id.append(url_id)
            break


with open(url_data_path + 'processed_headers.json', "w") as write_file:
    json.dump(url_id_headers, write_file)  