from glob import glob
import pathlib
import pyodbc
import pandas as pd
import json
import re
from bs4 import BeautifulSoup
from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsNERTagger,
    Doc
)
from nltk import ngrams
from nltk.stem.snowball import SnowballStemmer

def read_data(path_to_file):
    f = open(path_to_file, 'r', encoding='utf-8')
    data = f.read()
    f.close()
    return data

def normalize_text(text):
    return ' '.join(re.sub(r'[^а-я]',' ', text.lower()).split())

def search_key_words(text):
    global kw_grams
    global kw_bigrams
    global kw_threegrams
    global stemmed_key_words
    text = [stemmer.stem(t) for t in text.split()]
    onegrams = [tuple([t]) for t in text]
    bigrams = ngrams(text,2)
    threebigrams = ngrams(text,2)
    for kw in kw_grams:
        for t in onegrams:
            if kw == t:
                return stemmed_key_words.get(kw)
    for kw in kw_bigrams:
        for t in bigrams:
            if kw == t:
                return stemmed_key_words.get(kw)
    for kw in kw_threegrams:
        for t in threebigrams:
            if kw == t:
                return stemmed_key_words.get(kw)            
    return -1





url_processed_data_path = str(pathlib.Path().resolve()) + '\\search_bot_module\\url_content_data\\processed\\'
segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
ner_tagger = NewsNERTagger(emb)
stemmer = SnowballStemmer("russian")

key_words =['спектакль', 'концерт', 'волонтер', 'фонд капитального ремонта', 'прием обращений', 'конференция','мероприятие',
            'ремонт', 'детский сад', 'школа', 'матч'
            ]

stemmed_key_words = {}
for el in key_words:
    if len(el.split())>1:
        res = []
        for sub_el in el.split():
           res.append(stemmer.stem(sub_el))
        stemmed_key_words[tuple(res)] = el
    else:
        stemmed_key_words[tuple([stemmer.stem(el)])] = el

     


kw_grams = [x for x in stemmed_key_words.keys() if len(x)==1]
kw_bigrams = [x for x in stemmed_key_words.keys() if len(x)==2]
kw_threegrams = [x for x in stemmed_key_words.keys() if len(x)==3]




cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-2ELPTI6;DATABASE=prom')
cursor = cnxn.cursor()

processed_data = pd.read_sql("select distinct a.URL_ID, a.p_tag_text, l.path_to_content from OARB.ADDRESS a inner join OARB.LINKS l on a.URL_ID = l.ID where a.processed = 3 order by a.URL_ID",cnxn)

cur_url_id = ''

for row in processed_data.itertuples():
    url_id = row.URL_ID
    if url_id != cur_url_id:
        path_to_content = row.path_to_content
        web_page_content = read_data(url_processed_data_path + path_to_content)
        cur_url_id = url_id
    p_tag_text = row.p_tag_text.strip()
    soup = BeautifulSoup(web_page_content, 'html.parser')
    for el in soup.find_all('p'):
        if el != '':
            if p_tag_text in el.text.strip():
                div_text = el.parent.getText()
                ind = div_text.find(p_tag_text)
                div_text = normalize_text(div_text[0:ind])
                doc = Doc(div_text)
                doc.segment(segmenter)
                doc.tag_morph(morph_tagger)
                tokens = [t.text for t in doc.tokens if t.pos in ['NOUN','ADJ','PROPN','CCONJ'] and len(t.text)>1]
                if len(tokens) > 0:
                    web_page_kw = search_key_words(' '.join(tokens))
                    if web_page_kw != -1:
                        cursor.execute("update [OARB].[ADDRESS] set [tag] = ? where [URL_ID] = ? and [p_tag_text] = ? and tag is null", web_page_kw, url_id, row.p_tag_text)


cnxn.commit()
cursor.close()
cnxn.close()