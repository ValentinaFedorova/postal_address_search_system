

from natasha import (
    Segmenter,
    MorphVocab,
    NewsEmbedding,
    NewsMorphTagger,
    NewsNERTagger,
    Doc
)
import pyodbc,re
import pandas as pd
import datetime
import numpy as np
import datetime
import os
from razdel import tokenize, sentenize
#from nltk.stem.snowball import SnowballStemmer


class AddrItem(object):
    def __init__(self, NAME="Unknown name", CODE="Unknown major"):
        self.NAME = NAME
        self.Index = CODE

vowels = ['а','у','о','ы','и','й','э','я','ю','ё','е']
symbols = ":().,!;'?-\"@#$%^&*+ "
key_words = ["г","обл","адрес","росреестр","ул","д","кв","республик","рх","рп","р","н",
             "корп","федерац","область","округ","город","улиц","квартир","п","пер","мкр",
             "муниципальн","район","дер","республика","улица"]

stop_words = ["российская","федерация","муниципальный","рб","жилое","помещение","проектный",
              "номер","этаже","подъезде","общей","проектной","площадью","учётом","холодных",
              "помещений","квартира","однокомнатная","расположенная","этажного","жилого",
              "находящаяся","находящегося","кадастровый","земельного","участка","управление",
              "рф","адресу"]

full_cities_name = []
local_towns = []
local_towns_code = []
local_towns_links = []
distinct_district_names = []
distinct_city_names = []
text = []
#Для Наташи
segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
ner_tagger = NewsNERTagger(emb)
#stemmer = SnowballStemmer("russian") 



    
  
def get_part_of_speech(word):
    vowels='уеыаоэиюя'
    adjective_suffixes = ["ого","его","ому","ему","ой","ый","ым","им","ом","ем","ей","ой",
                          "ую","юю","ое","ее","ая","яя","ий","ья","ье","ые"]
    suffix_2 = word[-2:]
    suffix_3 = word[-3:]
    if suffix_2 in adjective_suffixes:
        return 'ADJV'
    elif suffix_3 in adjective_suffixes:
        return 'ADJV'
    end=''
    for i in range(len(word)-1,0,-1):
        if word[i] in vowels:
            end+=word[i]
        else:
            break
    if end in ['а','e','и']:
        return 'NOUN'
    return 'UNDEF'
    

def lemmatize_sent(sent):
    sent = sent.lower()
    sent = sent.replace('р-н','район')
    sent = sent.replace(' р.п. ',' рп ')
    sent = re.sub(r'[^а-я]',' ', sent).strip()
    doc = Doc(sent)
    doc.segment(segmenter)
    tokens = []
    for t in doc.tokens:
        tk = t.text
        if tk not in symbols and tk not in stop_words:
            mas = tk.split('-')
            for el in mas:
                tokens.append(el) 
    return tokens



def word_base(word):
    index = len(word)
    for i in range(len(word)-1,0,-1):
        if word[i] in vowels:
            index = i
        else:
            break
    return word[:index]

# def word_base(word):
#     return stemmer.stem(word)

# def word_base_array(words):
#     return [stemmer.stem(word) for word in words]


def word_base_array(words):
    res = []
    for word in words:
        index = len(word)
        for i in range(len(word)-1,0,-1):
            if word[i] in vowels:
                index = i
            else:
                break
        res.append(word[:index].lower())
    return res

def parts_of_name(word):
    return re.sub(r'-',' ',word.strip()).split()


                
cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=MY_SERVER;DATABASE=prom')
df_cities = pd.read_sql('SELECT NAME,CODE,SOCR FROM [OARB].[KLADR] (nolock) order by [NAME]',cnxn, index_col="CODE")


df_obj = df_cities.select_dtypes(['object'])
df_cities[df_obj.columns] = df_obj.apply(lambda x: x.str.strip())
df_cities[df_obj.columns] = df_cities[df_obj.columns].apply(lambda x: x.str.lower())
df_cities['NAME'] = df_cities['NAME'].astype(str)


df_cities['PARTS'] = df_cities['NAME'].apply(parts_of_name) 


cities_parts_array = []
for row in df_cities.itertuples():
    row_name_parts = word_base_array(parts_of_name(row.NAME))
    row_code = row.Index
    for i in range(len(row_name_parts)):
        row_dict = {}
        row_dict['NAME'] = row_name_parts[i]
        row_dict['CODE'] = row_code + str(i+1)
        row_dict['SOCR'] = row.SOCR
        row_dict['FULL_CITY'] = row
        cities_parts_array.append(row_dict)
        
df_cities_parts = pd.DataFrame(cities_parts_array)   
df_cities_parts = df_cities_parts.set_index('CODE')     
print(df_cities_parts.head())    
    

subjects = ["ао","аобл","край","обл","респ"]
df_subjects = df_cities.filter(regex='\d\d00000000000', axis=0)


df_city_g = df_cities[df_cities['SOCR'] == 'г']

small_city_area = ["дп","кп","рп","нп","снт","тер. днт","тер. снт","тер. тсн","тер"]
df_small_city_area = df_cities[df_cities['SOCR'].isin(small_city_area)]


villagies_key_words = ["пгт","с/мо","с/п","д","п","с","с/о","дер"]
city_key_word = ["г","гор","город"]
district_key_words = ["р","район","р-н"]
df_villages = df_cities[df_cities['SOCR'].isin(villagies_key_words)]


df_mcr = df_cities[df_cities['SOCR'] == 'мкр']

df_quarter = df_cities[df_cities['SOCR'] == 'кв-л']



  
def possible_address_objects(tokens,obj_df):
    found = pd.DataFrame()
    for t in tokens:
        if len(t) > 1 and not t.isdigit():
            found = pd.concat([found,obj_df.query('NAME.str.contains("'+t+'")',engine = 'python')])

    return found
def get_context(tokens,possible_names):
    address_uniq_codes = set()
    context_part = pd.DataFrame()
    for i in range(len(tokens)):
        for index,row in possible_names.iterrows():
            parts = row['PARTS']
            parts_amount = len(parts)
            if parts_amount>1:
                startindex = max(0, i - parts_amount)
                endindex = min(len(tokens),startindex + 2*parts_amount)
                parts_cnt = 0
                for s in range(startindex,endindex):
                    for j in range(parts_amount):
                        if tokens[s] == parts[j]:
                            parts_cnt += 1
                if parts_cnt == parts_amount: 
                    if row['CODE'] not in address_uniq_codes:
                        context_part = context_part.append(row)
                        address_uniq_codes.add(row['CODE'])
            else:
                if tokens[i] == row['PARTS'][0]:
                    if row['CODE'] not in address_uniq_codes:
                        context_part = context_part.append(row)
                        address_uniq_codes.add(row['CODE'])
    return context_part



context = {}   
      
def to_lower_case(values):
    res = []
    for v in values:  
        res.append(v.lower()) 
    return res     


def get_next_token(tokens,pos):
    for i in range(pos+1, len(tokens)):
        if word_base(tokens[i]) in key_words:
            return ''
        if word_base(tokens[i]) not in key_words and len(tokens[i]) > 1 and not tokens[i].isdigit():
            return tokens[i]
    return ''

def get_prev_token(tokens,pos):
    for i in range(min(pos-1, len(tokens)-1), -1, -1):
        if word_base(tokens[i]) in key_words:
            return ''
        if word_base(tokens[i]) not in key_words and len(tokens[i]) > 1 and not tokens[i].isdigit():
            return tokens[i]
    return ''

def getRegion(tokens):
    kladr_possible_items = []
    croped_tokens = word_base_array(tokens)
    
    for i in range(len(tokens)):
        kladr_items = pd.DataFrame()
        if tokens[i] not in key_words and len(tokens[i]) > 1 and not tokens[i].isdigit(): 
            cur_token=tokens[i]
        else:
            continue
    
        kladr_items = df_subjects.query("NAME.str.contains('"+word_base(cur_token)+"')",engine = 'python')
        
        for row in kladr_items.itertuples():  
            parts = row.PARTS
            parts_amount = len(parts)
            if parts_amount>1:
                startindex = max(0, i - parts_amount)
                endindex = min(len(tokens), i + parts_amount + 1)
                parts_cnt = 0
                for s in range(startindex,endindex):
                    for j in range(parts_amount):
                        if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(tokens[s]) == get_part_of_speech(parts[j]):
                            parts_cnt += 1
                if parts_cnt == parts_amount: 
                    kladr_possible_items.append(row)
                    
            else:
                if word_base(cur_token) == word_base(row.PARTS[0]) and get_part_of_speech(cur_token) == get_part_of_speech(row.PARTS[0]):
                    kladr_possible_items.append(row)
    ret = []
    for i in kladr_possible_items:
        if i not in ret:
            ret.append(i)
    return ret


def getitemsBykeyword(regcities, tokens, keywords, constraints, both_direction, excludes):

    kladr_possible_items = []
    croped_tokens = word_base_array(tokens)
    curkeywords=[]
    
    max_len = 0
    for i in range(len(tokens)):
        kladr_items = pd.DataFrame()
        
        if tokens[i] in keywords and i not in excludes:
            if i<len(tokens)-1:
                if tokens[i] in small_city_area:
                    curkeywords = small_city_area
                elif tokens[i] in villagies_key_words:
                    curkeywords = villagies_key_words 
                elif tokens[i] in city_key_word:
                    curkeywords = city_key_word
                elif tokens[i] in district_key_words:
                    curkeywords = district_key_words
                else:
                    continue

                if both_direction and not tokens[i] in villagies_key_words:
                    cur_token1=get_next_token(tokens,i)
                    cur_token2=get_prev_token(tokens,i)
                    wbcur_token1 = word_base(cur_token1)
                    wbcur_token2 = word_base(cur_token2)
                    if len(wbcur_token1) == 1:
                       wbcur_token1 = ''
                    if len(wbcur_token2) == 1:
                       wbcur_token2 = ''
                       
                    if wbcur_token1!='' and wbcur_token2!='':
                        kladr_items = regcities.query( "SOCR ==  @curkeywords and ( NAME == '"+wbcur_token1+"' or NAME == '"+wbcur_token2+"' )",engine = 'python')
                        #kladr_items = regcities.query( "SOCR ==  @keywords and ( NAME.str.contains('"+cur_token1+"') or NAME.str.contains('"+cur_token2+"') )",engine = 'python')
                    elif wbcur_token1!='' and wbcur_token2=='':
                        kladr_items = regcities.query( "SOCR == @curkeywords and NAME == '" + wbcur_token1 + "'",engine = 'python')
                        #kladr_items = regcities.query( "SOCR == @keywords and NAME.str.contains('" + cur_token1 + "')",engine = 'python')
                    elif wbcur_token1=='' and wbcur_token2!='':
                        kladr_items = regcities.query( "SOCR == @curkeywords and NAME == '" + wbcur_token2 + "'",engine = 'python')
                        #kladr_items = regcities.query( "SOCR == @keywords and NAME.str.contains('" + cur_token2 + "')",engine = 'python')
                    else:
                        continue  
                    for k_row in kladr_items.itertuples():
                        row = k_row.FULL_CITY
                        parts = row.PARTS
                        parts_amount = len(parts)
                        if parts_amount>1:
                            startindex = max(0, i - parts_amount - 1)
                            endindex = min(len(tokens),i + parts_amount + 1)
                            parts_cnt = 0
                            for s in range(startindex,endindex):
                                for j in range(parts_amount):
                                    if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(tokens[s]) == get_part_of_speech(parts[j]):
                                        parts_cnt += 1
                            if parts_cnt == parts_amount: 
                                kladr_possible_items.append(row)
                                if parts_amount > max_len:
                                    max_len = parts_amount
                                
                        else:
                            if word_base(cur_token1) == word_base(row.PARTS[0]) and get_part_of_speech(cur_token1) == get_part_of_speech(row.PARTS[0]):
                                kladr_possible_items.append(row)
                            if word_base(cur_token2) == word_base(row.PARTS[0]) and get_part_of_speech(cur_token2) == get_part_of_speech(row.PARTS[0]):
                                kladr_possible_items.append(row)


                else:
                    cur_token=get_next_token(tokens,i)
                    wbcur_token = word_base(cur_token)
                    if wbcur_token == 1:
                       wbcur_token = ''
                    if wbcur_token!='':
                        kladr_items = regcities.query( "SOCR ==  @curkeywords and NAME == '"+wbcur_token+"'",engine = 'python')
                        #kladr_items = regcities.query( "SOCR ==  @keywords and NAME.str.contains('"+cur_token+"')",engine = 'python')
                        if len(kladr_items)==0 and i>0:
                            cur_token = get_prev_token(tokens,i)
                            wbcur_token = word_base(cur_token)
                            if wbcur_token!='':
                                kladr_items = regcities.query( "SOCR == @curkeywords and NAME == '" + wbcur_token + "'",engine = 'python')
                                #kladr_items = regcities.query( "SOCR == @keywords and NAME.str.contains('" + cur_token + "')",engine = 'python')
                    for k_row in kladr_items.itertuples(): 
                        row = k_row.FULL_CITY
                        parts = row.PARTS
                        parts_amount = len(parts)
                        if parts_amount>1:
                            startindex = max(0, i - parts_amount)
                            endindex = min(len(tokens), i + parts_amount + 1)
                            parts_cnt = 0
                            for s in range(startindex,endindex):
                                for j in range(parts_amount):
                                    if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(tokens[s]) == get_part_of_speech(parts[j]):
                                        parts_cnt += 1
                            if parts_cnt == parts_amount: 
                                kladr_possible_items.append(row)
                                if parts_amount > max_len:
                                    max_len = parts_amount
                                
                        else:
                            if word_base(cur_token) == word_base(row.PARTS[0]) and get_part_of_speech(cur_token) == get_part_of_speech(row.PARTS[0]):
                                kladr_possible_items.append(row)
                          
            elif i>0:
                cur_token = get_prev_token(tokens,i)
                wbcur_token = word_base(cur_token)
                if wbcur_token == 1:
                    wbcur_token = ''
                if wbcur_token!='':
                    kladr_items = regcities.query( "SOCR == @curkeywords and NAME == '"+ wbcur_token +"'",engine = 'python')
                    #kladr_items = regcities.query( "SOCR == @keywords and NAME.str.contains('"+ cur_token +"')",engine = 'python')
    
    ret=[]
    for i in kladr_possible_items:
        if i not in ret:
            ret.append(i)
    return ret

def getitems(regcities, tokens, constraints, excludes):

    kladr_possible_items = []
 
    tokens = [x for x in tokens if len(x) > 2]
    croped_tokens = word_base_array(tokens)
    
    keywords = ["ао","аобл","край","обл","респ"]
    words=[]
    for i in range(len(tokens)):
        kladr_items = pd.DataFrame()
        if tokens[i] not in key_words and len(tokens[i]) > 1 and not tokens[i].isdigit() and i not in excludes: 
            cur_token=word_base(tokens[i])
        else:
            continue
        words.append("NAME == '"+cur_token+"'")
        #words.append("NAME.str.contains('"+cur_token+"')")
    
    strwords =" or ".join(words)
    if strwords=="":
        return []
    kladr_items = regcities.query("SOCR != @keywords and ("+strwords+" )",engine = 'python')
    
    for k_row in kladr_items.itertuples():  
        row = k_row.FULL_CITY
        parts = row.PARTS
        parts_amount = len(parts)
        
        parts_cnt = 0
        for s in range(0,len(tokens)):
            for j in range(parts_amount):
                if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(tokens[s]) == get_part_of_speech(parts[j]):
                    parts_cnt += 1
        if parts_cnt == parts_amount: 
            kladr_possible_items.append(row)
    ret=[]
    for i in kladr_possible_items:
        if i not in ret:
            ret.append(i)
    return ret

  
                
def process_key_words(tokens, cities_dict):
    tokens = to_lower_case(tokens)
    address_key_words = ['обл','область','край','респ','республика','город','г','гор','поселок',
                         'посёлок','пгт','днт','снт','рп','дп','кп','нп',"ул","улиц","улица","проспект",
                         "пр-кт","мкр","кв","квартира","к","дер"]
    context = {}
    excludes=[]
    areas = pd.DataFrame()
    areas_array = []
    possible_cities = []
    cities_by_area = pd.DataFrame()
    streets_by_city = pd.DataFrame()
    houses_by_city = pd.DataFrame()
    croped_tokens = word_base_array(tokens)

    
    
    for i in range(len(tokens)):
        if tokens[i] == 'обл' or tokens[i] == 'область' or tokens[i] == 'край':
            excludes.append(i)
            if i > 0:
                if tokens[i-1] not in address_key_words:
                    reg = possible_address_objects([tokens[i-1]],df_subjects)
                    if len(reg)>0:
                        areas = pd.concat([areas,reg])
                        excludes.append(i-1)
            if i + 1 < len(tokens):
                if tokens[i+1] not in address_key_words:
                    reg = possible_address_objects([tokens[i+1]],df_subjects)
                    if len(reg)>0:
                        areas = pd.concat([areas,reg])
                        excludes.append(i+1)
        elif tokens[i] == 'хмао':
            regobj = AddrItem('ханты-мансийский автономный округ - югра','8600000000000')
            # row['NAME'] = 'Ханты-Мансийский Автономный округ - Югра'
            # row['CODE'] = '8600000000000'
            areas_array.append(regobj)
        elif tokens[i] == 'рх':
            regobj = AddrItem('хакасия','1900000000000')
            areas_array.append(regobj) 
        elif tokens[i] == 'рмэ':
            regobj = AddrItem('марий эл','1200000000000')
            areas_array.append(regobj)
        elif tokens[i] == 'янао':
            regobj = AddrItem('ямало-ненецкий','8900000000000')
            areas_array.append(regobj)
        elif tokens[i] == 'респ' or tokens[i] == 'республика':
            excludes.append(i)
            startindex = max(0,i-2)
            endindex = min(len(tokens),i+3)
            variances = []
            pos_exclude=[]
            for j in range(startindex,endindex):
                if i != j:
                    parts = parts_of_name(tokens[j])
                    for p in parts:
                        if p not in address_key_words and len(p) > 1 and not p.isdigit():
                            variances.append(p)
            res = word_base_array(variances)
            reg = possible_address_objects(res,df_subjects)
            for row in reg.itertuples():  
                parts = row.PARTS
                parts_amount = len(parts)
                
                parts_cnt = 0
                for s in range(startindex,endindex):
                    for j in range(parts_amount):
                        if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(tokens[s]) == get_part_of_speech(parts[j]):
                            parts_cnt += 1
                if parts_cnt == parts_amount:
                    new_i=len(areas)
                    areas_array.append(row)
                    for s in range(startindex,endindex):
                        for j in range(parts_amount):
                            if croped_tokens[s] == word_base(parts[j]) and get_part_of_speech(tokens[s]) == get_part_of_speech(parts[j]):
                                excludes.append(s)
        if len(areas)> 0 or len(areas_array)>0:
            break
                
    for row in areas.itertuples():
        areas_array.append(row)

            
    if len(areas_array) == 0:
        context['area'] = getRegion(tokens)
    else:      
        context['area'] = areas_array
    
    if len(context['area']) > 1:
        max_len=0
        for row in context['area']:
            if len(row.PARTS) > max_len:
                max_len = len(row.PARTS)
        checked = []
        if max_len > 1:
            for row in context['area']:
                if len(row.PARTS) == max_len:
                    checked.append(row)
            context['area'] = checked


    areaquery=""
    cnt=0
    for row in context['area']:
        cnt+=1
        area_code = row.Index[:2]
        if areaquery!='':
            areaquery+=' or '
        # areaquery+="CODE.str.startswith('"+area_code+"')"
        areaquery=area_code+"\d{11}"
    if areaquery!='':
        if cities_dict.get(area_code) is None:
            #regcities = df_cities.filter(regex=areaquery, axis=0)
            regcities = df_cities_parts.filter(regex=areaquery, axis=0)
            cities_dict[area_code] = regcities
        else:
            regcities = cities_dict[area_code]
    else:
        regcities = df_cities_parts

    cities=getitemsBykeyword(regcities,tokens, ['р-н', 'район','р','г', 'город','гор']+small_city_area + villagies_key_words, areaquery,  True, excludes)
    cities_socr_g = []
    cities_socr_rn = []
    cities_socr_snt = []
    cities_socr_vlg = []

    
    

                
    
    for city_el in cities:
        if city_el.SOCR == 'г':
            cities_socr_g.append(city_el)
        elif city_el.SOCR == 'р-н':
            cities_socr_rn.append(city_el)
        elif city_el.SOCR in small_city_area:
            cities_socr_snt.append(city_el)
        elif city_el.SOCR in villagies_key_words:
            cities_socr_vlg.append(city_el)
            


    snts=[]
    for city in cities_socr_g:
        city_code=city.Index[:8]

        cur_snt = list(filter(lambda x: x.Index[:8] == city_code, cities_socr_snt))
        snts=snts+cur_snt
    if len(snts)==0:
        snts=cities_socr_snt
    villagies = []
    for rn in cities_socr_rn:
        rn_code=rn.Index[:5]

        cur_vlgs = list(filter(lambda x: x.Index[:5] == rn_code, cities_socr_vlg))
        villagies=villagies+cur_vlgs
    if len(villagies)==0:
        villagies=cities_socr_vlg    
    unic_city_names = []
    unic_city_codes = []
    actual_codes = ['01','02','03','04','05','06','07','08','09'] + [str(i) for i in range(10,51)]    
    
    if len(villagies) == 0 and len(snts) == 0 and len(cities_socr_g) == 0:
        possible_cities = getitems(regcities, tokens,areaquery, excludes)
    
    cities_wo_keys = []    
    if len(possible_cities) > 0 and len(cities_socr_rn) > 0:
        for rn in cities_socr_rn:
            rn_code=rn.Index[:5]    
            cur_cities = list(filter(lambda x: x.Index[:5] == rn_code, possible_cities))
            cities_wo_keys=cities_wo_keys+cur_cities
        
    elif len(possible_cities) > 0 and len(context['area']) > 0:
        for row in context['area']:
            area_code = row.Index[:2]
            cur_cities = list(filter(lambda x: x.Index[:2] == area_code, possible_cities))
            cities_wo_keys=cities_wo_keys+cur_cities
    if len(possible_cities) > 0 and len(cities_socr_rn) == 0:
        for p in possible_cities:
            if p.SOCR == 'р-н' and p not in cities_socr_rn:
                cities_socr_rn.append(p)
        cities_wo_keys = possible_cities


    if len(cities_socr_rn) > 1:
        max_len_rn = 0  
        checked_rn = []
        for row in cities_socr_rn:
            if len(row.PARTS) > max_len_rn:
                max_len_rn = len(row.PARTS)
        if max_len_rn > 1:
            for row in cities_socr_rn:
                if len(row.PARTS) == max_len_rn:
                    checked_rn.append(row)
            cities_socr_rn = checked_rn
    if len(cities_socr_g) > 1:
        max_len_g = 0  
        checked_rn = []
        for row in cities_socr_g:
            if len(row.PARTS) > max_len_g:
                max_len_g = len(row.PARTS)
        if max_len_g > 1:
            for row in cities_socr_g:
                if len(row.PARTS) == max_len_g:
                    checked_rn.append(row)
            cities_socr_g = checked_rn
            
        actual = []    
        if len(cities_socr_g) > 1:
            for row in cities_socr_g:
                if row.Index[-2:] == '00':
                    actual.append(row)
            if len(actual) > 0:
                cities_socr_g = actual
    if len(snts) > 1:
        max_len = 0  
        checked_rn = []
        for row in snts:
            if len(row.PARTS) > max_len:
                max_len = len(row.PARTS)
        if max_len > 1:
            for row in snts:
                if len(row.PARTS) == max_len:
                    checked_rn.append(row)
            snts = checked_rn
        actual = []    
        if len(snts) > 1:
            for row in snts:
                if row.Index[-2:] == '00':
                    actual.append(row)
            if len(actual) > 0:
                snts = actual
    if len(villagies) > 1:
        max_len = 0  
        checked_rn = []
        for row in snts:
            if len(row.PARTS) > max_len:
                max_len = len(row.PARTS)
        if max_len > 1:
            for row in villagies:
                if len(row.PARTS) == max_len:
                    checked_rn.append(row)
            villagies = checked_rn
        actual = []    
        if len(villagies) > 1:
            for row in villagies:
                if row.Index[-2:] == '00':
                    actual.append(row)
            if len(actual) > 0:
                villagies = actual
    if len(cities_wo_keys) > 1:
        max_len = 0  
        checked_rn = []
        for row in cities_wo_keys:
            if len(row.PARTS) > max_len:
                max_len = len(row.PARTS)
        if max_len > 1:
            for row in cities_wo_keys:
                if len(row.PARTS) == max_len:
                    checked_rn.append(row)
            cities_wo_keys = checked_rn
        actual = []    
        if len(cities_wo_keys) > 1:
            for row in cities_wo_keys:
                if row.Index[-2:] == '00':
                    actual.append(row)
            if len(actual) > 0:
                cities_wo_keys = actual

    return context['area'],cities_socr_rn,cities_socr_g,snts,villagies,cities_wo_keys
       
def add_found_items_to_db(text):

    result = {}
    #cursor = cnxn.cursor()
    cur=0
    t1 = datetime.datetime.now()
    cities_dict={}
    cur+=1
    region = ''
    region_code = ''
    district = []
    district_code = []
    city = []
    city_code = []
    processed = 0
    str_address = re.sub(r'\d{6}','',text)
    str_address = re.sub(r'\d{2}:\d{2}:\d{5,9}:\d{3,4}','',str_address)
    tokens = lemmatize_sent(str_address) 
    if len(tokens) == 0:
        return None
    
    
    areas,districts,cities,small_cities,villagies,possible_cities = process_key_words(tokens, cities_dict)
    if len(areas) == 1:  
        region = areas[0].NAME
        region_code = areas[0].Index
        
    
    for item in districts:
        district.append(item.NAME)
        district_code.append(item.Index)


    if len(villagies) != 1 and len(small_cities) != 1:
        for item in cities:
            city.append(item.NAME)
            city_code.append(item.Index)
            if len(cities)==1 and region_code=='':
                area_code = item.Index[:2]
                areas_df = df_subjects.query('CODE.str.startswith("'+area_code+'")',engine = 'python')
                for row in areas_df.itertuples():
                    region = row.NAME
                    region_code = row.Index
    for item in small_cities:
        city.append(item.NAME)
        city_code.append(item.Index)
        if len(small_cities)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('CODE.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.NAME
                region_code = row.Index
    for item in villagies:
        city.append(item.NAME)
        city_code.append(item.Index)
        if len(villagies)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('CODE.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.NAME
                region_code = row.Index

    for item in possible_cities:
        if item.SOCR in city_key_word or item.SOCR in small_city_area or item.SOCR in villagies_key_words:
            city.append(item.NAME)
            city_code.append(item.Index)
        if len(possible_cities)==1 and region_code=='':
            area_code = item.Index[:2]
            areas_df = df_subjects.query('CODE.str.startswith("'+area_code+'")',engine = 'python')
            for row in areas_df.itertuples():
                region = row.NAME
                region_code = row.Index
    if region_code=='' or len(city)==0:
        processed = 0
    else:
        processed = 1
    
    strcity =", ".join(city)
    str_city_code =", ".join(city_code)
    
    strdistrict =", ".join(district)
    str_district_code =", ".join(district_code)
    if len(strcity) > 500 or len(str_city_code) > 500 or len(strdistrict) > 500 or len(str_district_code) > 500:
        return None

    t2 = datetime.datetime.now()
    dt = t2 - t1
    print(dt.total_seconds())
    if region != '' or strdistrict != '' or strcity != '':
        result['region'] = region
        result['region_code'] = region_code
        result['strdistrict'] = strdistrict
        result['str_district_code'] = str_district_code
        result['strcity'] = strcity
        result['str_city_code'] = str_city_code
        return result
    return None


        
        



def read_file(file_path):
    f = open(file_path,'r',encoding='utf-8')
    file_content = f.read()
    f.close()
    return file_content

url_dataset_path = 'my_path'
prefix = 'clean_'
files = os.listdir(path=url_dataset_path)
files.remove('headers')
files.remove('processed')
for file in files:
    if file[:6] == prefix:
        filename_parts = file.split('_')
        url_id = filename_parts[1]
        file_content = read_file(url_dataset_path + '\\' + file)
        file_content = file_content.split('\n')
        for el in file_content:
            sents = list(sentenize(el))
            for s in sents:
                s_text = s.text
                if len(s_text) > 0:
                    found_items = add_found_items_to_db(s_text) 
                    if found_items is not None:
                        print(el)
                        cursor = cnxn.cursor()
                        if len(s_text) > 1000:
                             s_text = s_text[:999]
                        cursor.execute("insert into [OARB].[ADDRESS] ([URL_ID], [SENT], [PROCESSED], [REGION], [REGION_CODE], [DISTRICT], [DISTRICT_CODE], [CITY], [CITY_CODE],[p_tag_text]) values (?,?,?,?,?,?,?,?,?,?)",url_id, s_text, 1,found_items['region'],found_items['region_code'],found_items['strdistrict'],found_items['str_district_code'],found_items['strcity'],found_items['str_city_code'],el)
                        cnxn.commit()

os.system(r"path/copy_and_del.bat")

cursor.close()
cnxn.close()


print('Done')
     
 



