
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


segmenter = Segmenter()
morph_vocab = MorphVocab()
emb = NewsEmbedding()
morph_tagger = NewsMorphTagger(emb)
ner_tagger = NewsNERTagger(emb)

vowels = ['а','у','о','ы','и','й','э','я','ю','ё','е']
symbols = "().,!;'?-\"@#$%^&*+ "
key_words = ["г","обл","адрес","росреестр","ул","д","кв","республик","рх","рп","р","н",
             "корп","федерац","область","округ","город","улиц","квартир","п","пер","мкр",
             "муниципальн","район","дер","республика","улица"]

stop_words = ["федерация","муниципальный","рб","жилое","помещение","проектный",
              "номер","этаже","подъезде","общей","проектной","площадью","учётом","холодных",
              "помещений","квартира","однокомнатная","расположенная","этажного","жилого",
              "находящаяся","находящегося","кадастровый","земельного","участка","управление",
              "рф","адресу","адрес","росреестр","росреестра","республика","респ","край"]

cnxn = pyodbc.connect('DRIVER={SQL Server};SERVER=DESKTOP-2ELPTI6;DATABASE=prom')





streets_dict = {}    


def word_base(word):
    index = len(word)
    for i in range(len(word)-1,0,-1):
        if word[i] in vowels:
            index = i
        else:
            break
    return word[:index]

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

def lemmatize_sent(sent,excludes):
    sent = sent.replace('..','.')
    sent = sent.replace('пр-кт','проспект')
    #sent = sent.replace(' р.п. ',' рп ')
    doc = Doc(sent)
    doc.segment(segmenter)
    tokens = []
  
    for t in doc.tokens:
        tk = t.text.lower()
        if tk not in symbols and tk not in stop_words:
            mas = tk.split('-')
            for el in mas:
                if el not in excludes:
                # lemmatized_tokens.append(t.lemma)
                    tokens.append(el)    
    return tokens


#загрузка улиц по ключевым словам 
#как сравнивать contains или разбить на части?
df_area_street = pd.read_sql("SELECT [NAME],[SOCR],[CODE] FROM [OARB].[STREET] (nolock) where lower([NAME]) like '%край%' and lower([NAME]) not like '%крайняя%' and lower([NAME]) not like '%крайний%' and lower([NAME]) not like '%окрайная%' and lower([NAME]) not like '%крайние%' and lower([NAME]) not like '%крайная%' and lower([NAME]) not like '%крайнея%' and lower([NAME]) not like '%крайнюка%' and lower([NAME]) not like '%окрайный%' and lower([NAME]) not like '%крайнюковская%' and lower([NAME]) not like '%крайновых%' and lower([NAME]) not like '%макрай%' and lower([NAME]) not like '%крайникова%'" ,cnxn, index_col="CODE")
df_republic_street = pd.read_sql("SELECT [NAME],[SOCR],[CODE] FROM [OARB].[STREET] (nolock) where lower([NAME]) like '%респ%' or lower([NAME]) like '%республика%' and lower([NAME]) not like '%переспективная%' and lower([NAME]) not like '%респект%' and lower([NAME]) not like '%корреспондентский%' and lower([NAME]) not like '%чересполосный%' and lower([NAME]) not like '%корреспондентов%'",cnxn, index_col="CODE")



def load_city_streets(code):
    codes = code.split(",")
    streets_parts_array = []
    for c in codes:
        df_streets = pd.read_sql("SELECT lower(trim(NAME)) as NAME,trim(CODE) as CODE,lower(trim(SOCR)) as SOCR FROM [OARB].[STREET] (nolock) where trim(CODE) like '"+c[:11]+"%' order by [NAME]",cnxn, index_col="CODE")

        if len(df_streets) == 0:
            continue
        df_streets['PARTS'] = df_streets['NAME'].apply(parts_of_name) 
        for row in df_streets.itertuples():
            row_name_parts = word_base_array(parts_of_name(row.NAME))
            row_code = row.Index
            for i in range(len(row_name_parts)):
                row_dict = {}
                row_dict['NAME'] = row_name_parts[i]
                row_dict['CODE'] = row_code + str(i+1)
                row_dict['SOCR'] = row.SOCR
                row_dict['FULL_CITY'] = row
                streets_parts_array.append(row_dict)
    if len(streets_parts_array) == 0:
        return streets_parts_array
    df_street_parts = pd.DataFrame(streets_parts_array)   
    df_street_parts = df_street_parts.set_index('CODE') 
    return df_street_parts

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
    
def get_house_number(house_regex,str_address_wo_flat):
    #house_regex = r"(( |,|, )((дом)|(д)){1}(\.| |\. )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
    found_house_number = re.search(house_regex,str_address_wo_flat) 
    if found_house_number is not None:
        found_house_number = found_house_number.group(0)
        house_number = re.search(r'(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?)',found_house_number)
        if house_number is not None:
            found_house_number = house_number.group(0)
        else:
            found_house_number = ""
    else:
        found_house_number = ""
    return found_house_number
def getitemsBykeyword(regcities, tokens, excludes,str_address):

    kladr_possible_items = []
    croped_tokens = word_base_array(tokens)
    curkeywords=[]
    street_key_words = ["ул","улица","улице","пр","проспект","пл","площадь","аллея","кв-л","пер",
                        "переулок","мкр","микрорайон"]
    street_kw = ["ул","улица","улице"]
    avenue_kw = ["пр","проспект"]
    square_kw = ["пл","площадь"]
    side_street_kw = ["пер","переулок"]
    mcrs = ["мкр","микрорайон"]
    max_len = 0
    key_word_type = ""
    for i in range(len(tokens)):
        kladr_items = pd.DataFrame()
        #if tokens[i] in mcrs:
            #if i<len(tokens)-1:
                #if tokens[i+1].isdigit():
                   # kladr_items = regcities.query( "SOCR == 'мкр' and NAME == '"+tokens[i+1]+"' ",engine = 'python')
            #if len(kladr_items)==0 and i>0 and tokens[i-1].isdigit():
                #kladr_items = regcities.query( "SOCR == 'мкр' and NAME == '"+tokens[i-1]+"' ",engine = 'python')
            #for row in kladr_items.itertuples():
                #kladr_possible_items.append(row)
        if tokens[i] in street_key_words and tokens[i] not in excludes:
            if tokens[i] in street_kw:
                key_word_type = "STREET"
            elif tokens[i] in avenue_kw:
                key_word_type = "AVENUE"
            elif tokens[i] in square_kw:
                key_word_type = "SQUARE"
            elif tokens[i] in side_street_kw:
                key_word_type = "SIDE"
            elif tokens[i] in mcrs:
                key_word_type = "MCR"
            
            if i<len(tokens)-1:           
                
                cur_token=get_next_token(tokens,i)                    
                wbcur_token = word_base(cur_token)
               
                if len(wbcur_token) > 1:
                    if len(regcities) == 0:
                        continue
                    kladr_items = regcities.query( " NAME == '"+wbcur_token+"' ",engine = 'python')
                if len(kladr_items)==0:
                    cur_token=get_prev_token(tokens,i)                    
                    wbcur_token = word_base(cur_token)
                    if len(wbcur_token) > 1:
                        kladr_items = regcities.query( " NAME == '"+wbcur_token+"' ",engine = 'python')
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
                        if wbcur_token == word_base(row.PARTS[0]) and get_part_of_speech(cur_token) == get_part_of_speech(row.PARTS[0]):
                            kladr_possible_items.append(row)
    
    
    ret=[]
    checked = []
    for i in kladr_possible_items:
        if i not in ret:
            ret.append(i)
    if len(ret) > 1:
        if key_word_type == "STREET":
            for r in ret:
                if r.SOCR == 'ул':
                    checked.append(r)
        elif key_word_type == "AVENUE":
            for r in ret:
                if r.SOCR == 'пр-кт':
                    checked.append(r)
        elif key_word_type == "SQUARE":
            for r in ret:
                if r.SOCR == 'пл':
                    checked.append(r)
        elif key_word_type == "SIDE":
            for r in ret:
                if r.SOCR == 'пер':
                    checked.append(r)
        elif key_word_type == "MCR":
            for r in ret:
                if r.SOCR == 'мкр':
                    checked.append(r)
        else:
            checked = ret
    if len(checked) > 1:
        actual = []
        for c in checked:
            if c.Index[-2:] == '00':
                actual.append(c)
        if len(actual) == 1:
            checked = actual
    
    if len(checked) > 1:
        max_len_rn = 0  
        checked_rn = []
        for row in checked:
            if len(row.PARTS) > max_len_rn:
                max_len_rn = len(row.PARTS)
        if max_len_rn > 1:
            for row in checked:
                if len(row.PARTS) == max_len_rn:
                    checked_rn.append(row)
            checked = checked_rn
        
    if len(checked) == 0:            
        checked = ret
    if len(checked) == 1:
        street_end_index = str_address.find(checked[0].NAME) + len(checked[0].NAME)
    else:
        street_end_index = -1
    return checked, street_end_index

def getitems(regcities, tokens, excludes,str_address):

    kladr_possible_items = []
 
        
    croped_tokens = word_base_array(tokens)

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
        return [],-1
    kladr_items = regcities.query(strwords,engine = 'python')
    
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

    if len(ret) > 1:
        max_len_rn = 0  
        checked_rn = []
        for row in ret:
            if len(row.PARTS) > max_len_rn:
                max_len_rn = len(row.PARTS)
        if max_len_rn > 1:
            for row in ret:
                if len(row.PARTS) == max_len_rn:
                    checked_rn.append(row)
            ret = checked_rn

    if len(ret) == 1:
        street_end_index = str_address.find(ret[0].NAME) + len(ret[0].NAME)
    else:
        street_end_index = -1

    return ret, street_end_index



def load_addresses_from_db():
    addresses = pd.read_sql("SELECT [URL_ID],[SENT],[REGION],[REGION_CODE],[DISTRICT],[DISTRICT_CODE],[CITY],[CITY_CODE] FROM [OARB].[ADDRESS] (nolock) where [PROCESSED] = 1 and [REGION_CODE] <> '' and [CITY_CODE] <> '' order by [REGION],[CITY]",cnxn)
    
    cur = 0
    cursor = cnxn.cursor()
    t1 = datetime.datetime.now()
    full_city_code=''
    for addr in addresses.itertuples():
        url_id = addr.URL_ID
        region = addr.REGION
        region_code = addr.REGION_CODE
        district = addr.DISTRICT
        district_code = addr.DISTRICT_CODE
        city = addr.CITY
        city_arr = city.split(',')
        cur_city_code = addr.CITY_CODE
        cur_city_code_arr = cur_city_code.split(',')
        str_address = re.sub(r'\d{6}','',addr.SENT).lower()
        str_address = re.sub(r'\d{2}:\d{2}:\d{5,9}:\d{3,4}','',str_address)
        #Как быть со сравнением города, если есть несколько городов
        if full_city_code!=cur_city_code:
            city_code = addr.CITY_CODE
            streets_dict = load_city_streets(city_code)
            
            full_city_code=cur_city_code
        if len(streets_dict) == 0:
                continue
        excludes = parts_of_name(region)
        if district != '':
            for d in district.split(","):
                parts = re.sub(r'-',' ',d).split()
                for p in parts:
                    excludes.append(p)
        for c in city.split(","):            
            parts = re.sub(r'-',' ',c).split()
            for p in parts:
                excludes.append(p)
                
                
       

            
             
        street = ""
        street_code = ""
        #processed = 0
        #Добавить похожую проверку на республику, край, район и тд по ключевым словам
        reg_mcr = re.findall(r"(\d+)(\-)*[й]*\ ((мкр)|(микрорайон)|(мк-н))",str_address)
        if len(reg_mcr) > 0:
            for i in range(len(reg_mcr)):
                if reg_mcr[0][i].isdigit():
                    street = reg_mcr[0][i]
                    continue
        if street == "":
            tokens = lemmatize_sent(str_address,excludes) 
            if len(tokens) == 0:
                continue

            street, street_end_index = getitemsBykeyword(streets_dict, tokens, '',str_address)
     
            if len(street) == 0:
                street, street_end_index = getitems(streets_dict,tokens,[],str_address)

        if len(street) > 0:
            if street_end_index != -1:
                index_range = min(len(str_address),street_end_index+25)
                str_address = str_address[street_end_index:index_range]
                #проверяем номер квартиры
                str_address_wo_flat = ""
                flat_regex = r"((кв(\.)?)|(квартира)|(квар(\.)?))(\ )?\d{1,4}"
                found_flat_number = re.search(flat_regex,str_address) 
                if found_flat_number is not None:
                    found_flat_number = found_flat_number.group(0) 
                    str_address_wo_flat = str_address.replace(found_flat_number,'')
                    flat_number = re.search(r'\d{1,4}',found_flat_number)
                    if flat_number is not None:
                        found_flat_number = flat_number.group(0) 
                    else:
                        found_flat_number = ""
                else:
                    found_flat_number = ""
                    
                #проверяем номер дома
                #house_regex = r"(((дом)|(влд)|(вл)|(владение)|(уч(\.| №|.№))|(д)|(участок(\ ?№)?))(\.| |\. )?)?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(/)?(\d{1,4})?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)(\ ?(((,|, | )?корпус)|((,|, | )?корп(\.)?)|((,|, | )?к(\.)?))\ ?\d{1,3})?(((\ )|,|, )?((строение)|(стр(\.)?)|(с(\.)?))\ ?\d{1,3})?(((\ )|,|, )?((литер)|(литера)|(лит(\.)?)|(л(\.)?))\ ((\d{1,3})|((а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)))?(((\ )|,|, )?((помещение)|(пом(\.| )?)|(с(\.)?))\ ?\d{1,3})?"
                if str_address_wo_flat == "":
                    str_address_wo_flat = str_address
                house_regex = r"(( |,|, )((дом)|(д)){1}(\.| |\. )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
                found_house_number = get_house_number(house_regex,str_address_wo_flat)
                if found_house_number == "":
                    house_regex = r"(( |,|, )((влд)|(владение)){1}(\.| |\. )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
                    found_house_number = get_house_number(house_regex,str_address_wo_flat)
                if found_house_number == "":
                    house_regex = r"(( |,|, )((уч)|(участок)){1}(\.| |\. |№| №| № )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
                    found_house_number = get_house_number(house_regex,str_address_wo_flat)
                if found_house_number == "":
                    house_regex = r"(( |,|, )((литер)|(стр)|(строение)|(пом)|(помещение)|(корп)|(корпус)|(к)|(литера)){1}(\.| |\. |№| №| № )?(\d{1,4}(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?(\/)?(\d{1,4}?(а|б|в|г|д|е|ж|з|к|л|м|н|п|р|с|т)?)?))"
                    found_house_number = get_house_number(house_regex,str_address_wo_flat)
                if found_house_number == "":
                    house_regex = r"\d{1,4}"
                    found_house_number = get_house_number(house_regex,str_address_wo_flat)
                # \d{1,4}-\d{1,4}        
            
                    
        #Проверка на тип для того, чтобы не попадал микрорайон. Нужно ли искать его в датафрейме?    
        if len(street) == 1 and type(street) != str:
            street_code = street[0].Index
            if len(cur_city_code_arr) > 1:
                city_code_by_street = street_code[:11]
                cur_city_code_arr_croped = list(map(lambda x: x[:11],cur_city_code_arr))
                ind = cur_city_code_arr_croped.index(city_code_by_street)
                checked_city = city_arr[ind]
                checked_code = city_code_by_street
                cursor.execute("update [OARB].[ADDRESS] set [CITY] = ?, [CITY_CODE] = ? where [URL_ID] = ? and [SENT] = ?", checked_city,checked_code,url_id,addr.SENT)
       
            cursor.execute("update [OARB].[ADDRESS] set [PROCESSED] = 2, [STREET] = ?, [STREET_CODE] = ?, [HOUSE] = ? where [URL_ID] = ? and [SENT] = ?", street[0].NAME,street_code,found_house_number,url_id, addr.SENT)
        print (cur)
        cnxn.commit()
        #if cur % 100 == 0:
            #cnxn.commit()
    t2 = datetime.datetime.now()
    dt = t2 - t1
    print(dt.total_seconds())
        
        
load_addresses_from_db()     

          
cnxn.close()      
        
        
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    