from lxml import etree, html
import shutil
import requests
from pathlib import Path
import hashlib
import csv

# Host config 
server_host = "" # сайт-цель

catalog_sections = [ 
    "", 
    ""
] # Корневые разделы товаров категории (увы, подкатегории не парсятся, нужно вписывать их сюда)

use_pagination = True # если на страницах есть пагинация, надо будет изменить парметр xml_parse_pagination_element

#---------------------------------# 

# File Config 
work_dir = '' # Рабочая папка парсера, в ней должен лежать парсер, csv файл с готовыми товарами в неё же будет сохранён результат парсинга

input_file_name = ".csv" # если у вас есть список товаров в расширении csv, с недостающими данными и вам требуется спарсить с какого-нибудь другого сайта нужную инфу
output_file_name = 'items_output.csv'

# параметр указывает на то, что у вас есть фаил с готовыми товарами и вам нужно спарсить данные только для тех товаров, которые указаны в input_file_name
# на выходе вы получите недостающие поля для ваших товаров
merging_mode = True

# XML Params 

pagination_get_param = 'PAGEN_1' # Имя переменной, которая отвечает за пагинацию, к сожалению POST не поддерживается (sorry)
xml_parse_pagination_element = '/html/body/div[7]/div/div[2]/div[2]/div/div[6]/div/div/a' # xml path кнопки пагинации
xml_parse_target_on_catalog = '//*[@class="product-item-container"]/div/div[2]/h4/a' # xml селектор товара внутри каталога, нужен для парсинга ссылки на сам товар

xml_item_container = '//*[@class="main_proprs"]'
xml_parse_targets_item = { # используется, если парсится что-то конкретное, к примеру: есть xpath до конкретного значения, а не поля(фрейма) на котором он находится
    "IE_DETAIL_PICTURE":'//*[@id="two"]/div/div/img', # xpath до картинки товара (предусмотрена проверка на несоклько img внутри одного контейнера)
    "IP_PROP138":'', # Изображение в разрезе
    "IP_PROP145":'//*[@class="mini_props"][1]/span',
    "IE_BRAND":'//*[@class="mini_props"][2]/span', 
    "IP_PROP296":'//*[@class="main_proprs"]/table/tr[1]/td[2]/span', # Габарит редуктора по серии
    "IP_PROP295":'//*[@class="main_proprs"]/table/tr[2]/td[2]/span', # Тип редуктора (одноступенчатый)
    "IP_PROP274":'//*[@class="main_proprs"]/table/tr[3]/td[2]/span', # Тип установки (L - Фланец )
    "IP_PROP324":'//*[@class="main_proprs"]/table/tr[4]/td[2]/span', # Передаточное отношение, i (11)
    # "":'//*[@class="main_proprs"]/table/tr[5]/td[2]/span', # Обороты выходного вала, об/мин ( 126.4 )
    # "":'//*[@class="main_proprs"]/table/tr[6]/td[2]/span', # Мощность электродвигателя, кВт ( Y0.55 )
    "IP_PROP305":'//*[@class="main_proprs"]/table/tr[7]/td[2]/span', # Количество полюсов электродвигателя (4P (~1500 об/мин) )
    "IP_PROP298":'//*[@class="main_proprs"]/table/tr[8]/td[2]/span', # Момент, Нм ( 37 )
    # "":'//*[@class="main_proprs"]/table/tr[9]/td[2]/span', # Сервис-фактор мотор-редуктора ( 1.80 )
    
    "IE_PREVIEW_PICTURE":'',
}
# Желательно избегать путей, где есть id, ибо id товара внутри каждой КТ может быть разным

#CSV Merge params
xml_equal_type = 'IP_PROP145'
# По этому параметру в нужные строки будут вставляться недостающие(спаршенные) характеристики 

CMS_UPLOAD_DIR_NAME = './upload_export'
# данное значение особо важно, ибо в таблице будет указан путь до изображений.

CSV_READED = [];
with open('./' + input_file_name, encoding='utf-8') as f:
    file_read = csv.reader(f)
    CSV_READED = list(file_read)

# print(CSV_READED[0])
# print(CSV_READED[1]) 

def GetKeyByValue(value): 
    find_key = 0 
    for i in range(0, len(CSV_READED[0])): 
        if CSV_READED[0][i] == value: 
            find_key = i 
            break
    return find_key

def MergeTables(tab1, tab2): 
    global CSV_READED 
    key_i = 0
    for key, value in tab1.items(): 
        for i in range(0, len(CSV_READED)): #раскладываем CSV по строкам
            if value and value[xml_equal_type] == CSV_READED[i][GetKeyByValue(xml_equal_type)]:
                for uK, uV in value.items(): 
                    if CSV_READED[i][GetKeyByValue(uK)] == '':
                        CSV_READED[i][GetKeyByValue(uK)] = uV
                value = None 
    return CSV_READED


#-------------------------------------------# Somefunc
output_array = {}
catalog_section_elements = [] * len(catalog_sections) 

count_items = 0 
progress = 0 

if use_pagination: 
    start_page = 1 # не реализовано
    current_page = 1
    end_page = 0 


if use_pagination and (not pagination_get_param or pagination_get_param == ''): 
    print("Please set <pagination_get_param> for correctly working.")
    exit(1)
 
ignore_get_link = lambda link: link.split("?")[0] 

def ResultToCSV(result): 
    with open("./" + output_file_name, "w", newline="", encoding='utf-8') as f:
        writer = csv.writer(f)
        for r in result: 
            writer.writerow(r)
    
def XMLParse(xml_content, xml_path): 
    return xml_content.xpath(xml_path)

def XMLToString(xml_obj): 
    return etree.tostring(xml_obj, pretty_print=True)  

def XMLParseString(xml_path):
    return etree.tostring(XMLParse(xml_path), pretty_print=True) 

def HtmlToXML(content, *echooff: bool): 
    if not echooff:
        print('converting html to xml format') 
    return html.fromstring(content) 

def ParseHtmlDocument(link: str, *echooff: bool):
    head = {
        "X-Requested-With": "XMLHttpRequest",
        "Accept": "text/html",
        "Accept-Encoding": "gzip, deflate, br",
        "Connection": "keep-alive",
    }
    r = requests.get(link, headers=head)
    if r.status_code == 200: 
        if not echooff:
            print(f"{link} start parsing") 
        return r.content
    else:
        if not echooff:
            print(f"{link} parse error\nReason: Error response {r.status_code} code") 

def SavePicture(item_article, link_image): 
    response = requests.get(server_host + link_image, stream=True)

    fullpath = Path(CMS_UPLOAD_DIR_NAME + "/images/" + link_image)
    path = Path(fullpath.parent).mkdir(parents=True, exist_ok=True)

    with open(str(fullpath), 'wb') as out_file:
        shutil.copyfileobj(response.raw, out_file)
    del response

    return str(fullpath)

def XML_PARSE(page_link): 
    html_content = ParseHtmlDocument(page_link) 
    xml_content = HtmlToXML(html_content)

    if use_pagination: 
        parse_pagination = XMLParse(xml_content, xml_parse_pagination_element)
        end_page = int(parse_pagination[-1].get('href').split('?')[1].split('=')[1]) 
        print(f"Catalog {page_link} pages: {end_page}")

        for pages in range(2, end_page+1): 
            xml_content = HtmlToXML(ParseHtmlDocument(server_host + parse_pagination[0].get('href').split('?')[0] + '?' + pagination_get_param + '=' + str(pages)))

            parse_items_in_page = XMLParse(xml_content, xml_parse_target_on_catalog)

            for IXML in parse_items_in_page:
                catalog_section_elements.append(IXML.get('href'))
    else: 
        print(f"Catalog {page_link}")

    parse_items_in_page = XMLParse(xml_content, xml_parse_target_on_catalog)

    for IXML in parse_items_in_page: 
        catalog_section_elements.append(IXML.get('href'))


def ItemParseData(link_item): 
    global progress, server_host 
    progress = progress + 1 # Счётчик прогресса

    html_content = ParseHtmlDocument(server_host + link_item) 
    xml_content = HtmlToXML(html_content)

    item_article = (XMLParse(xml_content, xml_parse_targets_item['IP_PROP145'])[0].text)

    for key,value in xml_parse_targets_item.items(): 
        if value == '': 
            continue 
        if key == 'IP_PROP145': 
            output_array[link_item][key] = item_article 
            continue 
        if key == 'IE_DETAIL_PICTURE': 
            for image_link in XMLParse(xml_content, value):
                if 'razrez' in image_link.get('src'):
                    output_array[link_item]['IP_PROP138'] = SavePicture(item_article, image_link.get('src')).replace("\\", '/') 
                else: 
                    path = SavePicture(item_article, image_link.get('src')).replace('\\', '/') 

                    output_array[link_item]['IE_DETAIL_PICTURE'] = path  
                    output_array[link_item]['IE_PREVIEW_PICTURE'] = path
        else: 
            output_array[link_item][key] = (XMLParse(xml_content, value)[0].text)
    print(f'Items parsing {progress} of {count_items}')

    #print(xml_content)
    #print(XMLToString(parse_pagination)) 


for link in catalog_sections: 
    XML_PARSE(server_host + link) 

output_array = output_array.fromkeys(catalog_section_elements, {}) 
# создаём словарь, где ссылка на элемент - уникальный id  

count_items = len(catalog_section_elements) 

for item in catalog_section_elements: 
   output_array[item] = output_array[item].fromkeys(xml_parse_targets_item.keys())
   ItemParseData(item) 

# # For test one variable
# output_array[catalog_section_elements[0]] = output_array[catalog_section_elements[0]].fromkeys(xml_parse_targets_item.keys(),[])
# ItemParseData(catalog_section_elements[0]) 

# output_array[catalog_section_elements[1]] = output_array[catalog_section_elements[1]].fromkeys(xml_parse_targets_item.keys(),[])
# ItemParseData(catalog_section_elements[1]) 

# print(output_array)

result = MergeTables(output_array, CSV_READED)

ResultToCSV(result)