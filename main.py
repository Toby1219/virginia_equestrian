import asyncio
import logging
import inspect
import os
import shutil
import csv
import sqlite3
from curl_cffi.requests import AsyncSession
from rich.logging import RichHandler
from fake_useragent import UserAgent
from bs4 import BeautifulSoup
import pandas as pd
from dataclasses import dataclass, field, asdict
import time

#Enable this for widows only
#asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

def logs():
    path = 'logs'
    if not os.path.exists(path):
        os.mkdir(path)

    frame = inspect.currentframe().f_back 
    file_name = os.path.basename(frame.f_globals['__file__'])
    logger_name = f"{file_name}"

    logger = logging.getLogger(logger_name)
    logger.setLevel(level=logging.DEBUG)

    terminal = RichHandler()
    logger.addHandler(terminal)
    
    handle = logging.FileHandler("logs/scrape.log", mode='w')
    formats = logging.Formatter("%(name)s - %(levelname)s - %(message)s \n")
    handle.setFormatter(formats)
    logger.addHandler(handle)
   
    return logger
    
log = logs()

@dataclass
class ScrapedData:
    url: str = None
    Name: str = None
    Email: str =  None
    Website: str = None
    Phone_no : float = None
    Address : str = None
    Contact_person : str = None
    Details : str = None

@dataclass
class ResultsList:
    lists: list[ScrapedData] = field(default_factory=list)

    def dataframe(self):
        return pd.json_normalize((asdict(data) for data in self.lists), sep='_')

    def save_to_csv(self, filename:str):
        self.dataframe().to_csv(f'{filename}.csv', index=False)

    def save_to_excel(self, filename:str):
        self.dataframe().to_excel(f'{filename}.xlsx', index=False)

    def save_to_sqlite3(self, filename:str):
        conn = sqlite3.connect(f'{filename}.db')
        self.dataframe().to_sql(name='scrape_data', con=conn, index=False, if_exists='replace')
        conn.close()

    def save_to_json(self, filename:str):
        self.dataframe().to_json(f'{filename}.json', orient='records', indent=2, )


async def fetch(url:str)->bytes:
    useragent = {
        "User-Agent":UserAgent().random
    }
    async with AsyncSession() as session:
        response = await session.get(url, headers=useragent, timeout=80)
    try:
        log.info(f"{url}: {response.status_code}")
        return response
    except:
        log.info(f"{url}: {response.status_code}")
        log.error(f'Error at: {url}', exc_info=True)

async def pipeline(value:str)->str:
    char_removed =['<br>', '<t>', '[MAP]']
    new_value: str = ''
    for char in char_removed:
        new_value = value.replace(char, '').strip()
    return new_value

async def scraper_more_links(responses:bytes)->list[str]:
    soup = BeautifulSoup(responses.text, 'html5lib')
    div_table = soup.find('div', {"id":"col1"})

    selector = 'table > tbody > tr'

    table = div_table.select(selector=selector)[5:]
    link_found= []
    for a in table:
        try:
            sel = a.select_one('td:nth-child(4) > p > a').get('href')
            da = f'http://www.virginiaequestrian.com/{sel}'
            link_found.append(da)
        except:
            pass
    return link_found

async def get_name(responses:bytes)->str:
    soup = BeautifulSoup(responses.text, 'html5lib')
    div_table = soup.find('div', {"id":"col1"})

    selector_ = 'table > tbody > tr > td > table > tbody > tr > td > h1'

    listing_name = div_table.select_one(selector=selector_).text.split('(')[0].strip()
    return listing_name

async def scrape_data(responses:bytes)->ScrapedData:
    soup = BeautifulSoup(responses.text, 'html5lib')
    div_table = soup.find('div', {"id":"col1"})
    
    selector = 'table:nth-child(2) > tbody > tr:nth-child(2)'

    table = div_table.select(selector=selector)
    data = ''
    for tab in table:
        name = tab.p.b.text
        email = tab.div.text
        phone_no = None

        try:
            p_tag = tab.find_all('p')[2].text.strip()
            info = p_tag.replace("Website:", '').replace(":  ", ' ').strip().split()
            phone_no = ''.join(info[1:])
        except:
            phone_no = 'No Pone number'
        
        try:
            website = tab.select('p')[2].text.strip().replace('Website:', '').strip()
        except:
            if 'Phone: ' in website:
                pass
            website = 'No website link'
            
        raw_address = tab.select_one('p').text.split('\n\t')[3:][0:4]
        address =''.join(raw_address).strip().split('Contact Person:')[0].replace('\t', '').replace('\xa0', '')        
        try:
            contact_person = tab.select_one('p').text.strip().replace('\t', '').replace('\xa0', '').split('\n')[3:][5].split('Contact Person:')[1].strip()
        except:
            contact_person = 'No contact Person'
        try:
            details = tab.select('p')[3].text.strip().replace('<br>', '')
        except:
            details = 'No Details'

        data = ScrapedData(
            url=responses.url,
            Name= await pipeline(name),
            Email= await pipeline(email),
            Website= await pipeline(website),
            Phone_no= await pipeline(phone_no),
            Address= await pipeline(address),
            Contact_person= await pipeline(contact_person),
            Details= await pipeline(details)
        )
    log.debug(f"Fetched: {data}")
    return data

def open_files()->list:
        urls = []
        with open('links.csv', 'r') as f:
            csv_reader = csv.reader(f)
            urls=[col for row in csv_reader for col in row]
        log.info(f'{urls}')        
        return urls

def file_soter(path_:str)->None:
    #create folder using file name
    def create_folder(path:str)-> dict:
        """ creates folders from files names """
        value_folder =[]
        list_ = os.listdir(path)
        for files_ in list_:
            n, _ = os.path.splitext(files_)
            if n not in value_folder:
                value_folder.append(n)
        for folder_name in value_folder:
            if not os.path.exists(f"{path}/{folder_name}"):
                os.makedirs(f"{path}/{folder_name}")
            else:
                value_folder = [f for f in os.listdir(path_) if os.path.isdir(os.path.join(path_, f))]
        return value_folder
    
    def create_parttern(folder:list[str])-> dict:
        """ create parttern for moving files to folder """
        folders = {}
        keys_ = [v[0:4].strip() for v in folder]
        folders = dict(zip(keys_, folder))
        return folders

    def get_files(path):
        #move files into folders
        files = [f for f in os.listdir(path) if os.path.isfile(os.path.join(path, f))]
        return files
        
    v_folders = create_folder(path_)
    folders = create_parttern(v_folders)
    files = get_files(path_)
    for file in files:
        file_name = os.path.basename(file)
        #print(file)
        
        prefix = file_name[0:4].strip()
        if prefix in folders:
            dest = os.path.join(folders[prefix], file_name)    
            if os.path.exists(f"{path_}/{file}"):
                shutil.move(f"{path_}/{file}", f"{path_}/{dest}")
            else:
                log.error('Did not sort file properly. :( )', exc_info=True)
    log.debug('Done sorting files.... :) ')

async def data_colection_writer(result, f_name:str)->str:
    path = 'data_scraped'
    if not os.path.exists(path):
        os.mkdir(path)
    
    data_collectoin = ResultsList()
    for data in result:
        data_collectoin.lists.append(data)

    data_collectoin.save_to_csv(f'{path}/{f_name}')
    data_collectoin.save_to_json(f'{path}/{f_name}')
    data_collectoin.save_to_excel(f'{path}/{f_name}')

    file_soter(path_=path)
    return path

async def main():
    count = 1
    urls = open_files()
    for url in urls[0:11]:
        log.debug(f'Runing...:: {count} times')

        tasks= fetch(url)
        response = await tasks

        task2 = scraper_more_links(response)
        data_links = await task2
        log.debug(f'Minor links: {data_links}')

        task3 = get_name(response)
        list_name =  await task3  
    
        tasks4 = [fetch(url) for url in data_links]
        response2 = await asyncio.gather(*tasks4)
        
        tasks5 = [scrape_data(resp2) for resp2 in response2]
        result = await asyncio.gather(*tasks5)

        try:
            path = await data_colection_writer(result, list_name)
        except:
            log.error('Error:', exc_info=True)

        count += 1
    
    #file_soter(path)
    

if __name__ == '__main__':
    start_time = time.perf_counter()

    asyncio.run(main())

    end_time = time.perf_counter()
    execution_time = end_time - start_time
    log.debug(f"Execution time: {execution_time:.5f} seconds")
    print()
        
        
