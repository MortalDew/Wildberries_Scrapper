from datetime import date
from os import path, mkdir
import logging
import json
import asyncio

import xlsxwriter
from aiohttp import ClientSession, ServerDisconnectedError, ContentTypeError

class WildBerriesParser:

    def __init__(self):
        """
        Initialize a new instance of the WildBerriesParser class.

        This constructor sets up the parser object with default values
        for its attributes.

        Args:
            None

        Returns:
            None
        """
        if (not path.exists("bin")):
            mkdir("bin")
        logging.basicConfig(level=logging.INFO, filename="bin/py_log.log",filemode="w")
        self.headers = {'Accept': "*/*",
                        'User-Agent': "Mozilla/5.0 Gecko/20100101 Firefox/62.0"}
        self.run_date = date.today()
        self.directory = path.dirname(__file__)
        self.col = 0
        self.level = -1

    async def aio_req(self, url_f, *, headers_f: dict):
        """
        Wrapping for aiohttp request mechanism 

        Aiohttp is a library for async requests, this function
        mimic requests library format for interface

        Returns:
            json: response from url
        """
        async with ClientSession() as session:
            async with session.get(url=url_f, headers=headers_f) as response:
                return_response = await response.json()
        return return_response

    def download_current_catalogue(self) -> str:
        """
        Download the  catalogue from wildberries.ru and save it in JSON format.

        If an up-to-date catalogue already exists in the script's directory,
        it uses that instead.

        Returns:
            str: The path to the downloaded catalogue file.
        """
        local_catalogue_path = path.join(self.directory, 'bin', 'wb_catalogue.json')
        if (not path.exists(local_catalogue_path)
                or date.fromtimestamp(int(path.getmtime(local_catalogue_path)))
                > self.run_date):
            url_loc = ('https://static-basket-01.wb.ru/vol0/data/'
                   'main-menu-ru-ru-v2.json')
                   
            response = asyncio.run(self.aio_req(url_loc, headers_f=self.headers))
            with open(local_catalogue_path, 'w', encoding='UTF-8') as my_file:
                json.dump(response, my_file, indent=2, ensure_ascii=False)
        return local_catalogue_path
    
    def process_catalogue(self, local_catalogue_path: str) -> list:
        """
        Process the locally saved JSON catalogue into a list of dictionaries.

        This function reads the locally saved JSON catalogue file,
        invokes the traverse_json method to flatten the catalogue,
        and returns the resulting catalogue as a list of dictionaries.

        Args:
            local_catalogue_path (str): The path to the locally saved
              JSON catalogue file.

        Returns:
            list: A list of dictionaries representing the processed catalogue.
        """
        catalogue = []
        with open(local_catalogue_path, 'r') as my_file:
            asyncio.run(self.traverse_json(json.load(my_file), catalogue, -1))
        return catalogue

    async def traverse_json(self, parent_category: list, flattened_catalogue: list, level: int):     
        """
        Recursively traverse the JSON catalogue and flatten it to a list.

        This function runs recursively through the locally saved JSON
        catalogue and appends relevant information to the flattened_catalogue
        list.
        It handles KeyError exceptions that might occur due to inconsistencies
        in the keys of the JSON catalogue.

        Args:
            parent_category (list): A list containing the current category
              to traverse.
            flattened_catalogue (list): A list to store the flattened
              catalogue.

        Returns:
            None
        """
        for category in parent_category:
            level += 1
            try:
                flattened_catalogue.append({
                    'name': category['name'],
                    'url': category['url'],
                    'shard': category['shard'] if 'shard' in category else 99999,
                    'query': category['query'],
                    'level': level,
                    'id': category['id']
                })
                logging.info(f"name: {category['name']}, url: {category['url']}")
            except KeyError:
                level -= 1
                continue
            if 'childs' in category:
                await self.traverse_json(category['childs'], flattened_catalogue, level)
            # on id=130090 redirects to id=129073, which is still accessable 
            # from original position on catalog tree, so not visiting child nodes
            if 'childs' not in category and category['id']!=130090:
                level += 1
                await self.node_json(category, flattened_catalogue, level)
                level -= 1
            level -= 1

    async def node_json(self, category: list, flattened_catalogue: list, level: int):
        """
        Get all filter information for category and add needed one using keys

        Download the filter information for category from wildberries.ru and process it 
        and appends relevant information to the flattened_catalogue list. 

        Args:
            category (list): A list containing the current category
              to get information about.
            flattened_catalogue (list): A list to store the flattened
              catalogue.
            level (int): Recursion level

        Returns:
            None
        """
        if (category['name']=="Комбинезоны"):
            pass
        try:
            url_loc = (f"https://catalog.wb.ru/catalog/{category['shard']}/"
                    f"/v4/filters?appType=1&{category['query']}&curr=rub"
                    f"&dest=-8144334&spp=30")
            response = await self.aio_req(url_loc, headers_f=self.headers)
        except ServerDisconnectedError:
            logging.info("No filters available request")
            return
        except ContentTypeError:
            logging.info("No filters available json")
            return
 
        try:
            if (response['data']['filters'][0]['name']=="Категория"):
                for info in response['data']['filters'][0]['items']:
                    try:
                        flattened_catalogue.append({
                            'parent_name': category['name'],
                            'name': info['name'],
                            'level': level,
                            'id': info['id']
                        })
                        logging.info(f"name: {info['name']}, url: {category['name']}")
                    except:
                        continue
            else:
                logging.info("No filters available inner")
        except:
            logging.info("No filters available outer")
        
                
    def save_to_excel(self, file_name: str, catalogue: list, tabulation: bool) -> str:
        """
        Save the parsed data in xlsx format and return its path.

        This function takes the parsed data stored in the processed_catalogue list
        and saves it as an xlsx file with the specified file name and the current run date
        appended to it. The resulting file path is returned.

        Args:
            file_name (str): The desired file name for the saved xlsx file.

        Returns:
            str: The path of the saved xlsx file.
        """
        col = 0
        row = 0
        result_path = (f"{path.join(self.directory, file_name)}_"
                       f"{self.run_date.strftime('%Y-%m-%d')}.xlsx")
        workbook = xlsxwriter.Workbook(result_path)

        for record in catalogue:           
            
            if (tabulation):
                tab_level = col + 3*record['level']
            else:
                tab_level = col

            if (record['level'] == 0):
                if ('worksheet' in locals()):
                    worksheet.autofit()
                worksheet = workbook.add_worksheet(record['name'])
                row = 0

            worksheet.write(row, tab_level, record['level'])
            worksheet.write(row, tab_level + 1, record['id'])
            worksheet.write(row, tab_level + 2, record['name'])
            
            row += 1

        worksheet.autofit()
        workbook.close()
        return result_path
    
    def run_parser(self, tabulation: bool):
        """
        Run the whole script for parsing and data processing.

        This function runs the entire script by prompting the user to choose
        a format for resuting excel file: either use tabulation to make results
        easier to understand or not. Based on the user's choice, it executes 
        the corresponding option in save_to_excel function. Either way it 
        launches processing sequence. It downloads the current catalogue, 
        processes it, extracts the category data, retrieves all filters, extracts 
        the local category data and saves the parsed data to an Excel file. Also it
        create .log file, where information about progress of extraction is stored 
        and updated 

        Returns:
            None
        """
        local_catalogue_path = self.download_current_catalogue()
        print(f"Каталог сохранен: {local_catalogue_path}")
        processed_catalogue = self.process_catalogue(local_catalogue_path)
        self.save_to_excel("Result", processed_catalogue, tabulation)

if __name__ == '__main__':
    app = WildBerriesParser()
    instructons = """Использовать отступы в excel файле? y/n """
    mode = input(instructons)
    while (mode not in ('y', 'Y', 'yes', 'Yes', 'n', 'no', 'N', 'No')):
        instructons = """Пожалуйста, введите: y/n"""
        mode = input(instructons)
    if mode in ('y', 'Y', 'yes', 'Yes'):
        app.run_parser(True)
    else:
        app.run_parser(False)