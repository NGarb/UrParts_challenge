from utils.scraper_utils import get_soup_links, clean_text
from collections.abc import Iterable
from db import Inserter
import logging

# TODO multithreading
# TODO constants file

class Scraper:
    """
            Scraper class instantiates an inserter object for use inside main scrape_into_db function.
            This function iterates through all of the html pages related to the urparts catalogue and retrieves the relevant tags,
            cleans them and inserts into the postgres db.

            ...

            Attributes
            ----------
            data_inserter : instance of Inserter class
                provides access to all database insert methods

            Methods
            -------
            scrape_into_db:
                iterates over all tags by Beautiful Soup as it scrapes the contents of urparts catalogue and inserts into db

    """
    def __init__(self):
        self.data_inserter = Inserter()

    @property # TODO: look up explanation words
    def scrape_into_db(self):
        """
            Iterates over all tags by Beautiful Soup as it scrapes the contents of urparts catalogue and inserts into db

            :param no params required

            :return: does not return

            :raises: exception raised if model tag format unexpected
        """
        data_inserter = self.data_inserter
        makes_request_index = 'index.cfm/page/catalogue/'
        makes_div_class = 'c_container allmakes'
        makes = get_soup_links(makes_request_index, makes_div_class)
        for make in makes:
            make_str = clean_text(make.text)
            makeid = data_inserter.insert_into_table({'table_name': 'Make', 'insert_values': make_str})
            categories_url_index = make['href']
            categories_div_class = "c_container allmakes allcategories"
            categories = get_soup_links(categories_url_index, categories_div_class)
            for category in categories:
                category_str = clean_text(category.text)
                categoryid = data_inserter.insert_into_table({'table_name': 'Category', 'insert_values': category_str})
                models_url_index = category['href']
                models_div_class = "c_container allmodels"
                models = get_soup_links(models_url_index, models_div_class)
                for model in models:
                    try:
                        parts_url_index = model['href']
                        part_div_class = "c_container allparts"
                        parts = get_soup_links(parts_url_index, part_div_class)
                        if parts is None:
                            intmdt_url_index = model['href']
                            intmdt_div_class = "c_container modelSections"
                            intmdt_soup = get_soup_links(intmdt_url_index, intmdt_div_class)
                            intmdt_url = intmdt_soup[0]['href']
                            model = ' '.join(str(intmdt_soup[0]['href']).split('/')[-2:])  # TODO make sure correct model name
                            parts = get_soup_links(intmdt_url, 'c_container allparts')
                            model_str = clean_text(model)
                            logging.info(f'model: {model} has been processed via additional link')
                    except Exception as e:
                        # TODO: catch specific exception type
                        print(model, e)
                        logging.error(f'scraper model: {model} has error {e}')
                        break
                    if not isinstance(model, str):
                        model_str = clean_text(model.text)
                    modelid = data_inserter.insert_into_table({'table_name': 'Model', 'insert_values': model_str})
                    if not isinstance(parts, Iterable):
                        parts = [parts]
                    for part in parts:
                        if ' -' in part.text:
                            part_details = part.text.split('-', 1)
                            part_number = clean_text(part_details[0])
                            part_name = clean_text(part_details[1])
                        else:
                            part_number = clean_text(part.text)
                            part_name = 'NULL'
                            logging.info(f'part {part} found with only part number and no part name')
                        partid = data_inserter.insert_into_table({'table_name': 'Part', 'insert_values': part_name})
                        if None in [makeid, categoryid, modelid, partid]:
                            data_inserter.insert_into_connector_table_by_name(make_str, category_str, model_str, part_name, part_number)
                        else:
                            data_inserter.insert_into_connector_table_by_id(makeid, categoryid, modelid, partid, part_number)
        self.data_inserter.db_instance.close_connection()
