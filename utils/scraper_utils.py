from bs4 import BeautifulSoup
import requests
import string
from urllib.error import URLError, HTTPError
import logging

base_url = 'http://urparts.com/'

def get_soup_links(url_index_str: str, div_class: str):
    """
        Finds all link tags for a div class from a given url index (scrape a web-page for links at given url)

        Parameters
        ----------
            url_index_str : str
                link which will follow the base website url
            div_class : str
                upper div class containing all the links to be returned as a list
    """
    request_url = f'{base_url}{url_index_str}'
    request_html = requests.get(request_url).text
    soup_object = BeautifulSoup(request_html, 'lxml')
    try:
        soup_object_list = soup_object.find('div', class_=div_class).find_all('a', href=True)
        return soup_object_list
    except (URLError, HTTPError) as soup_error:
        logging.error(f'soup_error: {soup_error}')
        return None


def clean_text(text_str: str) -> str:
    """
        Receives scraped text and formats it to ensure consistency and text cleanness. Capitalizes text and removes punctuation.

        Parameters
        ----------
            text_str : str
                text to be cleaned
    """
    text_str = text_str.strip().upper()
    return text_str.translate(str.maketrans('', '', string.punctuation))