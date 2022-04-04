import psycopg2
from configparser import ConfigParser
import logging


class DataBaseSetUp:
    """
          A class to set up connections and configs for the db.

    ...

    Attributes
    ----------
    config : dict
        dictionary of set up parameters such as database name, server, etc

    Methods
    -------
    config:
        set up of the config ini file into a python dictionary
    open_connection:
        connect to database
    close_connection:
        close connection to database

    """
    def __init__(self):
        self.configs = self.config()
        self.connection = self.open_connection()

    def config(self, filename='./config/db.ini', section='postgresql') -> dict:
        """
            Parses the database connection (Postgres) from a defined config file. This takes a file in ini format and builds it into a dictionary object.

            :param filename: str, the relative path of the config file for this project
            :param section: str, definition of the type of database expected

            :return: dict, the

            :raises: exception raised if postgres definition not found in config file
        """

        parser = ConfigParser()
        parser.read(filename)
        config_params = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                config_params[param[0]] = param[1]
        else:
            raise Exception(f'Section {section} not found in the {filename} file')
            logging.error(f'Section {section} not found in the {filename} file')
        return config_params

    def open_connection(self):
        """
            Simple function setting up and returning a database connection via a param dict.
        """
        try:
            params = self.configs
            connection = psycopg2.connect(**params)
            return connection
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error('open connection psycopg2.DatabaseError: ', error)

    def close_connection(self):
        try:
            self.connection.close()
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error('close connection psycopg2.DatabaseError: ', error)

    def execute_statement(self, sql_statement, to_return=False):
        try:
            cur = self.connection.cursor()
            cur.execute(sql_statement)
            self.connection.commit()
            if to_return:
                return_val = cur.fetchone()[0]
            else:
                cur.close()
        except Exception as ex:
            # TODO - catch specific exception and then general exception after
            message = f'An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args!r}'
            logging.error(message)
            if to_return:
                return None
            else:
                pass
        if to_return:
            return return_val

