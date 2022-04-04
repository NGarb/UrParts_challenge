import psycopg2
from utils.db_utils import DataBaseSetUp
import logging


class Creator:
    """
        Creator class creates db tables and db functions needed to build up the UrParts postgres database

        ...

        Attributes
        ----------
        db_instance : connection object
            connection to a postgres db instance

        Methods
        -------
        create_tables:
            create tables Make, Category, Model, Part, Connector_table in the public schema of the postgres database
        create_table_insert_function:
            create a generic function which inserts into a table only if the entry does not already exist. Returns ID created.
        create_table_insert_functions:
            uses create_table_insert_function to create each insert function for make, category, model, part respectively.

    """
    def __init__(self):
        self.db_instance = DataBaseSetUp()
        self.connection = self.db_instance.open_connection()

    def create_tables(self):
        """ create tables in the PostgreSQL database"""
        commands = (
            """
            CREATE TABLE IF NOT EXISTS Make (
                ID SERIAL PRIMARY KEY,
                Name VARCHAR(255) NOT NULL
            )
            """,
            """ CREATE TABLE IF NOT EXISTS Category (
                    ID SERIAL PRIMARY KEY,
                    Name VARCHAR(255) NOT NULL
                    )
            """,
            """
            CREATE TABLE IF NOT EXISTS Model (
                    ID SERIAL PRIMARY KEY,
                    Name VARCHAR(255) NOT NULL                
            )
            """,
            """
            CREATE TABLE IF NOT EXISTS Part (
                    ID SERIAL PRIMARY KEY,
                    Name VARCHAR(255) NOT NULL              
            )
            """,
        """create table connector_table (MakeID INT NOT NULL 
                   , CategoryID INT NOT NULL
                   , ModelID INT NOT NULL
                   , PartID INT 
                   , PartNumber VARCHAR(255)
                   , primary key (MakeID, CategoryID, ModelID, PartID, PartNumber)
                   , FOREIGN KEY (MakeID)
                        REFERENCES Make (ID)
                        ON UPDATE CASCADE ON DELETE CASCADE
                   , FOREIGN KEY (CategoryID)
                        REFERENCES Category (ID)
                        ON UPDATE CASCADE ON DELETE CASCADE
                   , FOREIGN KEY (ModelID)
                        REFERENCES Model (ID)
                        ON UPDATE CASCADE ON DELETE CASCADE
                   , FOREIGN KEY (PartID)
                        REFERENCES Part (ID)
                        ON UPDATE CASCADE ON DELETE CASCADE)""")
        try:

            for command in commands:
                self.db_instance.execute_statement(command)
        except (Exception, psycopg2.DatabaseError) as error:
            logging.error('psycopg2.DatabaseError: ', error)

    def create_table_insert_function(self, table_name):
        create_functions_command = f'''CREATE OR REPLACE FUNCTION insert_into_{table_name}(ThingName varchar(255)) RETURNS numeric  AS $$
                                          DECLARE
                                          v_id numeric ;
                                          BEGIN
                                            SELECT id
                                            INTO v_id
                                            FROM {table_name}
                                            WHERE Name = ThingName;
                                    
                                            IF v_id IS NULL THEN
                                              INSERT INTO {table_name}(id, Name) VALUES (DEFAULT, ThingName)
                                              RETURNING id INTO v_id;
                                            END IF;
                                    
                                            RETURN v_id;
                                    
                                          END;
                                        $$ LANGUAGE plpgsql;
                                     '''
        self.db_instance.execute_statement(create_functions_command)

    def create_table_insert_functions(self):
        for table_name in ['Make', 'Category', 'Model', 'Part']:
            self.create_table_insert_function(table_name)

    def drop_tables_and_functions(self):
        drop_schema_command = """
                                DROP SCHEMA public CASCADE;
                                CREATE SCHEMA public;
                                """
        # easiest way to drop functions and tables is to drop and recreate the schema.
        # TODO : with more time, iterate through all tables in pg_tables and drop as well as iterate through all functions and drop
        self.db_instance.execute_statement(drop_schema_command)

    def truncate_tables(self):
        # TODO: can do something fancy like create a psql function which iterates through all tables and truncates.
        truncate_command = """
                            TRUNCATE TABLE MAKE;
                            TRUNCATE TABLE CATEGORY;
                            TRUNCATE TABLE MODEL;
                            TRUNCATE TABLE PART;
                            TRUNCATE TABLE CONNECTOR_TABLE
                            """
        self.db_instance.execute_statement(truncate_command)


class Inserter:
    """
            Inserter class provides methods for inserting into various tables in urparts db

            ...

            Attributes
            ----------
            db_instance : connection object
                Connection to a postgres db instance

            Methods
            -------
            insert_into_table:
                * For straight-forward tables with an auto-incrementing pk ID and a name column, this method insert a name into a table
                * This includes tables: Make, Category, Model, Part
            insert_into_connector_table_by_name:
                * Inserting into the connector table requires providing foreign keys.
                * This method retrieves all IDs by looking up the names should any foreign key not be found.
            insert_into_connector_table_by_id:
                * Inserting into the connector table requires providing foreign keys.
                * This method performs a simple insert into connector-table given all foreign key IDs.

        """
    def __init__(self):
        self.db_instance = DataBaseSetUp()
        self.connection = self.db_instance.open_connection()

    def insert_into_table(self, sql_params_dct):
        table_name = sql_params_dct['table_name']
        insert_values = sql_params_dct['insert_values']
        insert_statement = f'''SELECT * FROM insert_into_{table_name}(\'{insert_values}\') '''
        return_id = self.db_instance.execute_statement(insert_statement, to_return=True)
        return return_id

    def insert_into_connector_table_by_name(self, make_name, category_name, model_name, part_name, part_number):
        print('inserting by name', make_name, category_name, model_name, part_name, part_number)
        insert_statement = f'''INSERT INTO connector_table 
                            select make_tmp.id MakeID
                            , category_tmp.id CategoryID
                            , model_tmp.id ModelID
                            , part_tmp.id PartID
                            , part_num_tmp.PartNumber from 
                            (SELECT id, 'join_col' JoinCol from make where name = '{make_name}') make_tmp
                            left join 
                            (SELECT id, 'join_col' JoinCol from Category where name = '{category_name}') category_tmp 
                            on make_tmp.JoinCol = category_tmp.JoinCol
                            left join 
                            (SELECT id, 'join_col' JoinCol from Model where name = '{model_name}') model_tmp
                            on make_tmp.JoinCol = model_tmp.JoinCol
                            left join
                            (SELECT id, 'join_col' JoinCol from Part where name = '{part_name}') part_tmp
                            on make_tmp.JoinCol = part_tmp.JoinCol
                            left join
                            (select '{part_number}' PartNumber, 'join_col' JoinCol ) part_num_tmp
                            on make_tmp.JoinCol = part_num_tmp.JoinCol'''
        self.db_instance.execute_statement(insert_statement)

    def insert_into_connector_table_by_id(self, make_id, category_id, model_id, part_id, part_number):
        insert_statement = f'''INSERT INTO connector_table 
                            select {make_id},{category_id},{model_id},{part_id},\'{part_number}\'
                            ON CONFLICT DO NOTHING '''
        self.db_instance.execute_statement(insert_statement)


class Retreiver:
    """
                Retreiver class provides methods for data retrieval from the postgres urparts db.

                ...

                Attributes
                ----------
                db_instance : connection object
                    Connection to a postgres db instance

                Methods
                -------
                get_id:
                    * Given a name and a table, this method retrieves the corresponding ID
                fetch_data_from_db:
                    * Given a sql request string, this method fetches all related data

    """
    def __init__(self):
        self.db_instance = DataBaseSetUp()
        self.connection = self.db_instance.open_connection()

    def get_id(self, table_name, value_name):
        get_id_sql = f'''SELECT ID FROM {table_name} WHERE NAME = \'{value_name}\' '''
        return_id = self.db_instance.execute_statement(get_id_sql, to_return=True)
        return return_id

    def fetch_data_from_db(self, sql_request_str):
        try:
            cur = self.connection.cursor()
            cur.execute(sql_request_str)
            urparts_data = cur.fetchall()
            self.connection.commit()
            cur.close()
            return urparts_data
        except Exception as e:
            # TODO - catch specific exception and then general exception after
            message = f'An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args!r}'
            logging.error(message)
            return None
