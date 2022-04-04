# Urparts Scraping and API challenge

Urparts is a machine manufacturer from which this project scrapes the catalogue, stores it in a postgres sql database and supplies the data in an API.

## Installation 
Since the app is dockerized, to run the project, run the docker-compose command
``` bash 
docker-compose up 
```

## Usage 
* main: 
    * creates tables and functions
    * scrapes catalogue
    * inserts catalogue records into postgres
* API
    * available and docuemnted at: http://127.0.0.1:5000/docs after running
    

