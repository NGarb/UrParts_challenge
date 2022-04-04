from db import Retreiver
from fastapi import FastAPI, HTTPException, Query, Depends
from typing import Optional
from fastapi.encoders import jsonable_encoder
import uvicorn
import logging

# TODO read from configs
logging.basicConfig(
    filename='./logs/HISTORYlistener_API.log',
    level=logging.DEBUG,
    format='%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)

# TODO : description markdown not rendering
description = """ UrPartsApp API provides mechanical and part data from UrParts catalogue 
                  It enables retrieval of all data by:
                  
                  * Make
                  * Category
                  * Model
                  * Part
              """

tags_metadata = [
    {
        "name": "get-urparts-data",
        "description": "Retrieve all data from UrParts catalogue based on *Name*. Will return error response if Name does not exist",
    }
]

app = FastAPI(
        title="UrPartsApp",
        description=description,
        version="0.0.1",
        openapi_tags=tags_metadata
)

make_query_descr = "You can retrieve all the data and descriptions linked to a **make** or manufacturer of your choice. Please insert *name* of make in capital lettering."
category_query_descr = "You can retrieve all the data and descriptions linked to a mechanical **category** of your choice. Please insert *name* of make in capital lettering."
model_query_descr = "You can retrieve all the data and descriptions linked to a **model** of your choice. Please insert *name* of make in capital lettering."
part_query_descr = "You can retrieve all the data and descriptions linked to a **part** of your choice. Please insert *name* of make in capital lettering."

retreiver = Retreiver()

@app.get('/get-urparts-data')
def get_urparts_data(make: Optional[str] = Query(None, description=make_query_descr),
                     category: Optional[str] = Query(None, description=category_query_descr),
                     model: Optional[str] = Query(None, description=model_query_descr),
                     part: Optional[str] = Query(None, description=part_query_descr)):
    """
        API get method for the /get-urparts-data endpoint. This retrieves all data from the urparts database unless filtered by one of the query parameters.
        The function reads in any query parameters specified via the vars() function. These are built up into a sql string by first looking up the id of the parameter name and then joining all tables.

        * *param* make: str, the name of the make requested - all data will be filtered for this make. Default: None
        * *param* category: str, the name of the category requested - all data will be filtered for this category. Default: None
        * *param* model: str, the name of the model requested - all data will be filtered for this model. Default: None
        * *param* part: str, the name of the part requested - all data will be filtered for this part. Default: None

        * *return*: on success, return a list of lists where each list is a data row

        * *raises* : HTTPException exception raised if query parameter item can not be found
    """
    ids_to_retrieve = [x for x in vars().items() if x[1]]
    logging.debug(f'requested query parameters: {ids_to_retrieve}')
    sql_param_dct = {}
    for item in ids_to_retrieve:
        param_name = f'{item[0]}id'
        thing_id = retreiver.get_id(item[0], item[1])
        if thing_id is not None:
            sql_param_dct[param_name] = thing_id
        else:
            raise HTTPException(status_code=404, detail="Requested Name not found")
    query_str = f''' 
                select make.name make_name
                       , category.name category_name
                       , model.name model_name
                       , part.name part_name
                       , partnumber
                from connector_table ct 
                left join make on ct.makeid = make.id
                left join category on ct.categoryid = category.id
                left join model on ct.modelid = model.id
                left join part on ct.partid = part.id
                where  
                '''
    for param_name, param_val in sql_param_dct.items():
        query_str += f' ct.{param_name} = \'{param_val}\' and '
    query_str = query_str[:-4]
    try:
        req_data = retreiver.fetch_data_from_db(query_str)
        # TODO can put response data into pandas dataframe and dict format and then pass this as kwargs to UrPartsModel (pydantic model)
        resp = jsonable_encoder(req_data)
        # TODO add pagination
        return resp
    except Exception as ex:
        # TODO - catch specific exception and then general exception after
        message = f'An exception of type {type(ex).__name__} occurred. Arguments:\n{ex.args!r}'
        logging.error(message)
        raise HTTPException(status_code=404, detail=message)


if __name__ == '__main__':

    try:
        uvicorn.run("urparts_api:app", host="127.0.0.1", port=5000, log_level="info")
    except KeyboardInterrupt:
        retreiver.db_instance.close_connection()



