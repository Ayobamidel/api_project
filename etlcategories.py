import requests #interact with APIs
import pandas as pd #data manipulation
from sqlalchemy import create_engine 
import configparser 
import urllib.parse


def etl_process():
    #load my credentials from my config
    config = configparser.ConfigParser(interpolation=None)
    config.read('config.ini')

    # Create postgres engine
    postgres_config = config['postgres']

    # URL encode the password
    encoded_password = urllib.parse.quote(postgres_config['password'], safe='')

    engine = create_engine(
        f"postgresql://{postgres_config['user']}:{encoded_password}@{postgres_config['host']}:{postgres_config['port']}/{postgres_config['database']}"
    )

# Northwind API request for Categories
    url =  'https://demodata.grapecity.com/northwind/api/v1/Categories'
    response = requests.get(url)
    data = response.json()

    # Load data into postgres
    df = pd.json_normalize(data)
    df.to_sql('categories_raw', engine, if_exists = 'replace', index= False)
    engine.dispose()

etl_process()