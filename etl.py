import requests
import pandas as pd
from sqlalchemy import create_engine


def extract() -> dict:
    """
    This API extracts data from http://universities.hipolabs.com
    """
    API_URL = "http://universities.hipolabs.com/search?country=United+States"
    data = requests.get(API_URL).json()
    return data

def transform(data:dict) -> pd.DataFrame:
    """ Transfroms the dataset into desired structure and filters """
    df = pd.DataFrame(data)
    print(f'Total number of Universities from API is {len(data)}')
    df = df[df['name'].str.contains('California')]
    print(f'Numner of Universities in California {len(df)}')
    df['domains'] = [','.join(map(str, l)) for l in df['domains']]
    df['web_pages'] = [','.join(map(str, l)) for l in df['web_pages']]
    df = df.reset_index(drop=True)
    return df[['domains', 'country', 'web_pages', 'name']]

def load(df:pd.DataFrame) -> None:
    """ Loads into SQLite database """
    disk_engine = create_engine('sqlite:///my_lite_store.db')
    df.to_sql('cal_uni', disk_engine, if_exists='replace')
    
data = extract()
df = transform(data)
load(df)