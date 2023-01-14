import pandas as pd
import json

from utils import get_JSON_with_options
from fake_useragent import UserAgent
import gender_guesser.detector as gender


ASIAN_NAME_GENDER_API_BASE_URL = "https://v2.namsor.com/NamSorAPIv2/api2/json/genderGeo/"
ASIAN_NAME_GENDER_API_KEY = "64ee52a811d03f22171801a8eb49827b"

DATA_FOLDER = '/tmp/data'

GENDER_MAPPING = {
    'male': "M",
    'female': "F",
}

genderDetector = gender.Detector()

def find_gender(first_name, last_name):
    genderResult = genderDetector.get_gender(first_name.split(" ")[0])
    if genderResult == 'female' or genderResult == 'male':
        return GENDER_MAPPING[genderResult]
    else:
        genderResult = genderDetector.get_gender(last_name)
        if genderResult == 'female' or genderResult == 'male':
            return GENDER_MAPPING[genderResult]
        else:
            return gender_internal_lookup(first_name, last_name)
            

def check_asian_name(name):
    url = ASIAN_NAME_GENDER_API_BASE_URL + name[0] + "/" + name[1] + "/CN"
    user_agent = UserAgent().random
    response = get_JSON_with_options(url, headers={"X-API-KEY": ASIAN_NAME_GENDER_API_KEY, 'User-Agent': user_agent})
    if response != None:
        if 'likelyGender' in response:
            return GENDER_MAPPING[response['likelyGender']]
        else:
            return "X"
    else:
        return "X"

def gender_internal_lookup(first_name, last_name):
    if "." in first_name or "." in last_name:
        return "X"
    else:
        gender = gender_db_lookup(first_name)
        if gender is None:
            with open(f'{DATA_FOLDER}/gender_internal_lookup.json', 'r') as f:
                data = json.loads(f.read())
            entry_key = first_name + last_name
            if entry_key in data:
                return data[entry_key]
            else:
                gender = check_asian_name([first_name, last_name])
                save_gender_entry(first_name, last_name, gender)
                return gender
        else:
            return gender

def save_gender_entry(first_name, last_name, gender):
    entry_key = first_name + last_name
    with open(f'{DATA_FOLDER}/gender_internal_lookup.json', 'r+') as f:
        data = json.load(f)
        data = {**data, entry_key: gender}
        f.seek(0)
        json.dump(data, f, indent=4)

def gender_db_lookup(first_name):
    df = pd.read_csv(f'{DATA_FOLDER}/name_gender_dataset.csv')
    df = df[df['name'] == first_name]
    if df.empty:
        return None
    else:
        return df['gender'].values[0]