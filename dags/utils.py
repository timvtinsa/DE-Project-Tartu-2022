import re
import json
import requests
from urllib.request import urlopen

def get_JSON(URL):
    try:
        response = urlopen(URL)
        decoded = response.read().decode("utf-8")
        return json.loads(decoded)
    except Exception as e:
        return None

def get_JSON_with_options(url, proxies=None, headers=None):
    response = requests.get(url, proxies=proxies, headers=headers)
    if response.status_code != 200:
        print("ERROR: ", response.status_code)
        return None
    return response.json()
    
def get_text(URL):
    response = urlopen(URL)
    decoded = response.read().decode("utf-8")
    return decoded

def clear_title(title):
    if title is not None:
        title = title.replace("\n", "")
        title = title.replace("'", "")
        title = title.replace("\\", "")
        title = title.replace('"', "")
        title = title.replace('  ', " ")
    return title if title else 'NULL'

def mysql_escape_string(s):
    if s is not None:
        s = s.replace("'", "''") 
    return s if s else 'NULL'

def normalize_title(title):
    return " ".join(title.strip().title().split()).replace(".", "")

def normalize_name(name):
    name = name.replace(".", ". ")
    name = name.replace("-", " -")
    name = name.replace("'", "")
    name_components = re.split(r"\s+", name.strip())

    def mapper(name):
        if re.fullmatch(r"-*\w", name):
            return (name + ".").title()
        return name.title()
        
    name = " ".join(list(map(mapper, name_components))).replace(" -", "-")
    parts = name.split()
    first_name = " ".join(parts[:-1])
    last_name = parts[len(parts)-1]
    return [first_name, last_name, name]

def parse_first_name(normalized_name):
    # return " ".join(normalized_name[:-1])
    return normalized_name.split()[0]

def parse_last_name(normalized_name):
    return normalized_name.split()[-1]

def is_full_name(normalized_name):
    return (len(normalized_name) > 1) and not re.search(r"\.", normalized_name)

def is_canditate_name(normalized_name):
    first_name = normalized_name[0]
    first_name_parts = first_name.split()
    for part in first_name_parts:
        if len(part) >= 1 and not re.search(r"\.", part):
            return True
    return False