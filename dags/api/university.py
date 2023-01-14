from utils import get_JSON

UNIVERSITY_API_BASE_URL = "http://universities.hipolabs.com"

def query_university_domain_API(domain):
    return get_JSON(UNIVERSITY_API_BASE_URL + "/search?domain=" + domain)

def get_university_name(email_domain):
    # In case of pa.msu.edu, university domain API returns nothing.
    # But msu.edu gives Michigan State University. So we need to 
    # only use last two parts of domain (msu.edu).
    split_by_dot = email_domain.split(".")
    if len(split_by_dot) > 2:
        email_domain = ".".join(split_by_dot[-2:])
    
    domain_API_json = query_university_domain_API(email_domain)
    if len(domain_API_json) > 0:
        return domain_API_json[0]["name"], domain_API_json[0]["country"]
    return "", ""
