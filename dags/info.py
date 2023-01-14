import gender_guesser.detector as gender

from unidecode import unidecode
from api.scholarly import *
from api.serp import *
from api.semantic_scholar import *
from api.crossref import *
from api.university import *
from utils import *

GENDER_MAPPING = {
    'male': "M",
    'female': "F",
}

genderDetector = gender.Detector()

def full_name(name):
    full_name = None

    if not is_full_name(name[2]):
        full_name = sch_full_name(name[2])
    else:
        full_name = name
    
    return full_name

def get_author_info(google_scholar_id, scholarly=True):
    json = {}
    name = []
    affiliation = ""
    if scholarly:
        json = query_scholarly_author(google_scholar_id)
        name = normalize_name(json["name"].split(",")[0])
        # name = full_name(name)
        affiliation = json["affiliation"]
    else:
        json = query_serpapi_author(google_scholar_id)
        name = normalize_name(json["author"]["name"])
        name = full_name(name)
        affiliation = json["author"]["affiliations"]

    genderResult = genderDetector.get_gender(name[0].split(" ")[0])
    gender = 'X'
    if genderResult == 'female' or genderResult == 'male':
        gender = GENDER_MAPPING[genderResult]

    role = ""
    university = ""
    university_lookup = ""
    country = ""

    if affiliation == "Unknown affiliation":
        role = "Unknown"
    elif "," in affiliation:
        role = affiliation.split(",")[0:-1]
        role = ", ".join(role)
        university = affiliation.split(",")[-1].strip()
    elif "at" in affiliation:
        parts = affiliation.split("at")
        role = parts[0].strip()
        university = parts[1].strip()
    else:
        role = "Unknown"

    if scholarly:
        email_domain = json["email_domain"].split("@")[-1]
        if email_domain != "":
            university_lookup, country = get_university_name(email_domain)
    else:
        email_info = json["author"]["email"].strip()
        if email_info != "":
            # Get rid of "Verified email at"
            email_domain = email_info.split()[-1]
            university_lookup, country = get_university_name(email_domain)
    
    if university == "":
        if university_lookup != "":
            university = university_lookup
        else:
            university = affiliation

    if country == "":
        country = "Unknown"

    university = clear_title(university)
    return name, gender, university, country, role

def get_paper_info(paper_title, scholarly=True):
    results_found = False
    title = paper_title
    if scholarly:
        json = query_scholarly_paper(paper_title)
        title = json["bib"]["title"]
        if "author_id" in json:
            results_found = True
    else:
        json = query_serpapi_title(paper_title)
        organic_results = json["organic_results"]
        if len(organic_results) > 0:
            results_found = True
            json = organic_results[0]
            title = json["title"]
    
    authors = {}
    if results_found and scholarly:
        for author in json["author_id"]:
            if author != "":
                name, gender, university, country, role = get_author_info(author)
                first_name = unidecode(name[0])
                last_name = unidecode(name[1])
                authors[" ".join(name)] = {
                    "google_scholar_id": author,
                    "gender": gender,
                    "affiliation": {
                        "university": university,
                        "role": role,
                        "country": country
                    },
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": first_name + " " + last_name,
                    "papers": []
                }

    elif results_found and not scholarly:
        # If some authors of the paper have a Google Scholar profile
        if "authors" in json["publication_info"]:
            authors_json = json["publication_info"]["authors"]
            # Some authors of the paper might be missing
            for json in authors_json:
                author_id = json["author_id"]

                name, gender, university, country, role = get_author_info(author_id, scholarly=False)
                first_name = unidecode(name[0])
                last_name = unidecode(name[1])
                authors[" ".join(name)] = {
                    "google_scholar_id": author_id,
                    "gender": gender,
                    "affiliation": {
                        "university": university,
                        "role": role,
                        "country": country
                    },
                    "first_name": first_name,
                    "last_name": last_name,
                    "full_name": first_name + " " + last_name,
                    "papers": []
                }

    return title, authors