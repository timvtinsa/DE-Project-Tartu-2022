import re
import os
import json
import time
import random
import pickle
import airflow
import numpy as np
import pandas as pd
from datetime import datetime, timedelta
import gender_guesser.detector as gender

from semanticscholar import SemanticScholar
from datetime import timedelta
from urllib.request import urlopen

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.operators.bash_operator import BashOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.task_group import TaskGroup



from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from custom_operator.neo4j_extended_operator import Neo4jExtendedOperator

DEFAULT_ARGS = {
    'owner': 'GROUP7',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}
DATA_FOLDER = '/tmp/data'

DUMMY_CONSTANTS = {
    'gender': 'M',
    'citations_count': 1,
    'h_index': 1
}

GENDER_MAPPING = {
    'male': "M",
    'female': "F",
}

MOCK_API = False
MOCK_FOLDER = "API_mock_data"

genderDetector = gender.Detector()

def get_JSON(URL):
    response = urlopen(URL)
    decoded = response.read().decode("utf-8")
    return json.loads(decoded)

def get_text(URL):
    response = urlopen(URL)
    decoded = response.read().decode("utf-8")
    return decoded

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

def clear_title(title):
    if title is not None:
        title = title.replace("\n", "")
        title = title.replace("'", "")
        title = title.replace("\\", "")
        title = title.replace('"', "")
        title = title.replace('  ', " ")
    return title if title else 'NULL'

def parse_first_name(normalized_name):
    return " ".join(normalized_name.split()[:-1])

def parse_last_name(normalized_name):
    return normalized_name.split()[-1]

def is_full_name(normalized_name):
    return (len(normalized_name) > 1) and not re.search(r"\.", normalized_name)

# Semantic Scholar API
sch = SemanticScholar(timeout=20)

def sch_find_papers(paper_title):
    while True:
        try:
            return sch.search_paper(paper_title)
        except:
            print("Request time out, retrying...")
            time.sleep(5)

def sch_find_authors(author_name):
    while True:
        try:
            return sch.search_author(author_name)
        except:
            print("Request time out, retrying...")
            time.sleep(5)
    

def sch_doi(paper_title):
    data = sch_find_papers(paper_title)
    normalized_title = normalize_title(paper_title)
    for paper in data:
        if normalize_title(paper.title) == normalized_title:
            return paper.externalIds.get("DOI", None)
    return None

def sch_full_name(author_name):
    data = sch_find_authors(author_name)
    norm_author_name = normalize_name(author_name)

    for author in data:
        match_found = False
        if author.aliases:
            names = [author.name] + author.aliases
        else:
            names = [author.name]

        full_name = None

        for name in names:
            norm_name = normalize_name(name)
            first_name = parse_first_name(norm_name)
            
            if is_full_name(first_name):
                full_name = name

            if norm_author_name == norm_name:
                match_found = True

        if match_found and full_name:
            return normalize_name(full_name)

# Google Scholar API
serpapi_key = ""

def query_serpapi_title(title):
    url = "https://serpapi.com/search.json?engine=google_scholar&q="
    query_title = "+".join(title.split())
    return get_JSON(url + query_title + "&hl=en&api_key=" + serpapi_key)

def query_serpapi_author(author_id):
    url = "https://serpapi.com/search.json?engine=google_scholar_author&author_id="
    return get_JSON(url + author_id + "&hl=en&api_key=" + serpapi_key)

def full_name(name):
    full_name = None
    first_name = parse_first_name(name)

    if not is_full_name(first_name):
        full_name = sch_full_name(name)
    else:
        full_name = name
    
    return full_name

def get_author_info(google_scholar_id):
    json = query_serpapi_author(google_scholar_id)

    name = normalize_name(json["author"]["name"])
    # name = full_name(name)

    genderResult = genderDetector.get_gender(name[0].split(" ")[0])
    gender = 'X'
    if genderResult == 'female' or genderResult == 'male':
        gender = GENDER_MAPPING[genderResult]
    
    affiliation = json["author"]["affiliations"]
    role = ""
    university = ""
    country = ""

    if "," in affiliation:
        role = affiliation.split(",")[0:-1]
        role = ", ".join(role)
        university = affiliation.split(",")[-1].strip()
    else:
        role = "Unknown"

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

def query_serpapi_cite(paper_id):
    url = "https://serpapi.com/search.json?engine=google_scholar_cite&q="
    return get_JSON(url + paper_id + "&hl=en&api_key=" + serpapi_key)

def get_info_from_serpapi(paper_title):
    while True:
        try:
            json = query_serpapi_title(paper_title)
            if not "organic_results" in json:
                print("No result found, retrying...")
                time.sleep(5)
                continue
            break
        except:
            print("Request error, retrying...")
            time.sleep(5)

    organic_results = json["organic_results"]
    
    title = paper_title
    authors = {}

    if (len(organic_results) > 0):
        first_result = organic_results[0]
        title = first_result["title"]
        
        # If some authors of the paper have a Google Scholar profile
        if "authors" in first_result["publication_info"]:
            authors_json = first_result["publication_info"]["authors"]
            # Some authors of the paper might be missing
            for json in authors_json:
                author_id = json["author_id"]

                name, gender, university, country, role = get_author_info(author_id)
                first_name = name[0]
                last_name = name[1]
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
                    "full_name": first_name + " " + last_name
                }

    return title, authors

# University Domain API
def query_university_domain_API(domain):
    url = "http://universities.hipolabs.com/search?domain="
    return get_JSON(url + domain)

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

# Crossref API
def query_crossref_API_works(doi):
    url = "https://api.crossref.org/works/"
    return get_JSON(url + doi)

def query_crossref_API_journals(issn):
    url = "https://api.crossref.org/journals/"
    return get_JSON(url + issn)

def query_crossref_API_bibtex(doi):
    url = 'http://api.crossref.org/works/' + doi + '/transform/application/x-bibtex'
    bibtex = get_text(url)
    for line in bibtex.splitlines():
        if line.startswith("@"):
            return line.split("{")[0].replace("@", "")

# Pipeline
def prepare_year(data_folder):
    # ----- ENTITY: YEAR -----
    # insert_statement = 'INSERT INTO year_of_publication (year)\n VALUES \n' # Postgres
    insert_statement = 'INSERT IGNORE INTO year_of_publication (year)\n VALUES \n' # MySQL
    insert_graph_statement = ''
    year = 2023
    for i in range(200):
        year = year - 1
        # DATA WAREHOUSE
        if i < 199:
            insert_statement += f'({year}),\n'
        else:
            # insert_statement += f'({year}) \nON CONFLICT DO NOTHING; \n' # Postgres
            insert_statement += f'({year}); \n' # MySQL
        # GRAPH
        insert_graph_statement += f'CREATE (y{year}:Year {{year: {year}}})\n'

    # DATA WAREHOUSE
    with open(f'{data_folder}/dw/insert_year.sql', 'w') as f:
        f.write(insert_statement)
    # GRAPH
    with open(f'{data_folder}/graph/insert_years_graph.sql', 'w') as f:
        f.write(insert_graph_statement)

def prepare_scientific_domains(data_folder):
    # ----- ENTITY: SCIENTIFIC DOMAIN -----
    with open(f'{data_folder}/scientific_domain_lookup.json', 'r') as f:
        scientific_domain = json.load(f)
    # insert_statement = 'INSERT INTO scientific_domain (id, code, explicit_name)\n VALUES \n' # Postgres
    insert_statement = 'INSERT IGNORE INTO scientific_domain (id, code, explicit_name)\n VALUES \n' # MySQL
    insert_graph_statement = ''
    for i, item in enumerate(scientific_domain):
        # DATA WAREHOUSE
        if i < len(scientific_domain) - 1:
            insert_statement += f'({item["id"]}, \'{item["tag"]}\', \'{item["name"]}\'),\n'
        else:
            # insert_statement += f'({item["id"]}, \'{item["tag"]}\', \'{item["name"]}\') \nON CONFLICT DO NOTHING; \n' # Postgres
            insert_statement += f'({item["id"]}, \'{item["tag"]}\', \'{item["name"]}\');\n' # MySQL
        # GRAPH
        insert_graph_statement += f'CREATE (sd{item["id"]}:ScientificDomain {{id: {item["id"]}, code: "{item["tag"]}", explicit_name: "{item["name"]}"}})\n'

    # DATA WAREHOUSE
    with open(f'{data_folder}/dw/insert_scientific_domain.sql', 'w') as f:
        f.write(insert_statement)
    # GRAPH
    with open(f'{data_folder}/graph/insert_scientific_domain_graph.sql', 'w') as f:
        f.write(insert_graph_statement)


def read_data(data_folder):
    with open(f'{data_folder}/dataframe.json', 'r') as f:
        lines = f.readlines()
    N = 100
    papers = pd.DataFrame([json.loads(x) for x in lines])
    papers = papers.head(N)
    return papers

def clean_data(papers):
    # Remove the abstract from the papers
    papers = papers.drop('abstract', axis=1)

    # Remove papers whose title is empty or length is less than one word
    papers = papers[papers['title'].str.split().str.len() > 1]

    # Clean the title
    papers['title'] = papers['title'].apply(clear_title)
    for i, row in papers.iterrows():
        # This statement is useful here if the title contain mathematical latex string
        # Using semantic scholar the mathematical string is resolved in an utf-8 readable string
        if re.search(r"\{.*\}", row.title): 
            papers_resolved = sch_find_papers(row.title)
            for paper in papers_resolved:
                papers.at[i, 'title'] = paper['title']
                break
    
    # Add an empty column for the number of citations
    papers['citedByCount'] = 0

    return papers

def lookup_scientific_domain(data_folder, scientific_domain_code):
    with open(f'{data_folder}/scientific_domain_lookup.json', 'r') as f:
        scientific_domain = json.load(f)
    for item in scientific_domain:
        if item['tag'] == scientific_domain_code:
            return item

def remove_null_strings_from_papers():
    remove_nulls = ''
    remove_nulls += f"UPDATE papers SET doi = NULL where doi = 'NULL';\n"
    remove_nulls += f"UPDATE papers SET comments = NULL where comments = 'NULL';\n"
    remove_nulls += f"UPDATE papers SET report_no = NULL where report_no = 'NULL';\n"
    remove_nulls += f"UPDATE papers SET license = NULL where license = 'NULL';\n"
    remove_nulls += f"UPDATE papers SET publication_venue_id = NULL where publication_venue_id = -1;\n"
    return remove_nulls 

def prepare_data(data_folder):
    papers = read_data(data_folder)
    papers = clean_data(papers)
    authors = {}
    affiliations = {}
    publication_venues = {}
    # OLAP SQL
    # Postgres
    # insert_authors = 'INSERT INTO authors (id, first_name, last_name, gender, author_affiliation_id)\n VALUES \n'
    # insert_papers = 'INSERT INTO papers (id, year_id, title, doi, comments, report_no, license)\n VALUES \n'
    # insert_authors_to_papers = 'INSERT INTO author_to_paper (author_id, paper_id)\n VALUES \n'
    # insert_scientific_domains_to_paper = 'INSERT INTO scientific_domain_to_paper (scientific_domain_id, paper_id)\n VALUES \n'
    # insert_authors_affiliation = 'INSERT INTO authors_affiliation (id, university, country, role)\n VALUES \n(0, \'Unknown\', \'Unknown\', \'Unknown\'),\n'
    # insert_publication_venues = 'INSERT INTO publication_venue (id, type, publisher, title, issn)\n VALUES \n' 

    # MySQL
    insert_authors = 'INSERT IGNORE INTO authors (id, first_name, last_name, gender, author_affiliation_id)\n VALUES \n'
    insert_papers = 'INSERT IGNORE INTO papers (id, publication_venue_id, year_id, title, doi, comments, report_no, license)\n VALUES \n'
    insert_authors_to_papers = 'INSERT IGNORE INTO author_to_paper (author_id, paper_id)\n VALUES \n'
    insert_scientific_domains_to_paper = 'INSERT IGNORE INTO scientific_domain_to_paper (scientific_domain_id, paper_id)\n VALUES \n'
    insert_authors_affiliation = 'INSERT IGNORE INTO authors_affiliation (id, university, country, role)\n VALUES \n(0, \'Unknown\', \'Unknown\', \'Unknown\'),\n'
    insert_publication_venues = 'INSERT IGNORE INTO publication_venue (id, type, publisher, title, issn)\n VALUES \n' 
    insert_cites = ''

    # GRAPH
    insert_authors_graph = ''
    insert_papers_graph = ''
    insert_authors_to_papers_graph = ''
    insert_coauthors_graph = ''
    insert_scientific_domain_to_paper_graph = ''
    insert_authors_affiliation_graph = ''
    insert_authors_to_affiliation_graph = ''
    insert_publication_venues_graph = ''
    insert_publication_venues_to_paper_graph = ''
    insert_paper_to_year_graph = ''
    insert_cites_graph = ''

    # read the json file ids 
    index_paper = 1
    index_author = 1
    index_affiliation = 1
    index_publication_venue = 1


    with open(f'{data_folder}/ids.json', 'r') as f:
        ids = json.load(f)
        index_paper = ids['paper']
        index_author = ids['author']
        index_affiliation = ids['affiliation']
        index_publication_venue = ids['publication_venue']

    for i, row in papers.iterrows():
        paper_authors = []

        # For authors who have a profile in Google Scholar
        title_serp, authors_serp = get_info_from_serpapi(row.title)
        for author in authors_serp.values():
            name = author['full_name']
            paper_authors.append(name)
            # ------- ENTITY: AUTHOR -------
            if not (name in authors):
                author['id'] = index_author
                authors[name] = author
                # DATA WAREHOUSE
                insert_authors += f'({index_author}, \'{author["first_name"]}\', \'{author["last_name"]}\', \'{author["gender"]}\''
                # GRAPH
                insert_authors_graph += f'CREATE (a{index_author}:Author {{id: {index_author}, first_name: "{author["first_name"]}", last_name : "{author["last_name"]}", gender: "{author["gender"]}"}})\n'
                index_author += 1

            # ------- ENTITY: AFFILIATION -------
            if author["affiliation"]['university'] != "" or author["affiliation"]['role'] != "":
                affiliation_key = author["affiliation"]['university'] + author["affiliation"]['role']
                if not (affiliation_key in affiliations):
                    author["affiliation"]["index"] = index_affiliation
                    affiliations[affiliation_key] = author["affiliation"]
                    # DATA WAREHOUSE 
                    insert_authors_affiliation += f'({index_affiliation}, \'{author["affiliation"]["university"]}\', \'{author["affiliation"]["country"]}\', \'{author["affiliation"]["role"]}\'),\n'
                    insert_authors += f', {index_affiliation}),\n'
                    # GRAPH
                    insert_authors_affiliation_graph += f'CREATE (af{index_affiliation}:Affiliation {{id: {index_affiliation}, name: "{author["affiliation"]["university"]}", country: "{author["affiliation"]["country"]}", role: "{author["affiliation"]["role"]}"}})\n'
                    index_affiliation += 1

                # ------- RELATIONSHIP: AUTHOR - AFFILIATION -------
                # GRAPH
                insert_authors_to_affiliation_graph += f'MATCH (a:Person), (af:Affiliation) WHERE a.id = {author["id"]} AND af.id = {index_affiliation} CREATE (a)-[:AFFILIATED_TO]->(af)\n'
            else:
                insert_authors += f', 0),\n' # Assigning unknown affiliation to author

        # Loop over all the parsed authors from dataset
        for author in row["authors_parsed"]:
            first_name = author[1].replace(",", "")
            last_name = author[0].replace(",", "")

            if first_name.strip() != "":
                names = normalize_name(first_name + " " + last_name)
                first_name = names[0]
                last_name = names[1]
                name = names[2]
            
            has_google_scholar_profile = False
            for author in paper_authors:
                if author == name or (author.split(" ")[0] == name.split(" ")[0] and author.split(" ")[-1] == name.split(" ")[-1]):
                    has_google_scholar_profile = True
                    break
            if not has_google_scholar_profile: # Author doesn't have a profile in Google Scholar
                # ------- ENTITY: AUTHOR -------
                genderResult = genderDetector.get_gender(first_name.split(" ")[0])
                gender = 'X'
                if genderResult == 'female' or genderResult == 'male':
                    gender = GENDER_MAPPING[genderResult]
                if not (name in authors):
                    authors[name] = {
                        "google_scholar_id": None,
                        "gender": gender,
                        "affiliation": {
                            "university": None,
                            "role": None,
                            "country": None
                        },
                        "first_name": first_name,
                        "last_name": last_name,
                        "full_name": name,
                        "id": index_author
                    }
                    paper_authors.append(name)
                    # DATA WAREHOUSE
                    insert_authors += f'({index_author}, \'{first_name}\', \'{last_name}\', \'{gender}\', 0),\n'
                    # GRAPH
                    insert_authors_graph += f'CREATE (a{index_author}:Author {{id: {index_author}, first_name: "{first_name}", last_name : "{last_name}", gender: "{gender}"}})\n'
                    index_author += 1

            # ------- RELATIONSHIP: AUTHOR - PAPER -------
            key = ""
            for author in authors.keys():
                if author == name:
                    key = author
                    break
                elif author.split(" ")[0] == name.split(" ")[0] and author.split(" ")[-1] == name.split(" ")[-1]:
                    key = author
                    break
            # DATA WAREHOUSE    
            insert_authors_to_papers += f'({authors[key]["id"]}, {i}),\n'
            # GRAPH
            insert_authors_to_papers_graph += f'MATCH (a:Author), (p:Paper) WHERE a.id = {authors[key]["id"]} AND p.id = {i+1} CREATE (a)-[:AUTHOR]->(p)\n'

        # ------- RELATIONSHIP: AUTHOR - CO_AUTHOR -------
        for author in paper_authors:
            for coauthor in paper_authors:
                if author != coauthor:
                    # GRAPH
                    insert_coauthors_graph += f'MATCH (a1:Author), (a2:Author) WHERE a1.id = {authors[author]["id"]} and a2.id = {authors[coauthor]["id"]} CREATE (a1)-[:CO_AUTHOR]->(a2)\n'

        # ------- ENTITY: PUBLICATION VENUE -------
        publication_venue_key = ''
        if row.doi:
            doi = row.doi
            if " " in doi:
                doi = doi.split(" ")[0]
        else:
            doi_lookup = sch_doi(row.title)
            if doi_lookup:
                doi = doi_lookup
            else:
                doi = "NULL"
        if doi != "NULL":
            doi = doi.replace("\\", "")
            papers.at[i, "doi"] = doi
            print(f"DOI: {doi}")
            crossref_paper = query_crossref_API_works(doi)
            type = ""
            source = ""
            publisher = ""
            issn = ""
            print("PAPER: " + row.title)
            if crossref_paper:
                message = crossref_paper["message"]
                publisher = message["publisher"]
                if "ISSN" in message:
                    issn = message["ISSN"][0]
                    crossref_journal = query_crossref_API_journals(issn)
                    if crossref_journal:
                        source = crossref_journal["message"]["title"]
                elif "event" in message:
                    source = message["event"]["name"]
                type = query_crossref_API_bibtex(doi)

                if "reference" in message:
                    references = message["reference"]
                    ref_found = False
                    for reference in references:
                        if "DOI" in reference:
                            reference_doi = reference["DOI"]
                            reference_doi = reference_doi.replace("\\", "")
                            # check if reference_doi is in papers dataframe
                            
                            if reference_doi in papers["doi"].values:
                                # get paper id
                                reference_id = papers[papers["doi"] == reference_doi].index[0] + 1
                                # ------- RELATIONSHIP: PAPER - PAPER -------
                                # GRAPH
                                insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.id = {i+1} and p2.id = {reference_id} CREATE (p1)-[:CITES]->(p2)\n'
                                papers.at[reference_id-1, "citedByCount"] += 1
                                ref_found = True
                        if not ref_found:
                            if "unstructured" in reference:
                                reference_title = reference["unstructured"]
                                for j, paper in papers.iterrows():
                                    if paper["title"] in reference_title:
                                        # Increase citedByCount of paper which title is in reference_title
                                        papers.at[j, "citedByCount"] += 1
                                        # ------- RELATIONSHIP: PAPER - PAPER -------
                                        # GRAPH
                                        insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.id = {i+1} and p2.id = {j+1} CREATE (p1)-[:CITES]->(p2)\n'
                                        break

            # ------- ENTITY: PUBLICATION VENUE -------
            publication_venue_key = type + source + publisher
            if not (publication_venue_key in publication_venues):
                publication_venues[publication_venue_key] = {
                    "id": index_publication_venue,
                    "type": type,
                    "source": source,
                    "publisher": publisher
                }
                index_publication_venue += 1
                # DATA WAREHOUSE
                insert_publication_venues += f'({index_publication_venue}, \'{type}\', \'{source}\', \'{publisher}\', \'{issn}\'),\n'
                # GRAPH
                insert_publication_venues_graph += f'CREATE (pv{index_publication_venue}:PublicationVenue {{id: {index_publication_venue}, type: "{type}", title: "{source}", publisher: "{publisher}"'
                if issn != "":
                    insert_publication_venues_graph += f', issn: "{issn}"}})\n'
                else:
                    insert_publication_venues_graph += "})\n"

            
            # ------- RELATIONSHIP: PAPER - PUBLICATION VENUE -------
            # GRAPH
            insert_publication_venues_to_paper_graph += f'MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.id = {publication_venues[publication_venue_key]["id"]} and p.id = {i+1} CREATE (pv)-[:PUBLISHED_IN]->(p)\n'
            
        # ------- ENTITY: PAPER -------
        comments = clear_title(row.comments)
        report_no = row["report-no"] if row["report-no"] else 'NULL'
        license = row.license if row.license else "NULL"
        creation_year = datetime.strptime(row["versions"][0]['created'], '%a, %d %b %Y %H:%M:%S %Z').year
        publication_venue_id = -1
        if publication_venue_key in publication_venues:
            publication_venue_id = publication_venues[publication_venue_key]["id"]
        paper_id = i + 1 + index_paper
        # DATA WAREHOUSE
        insert_papers += f'({paper_id}, {publication_venue_id}, {creation_year}, \'{row.title}\', \'{doi}\', \'{comments}\', \'{report_no}\', \'{license}\'),\n'
        # GRAPH
        insert_papers_graph += f'CREATE (p{paper_id}:Paper {{id: {paper_id}, year_id: "{creation_year}", title: "{row.title}", doi: "{doi}", comments: "{comments}", report_no: "{report_no}", license: "{license}"}})\n'

        # ------- RELATIONSHIP: PAPER - YEAR -------
        # GRAPH
        insert_paper_to_year_graph += f'MATCH (p:Paper), (y:Year) WHERE p.id = {paper_id} and y.year = {creation_year} CREATE (p)-[:PUBLISHED_IN]->(y)\n'

        # ------- RELATIONSHIP: PAPER - SCIENTIFIC DOMAIN -------
        scientific_domain_codes = row.categories.split(" ")
        for scientific_domain_code in scientific_domain_codes:
            scientific_domain = lookup_scientific_domain(data_folder, scientific_domain_code)
            if scientific_domain:
                # DATA WAREHOUSE
                insert_scientific_domains_to_paper += f'{scientific_domain["id"], i},\n'
                # GRAPH
                insert_scientific_domain_to_paper_graph += f'MATCH (p:Paper), (s:ScientificDomain) WHERE p.id = {paper_id} and s.id = {scientific_domain["id"]} CREATE (p)-[:BELONGS_TO]->(s)\n'

    for i, row in papers.iterrows():
        insert_cites += f"UPDATE papers SET citedByCount = {papers.at[i, 'citedByCount']} WHERE id = {paper_id};\n" 

    index_paper = index_paper + papers.shape[0]

    # DATA WAREHOUSE
    insert_authors = insert_authors[:-2] # Postgres
    # insert_authors += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    insert_authors += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_authors.sql', 'w') as f:
        f.write(insert_authors)

    insert_papers = insert_papers[:-2] # Postgres
    # insert_papers += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    insert_papers += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_papers.sql', 'w') as f:
        f.write(insert_papers)
        f.write(insert_cites)
        f.write(remove_null_strings_from_papers())
    
    insert_authors_to_papers = insert_authors_to_papers[:-2] # Postgres
    # insert_authors_to_papers += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    insert_authors_to_papers += ';\n' # MySQL 
    with open(f'{data_folder}/dw/insert_authors_to_papers.sql', 'w') as f:
        f.write(insert_authors_to_papers)

    insert_scientific_domains_to_paper = insert_scientific_domains_to_paper[:-2] # Postgres
    # insert_scientific_domains_to_paper += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    insert_scientific_domains_to_paper += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_scientific_domains_to_paper.sql', 'w') as f:
        f.write(insert_scientific_domains_to_paper)

    insert_publication_venues = insert_publication_venues[:-2] # Postgres
    # insert_publication_venues += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    insert_publication_venues += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_publication_venues.sql', 'w') as f:
        f.write(insert_publication_venues)
        
    insert_authors_affiliation = insert_authors_affiliation[:-2] # Postgres
    # insert_authors_affiliation += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    insert_authors_affiliation += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_authors_affiliation.sql', 'w') as f:
        f.write(insert_authors_affiliation)

    # GRAPH
    with open(f'{data_folder}/graph/insert_authors_graph.sql', 'w') as f:
        f.write(insert_authors_graph)

    with open(f'{data_folder}/graph/insert_coauthors_graph.sql', 'w') as f:
        f.write(insert_coauthors_graph)

    with open(f'{data_folder}/graph/insert_papers_graph.sql', 'w') as f:
        f.write(insert_papers_graph)

    with open(f'{data_folder}/graph/insert_authors_to_papers_graph.sql', 'w') as f:
        f.write(insert_authors_to_papers_graph)
    
    with open(f'{data_folder}/graph/insert_scientific_domain_to_paper_graph.sql', 'w') as f:
        f.write(insert_scientific_domain_to_paper_graph)

    with open(f'{data_folder}/graph/insert_publication_venues_graph.sql', 'w') as f:
        f.write(insert_publication_venues_graph)

    with open(f'{data_folder}/graph/insert_publication_venues_to_paper_graph.sql', 'w') as f:
        f.write(insert_publication_venues_to_paper_graph)

    with open(f'{data_folder}/graph/insert_authors_affiliation_graph.sql', 'w') as f:
        f.write(insert_authors_affiliation_graph)

    with open(f'{data_folder}/graph/insert_authors_to_affiliation_graph.sql', 'w') as f:
        f.write(insert_authors_to_affiliation_graph)

    with open(f'{data_folder}/graph/insert_paper_to_year_graph.sql', 'w') as f:
        f.write(insert_paper_to_year_graph)

    with open(f'{data_folder}/graph/insert_cites_graph.sql', 'w') as f:
        f.write(insert_cites_graph)

    # BOTH
    prepare_scientific_domains(data_folder)
    prepare_year(data_folder)

    # write ids in the file
    with open(f'{data_folder}/ids.json', 'w') as f:
        ids = {
            "author": index_author,
            "paper": index_paper,
            "affiliation": index_affiliation,
            "publication_venue": index_publication_venue,
        }
        json.dump(ids, f)


with DAG(
    dag_id='project',  # name of dag
    schedule_interval='*/10 * * * *',  
    start_date=airflow.utils.dates.days_ago(1),
    catchup=False,  # in case execution has been paused, should it execute everything in between
    # the PostgresOperator will look for files in this folder
    template_searchpath=DATA_FOLDER,
    default_args=DEFAULT_ARGS,  # args assigned to all operators
) as project:

    prepare_data_for_insert = PythonOperator(
        task_id='prepare_data_for_insert',
        dag=project,
        trigger_rule='none_failed',
        python_callable=prepare_data,
        op_kwargs={
            'data_folder': DATA_FOLDER,
        },
    )

    # DATA WAREHOUSE
    with TaskGroup("dw_insert_data", tooltip="insert data in the data warehouse") as dw_insert_data:
        # dw_create_tables = PostgresOperator(
        #     task_id='dw_create_tables',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/schema.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_authors = PostgresOperator(
        #     task_id='dw_insert_authors',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_authors.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_scientific_domain = PostgresOperator(
        #     task_id='dw_insert_scientific_domain',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_scientific_domain.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_papers = PostgresOperator(
        #     task_id='dw_insert_papers',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_papers.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_authors_to_papers = PostgresOperator(
        #     task_id='dw_insert_authors_to_papers',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_authors_to_papers.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_publication_venues = PostgresOperator(
        #     task_id='dw_insert_publication_venues',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_publication_venues.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_year = PostgresOperator(
        #     task_id='dw_insert_year',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_year.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_authors_affiliation = PostgresOperator(
        #     task_id='dw_insert_authors_affiliation',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_authors_affiliation.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_scientific_domain_to_paper = PostgresOperator(
        #     task_id='dw_insert_scientific_domain_to_paper',
        #     dag=project,
        #     postgres_conn_id='airflow_pg',
        #     sql='./dw/insert_scientific_domain_to_paper.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        dw_create_tables = MySqlOperator(
            task_id='dw_create_tables',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/schema.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors = MySqlOperator(
            task_id='dw_insert_authors',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_authors.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_scientific_domain = MySqlOperator(
            task_id='dw_insert_scientific_domain',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_scientific_domain.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_papers = MySqlOperator(
            task_id='dw_insert_papers',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_papers.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors_to_papers = MySqlOperator(
            task_id='dw_insert_authors_to_papers',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_authors_to_papers.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_publication_venues = MySqlOperator(
            task_id='dw_insert_publication_venues',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_publication_venues.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_year = MySqlOperator(
            task_id='dw_insert_year',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_year.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors_affiliation = MySqlOperator(
            task_id='dw_insert_authors_affiliation',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_authors_affiliation.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_scientific_domain_to_paper = MySqlOperator(
            task_id='dw_insert_scientific_domain_to_paper',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql='./dw/insert_scientific_domains_to_paper.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_create_tables >> dw_insert_authors >> dw_insert_scientific_domain >> dw_insert_papers >> dw_insert_authors_to_papers >> dw_insert_publication_venues >> dw_insert_year >> dw_insert_authors_affiliation >> dw_insert_scientific_domain_to_paper


    global cypher_script
    def read_statement(ti, data_folder, file_name):
        with open(f'{data_folder}/graph/{file_name}', 'r') as f:
            cypher_script = f.read()
            print(cypher_script)
            ti.xcom_push(key='cypher_script', value=cypher_script)

    # GRAPH DATABASE CONSTRAINTS
    with TaskGroup("graph_insert_data", tooltip="insert data in the graph database") as graph_insert_data:
        graph_create_constraints = Neo4jExtendedOperator(
            task_id='graph_create_constraints',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/constraints.sql',
            is_sql_file=True,
            dag=project,
        )

        # insert the authors in the graph database
        graph_insert_authors = Neo4jExtendedOperator(
            task_id='graph_insert_authors',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_authors_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert the papers in the graph database
        graph_insert_papers = Neo4jExtendedOperator(
            task_id='graph_insert_papers',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_papers_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert cites in the graph database
        graph_insert_cites = Neo4jExtendedOperator(
            task_id='graph_insert_cites',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_cites_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )
        
        # insert the scientific domains in the graph database
        graph_insert_scientific_domain = Neo4jExtendedOperator(
            task_id='graph_insert_scientific_domain',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_scientific_domain_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert scientific domains to papers in the graph database
        graph_insert_scientific_domain_to_paper = Neo4jExtendedOperator(
            task_id='graph_insert_scientific_domain_to_paper',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_scientific_domain_to_paper_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert years in the graph database
        graph_insert_years = Neo4jExtendedOperator(
            task_id='graph_insert_years',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_years_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert publication venues in the graph database
        graph_insert_publication_venues = Neo4jExtendedOperator(
            task_id='graph_insert_publication_venues',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_publication_venues_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert publication venues to papers in the graph database
        graph_insert_publication_venues_to_paper = Neo4jExtendedOperator(
            task_id='graph_insert_publication_venues_to_paper',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_publication_venues_to_paper_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert author's affiliation in the graph database
        graph_insert_authors_affiliation = Neo4jExtendedOperator(
            task_id='graph_insert_authors_affiliation',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_authors_affiliation_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert authors to papers in the graph database
        graph_insert_authors_to_papers = Neo4jExtendedOperator(
            task_id='graph_insert_authors_to_papers',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_authors_to_papers_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert authors to affiliation in the graph database
        graph_insert_authors_to_affiliation = Neo4jExtendedOperator(
            task_id='graph_insert_authors_to_affiliation',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_authors_to_affiliation_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert coauthors in the graph database
        graph_insert_coauthors = Neo4jExtendedOperator(
            task_id='graph_insert_coauthors',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_coauthors_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        # insert paper to year in the graph database
        graph_insert_paper_to_year = Neo4jExtendedOperator(
            task_id='graph_insert_paper_to_year',
            neo4j_conn_id='neo4j_connection',
            sql=f'{DATA_FOLDER}/graph/insert_paper_to_year_graph.sql',
            is_sql_file=True,
            dag=project,
            retries=1,
            retry_delay=timedelta(seconds=10),
        )

        graph_create_constraints >> graph_insert_authors 
        graph_create_constraints >> graph_insert_papers 
        graph_create_constraints >> graph_insert_scientific_domain
        graph_insert_scientific_domain >> graph_insert_scientific_domain_to_paper
        graph_create_constraints >> graph_insert_years
        graph_create_constraints >> graph_insert_authors_affiliation 
        graph_create_constraints >> graph_insert_publication_venues 
        [graph_insert_papers, graph_insert_years] >> graph_insert_paper_to_year
        [graph_insert_publication_venues, graph_insert_papers] >> graph_insert_publication_venues_to_paper
        [graph_insert_authors, graph_insert_papers] >> graph_insert_authors_to_papers
        [graph_insert_authors, graph_insert_authors_affiliation] >> graph_insert_authors_to_affiliation 
        graph_insert_authors >> graph_insert_coauthors
        graph_insert_papers >> graph_insert_cites


prepare_data_for_insert >> dw_insert_data
prepare_data_for_insert >> graph_insert_data