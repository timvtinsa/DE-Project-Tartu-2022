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
# from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.contrib.sensors.file_sensor import FileSensor
from airflow.utils.task_group import TaskGroup

# from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
# from custom_operator.neo4j_extended_operator import Neo4jExtendedOperator

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
sch = SemanticScholar()

def sch_find_papers(paper_title):
    try:
        res = sch.search_paper(paper_title)
        return res
    except Exception as e:
        print(e)
        return []

def sch_find_authors(author_name):
    try:
        res = sch.search_author(author_name)
        return res
    except Exception as e:
        print(e)
        return []

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
serpapi_key = "73b0cf9879f04768e5e4ad20573ccec8d7609f2731008211565711097358085f"

def query_serpapi_title(title):
    try:
        url = "https://serpapi.com/search.json?engine=google_scholar&q="
        query_title = "+".join(title.split())
        data = get_JSON(url + query_title + "&hl=en&api_key=" + serpapi_key)
        return data
    except:
        file_name = os.path.join(MOCK_FOLDER, "serpapi_title.json")
        if not os.path.exists(file_name):
            return []
        with open(file_name) as f:
            data = json.load(f)
        return data

def query_serpapi_author(author_id):
    try:
        url = "https://serpapi.com/search.json?engine=google_scholar_author&author_id="
        data = get_JSON(url + author_id + "&hl=en&api_key=" + serpapi_key)
        return data
    except:
        file_name = os.path.join(MOCK_FOLDER, "serpapi_author_boYjNZQAAAAJ.json")
        if not os.path.exists(file_name):
            return []
        with open(file_name) as f:
            data = json.load(f)
        return data

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
    try:
        name = normalize_name(json["author"]["name"])
    except Exception as e:
        print(e)
        name = ""
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
        
    return name, gender, university, country, role

def query_serpapi_cite(paper_id):
    url = "https://serpapi.com/search.json?engine=google_scholar_cite&q="
    return get_JSON(url + paper_id + "&hl=en&api_key=" + serpapi_key)

def get_info_from_serpapi(paper_title):
    json = query_serpapi_title(paper_title)
    try:
        organic_results = json["organic_results"]
    except Exception as e: 
        print(e)
        organic_results = []
    
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
    insert_statement = 'INSERT INTO year_of_publication (year)\n VALUES \n' # Postgres
    # insert_statement = 'INSERT IGNORE INTO year_of_publication (year)\n VALUES \n' # MySQL
    year = 2023
    for i in range(200):
        year = year - 1
        # DATA WAREHOUSE
        if i < 199:
            insert_statement += f'({year}),\n'
        else:
            insert_statement += f'({year}) \nON CONFLICT DO NOTHING; \n' # Postgres
            # insert_statement += f'({year}); \n' # MySQL

    # DATA WAREHOUSE
    with open(f'{data_folder}/dw/insert_year.sql', 'w') as f:
        f.write(insert_statement)

def prepare_scientific_domains(data_folder):
    # ----- ENTITY: SCIENTIFIC DOMAIN -----
    with open(f'{data_folder}/scientific_domain_lookup.json', 'r') as f:
        scientific_domain = json.load(f)
    insert_statement = 'INSERT INTO scientific_domain (id, code, explicit_name)\n VALUES \n' # Postgres
    # insert_statement = 'INSERT IGNORE INTO scientific_domain (id, code, explicit_name)\n VALUES \n' # MySQL
    for i, item in enumerate(scientific_domain):
        # DATA WAREHOUSE
        if i < len(scientific_domain) - 1:
            insert_statement += f'({item["id"]}, \'{item["tag"]}\', \'{item["name"]}\'),\n'
        else:
            insert_statement += f'({item["id"]}, \'{item["tag"]}\', \'{item["name"]}\') \nON CONFLICT DO NOTHING; \n' # Postgres
            # insert_statement += f'({item["id"]}, \'{item["tag"]}\', \'{item["name"]}\');\n' # MySQL

    # DATA WAREHOUSE
    with open(f'{data_folder}/dw/insert_scientific_domain.sql', 'w') as f:
        f.write(insert_statement)


def read_data(data_folder):
    with open(f'{data_folder}/dataframe.json', 'r') as f:
        lines = f.readlines()
    N = 10
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
    
    # Add an empty column for the number of citations
    papers['citedByCount'] = 0

    return papers

def lookup_scientific_domain(data_folder, scientific_domain_code):
    with open(f'{data_folder}/scientific_domain_lookup.json', 'r') as f:
        scientific_domain = json.load(f)
    for item in scientific_domain:
        if item['tag'] == scientific_domain_code:
            return item

def prepare_data(data_folder):
    papers = read_data(data_folder)
    papers = clean_data(papers)
    authors = {}
    affiliations = {}
    publication_venues = {}
    # OLAP SQL
    # Postgres
    insert_authors = 'INSERT INTO authors (id, first_name, last_name, gender, author_affiliation_id)\n VALUES \n'
    insert_papers = 'INSERT INTO papers (id, year_id, title, doi, comments, report_no, license)\n VALUES \n'
    insert_authors_to_papers = 'INSERT INTO author_to_paper (author_id, paper_id)\n VALUES \n'
    insert_scientific_domain_to_paper = 'INSERT INTO scientific_domain_to_paper (scientific_domain_id, paper_id)\n VALUES \n'
    insert_authors_affiliation = 'INSERT INTO authors_affiliation (id, university, country, role)\n VALUES \n(0, \'Unknown\', \'Unknown\', \'Unknown\'),\n'
    insert_publication_venues = 'INSERT INTO publication_venue (id, type, publisher, title, issn)\n VALUES \n' 
    insert_cites = ''

    # MySQL
    # insert_authors = 'INSERT IGNORE INTO authors (id, first_name, last_name, gender, author_affiliation_id)\n VALUES \n'
    # insert_papers = 'INSERT IGNORE INTO papers (id, year_id, title, doi, comments, report_no, license)\n VALUES \n'
    # insert_authors_to_papers = 'INSERT IGNORE INTO author_to_paper (author_id, paper_id)\n VALUES \n'
    # insert_scientific_domains_to_paper = 'INSERT IGNORE INTO scientific_domain_to_paper (scientific_domain_id, paper_id)\n VALUES \n'
    # insert_authors_affiliation = 'INSERT IGNORE INTO authors_affiliation (id, university, country, role)\n VALUES \n(0, \'Unknown\', \'Unknown\', \'Unknown\'),\n'
    # insert_publication_venues = 'INSERT IGNORE INTO publication_venue (id, type, publisher, title, issn)\n VALUES \n' 
    # insert_cites = ''

    index_author = 1
    index_affiliation = 1
    index_publication_venue = 1
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
                    index_affiliation += 1

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
            insert_authors_to_papers += f'({authors[key]["id"]}, {i+1}),\n'

        # ------- RELATIONSHIP: AUTHOR - CO_AUTHOR -------
        # for author in paper_authors:
        #     for coauthor in paper_authors:
        #         if author != coauthor:

        # ------- ENTITY: PUBLICATION VENUE -------
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
            journal = ""
            publisher = ""
            issn = ""
            print("PAPER: " + row.title)
            if crossref_paper:
                message = crossref_paper["message"]
                publisher = message["publisher"]
                issn = message["ISSN"][0] if message["ISSN"] else "NULL"
                crossref_journal = query_crossref_API_journals(issn)
                if crossref_journal:
                    journal = crossref_journal["message"]["title"]
                type = query_crossref_API_bibtex(doi)
                try:
                    references = message["reference"]
                except KeyError:
                    references = []    
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
                            # insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.id = {i+1} and p2.id = {reference_id} CREATE (p1)-[:CITES]->(p2)\n'
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
                                    # insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.id = {i+1} and p2.id = {j+1} CREATE (p1)-[:CITES]->(p2)\n'
                                    break

            publication_venue_key = type + journal + publisher + issn
            if not (publication_venue_key in publication_venues):
                publication_venues[publication_venue_key] = {
                    "id": index_publication_venue,
                    "type": type,
                    "journal": journal,
                    "publisher": publisher
                }
                index_publication_venue += 1
                # DATA WAREHOUSE
                insert_publication_venues += f'({index_publication_venue}, \'{type}\', \'{journal}\', \'{publisher}\', \'{issn}\'),\n'
                # GRAPH
                # insert_publication_venues_graph += f'CREATE (pv{index_publication_venue}:PublicationVenue {{id: {index_publication_venue}, type: "{type}", journal: "{journal}", publisher: "{publisher}", issn: "{issn}"}})\n'
            
            # ------- RELATIONSHIP: PAPER - PUBLICATION VENUE -------
            # GRAPH
            # insert_publication_venues_to_paper_graph += f'MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.id = {publication_venues[publication_venue_key]["id"]} and p.id = {i+1} CREATE (pv)-[:PUBLISHED_IN]->(p)\n'
            
        # ------- ENTITY: PAPER -------
        comments = clear_title(row.comments)
        report_no = row["report-no"] if row["report-no"] else 'NULL'
        license = row.license if row.license else 'NULL'
        creation_year = datetime.strptime(row["versions"][0]['created'], '%a, %d %b %Y %H:%M:%S %Z').year
        # DATA WAREHOUSE
        insert_papers += f'({i+1}, {creation_year}, \'{row.title}\', \'{doi}\', \'{comments}\', \'{report_no}\', \'{license}\'),\n'
        
        # ------- RELATIONSHIP: PAPER - SCIENTIFIC DOMAIN -------
        scientific_domain_codes = row.categories.split(" ")
        for scientific_domain_code in scientific_domain_codes:
            scientific_domain = lookup_scientific_domain(data_folder, scientific_domain_code)
            if scientific_domain:
                # DATA WAREHOUSE
                insert_scientific_domain_to_paper += f'{scientific_domain["id"], i},\n'

    for i, row in papers.iterrows():
        insert_cites += f"UPDATE papers SET citedByCount = {papers.at[i, 'citedByCount']} WHERE id = {i+1};\n" 

    # DATA WAREHOUSE
    insert_authors = insert_authors[:-2] # Postgres
    insert_authors += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    # insert_authors += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_authors.sql', 'w') as f:
        f.write(insert_authors)

    insert_papers = insert_papers[:-2] # Postgres
    insert_papers += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    # insert_papers += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_papers.sql', 'w') as f:
        f.write(insert_papers)
        f.write(insert_cites)
    
    insert_authors_to_papers = insert_authors_to_papers[:-2] # Postgres
    insert_authors_to_papers += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    # insert_authors_to_papers += ';\n' # MySQL 
    with open(f'{data_folder}/dw/insert_authors_to_papers.sql', 'w') as f:
        f.write(insert_authors_to_papers)

    insert_scientific_domain_to_paper = insert_scientific_domain_to_paper[:-2] # Postgres
    insert_scientific_domain_to_paper += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    # insert_scientific_domains_to_paper += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_scientific_domain_to_paper.sql', 'w') as f:
        f.write(insert_scientific_domain_to_paper)

    insert_publication_venues = insert_publication_venues[:-2] # Postgres
    insert_publication_venues += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    # insert_publication_venues += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_publication_venues.sql', 'w') as f:
        f.write(insert_publication_venues)
        
    insert_authors_affiliation = insert_authors_affiliation[:-2] # Postgres
    insert_authors_affiliation += '\n ON CONFLICT DO NOTHING;\n' # Postgres
    # insert_authors_affiliation += ';\n' # MySQL
    with open(f'{data_folder}/dw/insert_authors_affiliation.sql', 'w') as f:
        f.write(insert_authors_affiliation)

    # BOTH
    prepare_scientific_domains(data_folder)
    prepare_year(data_folder)


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
        dw_create_tables = PostgresOperator(
            task_id='dw_create_tables',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/schema.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors = PostgresOperator(
            task_id='dw_insert_authors',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_authors.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_scientific_domain = PostgresOperator(
            task_id='dw_insert_scientific_domain',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_scientific_domain.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_papers = PostgresOperator(
            task_id='dw_insert_papers',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_papers.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors_to_papers = PostgresOperator(
            task_id='dw_insert_authors_to_papers',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_authors_to_papers.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_publication_venues = PostgresOperator(
            task_id='dw_insert_publication_venues',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_publication_venues.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_year = PostgresOperator(
            task_id='dw_insert_year',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_year.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors_affiliation = PostgresOperator(
            task_id='dw_insert_authors_affiliation',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_authors_affiliation.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_scientific_domain_to_paper = PostgresOperator(
            task_id='dw_insert_scientific_domain_to_paper',
            dag=project,
            postgres_conn_id='airflow_pg',
            sql='./dw/insert_scientific_domain_to_paper.sql',
            trigger_rule='none_failed',
            autocommit=True,
        )

        # dw_create_tables = MySqlOperator(
        #     task_id='dw_create_tables',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/schema.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_authors = MySqlOperator(
        #     task_id='dw_insert_authors',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_authors.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_scientific_domain = MySqlOperator(
        #     task_id='dw_insert_scientific_domain',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_scientific_domain.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_papers = MySqlOperator(
        #     task_id='dw_insert_papers',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_papers.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_authors_to_papers = MySqlOperator(
        #     task_id='dw_insert_authors_to_papers',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_authors_to_papers.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_publication_venues = MySqlOperator(
        #     task_id='dw_insert_publication_venues',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_publication_venues.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_year = MySqlOperator(
        #     task_id='dw_insert_year',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_year.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_authors_affiliation = MySqlOperator(
        #     task_id='dw_insert_authors_affiliation',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_authors_affiliation.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        # dw_insert_scientific_domain_to_paper = MySqlOperator(
        #     task_id='dw_insert_scientific_domain_to_paper',
        #     dag=project,
        #     mysql_conn_id='mysql_connection',
        #     sql='./dw/insert_scientific_domains_to_paper.sql',
        #     trigger_rule='none_failed',
        #     autocommit=True,
        # )

        dw_create_tables >> dw_insert_authors >> dw_insert_scientific_domain >> dw_insert_papers >> dw_insert_authors_to_papers >> dw_insert_publication_venues >> dw_insert_year >> dw_insert_authors_affiliation >> dw_insert_scientific_domain_to_paper

prepare_data_for_insert >> dw_insert_data