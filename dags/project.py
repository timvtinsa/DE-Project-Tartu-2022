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
from scholarly import scholarly, ProxyGenerator
from datetime import timedelta
from urllib.request import urlopen
from unidecode import unidecode

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

from utils import *
from info import *
from api.gender import *

from airflow.providers.neo4j.operators.neo4j import Neo4jOperator
from custom_operator.neo4j_extended_operator import Neo4jExtendedOperator

DEFAULT_ARGS = {
    'owner': 'GROUP7',
    'depends_on_past': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5)
}

DATA_FOLDER = '/tmp/data'

def prepare_year(data_folder):
    # ----- ENTITY: YEAR -----
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
        insert_graph_statement += f'MERGE (y{year}:Year {{year: {year}}})\n'

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
    insert_statement = 'INSERT INTO scientific_domain (code, explicit_name)\n VALUES \n' # MySQL
    insert_graph_statement = ''
    for i, item in enumerate(scientific_domain):
        # DATA WAREHOUSE
        if i < len(scientific_domain) - 1:
            insert_statement += f'(\'{item["tag"]}\', \'{item["name"]}\'),\n'
        else:
            insert_statement += f'(\'{item["tag"]}\', \'{item["name"]}\') ON DUPLICATE KEY UPDATE explicit_name = VALUES(explicit_name);\n' # MySQL
        # GRAPH
        insert_graph_statement += f'MERGE (sd{item["id"]}:ScientificDomain {{code: "{item["tag"]}", explicit_name: "{item["name"]}"}})\n'

    # DATA WAREHOUSE
    with open(f'{data_folder}/dw/insert_scientific_domain.sql', 'w') as f:
        f.write(insert_statement)
    # GRAPH
    with open(f'{data_folder}/graph/insert_scientific_domain_graph.sql', 'w') as f:
        f.write(insert_graph_statement)

def read_data(data_folder):
    with open(f'{data_folder}/dataframe.json', 'r') as f:
        lines = f.readlines()
    N = 1000
    papers = pd.DataFrame([json.loads(x) for x in lines])
    papers = papers.head(N)
    return papers

def clean_data(papers):
    # Remove the abstract from the papers
    papers = papers.drop('abstract', axis=1)

    # Remove papers whose title is empty or length is less than one word
    papers = papers[papers['title'].str.split().str.len() > 1]

    # Clean the title and the doi
    papers['title'] = papers['title'].apply(clear_title)
    for i, row in papers.iterrows():
        # This statement is useful here if the title contain mathematical latex string
        # Using semantic scholar the mathematical string is resolved in an utf-8 readable string
        if re.search(r"\{.*\}", row.title): 
            print("[RESOLVING TITLE] " + row.title)
            papers_resolved = sch_find_papers(row.title)
            for paper in papers_resolved:
                new_title = clear_title(paper['title'])
                print("[RESOLVED TITLE] " + new_title)
                papers.at[i, 'title'] = new_title
                break

        # This statement is useful here if the doi is null in the dataset
        doi = ""
        if row.doi:
            doi = row.doi
            if " " in doi:
                doi = doi.split(" ")[0]
        else:
            print("[RESOLVING DOI]")
            doi_lookup = sch_doi(row.title)
            if doi_lookup:
                print("[RESOLVED DOI] " + doi_lookup)
                doi = doi_lookup
            else:
                print("[RESOLVED DOI] NOT FOUND")
                doi = "NULL"
            
        if doi != "NULL":
            doi = doi.replace("\\", "")
        papers.at[i, "doi"] = doi

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
    return remove_nulls 


def prepare_author_citations_count(authors, papers):
    insert_authors_citations_count = ""
    insert_authors_citations_count_graph = ""
    for author_key in authors:
        papers_published = authors[author_key]["papers"]
        total_citations = 0
        for paper in papers_published:
            nb_citations = papers.at[paper-1, "citedByCount"]
            total_citations += nb_citations
        # DATA WAREHOUSE
        insert_authors_citations_count += f"UPDATE authors SET citations_count = citations_count + {total_citations} WHERE first_name = '{mysql_escape_string(authors[author_key]['first_name'])}' AND last_name = '{mysql_escape_string(authors[author_key]['last_name'])}';\n"
        # GRAPH
        insert_authors_citations_count_graph += f'MATCH (a:Author) WHERE a.first_name = "{authors[author_key]["first_name"]}" AND a.last_name = "{authors[author_key]["last_name"]}" SET a.citations_count = a.citations_count + {total_citations}\n'
    return insert_authors_citations_count, insert_authors_citations_count_graph

def prepare_data(data_folder):
    papers = read_data(data_folder)
    papers = clean_data(papers)
    authors = {}
    affiliations = {}
    publication_venues = {}

    # DATA WAREHOUSE
    insert_authors = 'INSERT INTO authors (first_name, last_name, gender, citations_count, author_affiliation_id)\n VALUES \n'
    insert_papers = 'INSERT INTO papers (arxiv_id, publication_venue_id, year_id, title, doi, comments, report_no, license)\n VALUES \n'
    insert_authors_to_papers = 'INSERT INTO author_to_paper (author_id, paper_id)\n VALUES \n'
    insert_scientific_domains_to_paper = 'INSERT IGNORE INTO scientific_domain_to_paper (scientific_domain_id, paper_id)\n VALUES \n'
    insert_authors_affiliation = 'INSERT IGNORE INTO authors_affiliation (university, country, role)\n VALUES \n(\'Unknown\', \'Unknown\', \'Unknown\'),\n'
    insert_publication_venues = 'INSERT INTO publication_venue (type, publisher, title, issn)\n VALUES \n(\'Unknown\', \'Unknown\', \'Unknown\', \'Unknown\'),\n' 
    insert_cites = ''
    insert_authors_to_affiliation = ''
    insert_papers_to_publication_venue = ''

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

    index_author = 1
    index_affiliation = 1
    index_publication_venue = 1

    for i, row in papers.iterrows():
        print("[INGESTING PAPER] " + str(i) + " - " + row.title)
        paper_authors = []
        paper_id = i + 1

        # For authors who have a profile in Google Scholar
        authors_serp = {}
        # title_serp, authors_serp = get_paper_info(row.title)
        for author in authors_serp.values():
            name = author['full_name']
            paper_authors.append(name)
            # ------- ENTITY: AUTHOR -------
            if not (name in authors):
                author['id'] = index_author
                authors[name] = author
                # DATA WAREHOUSE
                insert_authors += f'(\'{mysql_escape_string(author["first_name"])}\', \'{mysql_escape_string(author["last_name"])}\', \'{author["gender"]}\', 0, 1),\n'
                # GRAPH
                insert_authors_graph += f'MERGE (a{index_author}:Author {{first_name: "{author["first_name"]}", last_name : "{author["last_name"]}", gender: "{author["gender"]}"}}, citations_count: 0)\n'
                index_author += 1

            # This will be useful for author's h-index computation
            authors[name]['papers'].append(i+1)

            # ------- ENTITY: AFFILIATION -------
            if author["affiliation"]['university'] != "" or author["affiliation"]['role'] != "":
                affiliation_key = author["affiliation"]['university'] + author["affiliation"]['role']
                if not (affiliation_key in affiliations):
                    author["affiliation"]["index"] = index_affiliation
                    affiliation = author["affiliation"]
                    affiliations[affiliation_key] = affiliation
                    # DATA WAREHOUSE 
                    insert_authors_affiliation += f'(\'{affiliation["university"]}\', \'{affiliation["country"]}\', \'{affiliation["role"]}\'),\n'
                    insert_authors_to_affiliation += f'UPDATE authors SET affiliation_id = (SELECT id FROM authors_affiliation WHERE university = \'{affiliation["university"]}\' AND country = \'{affiliation["country"]}\' AND role = \'{affiliation["role"]}\') WHERE id = {author["id"]};\n'
                    # GRAPH
                    insert_authors_affiliation_graph += f'MERGE (af{index_affiliation}:Affiliation {{name: "{affiliation["university"]}", country: "{affiliation["country"]}", role: "{affiliation["role"]}"}})\n'
                    index_affiliation += 1

                # ------- RELATIONSHIP: AUTHOR - AFFILIATION -------
                insert_authors_to_affiliation_graph += f'MATCH (a:Author), (af:Affiliation) WHERE a.first_name = "{author["first_name"]}" AND a.last_name = "{author["last_name"]}" '
                insert_authors_to_affiliation_graph += f'AND af.university = "{author["affiliation"]["university"]}" AND af.country = "{affiliation["country"]}" AND af.role = "{affiliation["role"]}" '
                insert_authors_to_affiliation_graph += f'MERGE (a)-[:AFFILIATED_TO]->(af)\n'
            else:
                insert_authors_to_affiliation += f'UPDATE authors SET affiliation_id = 1 WHERE first_name = \'{mysql_escape_string(author["first_name"])}\' AND last_name = \'{mysql_escape_string(author["last_name"])}\';\n'

        # Loop over all the parsed authors from dataset
        for author in row["authors_parsed"]:
            first_name = unidecode(author[1].replace(",", ""))
            last_name = unidecode(author[0].replace(",", ""))

            if first_name.strip() != "":
                names = normalize_name(first_name + " " + last_name)
                name = full_name(names)
                first_name = unidecode(name[0])
                last_name = unidecode(name[1])
                name = name[2]
            
            has_google_scholar_profile = False
            for author in paper_authors:
                # Compare only first letters because authros_parsed has only first letter of the first name.
                if author == name or (author.split(" ")[0][0] == name.split(" ")[0][0] and author.split(" ")[-1] == name.split(" ")[-1]):                    
                    has_google_scholar_profile = True
                    break
            if not has_google_scholar_profile: # Author doesn't have a profile in Google Scholar
                # ------- ENTITY: AUTHOR -------
                gender = find_gender(first_name, last_name)
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
                        "id": index_author,
                        "papers": []
                    }
                    paper_authors.append(name)
                    # DATA WAREHOUSE
                    insert_authors += f'(\'{mysql_escape_string(first_name)}\', \'{mysql_escape_string(last_name)}\', \'{gender}\', 0, 1),\n'
                    # GRAPH
                    insert_authors_graph += f'MERGE (a{index_author}:Author {{first_name: "{first_name}", last_name : "{last_name}", gender: "{gender}"}})\n'
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
                elif author.split(" ")[0][0] == name.split(" ")[0][0] and author.split(" ")[-1] == name.split(" ")[-1]:
                    key = author
                    break

            # DATA WAREHOUSE    
            insert_authors_to_papers += f'({authors[key]["id"]}, (SELECT id FROM papers WHERE arxiv_id = \'{row.id}\')),\n'
            # GRAPH
            insert_authors_to_papers_graph += f'MATCH (a:Author), (p:Paper) WHERE a.first_name = "{authors[key]["first_name"]}" AND a.last_name = "{authors[key]["last_name"]}" '
            insert_authors_to_papers_graph += f'AND p.arxiv_id = "{row.id}" MERGE (a)-[:AUTHOR]->(p)\n'

        # ------- RELATIONSHIP: AUTHOR - CO_AUTHOR -------
        for author in paper_authors:
            for coauthor in paper_authors:
                if author != coauthor:
                    # GRAPH
                    insert_coauthors_graph += f'MATCH (a1:Author), (a2:Author) WHERE a1.first_name = "{authors[author]["first_name"]}" AND a1.last_name = "{authors[author]["last_name"]}" '
                    insert_coauthors_graph += f'AND a2.first_name = "{authors[coauthor]["first_name"]}" AND a2.last_name = "{authors[coauthor]["last_name"]}" '
                    insert_coauthors_graph += f'MERGE (a1)-[:CO_AUTHOR]->(a2)\n'

        # ------- ENTITY: PUBLICATION VENUE -------
        publication_venue_key = ''
        if row.doi and row.doi != "NULL":
            doi = row.doi
            crossref_paper = query_crossref_API_works(doi)
            type = ""
            source = ""
            publisher = ""
            issn = ""
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
                type, source_title = query_crossref_API_bibtex(doi)
                if source_title and source == "":
                    source = source_title

                if "reference" in message:
                    references = message["reference"]
                    ref_found = False
                    for reference in references:
                        if "DOI" in reference:
                            reference_doi = reference["DOI"]
                            reference_doi = reference_doi.replace("\\", "")
                            if reference_doi != row.doi:
                                if reference_doi in papers["doi"].values:
                                    reference_id = papers[papers["doi"] == reference_doi].index[0] + 1
                                    # ------- RELATIONSHIP: PAPER - PAPER -------
                                    # DATA WAREHOUSE
                                    insert_cites += f'UPDATE authors SET citations_count = citations_count + 1 WHERE id IN (SELECT author_id FROM author_to_paper WHERE paper_id = (SELECT id FROM papers WHERE doi = "{reference_doi}"));\n'
                                    # GRAPH
                                    insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.arxiv_id = "{row.id}" AND p2.arxiv_id = "{papers.at[reference_id-1, "id"]}"'
                                    insert_cites_graph += f' MERGE (p1)-[:CITES]->(p2)\n'
                                    papers.at[reference_id-1, "citedByCount"] += 1
                                    ref_found = True
                                else:
                                    # DATA WAREHOUSE
                                    insert_cites += f'UPDATE papers SET citedByCount = citedByCount + 1 WHERE doi = "{reference_doi}";\n'
                                    insert_cites += f'UPDATE authors SET citations_count = citations_count + 1 WHERE id IN (SELECT author_id FROM author_to_paper WHERE paper_id = (SELECT id FROM papers WHERE doi = "{reference_doi}"));\n'

                                    # GRAPH
                                    insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.arxiv_id = "{row.id}" AND p2.doi = "{reference_doi}" '
                                    insert_cites_graph += f'MERGE (p1)-[:CITES]->(p2) ON CREATE SET p2.citedByCount = p2.citedByCount + 1\n'
                        if not ref_found:
                            if "unstructured" in reference:
                                reference_title = reference["unstructured"]
                                if reference_title != row.title:
                                    for j, paper in papers.iterrows():
                                        if paper["title"] in reference_title:
                                            # get paper id
                                            reference_id = papers[papers["title"] == paper["title"]].index[0] + 1
                                            # Increase citedByCount of paper which title is in reference_title
                                            papers.at[j, "citedByCount"] += 1

                                            # ------- RELATIONSHIP: PAPER - PAPER -------
                                            # GRAPH
                                            insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.arxiv_id = "{row.id}" AND p2.arxiv_id = "{papers.at[reference_id-1, "id"]}"'
                                            insert_cites_graph += f' MERGE (p1)-[:CITES]->(p2)\n'
                                            break

                                    # The next lines are for the case that the reference_title is not in the papers table but has already been ingested in the graph database
                                    insert_cites_graph += f'MATCH (p1:Paper), (p2:Paper) WHERE p1.arxiv_id = "{row.id}" AND "{clear_title(reference_title)}" CONTAINS p2.title '
                                    insert_cites_graph += f'MERGE (p1)-[:CITES]->(p2)\n'

            # ------- ENTITY: PUBLICATION VENUE -------
            publication_venue_key = type + source + publisher
            if not (publication_venue_key in publication_venues) and publication_venue_key != "":
                publication_venues[publication_venue_key] = {
                    "id": index_publication_venue,
                    "type": type,
                    "source": source,
                    "publisher": publisher,
                    "papers": []
                }
                index_publication_venue += 1
                # DATA WAREHOUSE
                insert_publication_venues += f'(\'{type}\', \'{mysql_escape_string(publisher)}\', \'{mysql_escape_string(source)}\', '
                if issn != "":
                    insert_publication_venues += f'\'{issn}\'),\n'
                else:
                    insert_publication_venues += f'NULL),\n'
                # GRAPH
                insert_publication_venues_graph += f'MERGE (pv{index_publication_venue}:PublicationVenue {{type: "{type}", title: "{cypher_escape_string(source)}", publisher: "{cypher_escape_string(publisher)}"'
                if issn != "":
                    insert_publication_venues_graph += f', issn: "{issn}"}})\n'
                else:
                    insert_publication_venues_graph += "})\n"

            if publication_venue_key != "":
                # ------- RELATIONSHIP: PAPER - PUBLICATION VENUE -------
                # DATA WAREHOUSE
                insert_papers_to_publication_venue += f'UPDATE papers SET publication_venue_id = (SELECT id FROM publication_venue WHERE type = \'{publication_venues[publication_venue_key]["type"]}\' AND title = \'{mysql_escape_string(publication_venues[publication_venue_key]["source"])}\' AND publisher = \'{mysql_escape_string(publication_venues[publication_venue_key]["publisher"])}\') WHERE arxiv_id = \'{row.id}\';\n'
                # GRAPH
                insert_publication_venues_to_paper_graph += f'MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.type = "{publication_venues[publication_venue_key]["type"]}" AND pv.title = "{cypher_escape_string(publication_venues[publication_venue_key]["source"])}" AND pv.publisher = "{cypher_escape_string(publication_venues[publication_venue_key]["publisher"])}" AND p.arxiv_id = "{row.id}" '
                insert_publication_venues_to_paper_graph += f'MERGE (p)-[:PUBLISHED_IN]->(pv)\n'
                publication_venues[publication_venue_key]["papers"].append(i+1)
        else:
            doi = "NULL"

        # ------- ENTITY: PAPER -------
        comments = clear_title(row.comments)
        report_no = row["report-no"] if row["report-no"] else 'NULL'
        license = row.license if row.license else "NULL"
        creation_year = datetime.strptime(row["versions"][0]['created'], '%a, %d %b %Y %H:%M:%S %Z').year

        # DATA WAREHOUSE
        insert_papers += f'(\'{row.id}\', 1, {creation_year}, \'{row.title}\', \'{doi}\', \'{comments}\', \'{report_no}\', \'{license}\'),\n'
        # GRAPH
        insert_papers_graph += f'MERGE (p{paper_id}:Paper {{arxiv_id: "{row.id}", year_id: "{creation_year}", title: "{row.title}", doi: "{doi}", comments: "{clear_title(comments)}", report_no: "{clear_title(report_no)}", license: "{license}"}})\n'

        # ------- RELATIONSHIP: PAPER - YEAR -------
        # GRAPH
        insert_paper_to_year_graph += f'MATCH (p:Paper), (y:Year) WHERE p.arxiv_id = "{row.id}" AND y.year = {creation_year} MERGE (p)-[:PUBLISHED_IN]->(y)\n'

        # ------- RELATIONSHIP: PAPER - SCIENTIFIC DOMAIN -------
        scientific_domain_codes = row.categories.split(" ")
        for scientific_domain_code in scientific_domain_codes:
            scientific_domain = lookup_scientific_domain(data_folder, scientific_domain_code)
            if scientific_domain:
                # DATA WAREHOUSE
                insert_scientific_domains_to_paper += f'(\'{scientific_domain["id"]}\', (SELECT id FROM papers WHERE arxiv_id = \'{row.id}\')),\n'
                # GRAPH
                insert_scientific_domain_to_paper_graph += f'MATCH (p:Paper), (s:ScientificDomain) WHERE p.arxiv_id = "{row.id}" AND s.code = "{scientific_domain_code}" MERGE (p)-[:BELONGS_TO]->(s)\n'

    for i, row in papers.iterrows():
        insert_cites += f"UPDATE papers SET citedByCount = {papers.at[i, 'citedByCount']} WHERE arxiv_id = {row.id};\n"

    insert_author_citations_count, insert_author_citations_count_graph = prepare_author_citations_count(authors, papers)
    # insert_publication_venue_h_index, insert_publication_venue_h_index_graph = prepare_publication_venue_hindex(publication_venues, papers)

    # DATA WAREHOUSE
    insert_authors = insert_authors[:-2]
    insert_authors += ' ON DUPLICATE KEY UPDATE gender = VALUES(gender), citations_count = VALUES(citations_count), h_index = VALUES(h_index), author_affiliation_id = VALUES(author_affiliation_id);\n'
    with open(f'{data_folder}/dw/insert_authors.sql', 'w') as f:
        f.write(insert_authors)
        f.write(insert_author_citations_count)

    insert_papers = insert_papers[:-2]
    insert_papers += ' ON DUPLICATE KEY UPDATE publication_venue_id = VALUES(publication_venue_id), year_id = VALUES(year_id), title = VALUES(title), doi = VALUES(doi), comments = VALUES(comments), report_no = VALUES(report_no), license = VALUES(license);\n'
    with open(f'{data_folder}/dw/insert_papers.sql', 'w') as f:
        f.write(insert_papers)
        f.write(insert_cites)
        f.write(insert_papers_to_publication_venue)
        f.write(remove_null_strings_from_papers())
    
    insert_authors_to_papers = insert_authors_to_papers[:-2]
    insert_authors_to_papers += ';\n'
    with open(f'{data_folder}/dw/insert_authors_to_papers.sql', 'w') as f:
        f.write(insert_authors_to_papers)

    insert_scientific_domains_to_paper = insert_scientific_domains_to_paper[:-2]
    insert_scientific_domains_to_paper += ';\n'
    with open(f'{data_folder}/dw/insert_scientific_domains_to_paper.sql', 'w') as f:
        f.write(insert_scientific_domains_to_paper)

    insert_publication_venues = insert_publication_venues[:-2]
    insert_publication_venues += ' ON DUPLICATE KEY UPDATE issn = VALUES(issn);\n'
    with open(f'{data_folder}/dw/insert_publication_venues.sql', 'w') as f:
        f.write(insert_publication_venues)
        
    insert_authors_affiliation = insert_authors_affiliation[:-2]
    insert_authors_affiliation += ';\n'
    with open(f'{data_folder}/dw/insert_authors_affiliation.sql', 'w') as f:
        f.write(insert_authors_affiliation)
        f.write(insert_authors_to_affiliation)

    # GRAPH
    with open(f'{data_folder}/graph/insert_authors_graph.sql', 'w') as f:
        f.write(insert_authors_graph)
        f.write(insert_author_citations_count_graph)

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

with DAG(
    dag_id='project',  # name of dag
    schedule_interval='0 6 * * *',  # Run this DAG every day at 6:00 AM
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
            sql="./dw/insert_authors.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_scientific_domain = MySqlOperator(
            task_id='dw_insert_scientific_domain',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_scientific_domain.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_papers = MySqlOperator(
            task_id='dw_insert_papers',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_papers.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors_to_papers = MySqlOperator(
            task_id='dw_insert_authors_to_papers',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_authors_to_papers.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_publication_venues = MySqlOperator(
            task_id='dw_insert_publication_venues',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_publication_venues.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_year = MySqlOperator(
            task_id='dw_insert_year',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_year.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_authors_affiliation = MySqlOperator(
            task_id='dw_insert_authors_affiliation',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_authors_affiliation.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_insert_scientific_domain_to_paper = MySqlOperator(
            task_id='dw_insert_scientific_domain_to_paper',
            dag=project,
            mysql_conn_id='mysql_connection',
            sql="./dw/insert_scientific_domains_to_paper.sql",
            trigger_rule='none_failed',
            autocommit=True,
        )

        dw_create_tables >> dw_insert_authors_affiliation >> dw_insert_authors
        dw_create_tables >> dw_insert_scientific_domain
        dw_create_tables >> [dw_insert_publication_venues, dw_insert_year] >> dw_insert_papers 
        [dw_insert_authors, dw_insert_papers] >> dw_insert_authors_to_papers
        [dw_insert_scientific_domain, dw_insert_papers] >> dw_insert_scientific_domain_to_paper

    # GRAPH DATABASE CONSTRAINTS
    with TaskGroup("graph_insert_data", tooltip="insert data in the graph database") as graph_insert_data:
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

        graph_insert_scientific_domain >> graph_insert_scientific_domain_to_paper
        [graph_insert_papers, graph_insert_years] >> graph_insert_paper_to_year
        [graph_insert_publication_venues, graph_insert_papers] >> graph_insert_publication_venues_to_paper
        [graph_insert_authors, graph_insert_papers] >> graph_insert_authors_to_papers
        [graph_insert_authors, graph_insert_authors_affiliation] >> graph_insert_authors_to_affiliation 
        graph_insert_authors >> graph_insert_coauthors
        graph_insert_papers >> graph_insert_cites

prepare_data_for_insert >> dw_insert_data
prepare_data_for_insert >> graph_insert_data