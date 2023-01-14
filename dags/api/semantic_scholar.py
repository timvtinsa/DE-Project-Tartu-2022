import time

from semanticscholar import SemanticScholar
from utils import *

sch = SemanticScholar()

def sch_find_papers(paper_title):
    while True:
        try:
            print("[SEMANTIC SCHOLAR] Querying paper: " + paper_title)
            return sch.search_paper(paper_title)
        except Exception as e:
            if "HTTPSConnectionPool(host='api.semanticscholar.org', port=443): Read timed out." in str(e):
                return None
            elif str(e) == '\'data\'':
                return None
            print(e)
            print("Request error, retrying...")
            time.sleep(5)

def sch_find_authors(author_name):
    
    while True:
        try:
            print("[SEMANTIC SCHOLAR] Querying author: " + author_name)
            return sch.search_author(author_name)
        except Exception as e:
            if "HTTPSConnectionPool(host='api.semanticscholar.org', port=443): Read timed out." in str(e):
                return None
            elif str(e) == '\'data\'':
                return None
            print(e)
            print("Request error, retrying...")
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

    if data != None and len(data) > 0:
        author = data[0]
        names = [author.name]
        if author.aliases:
            names += author.aliases

        # remove duplicates in names
        names = list(dict.fromkeys(names))
        candidates = []

        for name in names:
            norm_name = normalize_name(name)

            if is_canditate_name(norm_name):
                candidates.append(name)

        if len(candidates) > 0:
            # keep the candidate with the longest name
            final_name = max(candidates, key=len)
            return normalize_name(final_name)
        else:
            return norm_author_name
    else:
        return norm_author_name

