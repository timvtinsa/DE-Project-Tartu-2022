from utils import get_JSON, get_text

CROSSREF_API_BASE_URL = "https://api.crossref.org"

def query_crossref_API_works(doi):
    print("[CROSSREF] Querying paper: " + doi)
    url = CROSSREF_API_BASE_URL + "/works/" + doi
    return get_JSON(url)

def query_crossref_API_journals(issn):
    print("[CROSSREF] Querying journal: " + issn)
    url = CROSSREF_API_BASE_URL + "/journals/" + issn
    return get_JSON(url)

def query_crossref_API_bibtex(doi):
    print("[CROSSREF] Querying bibtex: " + doi)
    url = CROSSREF_API_BASE_URL+ "/works/" + doi + "/transform/application/x-bibtex"
    bibtex = get_text(url)
    for line in bibtex.splitlines():
        if line.startswith("@"):
            return line.split("{")[0].replace("@", "")