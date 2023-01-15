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
    type = ""
    source_title = ""
    for line in bibtex.splitlines():
        if line.startswith("@"):
            type = line.split("{")[0].replace("@", "")
        if type == "article" and "journal" in line:
            source_title = line.split("{")[1].replace("}", "")
        if type != "" and type != "article" and "booktitle" in line:
            source_title = line.split("=")[1].replace("{", "").replace("}", "").replace("\\textendash", "-")
    return type, source_title