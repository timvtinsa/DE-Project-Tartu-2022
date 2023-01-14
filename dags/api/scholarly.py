import time
from scholarly import scholarly, ProxyGenerator

pg = ProxyGenerator()
pg.FreeProxies()
scholarly.use_proxy(pg)

def query_scholarly_author(author_id):
    print("[SCHOLARLY] Querying author: " + author_id)
    while True:
        try:
            res = scholarly.search_author_id(author_id)
            print(res)
            return res
        except Exception as e:
            print(e)
            print("Proxy error, retrying...")
            time.sleep(5)

def query_scholarly_paper(title):
    print("[SCHOLARLY] Querying paper: " + title)
    while True:
        try:
            res = next(scholarly.search_pubs(title))
            print(res)
            return res
        except Exception as e:
            print(e)
            print("Proxy error, retrying...")
            time.sleep(5)