MATCH (a:Person), (af:Affiliation) WHERE a.id = 1 AND af.id = 2 CREATE (a)-[:AFFILIATED_TO]->(af)
MATCH (a:Person), (af:Affiliation) WHERE a.id = 2 AND af.id = 3 CREATE (a)-[:AFFILIATED_TO]->(af)
MATCH (a:Person), (af:Affiliation) WHERE a.id = 3 AND af.id = 4 CREATE (a)-[:AFFILIATED_TO]->(af)
