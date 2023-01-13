MATCH (p:Paper), (s:ScientificDomain) WHERE p.id = 2 and s.id = 98 CREATE (p)-[:BELONGS_TO]->(s)
