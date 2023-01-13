MATCH (a:Author), (p:Paper) WHERE a.id = 1 AND p.id = 1 CREATE (a)-[:AUTHOR]->(p)
MATCH (a:Author), (p:Paper) WHERE a.id = 4 AND p.id = 1 CREATE (a)-[:AUTHOR]->(p)
MATCH (a:Author), (p:Paper) WHERE a.id = 2 AND p.id = 1 CREATE (a)-[:AUTHOR]->(p)
MATCH (a:Author), (p:Paper) WHERE a.id = 3 AND p.id = 1 CREATE (a)-[:AUTHOR]->(p)
