-- Find citations between papers.
MATCH (p1:Paper)-[:CITES]->(p2:Paper) RETURN * LIMIT 10;

-- Find the top 5 most cited papers.
MATCH (p:Paper)-[:CITES]->(p2:Paper) RETURN p2.title, count(p) AS citation_count ORDER BY citation_count DESC LIMIT 5;

-- Longest citation path among papers. 
MATCH (p1:Paper)-[c1:CITES*]->(p2:Paper) RETURN p1.title, p2.title, length(c1) AS path_length ORDER BY path_length DESC LIMIT 1;

-- Find the top 5 authors with the most citations.
MATCH (a:Author)-[:AUTHOR]->(p:Paper)-[:CITES]->(p2:Paper) RETURN a.first_name, a.last_name, count(p2) AS citation_count ORDER BY citation_count DESC LIMIT 5;

-- Find the triangles in the co-authorship graph.
MATCH (a:Author)-[:CO_AUTHOR]->(b:Author)-[:CO_AUTHOR]->(c:Author) WHERE a <> c RETURN a.first_name, a.last_name, b.first_name, b.last_name, c.first_name, c.last_name LIMIT 10;

-- Find all the authors who have co-authored in a specific scientitifc domain.
MATCH (a:Author)-[:CO_AUTHOR]->(b:Author)-[:CO_AUTHOR]->(c:Author) WHERE a <> c AND (a)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) AND (c)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) RETURN a.first_name, a.last_name, b.first_name, b.last_name, c.first_name, c.last_name LIMIT 10;

-- Find the top 5 scientific domains with the most papers.
MATCH (d:ScientificDomain)<-[:BELONGS_TO]-(p:Paper) RETURN d.code, count(p) AS paper_count ORDER BY paper_count DESC LIMIT 5;

-- Find the top 5 scientific domains with the most citations.
MATCH (d:ScientificDomain)<-[:BELONGS_TO]-(p:Paper)-[:CITES]->(p2:Paper) RETURN d.code, count(p2) AS citation_count ORDER BY citation_count DESC LIMIT 5;

-- Find all the authors of a specific scientific domain.
MATCH (a:Author)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) RETURN a.first_name, a.last_name LIMIT 10;

-- Find the co-authors of a specifc scientific domain with the path length of 8
MATCH (a:Author)-[r:CO_AUTHOR*8]->(b:Author) WHERE a <> b AND (a)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) AND (b)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) RETURN DISTINCT a.first_name, a.last_name, b.first_name, b.last_name, length(r) AS path_length LIMIT 10;

-- Find the shortest co-author path between two given authors of the same scientific domain using shortest path algorithm.
MATCH (a:Author {first_name: 'Daniel', last_name: 'Boyanovsky'}), (b:Author {first_name: 'Richard', last_name: 'Holman'}), p = shortestPath((a)-[:CO_AUTHOR*]-(b)) WHERE a <> b AND (a)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) AND (b)-[:AUTHOR]->(:Paper)-[:BELONGS_TO]->(:ScientificDomain {code: 'hep-ph'}) RETURN p;
