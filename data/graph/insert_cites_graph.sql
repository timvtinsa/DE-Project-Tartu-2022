MATCH (p1:Paper), (p2:Paper) WHERE p1.id = 3 and p2.id = 4 CREATE (p1)-[:CITES]->(p2)
