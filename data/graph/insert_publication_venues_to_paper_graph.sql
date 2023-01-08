MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.id = 1 and p.id = 1 CREATE (pv)-[:PUBLISHED_IN]->(p)
MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.id = 2 and p.id = 2 CREATE (pv)-[:PUBLISHED_IN]->(p)
MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.id = 3 and p.id = 3 CREATE (pv)-[:PUBLISHED_IN]->(p)
