MATCH (pv:PublicationVenue), (p:Paper) WHERE pv.id = 1 and p.id = 1 CREATE (pv)-[:PUBLISHED_IN]->(p)
