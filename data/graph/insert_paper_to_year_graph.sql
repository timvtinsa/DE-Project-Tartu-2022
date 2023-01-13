MATCH (p:Paper), (y:Year) WHERE p.id = 2 and y.year = 2007 CREATE (p)-[:PUBLISHED_IN]->(y)
