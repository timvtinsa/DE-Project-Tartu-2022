MATCH (p:Paper), (y:Year) WHERE p.id = 1 and y.year = 2007 CREATE (p)-[:PUBLISHED_IN]->(y)
MATCH (p:Paper), (y:Year) WHERE p.id = 2 and y.year = 2007 CREATE (p)-[:PUBLISHED_IN]->(y)
MATCH (p:Paper), (y:Year) WHERE p.id = 3 and y.year = 2007 CREATE (p)-[:PUBLISHED_IN]->(y)
MATCH (p:Paper), (y:Year) WHERE p.id = 4 and y.year = 2007 CREATE (p)-[:PUBLISHED_IN]->(y)
