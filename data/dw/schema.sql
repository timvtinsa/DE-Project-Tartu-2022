CREATE TABLE IF NOT EXISTS authors (
id INT PRIMARY KEY,
first_name VARCHAR(255) NOT NULL,
last_name VARCHAR(255) NOT NULL,
gender CHAR(1) NOT NULL,
citations_count INT NULL,
google_scholar_id VARCHAR(255) NULL,
h_index INT NULL,
author_affiliation_id INT NULL,
UNIQUE(first_name, last_name)
);

CREATE TABLE IF NOT EXISTS authors_affiliation (
id INT PRIMARY KEY,
university VARCHAR(255) NOT NULL,
country VARCHAR(255) NOT NULL,
role VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS publication_venue (
id INT PRIMARY KEY,
type VARCHAR(255) NOT NULL,
publisher VARCHAR(255) NOT NULL,
title VARCHAR(255) NOT NULL,
issn VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS scientific_domain (
id INT PRIMARY KEY,
code VARCHAR(255) NOT NULL,
explicit_name VARCHAR(255) NULL
);

CREATE TABLE IF NOT EXISTS year_of_publication (
year INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS papers (
id INT PRIMARY KEY,
publication_venue_id INT NULL,
year_id INT NULL,
title VARCHAR(255) NULL,
doi VARCHAR(255) NULL,
comments TEXT NULL,
report_no VARCHAR(255) NULL,
license VARCHAR(255) NULL,
citedByCount INT NULL
-- CONSTRAINT fk_scientific_domain FOREIGN KEY(scientific_domain_id) REFERENCES scientific_domain(id)
-- We need to replace this foreign by a table with a many-to-many relationship because a paper can have multiple scientific domains
);

CREATE TABLE IF NOT EXISTS author_to_paper (
id SERIAL PRIMARY KEY,
author_id INT NOT NULL,
paper_id INT NOT NULL,
CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(id),
CONSTRAINT fk_paper FOREIGN KEY(paper_id) REFERENCES papers(id)
);

CREATE TABLE IF NOT EXISTS scientific_domain_to_paper (
id SERIAL PRIMARY KEY,
scientific_domain_id INT NOT NULL,
paper_id INT NOT NULL
);