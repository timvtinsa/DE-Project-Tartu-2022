CREATE TABLE IF NOT EXISTS authors (
id INT PRIMARY KEY,
first_name VARCHAR(255) NOT NULL,
last_name VARCHAR(255) NOT NULL,
gender CHAR(1) NOT NULL,
citations_count INT NOT NULL,
h_index INT NOT NULL,
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
explicit_name VARCHAR(255) NOT NULL
);

CREATE TABLE IF NOT EXISTS year_of_publication (
year INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS papers (
id INT PRIMARY KEY,
author_id INT NOT NULL,
author_affiliation_id INT NOT NULL,
publication_venue_id INT NOT NULL,
scientific_domain_id INT NOT NULL,
year_id INT NOT NULL,
title VARCHAR(255) NOT NULL,
doi VARCHAR(255) NOT NULL,
comments VARCHAR(255) NOT NULL,
report_no VARCHAR(255) NOT NULL,
licence VARCHAR(255) NULL,
citedByCount INT NOT NULL
);