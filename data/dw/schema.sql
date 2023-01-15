CREATE TABLE IF NOT EXISTS authors (
id INT AUTO_INCREMENT PRIMARY KEY,
first_name VARCHAR(100) NOT NULL,
last_name VARCHAR(100) NOT NULL,
gender CHAR(1) NOT NULL,
citations_count INT NULL,
h_index INT NULL,
author_affiliation_id INT NULL,
UNIQUE(first_name, last_name)
);

CREATE TABLE IF NOT EXISTS authors_affiliation (
id INT AUTO_INCREMENT PRIMARY KEY,
university VARCHAR(100) NOT NULL,
country VARCHAR(100) NOT NULL,
role VARCHAR(100) NOT NULL,
UNIQUE KEY authors_affiliation_key (university, country, role)
);

CREATE TABLE IF NOT EXISTS publication_venue (
id INT AUTO_INCREMENT PRIMARY KEY,
type VARCHAR(200) NOT NULL,
publisher VARCHAR(200) NOT NULL,
title VARCHAR(200) NOT NULL,
issn VARCHAR(100),
h_index INT NULL,
UNIQUE KEY publication_venue_key (type, publisher, title)
);

CREATE TABLE IF NOT EXISTS scientific_domain (
id INT AUTO_INCREMENT PRIMARY KEY,
code VARCHAR(100) NOT NULL,
explicit_name VARCHAR(255) NULL,
UNIQUE KEY scientific_domain_key (code)
);

CREATE TABLE IF NOT EXISTS year_of_publication (
year INT PRIMARY KEY
);

CREATE TABLE IF NOT EXISTS papers (
id INT AUTO_INCREMENT PRIMARY KEY,
arxiv_id VARCHAR(100) NULL,
publication_venue_id INT NULL,
year_id INT NULL,
title VARCHAR(255) NULL,
doi VARCHAR(255) NULL,
comments TEXT NULL,
report_no VARCHAR(255) NULL,
license VARCHAR(255) NULL,
citedByCount INT NULL,
UNIQUE KEY papers_key (arxiv_id)
);

CREATE TABLE IF NOT EXISTS author_to_paper (
id INT AUTO_INCREMENT PRIMARY KEY,
author_id INT NOT NULL,
paper_id INT NOT NULL,
CONSTRAINT fk_author FOREIGN KEY(author_id) REFERENCES authors(id),
CONSTRAINT fk_paper FOREIGN KEY(paper_id) REFERENCES papers(id)
);

CREATE TABLE IF NOT EXISTS scientific_domain_to_paper (
id INT AUTO_INCREMENT PRIMARY KEY,
scientific_domain_id INT NOT NULL,
paper_id INT NOT NULL
);