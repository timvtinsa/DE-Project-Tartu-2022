-- Find the papers which are cited by other papers and their scientific domains.
SELECT * 
FROM papers p
JOIN scientific_domain_to_paper sdp ON sdp.paper_id = p.id
JOIN scientific_domain sd ON sdp.scientific_domain_id = sd.id
WHERE p.citedByCount > 0;

-- Rank the authors (top 5) of a given scientific domain based on their citation count.
SELECT a.id, a.first_name, a.last_name, sum(p.citedByCount) AS citation_count
FROM authors a
JOIN author_to_paper pa ON a.id = pa.author_id
JOIN papers p ON pa.paper_id = p.id
JOIN scientific_domain_to_paper sdp ON p.id = sdp.paper_id
JOIN scientific_domain sd ON sdp.scientific_domain_id = sd.id
WHERE sd.code = 'math.GR'
GROUP BY a.id, a.first_name, a.last_name
ORDER BY citation_count DESC
LIMIT 5;

-- Number of publications in a given scientific domain over a given time period.
SELECT sd.code, sd.explicit_name, count(p.id) AS publication_count
FROM scientific_domain sd
JOIN scientific_domain_to_paper sdp ON sdp.scientific_domain_id = sd.id
JOIN papers p ON sdp.paper_id = p.id
JOIN year_of_publication yop ON p.year_id = yop.year
WHERE yop.year BETWEEN 2010 AND 2019
AND sd.code = 'math.GR';

-- Histograms the number of publications in a given scientific domain over a given time period.
SELECT sd.code, sd.explicit_name, yop.year, count(p.id) AS publication_count
FROM scientific_domain sd
JOIN scientific_domain_to_paper sdp ON sdp.scientific_domain_id = sd.id
JOIN papers p ON sdp.paper_id = p.id
JOIN year_of_publication yop ON p.year_id = yop.year
WHERE yop.year BETWEEN 2010 AND 2019
AND sd.code = 'math.GR'
GROUP BY sd.code, sd.explicit_name, yop.year
ORDER BY yop.year;

-- Histograms number of papers written by a specific author by year over a given time period.
SELECT a.id, a.first_name, a.last_name, yop.year, count(p.id) AS publication_count
FROM authors a
JOIN author_to_paper pa ON a.id = pa.author_id
JOIN papers p ON pa.paper_id = p.id
JOIN year_of_publication yop ON p.year_id = yop.year
WHERE yop.year BETWEEN 2010 AND 2019
AND a.id = 1
GROUP BY a.id, a.first_name, a.last_name, yop.year
ORDER BY yop.year;





