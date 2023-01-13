INSERT INTO authors (id, first_name, last_name, gender, citations_count, h_index, author_affiliation_id)
 VALUES 
(1, 'Csaba', 'Balazs', 'M', 11875, 55, 1),
(2, 'Pavel', 'Nadolsky', 'M', 42633, 53, 2),
(3, 'C.-P.', 'Yuan', 'X', 37458, 83, 3),
(4, 'E. L.', 'Berger', 'X', 0, 0, 0)
 ON CONFLICT DO NOTHING;
