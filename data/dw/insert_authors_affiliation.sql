INSERT INTO authors_affiliation (id, university, country, role)
 VALUES 
(0, 'Unknown', 'Unknown', 'Unknown'),
(1, 'Monash University', 'Australia', 'Unknown'),
(2, 'Southern Methodist University', 'United States', 'Professor of Theoretical Physics'),
(3, 'Michigan State University', 'United States', 'Professor,  Department of Physics and Astronomy')
 ON CONFLICT DO NOTHING;
