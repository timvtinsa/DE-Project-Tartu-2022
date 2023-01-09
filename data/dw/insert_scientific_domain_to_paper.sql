INSERT INTO scientific_domain_to_paper (scientific_domain_id, paper_id)
 VALUES 
(98, 0),
(53, 1),
(5, 1),
(120, 2),
(53, 3),
(52, 4),
(58, 4),
(87, 5),
(95, 6),
(88, 7),
(53, 9)
 ON CONFLICT DO NOTHING;
