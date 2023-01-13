INSERT INTO author_to_paper (author_id, paper_id)
 VALUES 
(1, 1),
(4, 1),
(2, 1),
(3, 1)
 ON CONFLICT DO NOTHING;
