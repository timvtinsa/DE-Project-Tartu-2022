INSERT IGNORE INTO author_to_paper (author_id, paper_id)
 VALUES 
(1, 0),
(3, 0),
(2, 0),
(4, 1),
(5, 2),
(6, 3),
(7, 3),
(8, 3);
UPDATE papers SET citedByCount = 0 WHERE id = 1;
UPDATE papers SET citedByCount = 0 WHERE id = 2;
UPDATE papers SET citedByCount = 0 WHERE id = 3;
UPDATE papers SET citedByCount = 1 WHERE id = 4;
