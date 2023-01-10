INSERT INTO authors (id, first_name, last_name, gender, citations_count, h_index, author_affiliation_id)
 VALUES 
(1, 'Csaba', 'Balazs', 'M', 11866, 55, 1),
(2, 'Pavel', 'Nadolsky', 'M', 42585, 53, 2),
(3, 'C.-P.', 'Yuan', 'X', 37428, 83, 3),
(4, 'C.', 'Balázs', 'X', 0, 0, 0),
(5, 'E. L.', 'Berger', 'X', 0, 0, 0),
(6, 'P. M.', 'Nadolsky', 'X', 0, 0, 0),
(7, 'Louis', 'Theran', 'M', 809, 16, 4),
(8, 'Ileana', 'Streinu', 'F', 0, 0, 0),
(9, 'Hongjun', 'Pan', 'X', 0, 0, 0),
(10, 'David', 'Callan', 'M', 0, 0, 0),
(11, 'Wael', 'Abu-Shammala', 'M', 0, 0, 0),
(12, 'Alberto', 'Torchinsky', 'M', 0, 0, 0),
(13, 'Law', 'Ck', 'X', 752, 16, 5),
(14, 'Y. H.', 'Pong', 'X', 0, 0, 0),
(15, 'C. K.', 'Law', 'X', 0, 0, 0),
(16, 'Alejandro', 'Corichi', 'M', 5247, 35, 6),
(17, 'José A.', 'Zapata', 'M', 817, 14, 7),
(18, 'Tatjana', 'Vukasinac', 'F', 0, 0, 0),
(19, 'Jose A.', 'Zapata', 'M', 0, 0, 0),
(20, 'Damian', 'Swift', 'M', 5757, 38, 8),
(21, 'Bruno', 'Merín', 'M', 10261, 47, 9),
(22, 'Luisa', 'Rebull', 'F', 12675, 58, 10),
(23, 'Paul', 'Harvey', 'M', 0, 0, 0),
(24, 'Bruno', 'Merin', 'M', 0, 0, 0),
(25, 'Tracy L.', 'Huard', 'X', 0, 0, 0),
(26, 'Nicholas', 'Chapman', 'M', 0, 0, 0),
(27, 'Neal J.', 'Evans', 'M', 0, 0, 0),
(28, 'Philip C.', 'Myers', 'M', 0, 0, 0),
(29, 'Sergei', 'Ovchinnikov', 'M', 0, 0, 0)
 ON CONFLICT DO NOTHING;
