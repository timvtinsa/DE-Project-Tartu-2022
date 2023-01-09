INSERT INTO publication_venue (id, type, publisher, title, issn)
 VALUES 
(2, 'article', 'Physical Review D', 'American Physical Society (APS)', '1550-7998'),
(3, 'article', 'Graphs and Combinatorics', 'Springer Science and Business Media LLC', '0911-0119'),
(4, 'article', 'International Journal of Physics', 'Science and Education Publishing Co., Ltd.', '2333-4568'),
(5, 'article', 'Physical Review A', 'American Physical Society (APS)', '1050-2947'),
(6, 'article', 'Journal of Applied Physics', 'AIP Publishing', '0021-8979'),
(7, 'article', 'The Astrophysical Journal', 'American Astronomical Society', '0004-637X'),
(8, 'article', 'Discrete Mathematics', 'Elsevier BV', '0012-365X')
 ON CONFLICT DO NOTHING;
