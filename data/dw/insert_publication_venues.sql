INSERT INTO publication_venue (id, type, publisher, title, issn)
 VALUES 
(2, 'article', 'Physical Review D', 'American Physical Society (APS)', '1550-7998')
 ON CONFLICT DO NOTHING;
