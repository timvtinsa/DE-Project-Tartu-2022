INSERT INTO papers (id, publication_venue_id, year_id, title, doi, comments, report_no, license)
 VALUES 
(2, 1, 2007, 'Calculation of prompt diphoton production cross sections at Tevatron and LHC energies', '10.1103/PhysRevD.76.013009', '37 pages, 15 figures; published version', 'ANL-HEP-PR-07-12', 'NULL')
 ON CONFLICT DO NOTHING;
UPDATE papers SET citedByCount = 0 WHERE id = 1;
UPDATE papers SET doi = NULL where doi = 'NULL';
UPDATE papers SET comments = NULL where comments = 'NULL';
UPDATE papers SET report_no = NULL where report_no = 'NULL';
UPDATE papers SET license = NULL where license = 'NULL';
UPDATE papers SET publication_venue_id = NULL where publication_venue_id = -1;
