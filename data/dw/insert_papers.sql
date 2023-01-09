INSERT INTO papers (id, year_id, title, doi, comments, report_no, license)
 VALUES 
(1, 2007, 'Calculation of prompt diphoton production cross sections at Tevatron and LHC energies', '10.1103/PhysRevD.76.013009', '37 pages, 15 figures; published version', 'ANL-HEP-PR-07-12', 'NULL'),
(2, 2007, 'Sparsity-certifying Graph Decompositions', '10.1007/S00373-008-0834-4', 'To appear in Graphs and Combinatorics', 'NULL', 'http://arxiv.org/licenses/nonexclusive-distrib/1.0/'),
(3, 2007, 'The evolution of the Earth-Moon system based on the dark matter field fluid model', '10.12691/IJP-8-1-3', '23 pages, 3 figures', 'NULL', 'NULL'),
(4, 2007, 'A determinant of Stirling cycle numbers counts unlabeled acyclic single-source automata', 'NULL', '11 pages', 'NULL', 'NULL'),
(5, 2007, 'From dyadic $Lambda_{alpha}$ to $Lambda_{alpha}$', 'NULL', 'NULL', 'NULL', 'NULL'),
(6, 2007, 'Bosonic characters of atomic Cooper pairs across resonance', '10.1103/PhysRevA.75.043613', '6 pages, 4 figures, accepted by PRA', 'NULL', 'NULL'),
(7, 2007, 'Polymer Quantum Mechanics and its Continuum Limit', '10.1103/PhysRevD.76.044016', '16 pages, no figures. Typos corrected to match published version', 'IGPG-07/03-2', 'NULL'),
(8, 2007, 'Numerical solution of shock and ramp compression for general material properties', '10.1063/1.2975338', 'Minor corrections', 'LA-UR-07-2051, LLNL-JRNL-410358', 'http://arxiv.org/licenses/nonexclusive-distrib/1.0/'),
(9, 2007, 'The Spitzer c2d Survey of Large, Nearby, Insterstellar Clouds. IX. The Serpens YSO Population As Observed With IRAC and MIPS', '10.1086/518646', 'NULL', 'NULL', 'NULL'),
(10, 2007, 'Partial cubes: structures, characterizations, and constructions', '10.1016/j.disc.2007.10.025', '36 pages, 17 figures', 'NULL', 'NULL')
 ON CONFLICT DO NOTHING;
UPDATE papers SET citedByCount = 0 WHERE id = 1;
UPDATE papers SET citedByCount = 0 WHERE id = 2;
UPDATE papers SET citedByCount = 0 WHERE id = 3;
UPDATE papers SET citedByCount = 0 WHERE id = 4;
UPDATE papers SET citedByCount = 0 WHERE id = 5;
UPDATE papers SET citedByCount = 0 WHERE id = 6;
UPDATE papers SET citedByCount = 0 WHERE id = 7;
UPDATE papers SET citedByCount = 0 WHERE id = 8;
UPDATE papers SET citedByCount = 0 WHERE id = 9;
UPDATE papers SET citedByCount = 0 WHERE id = 10;
