INSERT INTO authors_affiliation (id, university, country, role)
 VALUES 
(0, 'Unknown', 'Unknown', 'Unknown'),
(1, 'Monash University', 'Australia', 'Unknown'),
(2, 'Southern Methodist University', 'United States', 'Professor of Theoretical Physics'),
(3, 'Michigan State University', 'United States', 'Professor,  Department of Physics and Astronomy'),
(4, 'Lecturer at University of St Andrews', 'Unknown', 'Unknown'),
(5, 'University of Hong Kong', 'Hong Kong', 'Unknown'),
(6, 'UNAM', 'Mexico', 'Professor of Physics'),
(7, 'UNAM', 'Mexico', 'Centro de Ciencias Matematicas'),
(8, 'Lawrence Livermore National Laboratory', 'Unknown', 'Unknown'),
(9, 'ESA', 'Unknown', 'European Space Astronomy Centre,  ESAC'),
(10, 'California Institute of Technology', 'United States', 'Unknown')
 ON CONFLICT DO NOTHING;
