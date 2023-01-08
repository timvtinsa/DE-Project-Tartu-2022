CREATE CONSTRAINT ON (a:Author) ASSERT a.id IS UNIQUE
CREATE CONSTRAINT ON (p:Paper) ASSERT p.id IS UNIQUE
CREATE CONSTRAINT ON (s:ScientificDomain) ASSERT s.id IS UNIQUE
CREATE CONSTRAINT ON (af:Affiliation) ASSERT af.id IS UNIQUE
CREATE CONSTRAINT ON (pv:PublicationVenue) ASSERT pv.id IS UNIQUE
CREATE CONSTRAINT ON (y:Year) ASSERT y.year IS UNIQUE