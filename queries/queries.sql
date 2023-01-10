select count(*), aa.country from papers p
inner join author_to_paper atp on p.id = atp.paper_id
inner join authors a on atp.author_id = a.id 
inner join authors_affiliation aa on a.author_affiliation_id = aa.id
where aa.country != 'Unknown' and p.year_id = 2007
group by aa.country
order by count desc
limit 5

WITH params (var) as (
   values ('Combinatorics')
)
select a.first_name, a.last_name, a.citations_count, sd.explicit_name from papers p
inner join author_to_paper atp on p.id = atp.paper_id
inner join authors a on atp.author_id = a.id
inner join scientific_domain_to_paper sdp on p.id = sdp.paper_id
inner join scientific_domain sd on sdp.scientific_domain_id = sd.id
join params on true
where sd.explicit_name = var
order by a.citations_count desc
