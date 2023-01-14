-- Compute the author h-index for each author
-- for each author

select author_id, count(*) as h_index
from author_to_paper
group by author_id
order by h_index desc

-- for each author, get the number of citations for each paper

select author_id, paper_id, citedByCount
from author_to_paper
join papers on author_to_paper.paper_id = papers.id

-- for each author, get the number of citations for each paper, and the number of papers with at least that many citations

select author_id, paper_id, citedByCount, count(*) as h_index
from author_to_paper
join papers on author_to_paper.paper_id = papers.id
group by author_id, paper_id, citedByCount
order by author_id, citedByCount desc

-- for each author, get the number of citations for each paper, and the number of papers with at least that many citations, and the h-index for that author

select author_id, paper_id, citedByCount, count(*) as h_index
from author_to_paper
join papers on author_to_paper.paper_id = papers.id
group by author_id, paper_id, citedByCount
order by author_id, citedByCount desc

-- for each author, get the number of citations for each paper, and the number of papers with at least that many citations, and the h-index for that author, and the max h-index for that author

select author_id, paper_id, citedByCount, count(*) as h_index, max(count(*)) over (partition by author_id) as max_h_index
from author_to_paper
join papers on author_to_paper.paper_id = papers.id
group by author_id, paper_id, citedByCount
order by author_id, citedByCount desc

