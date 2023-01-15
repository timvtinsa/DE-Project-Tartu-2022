-- Compute the author h-index for each author
SELECT a.id, a.first_name, a.last_name, count(p) AS h_index
FROM authors a
JOIN author_to_paper pa ON a.id = pa.author_id
JOIN paper p ON pa.paper_id = p.id
GROUP BY a.id, a.first_name, a.last_name
ORDER BY h_index DESC
LIMIT 10;