SELECT 
    id,
    name,
    created_at,
    updated_at,
    status
FROM t_project.source_table
WHERE status = 'active'
