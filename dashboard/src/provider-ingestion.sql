SELECT success_date,
    {{PROVIDERS}}
FROM (
    SELECT split_part(dag_id, '.', 1) as true_dag_id,
        execution_date::date as success_date,
        SUM(CASE WHEN state IN ('success', 'skipped') THEN 1 ELSE 0 END) as total_success_count,
        SUM(CASE WHEN state NOT IN ('success', 'skipped') THEN 1 ELSE 0 END) as total_nonsuccess_count
    FROM task_instance
    GROUP BY split_part(dag_id, '.', 1),
        execution_date::date
    ORDER BY execution_date::date
        ) flat
GROUP BY success_date
ORDER BY success_date DESC
    ;
