/*
 * This query will return a row for each date in the airflow database and a column for each
 * provider in the 'PROVIDERS' list. If the status of all of the airflow tasks for a given
 * provider's pipeline were either 'success' or 'skipped' on a given date, that date is given
 * a '1' (success), otherwise it will be given a '0' (failure)
 */
SELECT success_date,
    {{PROVIDERS}}
FROM (
    SELECT split_part(dag_id, '.', 1) as true_dag_id,
        execution_date::date as success_date,
        SUM(CASE WHEN state IN ('success', 'skipped') THEN 1 ELSE 0 END) as total_success_count,
        SUM(CASE WHEN state NOT IN ('success', 'skipped') AND NOT EXISTS (

            -- ignore this failed task instance if the same task
            -- exists in the DB with a 'success' state for this
            -- execution date
            SELECT ti2.task_id
            FROM task_instance ti2
            WHERE ti.task_id = ti2.task_id
                AND ti.dag_id = ti2.dag_id
                AND ti.execution_date::date = ti2.execution_date::date
                AND ti2.state = 'success'

                ) THEN 1 ELSE 0
            END) as total_nonsuccess_count
    FROM task_instance ti
    GROUP BY split_part(dag_id, '.', 1),
        execution_date::date
    ORDER BY execution_date::date
        ) flat
GROUP BY success_date
ORDER BY success_date DESC
    ;
