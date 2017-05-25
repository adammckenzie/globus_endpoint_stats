copy (
    select source_endpoint_display_name as "Source",
        destination_endpoint_display_name as "Destination",
        COUNT(DISTINCT(task_id)) as "Task Count",
        COUNT(DISTINCT(owner_string)) as "Users",
        SUM(bytes_transferred) as "Bytes Transferred",
        SUM(files_transferred) as "Files Transferred",
        avg(effective_bytes_per_second) as "Average Effective Bytes / Second"
    from task
    GROUP BY source_endpoint_display_name, destination_endpoint_display_name
    ORDER BY source_endpoint_display_name, destination_endpoint_display_name
) to '/tmp/endpoint-usage.csv' CSV HEADER DELIMITER ',';

copy (
    select owner_string as "User",
        owner_id as "Id",
        COUNT(DISTINCT(task_id)) as "Task Count",
        SUM(bytes_transferred) as "Bytes Transferred",
        SUM(files_transferred) as "Files Transferred",
        avg(effective_bytes_per_second) as "Average Effective Bytes / Second"
    from task
    GROUP BY owner_id, owner_string
    ORDER BY owner_id, owner_string
) to '/tmp/user-usage.csv' CSV HEADER DELIMITER ',';
