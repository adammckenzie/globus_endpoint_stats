copy (
    select source_endpoint_display_name as "Source",
        destination_endpoint_display_name as "Destination",
        COUNT(DISTINCT(task_id)) as "Task Count",
        COUNT(DISTINCT(owner_string)) as "Users",
        SUM(bytes_transferred) as "Bytes Transferred",
        SUM(files_transferred) as "Files Transferred",
        avg(effective_bytes_per_second) as "Average Effective Bytes / Second"
    from task
    where owner_id != 'c80843f4-d274-11e5-bd8c-e3eea41d9348' AND
        owner_id != 'fdd49c22-b8a4-48a0-9c59-be0f2b62a3da' AND
        owner_id != 'a53a46d8-d274-11e5-9b69-ef0cc3843c3a'
    GROUP BY source_endpoint_display_name, destination_endpoint_display_name
    ORDER BY source_endpoint_display_name, destination_endpoint_display_name
) to '/tmp/endpoint-usage-without-silo.csv' CSV HEADER DELIMITER ',';

copy (
    select owner_string as "User",
        owner_id as "Id",
        COUNT(DISTINCT(task_id)) as "Task Count",
        SUM(bytes_transferred) as "Bytes Transferred",
        SUM(files_transferred) as "Files Transferred",
        avg(effective_bytes_per_second) as "Average Effective Bytes / Second"
    from task
    where owner_id != 'c80843f4-d274-11e5-bd8c-e3eea41d9348' AND
        owner_id != 'fdd49c22-b8a4-48a0-9c59-be0f2b62a3da' AND
        owner_id != 'a53a46d8-d274-11e5-9b69-ef0cc3843c3a'
    GROUP BY owner_id, owner_string
    ORDER BY owner_id, owner_string
) to '/tmp/user-usage-without-silo.csv' CSV HEADER DELIMITER ',';
