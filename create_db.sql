/* CREATE DATABASE globus_usage; */
/* CREATE USER globus_usage_user PASSWORD 'The password'; */
/* GRANT ALL on DATABASE globus_usage to globus_usage_user; */
/* GRANT ALL on TABLE task to globus_usage_user; */

CREATE TABLE task (
    task_id varchar(255) NOT NULL PRIMARY KEY,
    task_type varchar(255) DEFAULT NULL,
    status varchar(255) DEFAULT NULL,
    owner_id varchar(255) DEFAULT NULL,
    owner_string varchar(255) DEFAULT NULL,
    request_time timestamp DEFAULT NULL,
    completion_time timestamp DEFAULT NULL,
    source_endpoint_id varchar(255) DEFAULT NULL,
    source_endpoint_display_name varchar(255) DEFAULT NULL,
    destination_endpoint_id varchar(255) DEFAULT NULL,
    destination_endpoint_display_name varchar(255) DEFAULT NULL,
    files_transferred bigint DEFAULT NULL,
    bytes_transferred bigint DEFAULT NULL,
    effective_bytes_per_second varchar(255) DEFAULT NULL
);
