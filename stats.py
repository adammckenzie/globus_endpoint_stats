from datetime import datetime, timedelta
import ConfigParser
import globus_sdk
import psycopg2

# The Globus SDK needs to be installed first: https://globus-sdk-python.readthedocs.io/en/stable/installation/
# short answer: pip install globus-sdk

def get_next_interval(current, end_date, interval):
    if current + timedelta(days=interval) >= end_date:
        return end_date
    else:
        return current + timedelta(days=interval)

def output_tasks_from_endpoints(endpoints, start_date, end_date, interval, tc, connection, cursor):
    too_many_tasks = []
    no_tasks = []
    max_task_count = 0
    for endpoint_name, endpoint_id in endpoints:
        current = start_date
        total_task_count = 0
        while current != end_date:
            interval_task_count = 0
            print('================================================================')
            print('== Task List for ' + endpoint_name + ' during ' + str(current) + ' to ' + str(get_next_interval(current, end_date, interval)))
            print('================================================================')
            completion_time_filter = current.isoformat().replace("T", " ") + "," + get_next_interval(current, end_date, interval).isoformat().replace("T", " ")
            interval_task_count = output_tasks_from_endpoint(endpoint_name, endpoint_id, completion_time_filter, tc, connection, cursor)
            total_task_count += interval_task_count
            if interval_task_count > 999:
                print('================================================================')
                print('== The endpoint ' + endpoint_name + ' had more than 1000 tasks in a single month')
                print('================================================================')
                too_many_tasks.append((endpoint_name, endpoint_id))
            current = get_next_interval(current, end_date, interval)

            if interval_task_count > max_task_count:
                max_task_count = interval_task_count

        if total_task_count == 0:
            print('================================================================')
            print('== The endpoint ' + endpoint_name + ' had no tasks')
            print('================================================================')
            no_tasks.append((endpoint_name, endpoint_id))


    print('================================================================')
    print('== The maximum number of tasks in a single interval was: ' + str(max_task_count))
    print('================================================================')

    print('================================================================')
    print('== The following endpoints had too many tasks for this interval (decrease interval to get all tasks)')
    print('================================================================')
    too_many_tasks_output = ""
    for endpoint_name, endpoint_id in too_many_tasks:
        too_many_tasks_output += endpoint_name + "," + endpoint_id + ";"
    print(too_many_tasks_output)

    print('================================================================')
    print('== The following endpoints had no tasks, does the user not have access to the endpoint?')
    print('================================================================')
    no_tasks_output = ""
    for endpoint_name, endpoint_id in no_tasks:
        no_tasks_output += endpoint_name + "," + endpoint_id + ";"
    print(no_tasks_output)


def output_tasks_from_endpoint(name, endpoint, completion_time_filter, tc, connection, cursor):
    task_count = 0
    for task in tc.endpoint_manager_task_list(num_results=None, filter_endpoint=endpoint, filter_completion_time=completion_time_filter):
        task_count = task_count + 1
        cursor.execute("SELECT task_id FROM task WHERE task_id=%s", (task["task_id"], ))
        connection.commit()
        if cursor.rowcount > 0:
            print("We found the task already in the database '" + task["task_id"] + "'")
        else:
            cursor.execute("INSERT INTO task (task_id, task_type, status, owner_id, owner_string, request_time, completion_time, source_endpoint_id, source_endpoint_display_name, destination_endpoint_id, destination_endpoint_display_name, files_transferred, bytes_transferred, effective_bytes_per_second) SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s", (
            task["task_id"],
            task["type"],
            task["status"],
            task["owner_id"],
            task["owner_string"],
            task["request_time"],
            task["completion_time"],
            task["source_endpoint_id"],
            task["source_endpoint_display_name"],
            task["destination_endpoint_id"],
            task["destination_endpoint_display_name"],
            task["files_transferred"],
            task["bytes_transferred"],
            task["effective_bytes_per_second"]))
            connection.commit()
    return task_count

def get_transfer_client(client_id, transfer_rt, transfer_at, expires_at_s):
    client = globus_sdk.NativeAppAuthClient(client_id)
    client.oauth2_start_flow(refresh_tokens=True)
    authorizer = globus_sdk.RefreshTokenAuthorizer(
        transfer_rt, client, access_token=transfer_at, expires_at=int(expires_at_s))
    tc = globus_sdk.TransferClient(authorizer=authorizer)
    return tc

def get_endpoints(raw_endpoints):
    endpoints = []
    for endpoint in raw_endpoints.split(";"):
        endpoints.append((endpoint.split(",")[0], endpoint.split(",")[1]))
    return endpoints

def connect_to_database(Config):
        dbhost = Config.get("database", "dbhost")
        dbname = Config.get("database", "dbname")
        dbuser = Config.get("database", "dbuser")
        dbpassword = Config.get("database", "dbpassword")
        connection_timeout = Config.get("database", "timeout")
        print(dbhost + " " + dbname + " " + dbuser + " " + connection_timeout)

        conn_string = "host='" + dbhost + "' dbname='" + dbname + "' user='" + dbuser + "' password='" + dbpassword + "' connect_timeout=" + connection_timeout + "'"
        connection = psycopg2.connect(conn_string)
        cursor = connection.cursor()
        print("Connected to the database\n")
        return connection, cursor

def main():
    Config = ConfigParser.ConfigParser()
    Config.read("config.ini")
    client_id = Config.get("auth", "client_id")
    refresh_token = Config.get("auth", "refresh_token")
    active_token = Config.get("auth", "active_token")
    token_expires = Config.get("auth", "token_expires")
    raw_endpoints = Config.get("endpoints", "endpoints")
    start_date = datetime.strptime(Config.get("task", "start_date"), '%Y-%m-%dT%H:%M:%S')
    end_date = datetime.strptime(Config.get("task", "end_date"), '%Y-%m-%dT%H:%M:%S')
    interval = int(Config.get("task", "interval"))
    connection, cursor = connect_to_database(Config)
    endpoints = get_endpoints(raw_endpoints)
    tc = get_transfer_client(client_id, refresh_token, active_token, token_expires)
    output_tasks_from_endpoints(endpoints, start_date, end_date, interval, tc, connection, cursor)

if __name__ == "__main__":
        main()
