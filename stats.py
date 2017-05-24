from datetime import datetime, timedelta
import ConfigParser
import globus_sdk
import psycopg2

# The Globus SDK needs to be installed first: https://globus-sdk-python.readthedocs.io/en/stable/installation/

def get_next_month(current):
    if current.month != 12:
        next_month = current.replace(month=current.month + 1)
    else:
        next_month = current.replace(year=current.year + 1, month=1)
    return next_month

def output_tasks_from_endpoints(endpoints, output_file, start_date, end_date, tc, connection, cursor):
    current = start_date
    for endpoint_name, endpoint_id in endpoints:
        while current <= end_date:
            print('================================================================')
            print('== Task List for ' + endpoint_name + ' during ' + str(current))
            print('================================================================')
            current = get_next_month(current)
            completion_time_filter = current.isoformat().replace("T", " ") + "," + get_next_month(current).isoformat().replace("T", " ")
            output_tasks_from_endpoint(endpoint_name, endpoint_id, output_file, completion_time_filter, tc, connection, cursor)


def output_tasks_from_endpoint(name, endpoint, output_file, completion_time_filter, tc, connection, cursor):
    output = open(output_file + name + '.csv', 'w')

    #header = '"task_id", "type", "status", "owner_id", "owner_string", "request_time", "completion_time", "source_endpoint_id", "source_endpoint_display_name", "destination_endpoint_id", "destination_endpoint_display_name", "files_transferred", "bytes_transferred", "effective_bytes_per_second"\n'

    #print(header)

    # output.write(header)

    for task in tc.endpoint_manager_task_list(num_results=None, filter_endpoint=endpoint, filter_completion_time=completion_time_filter):
        # task_string = '"{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}","{}"'.format(
        #     task["task_id"].encode("utf8"),
        #     task["type"].encode("utf8"),
        #     task["status"].encode("utf8"),
        #     task["owner_id"].encode("utf8"),
        #     task["owner_string"].encode("utf8"),
        #     task["request_time"].encode("utf8"),
        #     task["completion_time"].encode("utf8"),
        #     task["source_endpoint_id"].encode("utf8"),
        #     task["source_endpoint_display_name"].encode("utf8"),
        #     task["destination_endpoint_id"].encode("utf8"),
        #     task["destination_endpoint_display_name"].encode("utf8"),
        #     task["files_transferred"].encode("utf8"),
        #     task["bytes_transferred"].encode("utf8"),
        #     task["effective_bytes_per_second"].encode("utf8"))

        cursor.execute("SELECT task_id FROM task WHERE task_id=%s", (task["task_id"], ))
        connection.commit()
        if cursor.rowcount > 0:
            print("We found the task already in the database '" + task["task_id"] + "'")
        else:
            #print(task_string)
            # output.write(task_string + "\n")
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
    file_path = Config.get("output", "file_path")
    completion_time_filter = Config.get("task", "completion_time_filter")
    raw_endpoints = Config.get("endpoints", "endpoints")
    start_date = datetime.strptime(Config.get("task", "start_date"), '%Y-%m-%dT%H:%M:%S')
    end_date = datetime.strptime(Config.get("task", "end_date"), '%Y-%m-%dT%H:%M:%S')
    connection, cursor = connect_to_database(Config)
    endpoints = get_endpoints(raw_endpoints)
    tc = get_transfer_client(client_id, refresh_token, active_token, token_expires)
    output_tasks_from_endpoints(endpoints, file_path, start_date, end_date, tc, connection, cursor)

if __name__ == "__main__":
        main()
