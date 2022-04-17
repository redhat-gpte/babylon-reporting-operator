import os

# Set default timezone to UTC
os.environ['TZ'] = 'UTC'
os.environ['PGTZ'] = 'UTC'

import kubernetes
import base64
import json
import psycopg2
from psycopg2.extras import DictCursor
from psycopg2 import ProgrammingError as Psycopg2ProgrammingError
from psycopg2 import pool
import decimal
from datetime import datetime, timedelta, timezone
import re
import pytz
import tzlocal
from retrying import retry


def list_to_pg_array(elem):
    """Convert the passed list to PostgreSQL array
    represented as a string.

    Args:
        elem (list): List that needs to be converted.

    Returns:
        elem (str): String representation of PostgreSQL array.
    """
    elem = str(elem).strip('[]')
    elem = '{' + elem + '}'
    return elem


def convert_elements_to_pg_arrays(obj):
    """Convert list elements of the passed object
    to PostgreSQL arrays represented as strings.

    Args:
        obj (dict or list): Object whose elements need to be converted.

    Returns:
        obj (dict or list): Object with converted elements.
    """
    if isinstance(obj, dict):
        for (key, elem) in obj.items():
            if isinstance(elem, list):
                obj[key] = list_to_pg_array(elem)

    elif isinstance(obj, list):
        for i, elem in enumerate(obj):
            if isinstance(elem, list):
                obj[i] = list_to_pg_array(elem)

    return obj


def get_conn_params(secret_name='gpte-db-secrets'):
    """Get connection parameters from the passed dictionary.
    Return a dictionary with parameters to connect to PostgreSQL server.
    """
    params_dict = get_secret_data(secret_name)
    params_map = {
        "hostname": "host",
        "username": "user",
        "password": "password",
        "port": "port",
        "ssl_mode": "sslmode",
        "ca_cert": "sslrootcert",
        "dbname": "database"
    }

    kw = dict((params_map[k], v) for (k, v) in params_dict.items()
              if k in params_map and v != '' and v is not None)

    return kw


# Wait 2^x * 500 milliseconds between each retry, up to 5 seconds, then 5 seconds afterwards and 3 attempts
@retry(stop_max_attempt_number=3, wait_exponential_multiplier=500, wait_exponential_max=5000)
def connect_to_db(fail_on_conn=True):

    db_connection = None
    conn_params = get_conn_params()
    try:
        # TODO: Create parameter to max_connection and min_connection
        db_connection = pool.ThreadedConnectionPool(2, 4, **conn_params)
        if db_connection:
            print("Connection pool created successfully using ThreadedConnectionPool")

    except TypeError as e:
        if 'sslrootcert' in e.args[0]:
            print('Postgresql server must be at least '
                  'version 8.4 to support sslrootcert')
        if fail_on_conn:
            print("unable to connect to database: %s" % e)
        else:
            print("PostgreSQL server is unavailable: %s" % e)
            db_connection = None
    except Exception as e:
        if fail_on_conn:
            print("unable to connect to database: %s" % e)
        else:
            print("PostgreSQL server is unavailable: %s" % e)
            db_connection = None

    return db_connection


def execute_query(query, positional_args=None, autocommit=False):

    db_connection = connect_to_db()

    query_list = []
    if positional_args:
        positional_args = convert_elements_to_pg_arrays(positional_args)

    query_list.append(query)

    if db_connection is None:
        db_connection = connect_to_db()

    # This is a workaround to reconnect to database
    db_pool_conn = db_connection.getconn()
    try:
        db_pool_conn.close()
        db_connection.putconn(db_pool_conn)
    except psycopg2.InterfaceError:
        pass

    db_pool_conn = db_connection.getconn()

    encoding = 'utf-8'
    if encoding is not None:
        db_pool_conn.set_client_encoding(encoding)

    cursor = db_pool_conn.cursor(cursor_factory=DictCursor)

    # Prepare args:
    if positional_args:
        arguments = positional_args
    else:
        arguments = None

    # Set defaults:
    changed = False

    query_all_results = []
    rowcount = 0
    statusmessage = ''

    query_result = []

    # Execute query:
    for query in query_list:
        try:
            cursor.execute(query, arguments)
            statusmessage = cursor.statusmessage
            if cursor.rowcount > 0:
                rowcount += cursor.rowcount

            query_result = []
            try:
                for row in cursor.fetchall():
                    # Ansible engine does not support decimals.
                    # An explicit conversion is required on the module's side
                    row = dict(row)

                    for (key, val) in row.items():
                        if isinstance(val, decimal.Decimal):
                            row[key] = float(val)

                        elif isinstance(val, timedelta):
                            row[key] = str(val)

                    query_result.append(row)

            except Psycopg2ProgrammingError as e:
                if 'no results to fetch' in e:
                    print(f"ERROR: {e}")
                    query_result = []

            except Exception as e:
                print("Cannot fetch rows from cursor: %s" % e)

            query_all_results.append(query_result)

            if 'SELECT' not in statusmessage:
                if re.search(re.compile(r'(UPDATE|INSERT|DELETE)'), statusmessage):
                    s = statusmessage.split()
                    if len(s) == 3:
                        if s[2] != '0':
                            changed = True

                    elif len(s) == 2:
                        if s[1] != '0':
                            changed = True

                    else:
                        changed = True

                else:
                    changed = True


        except Exception as e:
            db_pool_conn.rollback()
            cursor.close()
            db_connection.putconn(db_pool_conn)
            print("Cannot execute SQL \n"
                  "Query: '%s' \n"
                  "Arguments: %s: \n"
                  "Error: %s, \n"
                  "query list: %s\n"
                  "" % (query, arguments, e, query_list))
            db_connection.closeall()

    try:
        if autocommit:
            db_pool_conn.commit()
        else:
            db_pool_conn.rollback()

        kw = dict(
            changed=changed,
            query=cursor.query,
            query_list=query_list,
            statusmessage=statusmessage,
            query_result=query_result,
            query_all_results=query_all_results,
            rowcount=rowcount,
        )

        cursor.close()
        db_connection.putconn(db_pool_conn)

        # closing database connection.
        # use closeall() method to close all the active connection if you want to turn of the application
        db_connection.closeall()
        del db_connection
        return kw
    except Exception as e:
        print(f"ERROR closing connection {e}")
        pass


def parse_null_value(value):
    if value == 'NULL':
        return 'default'
    if value == 'default':
        return 'default'
    if value and (value != 'NULL' or value != ''):
        result = "'{value}'".format(value=value)
    else:
        result = 'default'
    return result


def parse_dict_null_value(dictionary):
    new_dict = {}
    for k, v in dictionary.items():
        if isinstance(v, dict):
            new_dict[k] = parse_dict_null_value(v)
        else:
            new_dict.update({k: parse_null_value(v)})

    return new_dict

@retry(stop_max_attempt_number=3, wait_exponential_multiplier=500, wait_exponential_max=5000)
def get_secret_data(secret_name, secret_namespace=None):
    core_v1_api = kubernetes.client.CoreV1Api()
    if not secret_namespace:
        secret_namespace = "babylon-reporting"
    secret = core_v1_api.read_namespaced_secret(
        secret_name, secret_namespace
    )
    data = {k: base64.b64decode(v).decode('utf-8') for (k, v) in secret.data.items()}

    # Attempt to evaluate secret data valuse as YAML
    for k, v in data.items():
        try:
            data[k] = json.loads(v)
        except json.decoder.JSONDecodeError:
            pass
    return data


def generic_email(email):
    if 'generic' in email:
        return email.replace('+generic', '')
    elif '+shared' in email:
        return email.replace('+shared', '')
    elif '+test' in email:
        return email.replace('+test', '')
    else:
        return email


def parse_ldap_result(result_data, capitalize=True):
    user_data = {}
    for dn, entry in result_data:
        for k, v in entry.items():
            if capitalize:
                user_data.update({k: v[0].decode('utf-8')})
            else:
                user_data.update({k: v[0].decode('utf-8')})

    return user_data


def check_exists(table_name, identifier, column='id'):
    positional_args = [identifier]
    query = f"SELECT {column} FROM {table_name} WHERE {column} = %s'"
    results = execute_query(query, positional_args=positional_args, autocommit=True)

    if results['rowcount'] >= 1:
        return True
    else:
        return False


def last_lifecycle(provision_uuid):
    positional_args = [provision_uuid]
    query = f"SELECT MAX(logged_at), state \n" \
            f"FROM lifecycle_log ll \n" \
            f"WHERE provision_uuid = %s \n" \
            f"GROUP BY state \n" \
            f"ORDER BY 1 DESC \n" \
            f"LIMIT 1;"

    result = execute_query(query, positional_args=positional_args, autocommit=True)

    if result['rowcount'] >= 1:
        query_result = result['query_result'][0]
        return query_result.get('state')
    else:
        return None


def update_lifetime(provision_uuid):
    positional_args = [provision_uuid]
    query = f"SELECT max(logged_at) as logged_at \n" \
            f"FROM lifecycle_log ll \n" \
            f"WHERE provision_uuid = %s AND state = 'provisioning' \n" \
            f"LIMIT 1;"

    result = execute_query(query, positional_args=positional_args, autocommit=True)

    if result['rowcount'] >= 1:
        query_result = result['query_result'][0]
        provisioning_at = query_result.get('logged_at')
        lifetime = datetime.utcnow() - provisioning_at
        print(f"Provision UUID {provision_uuid} lifetime: {lifetime}")
        positional_args = [lifetime, provision_uuid]
        query = "UPDATE provisions SET lifetime_interval = %s WHERE uuid = %s RETURNING uuid;"
        execute_query(query, positional_args=positional_args, autocommit=True)


def provision_lifecycle(provision_uuid, current_state, username):

    # TODO: Calculate lifetime using current_state = 'destroy-completed'
    if current_state == 'destroy-completed':
        update_lifetime(provision_uuid)

    last_state = last_lifecycle(provision_uuid)

    if last_state == current_state:
        return

    print(f"Updating provision {provision_uuid} - last_state = {current_state}")
    current_date = datetime.now(timezone.utc)
    positional_args = [current_state, current_date, provision_uuid]
    query = f"SET TIMEZONE='GMT'; \n" \
            f"UPDATE provisions SET \n" \
            f"  last_state = %s, \n" \
            f"  modified_at = %s " \
            f"WHERE uuid = %s RETURNING uuid;"

    cur = execute_query(query, positional_args=positional_args, autocommit=True)

    if username is None:
        username = 'gpte-user'

    positional_args = [
        provision_uuid,
        current_state,
        username
    ]

    print(f"Inserting Lifecycle log for {provision_uuid} - {current_state} - {username}")
    query = f"SET TIMEZONE='GMT'; " \
            f"INSERT INTO lifecycle_log (provision_uuid, state, executor) \n" \
            f"VALUES (%s, %s, %s) RETURNING id;"

    cur = execute_query(query, positional_args=positional_args, autocommit=True)


def update_provision_result(provision_uuid, result='success'):
    positional_args = [result, provision_uuid]
    query = f"SET TIMEZONE='GMT'; UPDATE provisions SET provision_result = %s WHERE uuid = %s RETURNING uuid;"

    cur = execute_query(query, positional_args=positional_args, autocommit=True)


def save_resource_claim_data(resource_claim_uuid, resource_claim_name, resource_claim_namespace, resource_claim):
    if len(resource_claim) == 0:
        print('Resource Claim log size 0, do not insert into db')
        return

    if resource_claim_uuid is None:
        return

    resource_claim_metadata = resource_claim.get('metadata', {})
    resource_claim_metadata.pop('managedFields')

    resource_claim_spec = resource_claim.get('spec', {})
    if 'provision_messages' in resource_claim_spec:
        resource_claim_spec.pop('provision_messages')

    resource_claim_json = {
        'metadata': resource_claim_metadata,
        'spec': resource_claim_spec,
        'status': resource_claim.get('status', {})
    }

    insert_fields = {
        'provision_uuid': resource_claim_uuid,
        'resource_claim_name': resource_claim_name,
        'resource_claim_namespace': resource_claim_namespace,
        'resource_claim_json': json.dumps(resource_claim_json),
        'created_at': datetime.now(timezone.utc)
    }
    update_fields = {
        'resource_claim_name': resource_claim_name,
        'resource_claim_namespace': resource_claim_namespace,
        'resource_claim_json': json.dumps(resource_claim_json),
    }


    query, positional_args = create_sql_statement(insert_fields=insert_fields,
                                                        update_fields=update_fields,
                                                        table_name='resource_claim_log',
                                                        constraint='resource_claim_log_pk',
                                                        return_field='provision_uuid')
    cur = execute_query(query, positional_args=positional_args, autocommit=True)

def save_tower_extra_vars(resource_claim_uuid, resource_claim_name, resource_claim_namespace, provision_vars):

    if len(provision_vars) == 0:
        print('Provision vars size 0, do not insert into db')
        return

    insert_fields = {
        'provision_uuid': resource_claim_uuid,
        'resource_claim_name': resource_claim_name,
        'resource_claim_namespace': resource_claim_namespace,
        'tower_extra_vars_json': json.dumps(provision_vars),
        'created_at': datetime.now(timezone.utc)
    }
    update_fields = {
        'resource_claim_name': resource_claim_name,
        'resource_claim_namespace': resource_claim_namespace,
        'tower_extra_vars_json': json.dumps(provision_vars),
    }

    query, positional_args = create_sql_statement(insert_fields=insert_fields,
                                                        update_fields=update_fields,
                                                        table_name='resource_claim_log',
                                                        constraint='resource_claim_log_pk',
                                                        return_field='provision_uuid')
    cur = execute_query(query, positional_args=positional_args, autocommit=True)


def save_provision_vars(resource_claim_uuid, resource_claim_name, resource_claim_namespace, provision_vars):

    if len(provision_vars) == 0:
        print('Provision vars size 0, do not insert into db')
        return

    insert_fields = {
        'provision_uuid': resource_claim_uuid,
        'resource_claim_name': resource_claim_name,
        'resource_claim_namespace': resource_claim_namespace,
        'provision_vars_json': json.dumps(provision_vars, default=str),
        'created_at': datetime.now(timezone.utc)
    }
    update_fields = {
        'resource_claim_name': resource_claim_name,
        'resource_claim_namespace': resource_claim_namespace,
        'provision_vars_json': json.dumps(provision_vars, default=str),
    }

    query, positional_args = create_sql_statement(insert_fields=insert_fields,
                                                        update_fields=update_fields,
                                                        table_name='resource_claim_log',
                                                        constraint='resource_claim_log_pk',
                                                        return_field='provision_uuid')
    cur = execute_query(query, positional_args=positional_args, autocommit=True)


def timestamp_to_utc(timestamp_received):
    if timestamp_received:
        local_timezone = tzlocal.get_localzone()
        provision_job_start_timestamp = datetime.strptime(timestamp_received, '%Y-%m-%dT%H:%M:%S%z')
        provision_job_start_timestamp = provision_job_start_timestamp.astimezone(local_timezone)
        provision_job_start_timestamp = provision_job_start_timestamp.replace(tzinfo=pytz.utc)
        print(provision_job_start_timestamp.replace(tzinfo=pytz.utc))

        utc_tzinfo = pytz.timezone("UTC")

        timestamp_received = provision_job_start_timestamp.astimezone(utc_tzinfo)

    return timestamp_received


def create_sql_statement(insert_fields, update_fields, table_name, constraint, return_field):

    positional_args = []
    list_fields = list(insert_fields.keys())
    list_str = ", ".join(list_fields)

    query = f"SET TIMEZONE='GMT';\n INSERT INTO {table_name} (%s) \nVALUES( " % list_str
    list_size = len(list_fields) - 1
    for index, item in enumerate(list_fields):
        positional_args.append(insert_fields[item])
        if index < list_size:
            query += "%s, "
        else:
            query += "%s"

    query += ') \n'

    query += f"ON CONFLICT ON CONSTRAINT {constraint} DO \nUPDATE SET \n"

    for k, v in update_fields.items():
        positional_args.append(v)
        query += k + '= %s, \n'

    query = query[:-3]
    query += f"\nRETURNING {return_field};"
    debug = False
    if debug:
        print(f"Query Insert: \n{query} - {positional_args}")

    return query, positional_args

