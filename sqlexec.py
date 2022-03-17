#!/usr/bin/env python3
import os
import sys
import time
import os.path
from datetime import datetime
import traceback
import pandas as pd
from argparse import ArgumentParser
from functools import wraps
from multiprocessing import Process, Queue

def q(db):
    try:
        results = execute_query(db = db)
        if db['use_tunnel'] == 'True':
            db['db_host'] = old_db_host
        return results
    except Exception as ex:
        print(str(ex))
        sys.exit(-1)

def is_valid_file(parser, arg):
    if not os.path.exists(arg):
        parser.error("The file %s does not exist!" % arg)
    else:
        return open(arg, 'r')  # return an open file handle

def processify(func):
    '''Decorator to run a function as a process.
    Be sure that every argument and the return value
    is *pickable*.
    The created process is joined, so the code does not
    run in parallel.
    '''

    def process_func(q, *args, **kwargs):
        try:
            ret = func(*args, **kwargs)
        except Exception:
            ex_type, ex_value, tb = sys.exc_info()
            error = ex_type, ex_value, ''.join(traceback.format_tb(tb))
            ret = None
        else:
            error = None

        q.put((ret, error))

    # register original function with different name
    # in sys.modules so it is pickable
    process_func.__name__ = func.__name__ + 'processify_func'
    setattr(sys.modules[__name__], process_func.__name__, process_func)

    @wraps(func)
    def wrapper(*args, **kwargs):
        q = Queue()
        p = Process(target=process_func, args=[q] + list(args), kwargs=kwargs)
        p.start()
        ret, error = q.get()
        p.join()

        if error:
            ex_type, ex_value, tb_str = error
            message = '%s (in subprocess)' % (ex_value)
            raise ex_value
            sys.exit(-1)
            #raise ex_type(message)

        return ret
    return wrapper

@processify
def execute_query(db):

    """
    Execute an SQL query and return results.
    """

    db_engine   = db['engine']
    db_username = db['db_user']
    db_password = db['db_pass']
    db_host     = db['db_host']
    db_port     = db['db_port']
    db_name     = db['db_name']
    db_query    = db['query']
    conn_string = '%s://%s:%s@%s:%s/%s'%(db_engine,db_username,db_password,db_host,int(db_port),db_name)
    import sqlalchemy
    engine  = sqlalchemy.create_engine(conn_string)
    results = pd.read_sql_query(db_query,engine)
    return results

if __name__ == '__main__':

    parser = ArgumentParser(description="Connect and execute SQL statements.")

    parser.add_argument("-d", dest = "db_options",                            \
                        required   = True,                                    \
                        help       = "Database options file", metavar="FILE", \
                        type       = lambda x: is_valid_file(parser, x))

    parser.add_argument("-q", dest = "query",                            \
                        required   = True,                               \
                        help       = "SQL query file", metavar="FILE",   \
                        type       = lambda x: is_valid_file(parser, x))

    parser.add_argument('-e', dest = 'export',      \
                        required   =  False,        \
                        default    =  False,        \
                        action     = 'store_true')  \

    parser.add_argument('-i', dest = 'influxdb',    \
                        required   = False,         \
                        default    = False,         \
                        action     = 'store_true')  \

    parser.add_argument('-c', dest = 'compress',    \
                        required   =  False,        \
                        default    =  False,        \
                        action     = 'store_true')  \

    parser.add_argument('-t', dest = 'tty',          \
                        required   =  False,         \
                        default    =  True,          \
                        action     = 'store_false')  \

    parser.add_argument('-n', dest = "export_name", \
                        required   = False,         \
                        default    = "",            \
                        help       = "Export name", \
                        type       = str)

    parser.add_argument('-f', dest = "influxdb_conf", \
                        required   = False,         \
                        default    = "",            \
                        help       = "Influxdb configuration file", \
                        type       = str)

    args = parser.parse_args()


    with open(args.db_options.name) as f:
        db_options = f.readlines()

    with open(args.query.name) as f:
        query = f.read()

    db = {}
    db['engine']     = ""
    db['db_user']    = ""
    db['db_pass']    = ""
    db['db_name']    = ""
    db['db_host']    = ""
    db['db_port']    = ""
    db['ssh_host']   = ""
    db['ssh_port']   = 22
    db['ssh_key']    = ""
    db['ssh_user']   = ""
    db['ssh_pass']   = ""
    db['use_tunnel'] = "False"

    for line in db_options:
        line  = line.replace('\n','')
        k,v   = line.split(':')
        db[k] = v

    for i in ['engine','db_user','db_name','db_host','db_port']:
        if db[i] == "":
            print('%s cannot be null'%(i))
            sys.exit(1)

    if db['use_tunnel'] == "True":
        for i in ['ssh_host','ssh_user']:
            if db[i] == "":
                print('%s cannot be null'%(i))
                sys.exit(1)

    if db['use_tunnel'] not in ['True','False']:
        print('use_tunnel must be "True" or "False"')
        sys.exit(1)

    # if connection to the database is over an ssh tunnel.
    if db['use_tunnel'] == 'True':
        from sshtunnel import open_tunnel
        with open_tunnel((db['ssh_host'], int(db['ssh_port'])),\
                ssh_username = db['ssh_user'],                 \
                ssh_password = db['ssh_pass'],                 \
                ssh_pkey     = db['ssh_key'],                  \
                remote_bind_address = (db['db_host'], int(db['db_port']))) as tunnel:

                    db['db_port']   = tunnel.local_bind_port
                    old_db_host     = db['db_host']
                    db['db_host']   = '127.0.0.1'
                    db['query']     = query
                    ts              = str(datetime.now().strftime("%d-%b-%YT%H:%M:%S"))
                    start_execution = time.time()
                    execution_dur   = str(round(time.time() - start_execution,2))+"Sec"
                    results         = q(db=db)
    else:
        db['query']     = query
        ts              = str(datetime.now().strftime("%d-%b-%YT%H:%M:%S"))
        start_execution = time.time()
        results         = q(db=db)

    execution_dur     = str(round(time.time() - start_execution,2))+"Sec"
    msg = '%s\\%s\\%s\\%s\\%s'%(db['db_host'],db['db_name'],str(args.query.name),execution_dur,ts)

    # Create export
    if args.export == True:
        if len(args.export_name) > 0:
            export_fname = args.export_name
        else:
            ts = ts.replace(':','')
            ts = ts.replace('-','')
            qn = str(args.query.name).split('.')[0]
            export_fname = '%s_%s_%s_%s.csv'%(db['db_host'],db['db_name'],qn,ts)
        import csv
        if args.compress == True:
            compression_opts = dict( method = 'zip', archive_name = export_fname)
            export_fname = export_fname.split('.')[0]+".zip"
            results.to_csv(export_fname,            \
                    index       = False,            \
                    sep         = ",",              \
                    quoting     = csv.QUOTE_ALL,    \
                    compression = compression_opts, \
                    doublequote = True)
        else:
            results.to_csv(export_fname,         \
                    index       = False,         \
                    sep         = ",",           \
                    quoting     = csv.QUOTE_ALL, \
                    doublequote = True)
        msg = msg+"\\"+export_fname

    if args.tty == True:
        print(msg)
        print(len(msg)*'-')
        with pd.option_context('display.max_rows', None, 'display.max_columns', None):
            print(results)

    if args.influxdb == True:
        if len(args.influxdb_conf) > 0:

            influxdb_conf = args.influxdb_conf
            with open(influxdb_conf) as f:
                db_options = f.readlines()

            from influxdb_client import InfluxDBClient, Point, WriteOptions
            from influxdb_client.client.write_api import SYNCHRONOUS

            influx_db_options = {}
            influx_db_options['host']         = "127.0.0.1"
            influx_db_options['port']         = '8086'
            influx_db_options['measurement']  = ""
            influx_db_options['database']     = ""
            influx_db_options['ts']           = ""
            influx_db_options['drop']         = ""
            influx_db_options['tags']         = ""
            influx_db_options['token']        = ""
            influx_db_options['org']          = ""

            for line in db_options:
                line  = line.replace('\n','')
                k,v   = line.split(':')
                influx_db_options[k] = v

            for i in ['measurement','database','tags']:
                if influx_db_options[i] == "":
                    print('%s cannot be null'%(i))
                    sys.exit(1)

            with open(args.influxdb_conf) as f:
                influx_options = f.readlines()

            if len(influx_db_options['drop']) > 0:
                to_drop = influx_db_options['drop'].split(',')

            if len(influx_db_options['tags']) > 0:
                tags    = influx_db_options['tags'].split(',')

            if len(influx_db_options['drop']) > 0:
                results.drop(columns = to_drop,
                             inplace = True)

            if influx_db_options['ts'] != "":
                results.set_index(influx_db_options['ts'],inplace = True)
            else:
                results['ts'] = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                results.set_index(['ts'],inplace = True)

            with InfluxDBClient(url="http://%s:%s"%(influx_db_options['host'],          \
                                                    influx_db_options['port']),         \
                                                    token = influx_db_options['token'], \
                                                    org   = influx_db_options['org']) as _client:

                with _client.write_api(write_options=WriteOptions(batch_size=500)) as _write_client:
                    _write_client.write(influx_db_options['database'],                                 \
                                        "",                                                            \
                                        record = results,                                              \
                                        data_frame_measurement_name = influx_db_options['measurement'],\
                                        data_frame_tag_columns      = tags)
