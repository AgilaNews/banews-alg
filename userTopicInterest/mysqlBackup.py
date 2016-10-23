#!/usr/bin/env python
# -*- coding:utf-8 -*-
import os
import sys
kDir = os.path.dirname(os.path.abspath(__file__))
sys.path.insert(0, os.path.join(kDir, '..'))

import string
import shutil
import getopt
import os
import os.path
import syslog
import errno
import logging
import tempfile
import datetime
import MySQLdb
import subprocess
from operator import itemgetter
from optparse import OptionParser

import settings

TABLE_COLUMNS_DCT = {
        'tb_news': ['url_sign', 'source_url', 'channel_id', 'title',
                    'source_name', 'publish_time', 'fetch_time',
                    'json_text', 'content_type'],
    }

class MysqlBackup:

    def __init__(self, keep=90, tables=None, databases=None, store=None,
            user="root", port=None, password=None, host=None):
        self.host = host
        self.keep = keep
        self.tables = tables
        self.databases = databases
        self.store = store
        self.user = user
        self.password = password
        self.port = port
        self.conn = MySQLdb.connect(host=host,  user=user, passwd=password,
                port=port, db=databases)
        self.conn.autocommit(True)

    def get_cursor(self):
        return self.conn.cursor()

    def get_table_columns(self, table):
        if table in TABLE_COLUMNS_DCT:
            return TABLE_COLUMNS_DCT[table]
        sqlCmd = 'desc %s' % table
        cursor = self.get_cursor()
        cursor.execute(sqlCmd)
        resLst = cursor.fetchall()
        columnLst = map(lambda val: val[0], resLst)
        return columnLst

    def backup(self):
        padding = len(str(self.keep))
        backups = []

        # remove files older than keep days
        cutdate = datetime.datetime.now() - datetime.timedelta(days=self.keep)
        if os.path.isdir(self.store):
            for backup_file in os.listdir(self.store):
                bparts = backup_file.split("_")
                if bparts[0].isdigit():
                    dumpdate = datetime.datetime.strptime(bparts[0], "%Y%m%d%H%M%S")
                    if dumpdate < cutdate:
                        os.remove(os.path.join(self.store, backup_file))

        # get the current date and timestamp and the zero backup name
        tstamp = datetime.datetime.now().strftime("%Y-%m-%d")
        backupName = '%s_%s.data' % (tstamp, self.tables)
        backupDir = os.path.join(self.store, self.databases)
        if not os.path.isdir(backupDir):
            os.makedirs(backupDir)

        columnLst = self.get_table_columns(self.tables)
        sqlCmd = 'select {columnsStr} from {tables}'.format(
                columnsStr=','.join(map(lambda val: '`%s`'%val, columnLst)),
                tables=self.tables)
        cursor = self.get_cursor()
        cursor.execute(sqlCmd)
        resLst = cursor.fetchall()
        with open(os.path.join(backupDir, backupName), 'w') as fp:
            for idx, vals in enumerate(resLst):
                if idx % 100 == 0:
                    print 'deal %s ...' % idx
                newVals = []
                for curVal in vals:
                    curVal = str(curVal).replace('\n', '')
                    curVal = str(curVal).replace('\t', '')
                    newVals.append(curVal.strip())
                print >>fp, '\t'.join(newVals)

"""
Prints out the usage for the command line.
"""
def usage():
    usage = ['./mysqlBackup.py\n']
    usage.append('[--env] running environment, available: local, sandbox, online')
    usage.append('[--store] path to store backup data')
    usage.append('[--tables] tables to backup')
    message = string.join(usage)
    print message

"""
Main method that starts up the backup.
"""
def main(store, tables):
    # set the default values
    pid_file = tempfile.gettempdir() + os.sep + "mysqlbackup.pid"
    keep = 7
    databases = None
    user = None
    password = None
    host = None

    # if no arguments print usage
    env = settings.CURRENT_ENVIRONMENT_TAG
    if env not in ['local', 'online', 'sandbox']:
        usage()
        sys.exit()
    configDct = settings.ENVIRONMENT_CONFIG.get(env, {})
    mysqlCfgDct = configDct['mysql_config']
    databases = mysqlCfgDct.get('database', None)
    user = mysqlCfgDct.get('user', None)
    password = mysqlCfgDct.get('passwd', None)
    host = mysqlCfgDct.get('host', None)
    port = mysqlCfgDct.get('port', None)

    # check options are set correctly
    if user == None or store == None:
        logging.warning("Backup store directory (-t) and user (-u) are required")
        usage()
        sys.exit(errno.EPERM)

    # process backup, catch any errors, and perform cleanup
    try:
        # another backup can't already be running, if pid file doesn't exist, then
        # create it
        if os.path.exists(pid_file):
            logging.warning("Backup running, %s pid exists, exiting." % pid_file)
            sys.exit(errno.EBUSY)
        else:
            pid = str(os.getpid())
            f = open(pid_file, "w")
            f.write("%s\n" % pid)
            f.close()

        # create the backup object and call its backup method
        mysql_backup = MysqlBackup(keep=keep, databases=databases,
                store=store, user=user, port=port, password=password,
                host=host, tables=tables)
        mysql_backup.backup()

    except(Exception):
        logging.exception("Mysql backups failed.")
    finally:
        os.remove(pid_file)

# if we are running the script from the command line, run the main function
if __name__ == "__main__":
    parser = OptionParser()
    parser.add_option('--store', dest='store', default='/data/mysqlBackup')
    parser.add_option('--tables', dest='tables', default='tb_news')
    (options, args) = parser.parse_args()
    store = options.store
    tables = options.tables
    main(store, tables)
