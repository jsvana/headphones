#  This file is part of Headphones.
#
#  Headphones is free software: you can redistribute it and/or modify
#  it under the terms of the GNU General Public License as published by
#  the Free Software Foundation, either version 3 of the License, or
#  (at your option) any later version.
#
#  Headphones is distributed in the hope that it will be useful,
#  but WITHOUT ANY WARRANTY; without even the implied warranty of
#  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#  GNU General Public License for more details.
#
#  You should have received a copy of the GNU General Public License
#  along with Headphones.  If not, see <http://www.gnu.org/licenses/>.

###################################
# Stolen from Sick-Beard's db.py  #
###################################

from __future__ import with_statement

import mysql.connector
import sqlite3

import os
import headphones
from headphones import logger


SQLITE3_DB_FILE = 'headphones.db'

def getCacheSize():
    # this will protect against typecasting problems produced by empty string and None settings
    if not headphones.CONFIG.CACHE_SIZEMB:
        # sqlite will work with this (very slowly)
        return 0
    return int(headphones.CONFIG.CACHE_SIZEMB)


class DBConnection:
    def __init__(self):

        if headphones.CONFIG.DB_USE_MYSQL:
            self.connection = mysql.connector.connect(
                host=headphones.CONFIG.MYSQL_HOSTNAME,
                port=headphones.CONFIG.MYSQL_PORT,
                database=headphones.CONFIG.MYSQL_DATABASE,
                user=headphones.CONFIG.MYSQL_USERNAME,
                password=headphones.CONFIG.MYSQL_PASSWORD,
            )
        else:
            self.connection = sqlite3.connect(SQLITE3_DB_FILE, timeout=20)
            # don't wait for the disk to finish writing
            self.connection.execute("PRAGMA synchronous = OFF")
            # journal disabled since we never do rollbacks
            self.connection.execute("PRAGMA journal_mode = %s" % headphones.CONFIG.JOURNAL_MODE)
            # 64mb of cache memory,probably need to make it user configurable
            self.connection.execute("PRAGMA cache_size=-%s" % (getCacheSize() * 1024))
            self.connection.row_factory = sqlite3.Row

    def _mysql_action(self, query, args=None):
        # MySQL doesn't support the NOCASE collation and it orders case-
        # insensitive by default, so remove it.
        if headphones.CONFIG.DB_USE_MYSQL and 'COLLATE NOCASE' in query:
            query = query.replace('COLLATE NOCASE', '')

        # The MySQL connector uses %s instead of ? for parameters
        query = query.replace('?', '%s')

        # We require buffered=True for MySQL cursors because
        # frequently a SELECT is issued and the results are
        # not read. In this situation the MySQL connector
        # throws an exception.
        c = self.connection.cursor(buffered=True, dictionary=True)
        if args is None:
            c.execute(query)
        else:
            c.execute(query, [str(a) for a in args])

        if not query.startswith('SELECT'):
            self.connection.commit()

        return c

    def _sqlite3_action(self, query, args=None):
        sqlResult = None

        try:
            with self.connection as c:
                if args is None:
                    sqlResult = c.execute(query)
                else:
                    sqlResult = c.execute(query, args)
        except sqlite3.OperationalError, e:
            if "unable to open database file" in e.message or "database is locked" in e.message:
                logger.warn('Database Error: %s', e)
            else:
                logger.error('Database error: %s', e)
                raise

        except sqlite3.DatabaseError, e:
            logger.error('Fatal Error executing %s :: %s', query, e)
            raise

        return sqlResult

    def action(self, query, args=None):

        if query is None:
            return

        if headphones.CONFIG.DB_USE_MYSQL:
            return self._mysql_action(query, args)
        else:
            return self._sqlite3_action(query, args)

    def select(self, query, args=None):

        sqlResults = self.action(query, args).fetchall()
        print(sqlResults)

        if sqlResults is None or sqlResults == [None]:
            return []

        return sqlResults

    def _mysql_upsert(self, update_query, insert_query, args):
        c = self.action(update_query, args)
        if c.rowcount == 0:
            try:
                self.action(insert_query, args)
            except mysql.connector.errors.IntegrityError:
                logger.info('Queries failed: %s and %s', update_query, insert_query)

    def _sqlite3_upsert(self, update_query, insert_query, args):
        changesBefore = self.connection.total_changes

        self.action(update_query, args)

        if self.connection.total_changes == changesBefore:
            try:
                self.action(insert_query, args)
            except sqlite3.IntegrityError:
                logger.info('Queries failed: %s and %s', update_query, insert_query)

    def upsert(self, tableName, valueDict, keyDict):

        def genParams(myDict):
            return [x + " = ?" for x in myDict.keys()]

        update_query = "UPDATE " + tableName + " SET " + ", ".join(
            genParams(valueDict)) + " WHERE " + " AND ".join(genParams(keyDict))

        insert_query = (
            "INSERT INTO " + tableName + " (" + ", ".join(
                valueDict.keys() + keyDict.keys()) + ")" +
            " VALUES (" + ", ".join(["?"] * len(valueDict.keys() + keyDict.keys())) + ")"
        )

        args = valueDict.values() + keyDict.values()
        if headphones.CONFIG.DB_USE_MYSQL:
            self._mysql_upsert(update_query, insert_query, args)
        else:
            self._sqlite3_upsert(update_query, insert_query, args)
