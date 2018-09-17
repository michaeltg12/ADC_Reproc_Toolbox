import logging
import psycopg2
import time
from psycopg2 import InterfaceError, DatabaseError, \
    DataError, OperationalError, IntegrityError, \
    InternalError, ProgrammingError, NotSupportedError
from psycopg2.extras import NamedTupleCursor
from psycopg2.extensions import TransactionRollbackError
from armlib.services import retry_call


class FailedToCommit(Exception):
    pass


class ARMDatabaseConnection(object):
    def __init__(self,
                 connection_string,
                 autocommit=False,
                 retry_delay=10,
                 max_retry_delay=60,
                 retry_backoff=20,
                 tries=-1,
                 **kwargs):
        self.logger = logging.getLogger("root.database_connection")
        self.logger.info("Initializing Database Connection")
        self.autocommit = autocommit
        self.connection_string = connection_string
        self.conn = None
        self.retry_delay = retry_delay
        self.max_retry_delay = max_retry_delay
        self.retry_backoff = retry_backoff
        self.tries = tries
        self.reconnect_to_db()
        if 'cursor_args' in kwargs.keys():
            self.cursor_args = kwargs['cursor_args']
        else:
            self.cursor_args = {}

    def refresh_connection(self):
        if self.conn:
            self.conn.close()
        self.reconnect_to_db()

    def check_connection(self):
        '''
        This function will see if the connection closed unexpectedly in the past.
        It will reopen the connection if the connection is closed or if
        there was a failed transaction.

        '''
        if self.conn.closed:
            self.reconnect_to_db()
        elif self.conn.get_transaction_status() != 0:
            self.refresh_connection()

    def connect_to_db(self):
        conn = psycopg2.connect(self.connection_string)
        if self.autocommit:
            conn.autocommit = True
        return conn

    def get_db_connection(self):
        """Return the current db connection."""
        return self.conn

    def execute(self, query, args):
        if self.autocommit:
            self.check_connection()
        cur = self.conn.cursor(**self.cursor_args)
        cur.execute(query, args)

    def fexecute(self, query, args):
        if self.autocommit:
            self.check_connection()
        cur = self.conn.cursor(**self.cursor_args)
        cur.execute(query, args)
        results = cur.fetchall()
        return results

    def cfexecute(self, query, args):
        results = self.fexecute(query, args)
        self.commit()
        return results

    def commit(self):
        if self.conn and not self.conn.closed:
            self.conn.commit()
        else:
            raise FailedToCommit("Connection is Closed. Cannot Commit.")

    def reconnect_to_db(self):
        self.logger.info('Attempting to reestablish database connection.')
        self.conn = retry_call(self.connect_to_db,
                               fargs=[],
                               fkwargs={},
                               exceptions=(InterfaceError, OperationalError),
                               delay=self.retry_delay,
                               max_delay=self.max_retry_delay,
                               backoff=self.retry_backoff,
                               tries=self.tries,
                               logger=self.logger)
        self.logger.info('Database connection reestablished.')


class ARMAutocommitDatabaseConnection(ARMDatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['autocommit'] = True
        super(ARMAutocommitDatabaseConnection, self).__init__(*args, **kwargs)

    def _retry_operation(self, query, args, func):
        self.retries = 1
        while 1:
            try:
                return func(query, args)
            except (IntegrityError, DataError, ProgrammingError, NotSupportedError) as integrity_error:
                raise integrity_error
            except (DatabaseError, TransactionRollbackError) as db_error:
                if self.retries > 10:
                    raise db_error
                self.retries += 1
                # if not isinstance(db_error, TransactionRollbackError):
                self.refresh_connection()
                time.sleep(30)

    def retry_execute(self, query, args):
        func = super(ARMAutocommitDatabaseConnection, self).execute
        return self._retry_operation(query, args, func)

    def retry_fexecute(self, query, args):
        func = super(ARMAutocommitDatabaseConnection, self).fexecute
        return self._retry_operation(query, args, func)

    def execute(self, query, args):
        return self.retry_execute(query, args)

    def fexecute(self, query, args):
        return self.retry_fexecute(query, args)


class ARMNamedTupleConnection(ARMDatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['cursor_args'] = {'cursor_factory': NamedTupleCursor}
        super(ARMNamedTupleConnection, self).__init__(*args, **kwargs)


class ARMNamedTupleAutocommitConnection(ARMAutocommitDatabaseConnection):
    def __init__(self, *args, **kwargs):
        kwargs['cursor_args'] = {'cursor_factory': NamedTupleCursor}
        super(ARMAutocommitDatabaseConnection, self).__init__(*args, **kwargs)
