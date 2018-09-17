#!/apps/base/python2.7/bin/python
##############################################################################
#
#  COPYRIGHT (C) 2013 Battelle Memorial Institute.  All Rights Reserved.
#
##############################################################################
#
#  Author:
#     name:  Brian Ermold
#     phone: (509) 375-2277
#     email: brian.ermold@pnnl.gov
#
##############################################################################
#
#  REPOSITORY INFORMATION:
#    $Revision: 50437 $
#    $Author: sbeus $
#    $Date: 2013-12-13 19:39:34 +0000 (Fri, 13 Dec 2013) $
#
##############################################################################

import os
import sys
import psycopg2

class DSDBError(Exception):
    """Class used for all exceptions in this module."""
    pass

# end DSDBError Class
##############################################################################

__DSDBConnFilesCache = {};

def _DSDB__dsdb_load_conn_file(conn_file=None):
    """Load the database connection config file into the internal cache."""

    # Look for a conn_file file in the default locations
    # if one was not specified.

    if conn_file is None:

        search_order = [
            './db_connect',
            os.getenv('HOME', '/apps/ds/conf/dsdb') + "/.db_connect",
        ]

        for file in search_order:
            if os.path.isfile(file):
                conn_file = file
                break

    if not conn_file:
        raise DSDBError(
            'Could not find default .db_connect file in search order:\n' +
            ' -> {0}\n'.format("\n -> ".join(search_order)))

    # Get the last mod time of the conn_file file.

    load_conn_file = True
    mtime          = None

    try:
        statinfo = os.stat(conn_file)
        mtime    = statinfo.st_mtime
    except OSError as e:
        if conn_file in __DSDBConnFilesCache:
            # The conn_file has already been loaded but the stat function
            # failed for some reason. Continue using the cached information.
            load_conn_file = False
        else:
            raise DSDBError(
                'Could not stat file: {0}\n -> {1}\n'.format(
                conn_file, e.strerror))

    # Check if the conn_file has already been loaded and that
    # it's mod time has not changed.

    if mtime and conn_file in __DSDBConnFilesCache:
        if mtime == __DSDBConnFilesCache[conn_file]['mtime']:
            load_conn_file = False

    # Load the conn_file file into the internal cache.

    if load_conn_file:

        try:
            fh = open(conn_file, 'r')
            try:
                conn_entries = {}
                for line in fh:

                    line = line.strip()
                    if not line or line[0] == '#':
                        continue

                    cols = line.split()
                    if len(cols) < 5:
                        continue

                    conn_entries[cols[0]] = {
                        'host': cols[1],
                        'name': cols[2],
                        'user': cols[3],
                        'pass': cols[4],
                    }

                # end for each line in file

                __DSDBConnFilesCache[conn_file] = {
                    'file':    conn_file,
                    'mtime':   mtime,
                    'entries': conn_entries,
                }

            finally:
                fh.close()

        except IOError as e:
            raise DSDBError(
                'Could not read file: {0}\n -> {1}\n'.format(
                conn_file, e.strerror))

    # end load_conn_file

    return __DSDBConnFilesCache[conn_file]

# end _dsdb_load_conn_file
##############################################################################

class DSDB:

    # Initialize the DSDB object
    def __init__(self, **kwargs):
        self.conn = None
        self.conn_args = kwargs
        pass

    # Destroy the object
    def __del__(self):
        pass

    # Connect to the database
    def connect(self, **kwargs):
    
        if self.conn and ('reconnect' not in kwargs or not kwargs['reconnect']):
            return self.conn

        self.disconnect()
        
        args = self.conn_args.copy()
        args.update(kwargs)

        # Check if we need to load connection info from a config file

        if ('host' not in args or
            'user' not in args or
            'name' not in args or
            'pass' not in args):

            alias = 'dsdb'
            if 'alias' in args:
                self.__alias = alias = args['alias']

            conn_file = None
            if 'conn_file' in args:
                self.__conn_file = conn_file = args['conn_file']

            conn_dict = __dsdb_load_conn_file(conn_file)
            if alias not in conn_dict['entries']:
                raise DSDBError(
                    "Could not find DSDB alias '{0}' in file: {1}\n".format(
                    alias, conn_dict['file']))
                
            args = conn_dict['entries'][alias]
    
        self.conn = psycopg2.connect(
            host     = args['host'],
            database = args['name'],
            user     = args['user'],
            password = args['pass'],
        )
        return self.conn

    def disconnect(self):
        if self.conn is None: return
        self.conn.close()

    def query(self, sql, cols=None, args=None):
        self.connect()
        cur = self.conn.cursor()
        cur.execute(sql, args)
        result = [ ]
        for row in cur:
            if cols is not None and len(cols) == len(row):
                result.append(dict(zip(cols,row)))
            else: result.append(row)
        return result
    
    def sp(self, name, cols=None, args=None):
        a = map(lambda arg: '%s', range(len(args) if args is not None else 0))
        sql = '%s(%s)' % (name, ",".join(a))
        if cols is not None and len(cols) > 0:
            sql = 'SELECT * FROM %s' % sql
        else:
            sql = 'SELECT %s' % sql
        return self.query(sql, cols, args)


class DODDB(DSDB):

    def __init__(self, **kwargs):
        DSDB.__init__(self, **kwargs)
    
    def get(self, dsclass, datalevel, version):
        dod = { }
        
        dod['dims'] = self.sp('get_dod_dims',
            [ 'name', 'length', 'id' ],
            [ dsclass, datalevel, version ])
        
        dod['atts'] = self.sp('get_dod_atts',
            [ 'name', 'type', 'value', 'id' ],
            [ dsclass, datalevel, version ])
        
        dod['vars'] = self.sp('get_dod_vars',
            [ 'name', 'type', 'id' ],
            [ dsclass, datalevel, version ])
        
        vlookup = { }
        for v in dod['vars']:
            vlookup[v['name']] = v
            v['dims'] = [ ]
            v['atts'] = [ ]
        
        vdims = self.sp('get_dod_var_dims',
            [ 'vname', 'dname', 'dlen', 'vid', 'vdid' ],
            [ dsclass, datalevel, version, '%' ])
        
        for vdim in vdims:
            vlookup[vdim['vname']]['dims'].append(vdim['dname'])
        
        vatts = self.sp('get_dod_var_atts',
            [ 'vname', 'aname', 'atype', 'avalue', 'vid', 'aid' ],
            [ dsclass, datalevel, version, '%' ])
        
        for vatt in vatts:
            vlookup[vatt['vname']]['atts'].append({
                'name'  : vatt['aname'],
                'type'  : vatt['atype'],
                'value' : vatt['avalue'],
                'id'    : vatt['aid'],
            })
        
        return dod



if __name__ == '__main__':
    db = DODDB(conn_file='/apps/ds/conf/dsdb/.db_connect', alias='dsdb_ref')
    print(db.get('mfrsr', 'b1', '2.0'))
