import os
import sqlite3
from contextlib import closing, contextmanager
import datetime
import warnings

from .utils.iterutils import nonempty, repeat
from .model import CommandRecord

schema_version = '0.1.dev1'


def concat_expr(operator, conditions):
    """
    Concatenate `conditions` with `operator` and wrap it by ().

    It returns a string in a list or empty list, if `conditions` is empty.

    """
    expr = " {0} ".format(operator).join(conditions)
    return ["({0})".format(expr)] if expr else []


def normalize_directory(path):
    path = os.path.abspath(path)
    if path.endswith(os.path.sep):
        return path[:-len(os.path.sep)]
    else:
        return path


class DataBase(object):

    schemapath = os.path.join(
        os.path.dirname(os.path.abspath(__file__)), 'schema.sql')

    def __init__(self, dbpath):
        self.dbpath = dbpath
        self._db = None
        if not os.path.exists(dbpath):
            self._init_db()

    def _get_db(self):
        """Returns a new connection to the database."""
        return closing(sqlite3.connect(self.dbpath))

    def _init_db(self):
        """Creates the database tables."""
        from .__init__ import __version__ as version
        with self._get_db() as db:
            with open(self.schemapath) as f:
                db.cursor().executescript(f.read())
            db.execute(
                'INSERT INTO rash_info (rash_version, schema_version) '
                'VALUES (?, ?)',
                [version, schema_version])
            db.commit()

    @contextmanager
    def connection(self):
        """
        Context manager to keep around DB connection.

        :rtype: sqlite3.Connection

        """
        if self._db:
            yield self._db
        else:
            try:
                with self._get_db() as db:
                    self._db = db
                    yield self._db
            finally:
                self.db = None

    def import_json(self, json_path, **kwds):
        import json
        with open(json_path) as fp:
            try:
                dct = json.load(fp)
            except ValueError:
                warnings.warn(
                    'Ignoring invalid JSON file at: {0}'.format(json_path))
                return
        self.import_dict(dct, **kwds)

    def import_dict(self, dct, check_duplicate=True):
        crec = CommandRecord(**dct)
        if check_duplicate and nonempty(self.select_by_command_record(crec)):
            return
        with self.connection() as connection:
            db = connection.cursor()
            ch_id = self._insert_command_history(db, crec)
            self._insert_environ(db, ch_id, crec.environ)
            self._insert_pipe_status(db, ch_id, crec.pipestatus)
            connection.commit()

    def _insert_command_history(self, db, crec):
        command_id = self._get_maybe_new_command_id(db, crec.command)
        directory_id = self._get_maybe_new_directory_id(db, crec.cwd)
        terminal_id = self._get_maybe_new_terminal_id(db, crec.terminal)
        convert_ts = (lambda ts: None if ts is None
                      else datetime.datetime.utcfromtimestamp(ts))
        db.execute(
            '''
            INSERT INTO command_history
                (command_id, directory_id, terminal_id,
                 start_time, stop_time, exit_code)
            VALUES (?, ?, ?, ?, ?, ?)
            ''',
            [command_id, directory_id, terminal_id,
             convert_ts(crec.start), convert_ts(crec.stop), crec.exit_code])
        return db.lastrowid

    def _insert_environ(self, db, ch_id, environ):
        if not environ:
            return
        for (name, value) in environ.items():
            if name is None or value is None:
                continue
            ev_id = self._get_maybe_new_id(
                db, 'environment_variable',
                {'variable_name': name, 'variable_value': value})
            db.execute(
                '''
                INSERT INTO command_environment_map
                    (ch_id, ev_id)
                VALUES (?, ?)
                ''',
                [ch_id, ev_id])

    def _insert_pipe_status(self, db, ch_id, pipe_status):
        if not pipe_status:
            return
        for (i, code) in enumerate(pipe_status):
            db.execute(
                '''
                INSERT INTO pipe_status_map
                    (ch_id, program_position, exit_code)
                VALUES (?, ?, ?)
                ''',
                [ch_id, i, code])

    def _get_maybe_new_command_id(self, db, command):
        if command is None:
            return None
        return self._get_maybe_new_id(
            db, 'command_list', {'command': command})

    def _get_maybe_new_directory_id(self, db, directory):
        if directory is None:
            return None
        directory = normalize_directory(directory)
        return self._get_maybe_new_id(
            db, 'directory_list', {'directory': directory})

    def _get_maybe_new_terminal_id(self, db, terminal):
        if terminal is None:
            return None
        return self._get_maybe_new_id(
            db, 'terminal_list', {'terminal': terminal})

    def _get_maybe_new_id(self, db, table, columns):
        kvlist = list(columns.items())
        values = [v for (_, v) in kvlist]
        sql_select = 'SELECT id FROM "{0}" WHERE {1}'.format(
            table,
            ' AND '.join(map('"{0[0]}" = ?'.format, kvlist)),
        )
        for (id_val,) in db.execute(sql_select, values):
            return id_val
        sql_insert = 'INSERT INTO "{0}" ({1}) VALUES ({2})'.format(
            table,
            ', '.join(map('"{0[0]}"'.format, kvlist)),
            ', '.join('?' for _ in kvlist),
        )
        db.execute(sql_insert, values)
        return db.lastrowid

    def select_by_command_record(self, crec):
        return []
        raise NotImplementedError

    def search_command_record(self, **kwds):
        """
        Search command history.

        :rtype: [CommandRecord]

        """
        (sql, params, keys) = self._compile_sql_search_command_record(**kwds)
        with self.connection() as connection:
            cur = connection.cursor()
            for row in cur.execute(sql, params):
                yield CommandRecord(**dict(zip(keys, row)))

    def _compile_sql_search_command_record(
            cls, limit, pattern, cwd, cwd_glob, unique, **_):
        keys = ['command', 'cwd', 'terminal', 'start', 'stop', 'exit_code']
        columns = ['CL.command', 'DL.directory', 'TL.terminal',
                   'start_time', 'stop_time', 'exit_code']
        max_index = 3
        assert columns[max_index] == 'start_time'
        params = []
        conditions = []

        def add_or_match(template, name, args):
            conditions.extend(concat_expr(
                'OR', repeat(template.format(name), len(args))))
            params.extend(args)

        add_or_match('glob(?, {0})', 'CL.command', pattern)
        add_or_match('glob(?, {0})', 'DL.directory', cwd_glob)
        add_or_match('{0} = ?', 'DL.directory',
                     list(map(normalize_directory, cwd)))

        where = ''
        if conditions:
            where = 'WHERE {0} '.format(" AND ".join(conditions))

        group_by = ''
        if unique:
            columns[max_index] = 'MAX({0})'.format(columns[max_index])
            group_by = 'GROUP BY CL.command '

        sql = (
            'SELECT {0} '
            'FROM command_history '
            'LEFT JOIN command_list AS CL ON command_id = CL.id '
            'LEFT JOIN directory_list AS DL ON directory_id = DL.id '
            'LEFT JOIN terminal_list AS TL ON terminal_id = TL.id '
            '{1}{2} '
            'ORDER BY start_time '
            'LIMIT ?'
        ).format(', '.join(columns), where, group_by)
        params.append(limit)
        return (sql, params, keys)