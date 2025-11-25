# Copyright (C) 2013-  Takafumi Arakaki, Daniel Maturana+Opus4.5

# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

"""
Export rash history to other formats.
"""

import sys
from datetime import datetime


SUPPORTED_FORMATS = ['bash']


def export_run(format, output, **kwds):
    """
    Export command history to other formats.

    Supported formats:
    - bash: Bash history format with timestamps (HISTTIMEFORMAT compatible)

    """
    from .config import ConfigStore
    from .database import DataBase

    cfstore = ConfigStore()
    db = DataBase(cfstore.db_path)

    # Open output file with UTF-8 encoding
    if output == '-':
        output_file = sys.stdout
    else:
        output_file = open(output, 'w', encoding='utf-8')

    try:
        exporter = get_exporter(format)
        exporter(db, output_file)
    finally:
        if output != '-':
            output_file.close()


def get_exporter(format):
    """Get the exporter function for the given format."""
    exporters = {
        'bash': export_bash,
    }
    if format not in exporters:
        raise ValueError("Unsupported format: {0}. Supported formats: {1}".format(
            format, ', '.join(SUPPORTED_FORMATS)))
    return exporters[format]


def export_bash(db, output):
    """
    Export history to bash history format.

    Bash extended history format uses timestamps prefixed with #
    followed by the command on the next line:

        #1234567890
        command1
        #1234567891
        command2

    This preserves the timestamp information which can be read by bash
    when HISTTIMEFORMAT is set.

    Commands are exported in chronological order (oldest first) to match
    how bash history files are typically structured.
    """
    records = list(get_all_command_records(db))
    # Sort by start time, oldest first (chronological order)
    # Use sort key that handles both datetime and string timestamps
    records.sort(key=lambda r: get_sort_timestamp(r.start))

    try:
        for crec in records:
            if not crec.command:
                continue

            # Write timestamp if available
            if crec.start is not None:
                timestamp = datetime_to_unix(crec.start)
                output.write("#{0}\n".format(timestamp))

            # Write the command
            # Handle multi-line commands - bash stores them with literal newlines
            output.write("{0}\n".format(crec.command))
    except BrokenPipeError:
        # Handle broken pipe gracefully (e.g., when piped to head)
        pass


def get_all_command_records(db):
    """
    Retrieve all command records from the database.

    Returns a generator of CommandRecord objects with basic fields populated.
    """
    from .model import CommandRecord

    keys = ['command_history_id', 'command', 'session_history_id',
            'cwd', 'terminal', 'start', 'stop', 'exit_code']

    sql = """
    SELECT
        command_history.id, CL.command, session_id,
        DL.directory, TL.terminal,
        start_time, stop_time, exit_code
    FROM command_history
    LEFT JOIN command_list AS CL ON command_id = CL.id
    LEFT JOIN directory_list AS DL ON directory_id = DL.id
    LEFT JOIN terminal_list AS TL ON terminal_id = TL.id
    ORDER BY start_time ASC
    """

    with db.connection() as connection:
        for row in connection.execute(sql):
            yield CommandRecord(**dict(zip(keys, row)))


def get_sort_timestamp(dt):
    """
    Get a sortable timestamp value from datetime or string.

    Returns a datetime object for consistent sorting.
    """
    if dt is None:
        return datetime.min
    if isinstance(dt, datetime):
        return dt
    if isinstance(dt, str):
        try:
            return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            try:
                return datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
            except ValueError:
                return datetime.min
    # Assume numeric timestamp
    try:
        return datetime.fromtimestamp(float(dt))
    except (ValueError, TypeError, OSError):
        return datetime.min


def datetime_to_unix(dt):
    """
    Convert datetime to Unix timestamp.

    Handles both datetime objects and strings that SQLite might return.
    """
    if dt is None:
        return 0
    if isinstance(dt, datetime):
        # Convert to Unix timestamp
        return int(dt.timestamp())
    if isinstance(dt, str):
        # Parse SQLite datetime string format: "YYYY-MM-DD HH:MM:SS"
        try:
            parsed = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S")
            return int(parsed.timestamp())
        except ValueError:
            pass
        # Try with microseconds
        try:
            parsed = datetime.strptime(dt, "%Y-%m-%d %H:%M:%S.%f")
            return int(parsed.timestamp())
        except ValueError:
            pass
        # Maybe it's a numeric string
        try:
            return int(float(dt))
        except ValueError:
            return 0
    # Assume it's already a numeric timestamp
    try:
        return int(dt)
    except (ValueError, TypeError):
        return 0


def export_add_arguments(parser):
    parser.add_argument(
        '--format', '-f', required=True,
        choices=SUPPORTED_FORMATS,
        help='Output format. Supported formats: {0}'.format(
            ', '.join(SUPPORTED_FORMATS)))
    parser.add_argument(
        'output', nargs='?', default='-',
        help='Output file. Use - for stdout (default).')


commands = [
    ('export', export_add_arguments, export_run),
]
