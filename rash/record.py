"""
Record shell history.

This is a command to be called from shell-specific hooks.
This Python implementation is a reference implementation.
Probably it makes sense to write this command naively in
shells to make it faster.

The dumped data goes under the ``~/.config/rash/data/record``
directory.

"""

import os
import time
import json

from .utils.pathutils import mkdirp
from .config import ConfigStore


def get_tty():
    for i in range(3):
        try:
            return os.ttyname(i)
            break
        except OSError:
            pass


def get_environ(keys):
    """
    Get environment variables from :data:`os.environ`.

    :type keys: str
    :rtype: dict

    Some additional features.

    * If 'HOST' is not in :data:`os.environ`, this function
      automatically fetch it using :meth:`platform.node`.
    * If 'TTY' is not in :data:`os.environ`, this function
      automatically fetch it using :meth:`os.ttyname`.

    """
    items = ((k, os.environ.get(k)) for k in keys)
    subenv = dict((k, v) for (k, v) in items if v is not None)
    if 'HOST' in keys and not subenv.get('HOST'):
        import platform
        subenv['HOST'] = platform.node()
    if 'TTY' in keys and not subenv.get('TTY'):
        tty = get_tty()
        if tty:
            subenv['TTY'] = tty
    return subenv


def generate_session_id(data):
    """
    Generate session ID based on HOST, TTY, PID [#]_ and start time.

    :type data: dict
    :rtype: str

    .. [#] PID of the shell

    """
    host = data['environ']['HOST']
    tty = data['environ'].get('TTY') or 'NO_TTY'
    return ':'.join(map(str, [
        host, tty, os.getppid(), data['start']]))


def record_run(record_type, print_session_id, **kwds):
    """
    Record shell history.
    """
    if print_session_id and record_type != 'init':
        raise RuntimeError(
            '--print-session-id should be used with --record-type=init')

    # FIXME: make these configurable
    if record_type == 'init':
        envkeys = ['SHELL', 'TERM', 'HOST', 'TTY', 'USER', 'DISPLAY']
    elif record_type == 'exit':
        envkeys = []
    elif record_type == 'command':
        envkeys = ['PATH']

    conf = ConfigStore()
    json_path = os.path.join(conf.record_path,
                             record_type,
                             time.strftime('%Y-%m-%d'),
                             time.strftime('%H%M%S.json'))
    mkdirp(os.path.dirname(json_path))
    data = dict((k, v) for (k, v) in kwds.items() if v is not None)
    data.update(
        environ=get_environ(envkeys),
        cwd=os.getcwdu(),
    )
    if record_type in ['command', 'exit']:
        data.setdefault('stop', int(time.time()))
    elif record_type in ['init']:
        data.setdefault('start', int(time.time()))
    if print_session_id:
        data['session_id'] = generate_session_id(data)
        print(data['session_id'])
    with open(json_path, 'w') as fp:
        json.dump(data, fp)


def record_add_arguments(parser):
    parser.add_argument(
        '--record-type', default='command',
        choices=['command', 'init', 'exit'],
        help='type of record to store.')
    parser.add_argument(
        '--command',
        help="command that was ran.")
    parser.add_argument(
        '--exit-code', type=int,
        help="exit code $? of the command.")
    parser.add_argument(
        '--pipestatus', type=int, nargs='+',
        help="$pipestatus (zsh) / $PIPESTATUS (bash)")
    parser.add_argument(
        '--start', type=int,
        help='the time COMMAND is started.')
    parser.add_argument(
        '--stop', type=int,
        help='the time COMMAND is finished.')
    parser.add_argument(
        '--terminal',
        help='like $TERM, but can be anything (e.g., emacs / tmux).')
    parser.add_argument(
        '--session-id',
        help='''
        RASH session ID generated by --print-session-id.
        This option should be used with `command` or `exit` RECORD_TYPE.
        ''')
    parser.add_argument(
        '--print-session-id', default=False, action='store_true',
        help='''
        print generated session ID to stdout.
        This option should be used with `init` RECORD_TYPE.
        ''')


commands = [
    ('record', record_add_arguments, record_run),
]
