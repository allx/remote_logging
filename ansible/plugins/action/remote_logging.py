from __future__ import (absolute_import, division, print_function)
__metaclass__ = type

from ansible.plugins.action import ActionBase
from ansible.utils.vars import merge_hash

import os.path
import socket
import sys

class RemoteLogging(ActionBase):

    DEF_TASK_TIMEOUT = 86400
    DEF_TASK_POLL =1
    DEF_WAIT_TIMEOUT = 30

    ROUTING_IP = None

    def __init__(self, task, connection, play_context, loader, templar, shared_loader_obj):

        self.sock = None

        # find a routable IP from managed node to controller node
        if self.ROUTING_IP is None:
            s = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
            s.connect((connection._options.get('host', 'localhost'), 22))
            self.ROUTING_IP = s.getsockname()[0]
            s.close()

        super(RemoteLogging, self).__init__(task, connection, play_context, loader, templar, shared_loader_obj)

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.bind(('', 0))
        sock.listen(1)
        sock.settimeout(self.DEF_WAIT_TIMEOUT)
        self.sock = sock

    def service(self):
        try:
            conn, client_addr = self.sock.accept()
        except Exception as e:
            print ("Exception accepting socket connection", e)
            return

        while True:

            try:
                data = conn.recv(2048)
            except socket.error as ex:
                if str(ex) == "[Errno 35] Resource temporarily unavailable":
                    # TBD: limited retry?
                    continue
                print(ex)

            data = data.decode('utf-8').encode('utf-8')

            sys.stderr.write(data)
            sys.stderr.flush()
            if not data:
                break

    def __del__(self):
        if self.sock is not None:
            self.sock.close()

    def run(self, tmp=None, task_vars=None):

        self._supports_check_mode = True
        self._supports_async = True

        result = super(RemoteLogging, self).run(tmp, task_vars)
        del tmp

        # any task using remote_logging is executed asynchronously
        # if 'async' argument is provided, use provided 'async' and 'poll' value (default value of 'poll' is 5s)
        # otherwise, use DEF_TASK_TIMEOUT and DEF_TASK_POLL as vlues of 'async' and 'poll'

        if not self._task.async_val:
            self._task.async_val = self.DEF_TASK_TIMEOUT
            self._task.poll = self.DEF_TASK_POLL

        if not result.get('skipped'):
            if result.get('invocation', {}).get('module_args'):
                del result['invocation']['module_args']

            addr = self.ROUTING_IP
            port = self.sock.getsockname()[1]
            self._task.args['log_addr'] = addr
            self._task.args['log_port'] = port

            wrap_async = True
            result = merge_hash(result, self._execute_module(task_vars=task_vars, wrap_async=wrap_async))

            # blocks until task completed (or failed)
            self.service()

            if self._task.action == 'setup':
                result['_ansible_verbose_override'] = True

        if not wrap_async:
            self._remove_tmp_path(self._connection._shell.tmpdir)

        return result

