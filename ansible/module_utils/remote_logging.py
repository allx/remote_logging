from ansible.module_utils.basic import AnsibleModule

import socket
import six
import sys

class RemoteLogging(AnsibleModule):

    def __init__(self, argument_spec, bypass_checks=False, no_log=False,
            check_invalid_arguments=None, mutually_exclusive=None, required_together=None,
            required_one_of=None, add_file_common_args=False, supports_check_mode=False,
            required_if=None):

        argument_spec['log_addr'] = dict(type='str')
        argument_spec['log_port'] = dict(type='int')

        self.sock = None

        super(RemoteLogging, self).__init__(
                argument_spec, bypass_checks, no_log,
                check_invalid_arguments, mutually_exclusive, required_together,
                required_one_of, add_file_common_args, supports_check_mode,
                required_if)


        addr = self.params['log_addr']
        port = self.params['log_port']

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.connect((addr, port))
        except socket.error:
            self.log('cannot connect to %s:%s' % (addr, port))
            self.sock = None
            return

        self.sock = sock

    def __del__(self):
        if self.sock:
            self.sock.close()

    def message(self, msg):
        if self.sock:
            self.sock.sendall(msg + '\n')

    def run_command(self, args, header=None, f_filter=None, check_rc=False, close_fds=True, executable=None, data=None,
            binary_data=False, path_prefix=None, cwd=None, use_unsafe_shell=False,
            prompt_regex=None, environ_update=None, umask=None, encoding='utf-8',
            errors='surrogate_or_strict'):

        if not self.sock:
            return (1, "", "cannot connect to logging server")

        if header:
            self.sock.sendall(header)

        def receiver(out, err):
            if f_filter and out:
                f_filter(out)

            if isinstance(out, six.string_types):
                out = out.decode('utf-8').encode('utf-8')

            self.sock.sendall(out)

        (rc, stdout, stderr) = super(RemoteLogging, self).run_command(args, check_rc, close_fds, executable, data,
                binary_data, path_prefix, cwd, use_unsafe_shell,
                prompt_regex, environ_update, umask, encoding,
                errors, receiver)

        return (rc, stdout, stderr)

