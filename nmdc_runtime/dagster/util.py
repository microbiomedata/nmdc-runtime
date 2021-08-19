from subprocess import Popen, PIPE, STDOUT, CalledProcessError


def run_and_log(shell_cmd, context):
    process = Popen(shell_cmd, shell=True, stdout=PIPE, stderr=STDOUT)
    for line in process.stdout:
        context.log.info(line.decode())
    retcode = process.wait()
    if retcode:
        raise CalledProcessError(retcode, process.args)
