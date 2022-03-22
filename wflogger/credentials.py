import os

env = os.environ

HOME = env["HOME"]
creds_file = os.path.join(HOME, ".wflogger")
user_id = env.get("USER") or os.path.basename(HOME)
hostname = env["HOSTNAME"]

if not os.path.isfile(creds_file):
    raise IOError(f"Required credentials file does not exist: {creds_file}")

status = os.stat(creds_file)
if oct(status.st_mode)[-3:] != "400":
    raise PermissionError(f"File permissions on credentials file must be read-only for user: 0400")

creds = open(creds_file).read()

