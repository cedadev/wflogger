# How is access managed for wflogger?

Each user is created a new account that has read/write access to the DB.

As admin, create a group (role) for all user access, and grant it required access:

```
create role jasmin_workflows_users;
grant all on table workflow_logs to jasmin_workflows_users;
grant all on workflow_logs_id_seq to jasmin_workflows_users;
```

Create user "someone" with password "something", and give access:

```
psql -U <user> -h udb1.jasmin.ac.uk jasmin_workflows
# log in with password

create user someone with encrypted password 'something';
grant jasmin_workflows_users to someone;
```

Then create them a `.wflogger` credentials file:

```
host=udb1.jasmin.ac.uk dbname=jasmin_workflows port=5432 user=someone password=something
```

