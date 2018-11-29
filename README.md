Markdown guide: [GitHub Flavored Markdown](https://guides.github.com/features/mastering-markdown/).

# Check table owners
```sql
select t.table_name, t.table_type, c.relname, c.relowner, u.usename
from information_schema.tables t
join pg_catalog.pg_class c on (t.table_name = c.relname)
join pg_catalog.pg_user u on (c.relowner = u.usesysid)
where t.table_schema='SCHEMA';
```

# Check prerequisites of a specific etl job
```sql
SELECT job.module_name, job.prerequisite as prerequisite_id, job1.module_name AS prerequisite_name
FROM (SELECT module_name, unnest(prerequisites) AS prerequisite FROM etl_job) AS job
LEFT JOIN etl_job job1 ON job1.id = job.prerequisite
WHERE job.module_name = 'JOBNAME';
```

# Check which jobs have a specific job as a prerequisite
```sql
SELECT * FROM etl_job
WHERE prerequisites @> ARRAY[(SELECT id
                              FROM etl_job
                              WHERE module_name LIKE '%PREREQUISITE%')];
```

# Add prerequisite if not exists
```sql
UPDATE bdwh.etl_job
   SET prerequisites = prerequisites || 
    (SELECT array_agg(j.id) FROM bdwh.etl_job j WHERE j.module_name IN ('ADD_PREREQUISITE', 'ADD_PREREQUISITE') 
   AND NOT EXISTS 
    (SELECT 1 FROM bdwh.etl_job j2 WHERE j.id = ANY(j2.prerequisites) AND j2.module_name = 'LOAD_NAME'))
WHERE module_name = 'LOAD_NAME';
```

# Grant access to schema and usage on all tables
```sql
SET ROLE ADMIN;
GRANT USAGE ON SCHEMA owl TO "USERNAME";
GRANT SELECT ON ALL TABLES IN SCHEMA XXX TO "USERNAME";
```

# Copy table from CSV
```sql
CREATE TABLE bdwh.copied_data (
  aaa TEXT,
  bbb INTEGER,
  ccc DATE 
);

SET ROLE ADMIN;

COPY bdwh.copied_data(aaa , bbb , ccc)
FROM 'samba/FILENAME.csv' DELIMITER ',' CSV HEADER;

SELECT * FROM bdwh.copied_data;
```

# Check constraints of a particular table
```sql
SELECT * FROM information_schema.constraint_table_usage
WHERE table_name like '%TABLENAME%';

SELECT n.nspname AS schema_name,
       t.relname AS table_name,
       c.conname AS constraint_name
FROM pg_constraint c
  JOIN pg_class t ON c.conrelid = t.oid
  JOIN pg_namespace n ON t.relnamespace = n.oid
WHERE t.relname LIKE '%TABLENAME%';
```

# List all columns of a particular table
```sql
SELECT *
FROM information_schema.columns
WHERE table_schema LIKE '%SCHEMANAME%'
AND table_name LIKE '%TABLENAME%';
```

# Cast as type of a mentioned column:
```sql
src_id bdwh.dwh_data_source.data_source_id%TYPE
-- Gives src_id the same data type as the variable or collumn given before %TYPE
```

# Example of DO
```sql
DO LANGUAGE plpgsql
$$
DECLARE
  src_id        INTEGER := (SELECT data_source_id
                            FROM bdwh.dwh_data_source
                            WHERE data_collection_name = 'application'
                                  AND system_name = 'nfi');

  broker_src_id INTEGER := (SELECT data_source_id
                            FROM bdwh.dwh_data_source
                            WHERE
                              data_collection_name =
                              'brokerapplicationdata');

BEGIN
  EXECUTE '
  ALTER TABLE bdwh_part.nest_fi_application_attr_val
    ADD CHECK (data_source_id IN (' || src_id || ', ' || broker_src_id || '));';

END;
$$;
```

# Distinct ON
```sql
SELECT DISTINCT ON (crt_id)
crt_id, pmt_id, pmt_date, pmt_sum
FROM biz.dm_p_payment
WHERE branch = 'PLVF'
ORDER BY crt_id, pmt_date DESC;
```

# Checking database parameters
```sql
SELECT version(); --Database version
SELECT pg_postmaster_start_time(); --Server starttime
SELECT * FROM pg_stat_activity; -- Users activity statistics
```

# Check running processes / activity in database
```sql
SET ROLE ADMIN;
SELECT
  pid,
  usename,
  now() - pg_stat_activity.query_start AS duration,
  query,
  state
FROM pg_stat_activity;

SELECT pg_terminate_backend(17230); -- Terminate certain query
```

# Which jobs have a certain job/loading as a prerequisite 
```sql
SELECT * FROM etl_job where prerequisites @> ARRAY[(SELECT id FROM etl_job WHERE module_name LIKE '%TABLENAME%')];
```

# Show conflict locks 
```sql
SELECT blocked_locks.pid     AS blocked_pid,
         blocked_activity.usename  AS blocked_user,
         blocking_locks.pid     AS blocking_pid,
         blocking_activity.usename AS blocking_user,
         blocked_activity.query    AS blocked_statement,
         blocking_activity.query   AS current_statement_in_blocking_process,
         blocked_activity.application_name AS blocked_application,
         blocking_activity.application_name AS blocking_application
   FROM  pg_catalog.pg_locks         blocked_locks
    JOIN pg_catalog.pg_stat_activity blocked_activity  ON blocked_activity.pid = blocked_locks.pid
    JOIN pg_catalog.pg_locks         blocking_locks
        ON blocking_locks.locktype = blocked_locks.locktype
        AND blocking_locks.DATABASE IS NOT DISTINCT FROM blocked_locks.DATABASE
        AND blocking_locks.relation IS NOT DISTINCT FROM blocked_locks.relation
        AND blocking_locks.page IS NOT DISTINCT FROM blocked_locks.page
        AND blocking_locks.tuple IS NOT DISTINCT FROM blocked_locks.tuple
        AND blocking_locks.virtualxid IS NOT DISTINCT FROM blocked_locks.virtualxid
        AND blocking_locks.transactionid IS NOT DISTINCT FROM blocked_locks.transactionid
        AND blocking_locks.classid IS NOT DISTINCT FROM blocked_locks.classid
        AND blocking_locks.objid IS NOT DISTINCT FROM blocked_locks.objid
        AND blocking_locks.objsubid IS NOT DISTINCT FROM blocked_locks.objsubid
        AND blocking_locks.pid != blocked_locks.pid
     JOIN pg_catalog.pg_stat_activity blocking_activity ON blocking_activity.pid = blocking_locks.pid
   WHERE NOT blocked_locks.GRANTED;
```

# Check the column desciptions of a specific table
```sql
SELECT
 relname as table,
 attname as column,
 description
FROM pg_description
 JOIN pg_attribute t1 ON t1.attrelid = pg_description.objoid AND pg_description.objsubid = t1.attnum
 JOIN pg_class ON pg_class.oid = t1.attrelid
WHERE relname LIKE '%TABLENAME%';
```

# Lookup user groups and roles
```sql
SELECT
  r.rolname,
  r.rolsuper,
  r.rolinherit,
  r.rolcreaterole,
  r.rolcreatedb,
  r.rolcanlogin,
  r.rolconnlimit,
  r.rolvaliduntil,
  ARRAY(SELECT b.rolname
        FROM pg_catalog.pg_auth_members m
          JOIN pg_catalog.pg_roles b ON (m.roleid = b.oid)
        WHERE m.member = r.oid)                    AS memberof,
  pg_catalog.shobj_description(r.oid, 'pg_authid') AS description,
  r.rolreplication,
  r.rolbypassrls
FROM pg_catalog.pg_roles r
WHERE r.rolname !~ '^pg_'
ORDER BY 1;
```

# Maintaining user rights
Remove certain role from a user:
```sql
REVOKE myRole FROM myUser
```
Removing user from database
```sql
DROP OWNED BY myUser;
DROP USER myUser;
ALTER USER myUser NO LOGIN
```

Grant some specific role to a user
```sql
GRANT myRole TO myUser;
```

Create new user with login option
```sql
CREATE USER myUser WITH LOGIN;
```

Create schema for a user
```sql
CREATE SCHEMA "my.username"  AUTHORIZATION "my.surname";
```

Grant usage for a schema. This grants rights to see tables under that schema.
```sql
GRANT USAGE ON SCHEMA someSchema TO myUser;
```

Grant select on all tables
```sql
GRANT SELECT ON ALL TABLES IN SCHEMA mySchema TO myRole/myUser ;
```




