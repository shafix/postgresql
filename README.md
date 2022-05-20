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
SELECT * FROM bdwh.etl_job
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

# Show exclusive locks
```sql
SELECT
  locktype,
  relation :: REGCLASS,
  mode,
  transactionid      AS tid,
  virtualtransaction AS vtid,
  granted,
  l.pid,
  st.usename,
  now() - st.query_start AS duration,
  st.query,
  st.state
FROM pg_catalog.pg_locks l
  LEFT JOIN pg_catalog.pg_database db ON db.oid = l.database
  LEFT JOIN pg_catalog.pg_stat_activity st ON l.pid = st.pid
WHERE (db.datname = 'bigdata' OR db.datname IS NULL)
      AND NOT l.pid = pg_backend_pid();
```

# Show locks version 2
```sql 
SELECT
  blockingl.relation :: REGCLASS,
  blockeda.pid    AS blocked_pid,
  blockeda.query  AS blocked_query,
  blockedl.mode   AS blocked_mode,
  blockinga.pid   AS blocking_pid,
  blockinga.query AS blocking_query,
  blockingl.mode  AS blocking_mode
FROM pg_catalog.pg_locks blockedl
  JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
  JOIN pg_catalog.pg_locks blockingl ON (blockingl.relation = blockedl.relation
                                         AND blockingl.locktype = blockedl.locktype AND blockedl.pid != blockingl.pid)
  JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
WHERE NOT blockedl.granted AND blockinga.datname = 'bigdata';
```

# Show locks version 3
```sql
SELECT
  COALESCE(blockingl.relation::regclass::text,blockingl.locktype) as locked_item,
  now() - blockeda.query_start AS waiting_duration, blockeda.pid AS blocked_pid,
  blockeda.query as blocked_query, blockedl.mode as blocked_mode,
  blockinga.pid AS blocking_pid, blockinga.query as blocking_query,
  blockingl.mode as blocking_mode
FROM pg_catalog.pg_locks blockedl
JOIN pg_stat_activity blockeda ON blockedl.pid = blockeda.pid
JOIN pg_catalog.pg_locks blockingl ON(
  ( (blockingl.transactionid=blockedl.transactionid) OR
  (blockingl.relation=blockedl.relation AND blockingl.locktype=blockedl.locktype)
  ) AND blockedl.pid != blockingl.pid)
JOIN pg_stat_activity blockinga ON blockingl.pid = blockinga.pid
  AND blockinga.datid = blockeda.datid
WHERE NOT blockedl.granted
AND blockinga.datname = current_database();
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

# Check foreign server definitions
```sql
select
    srvname as name,
    srvowner::regrole as owner,
    fdwname as wrapper,
    srvoptions as options
from pg_foreign_server
join pg_foreign_data_wrapper w on w.oid = srvfdw;
```

# Trigger checking
```sql
SELECT * FROM pg_trigger WHERE tgname NOT LIKE 'RI_%';

select event_object_schema as table_schema,
       event_object_table as table_name,
       trigger_schema,
       trigger_name,
       string_agg(event_manipulation, ',') as event,
       action_timing as activation,
       action_condition as condition,
       action_statement as definition
from information_schema.triggers
group by 1,2,3,4,6,7,8
order by table_schema,
         table_name;

select pg_get_functiondef('FUNCTION_NAME'::regproc);

SELECT event_object_table,trigger_name,event_manipulation,action_statement,action_timing 
FROM information_schema.triggers 
ORDER BY event_object_table,event_manipulation;
```

# Checking who has what rights on what table
```sql
select
    coalesce(nullif(s[1], ''), 'public') as grantee,
    s[2] as privileges
from
    pg_class c
    join pg_namespace n on n.oid = relnamespace
    join pg_roles r on r.oid = relowner,
    unnest(coalesce(relacl::text[], format('{%s=arwdDxt/%s}', rolname, rolname)::text[])) acl,
    regexp_split_to_array(acl, '=|/') s
where relname = 'TABLE_NAME';
```

# Checking table sizes
```sql
SELECT
  relname                                  AS "table_name",
  nspname,
  pg_size_pretty( pg_table_size( C.oid ) ) AS "table_size"
FROM pg_class C
  LEFT JOIN pg_namespace N
    ON ( N.oid = C.relnamespace )
WHERE nspname NOT IN ( 'pg_catalog', 'information_schema' ) AND nspname = 'SCHEMA_NAME' AND nspname !~ '^pg_toast' AND relkind IN ( 'r' )
ORDER BY pg_table_size( C.oid ) DESC
LIMIT 100;
```

# Checking table live tuple / row counts, refreshing the statistics with analyze for several tables
``` sql
-- Analyzing tables to update live tuple counts
DO LANGUAGE plpgsql
$$
  DECLARE
    v_array_of_table_names TEXT [] := NULL;
    v_individual_table     TEXT;

    v_schema_name          TEXT    := '...'; -- Specify schema of the table(s)
    v_table_name           TEXT    := '...'; -- Specify (exactly or vaguely with % table(s))
  BEGIN

    SELECT array_agg(DISTINCT tablename)
    INTO v_array_of_table_names
    FROM pg_tables WHERE schemaname = v_schema_name AND tablename ILIKE v_table_name;

    FOREACH v_individual_table IN ARRAY v_array_of_table_names
      LOOP
        EXECUTE
              $q$
        ANALYZE bdwh_part.$q$ || v_individual_table || $q$;
      $q$;
        RAISE NOTICE '% analyzed', v_individual_table;
      END LOOP;
  END;
$$;

-- Live tuple counts
SELECT schemaname,relname,n_live_tup
FROM pg_stat_user_tables
WHERE schemaname = '...' -- Specify schema of the table(s)
  AND relname ILIKE '...' -- Specify (exactly or vaguely with % table(s))
ORDER BY relname DESC;
```

# Checking how much each column is filled in the table:
```
DO LANGUAGE plpgsql
$sql$
DECLARE
  v_column_array TEXT[];
  v_each_column TEXT;
  v_table_name TEXT := 'dm_nest_bg_application_v2'; -- Change table name!
  v_final_query TEXT;
  v_final_query_end TEXT;
  v_individual_query TEXT;
  v_total_result NUMERIC;
  v_column_result INTEGER;

BEGIN
  DROP TABLE IF EXISTS temp_table_columns_filled;
  CREATE TEMPORARY TABLE temp_table_columns_filled ( t_column TEXT, t_filled_percent DECIMAL(5,1) );

  v_final_query:= $$ SELECT count(*) AS total $$;
  v_final_query_end:= $$ FROM $$ || v_table_name || $$; $$;

  SELECT array_agg(column_name)
  INTO v_column_array
  FROM information_schema.columns WHERE table_name = v_table_name;

  FOREACH v_each_column IN ARRAY v_column_array
  LOOP
    v_individual_query := '
    ,count(*) FILTER (WHERE ' || v_each_column || ' IS NOT NULL) AS ' || v_each_column || ' ';
    v_final_query := v_final_query || v_individual_query;
  END LOOP;

  EXECUTE $$
  DROP TABLE IF EXISTS temp_result;
  CREATE TEMPORARY TABLE temp_result AS
  $$ || v_final_query || v_final_query_end || $$
  $$;

  EXECUTE $$ SELECT total FROM temp_result; $$ INTO v_total_result;

  FOREACH v_each_column IN ARRAY v_column_array
  LOOP
    EXECUTE $$ SELECT $$ || v_each_column || $$ FROM temp_result; $$ INTO v_column_result;
    RAISE NOTICE '% : %', v_each_column, (100 / v_total_result * v_column_result)::DECIMAL(5,1)::TEXT || '%';
    INSERT INTO temp_table_columns_filled(t_column, t_filled_percent) VALUES (v_each_column, (100 / v_total_result * v_column_result)::DECIMAL(5,1));
  END LOOP;

END
$sql$;
SELECT * FROM temp_table_columns_filled;
```

# Generate time series
```
DROP TABLE IF EXISTS temp_dates;
CREATE TEMPORARY TABLE temp_dates AS
SELECT
  (date_trunc('day', dd):: date) AS report_date_from,
  (date_trunc('day', dd):: date + INTERVAL '15 days')::DATE AS report_date_to
FROM generate_series( '2021-01-01'::timestamp, '2021-06-01'::timestamp, '15 days'::interval) dd;


DROP TABLE IF EXISTS temp_report_names;
CREATE TEMPORARY TABLE temp_report_names AS
SELECT unnest('{report_name_1,report_name_2}'::TEXT[]) AS report_name;

DROP TABLE IF EXISTS temp_branches;
CREATE TEMPORARY TABLE temp_branches AS
SELECT unnest('{XXBIG,XXBIG}'::TEXT[]) AS report_org;

SELECT 'node index.js ' || report_name || ' ' || report_org || ' ' || report_date_from || ' ' || report_date_to  FROM temp_dates t1
CROSS JOIN temp_report_names t2
CROSS JOIN temp_branches;
```

# Compare data mart column filled %
```
-------------------------------------------------------------------------------------------------------------------------------------------------------
DO LANGUAGE plpgsql
$sql$
  DECLARE
    v_column_array TEXT[];
    v_each_column TEXT;
    v_table_name TEXT := 'TABLE_NAME';
    v_table_schema TEXT := 'SCHEMA_NAME';
    v_final_query TEXT;
    v_final_query_end TEXT;
    v_individual_query TEXT;
    v_total_result NUMERIC;
    v_column_result INTEGER;

  BEGIN
    DROP TABLE IF EXISTS temp_table_columns_filled;
    CREATE TEMPORARY TABLE temp_table_columns_filled ( t_column TEXT, t_filled_percent DECIMAL(5,1) );

    v_final_query:= $$ SELECT count(*) AS total $$;
    v_final_query_end:= $$ FROM $$ || v_table_schema || $$.$$ || v_table_name || $$; $$;

    SELECT array_agg(column_name)
    INTO v_column_array
    FROM information_schema.columns WHERE table_name = v_table_name AND table_schema = v_table_schema;

    FOREACH v_each_column IN ARRAY v_column_array
      LOOP
        v_individual_query := '
    ,count(*) FILTER (WHERE ' || v_each_column || ' IS NOT NULL) AS ' || v_each_column || ' ';
        v_final_query := v_final_query || v_individual_query;
      END LOOP;

    EXECUTE $$
  DROP TABLE IF EXISTS temp_result;
  CREATE TEMPORARY TABLE temp_result AS
  $$ || v_final_query || v_final_query_end || $$
  $$;

    EXECUTE $$ SELECT total FROM temp_result; $$ INTO v_total_result;

    FOREACH v_each_column IN ARRAY v_column_array
      LOOP
        EXECUTE $$ SELECT $$ || v_each_column || $$ FROM temp_result; $$ INTO v_column_result;
        RAISE NOTICE '% : %', v_each_column, (100 / v_total_result * v_column_result)::DECIMAL(5,1)::TEXT || '%';
        INSERT INTO temp_table_columns_filled(t_column, t_filled_percent) VALUES (v_each_column, (100 / v_total_result * v_column_result)::DECIMAL(5,1));
      END LOOP;

  END
$sql$;
DROP TABLE IF EXISTS temp_table_columns_filled_1;
CREATE TEMPORARY TABLE temp_table_columns_filled_1 AS
SELECT * FROM temp_table_columns_filled;
-------------------------------------------------------------------------------------------------------------------------------------------------------
DO LANGUAGE plpgsql
$sql$
  DECLARE
    v_column_array TEXT[];
    v_each_column TEXT;
    v_table_name TEXT := 'TABLE_NAME';
    v_table_schema TEXT := 'SCHEMA_NAME';
    v_final_query TEXT;
    v_final_query_end TEXT;
    v_individual_query TEXT;
    v_total_result NUMERIC;
    v_column_result INTEGER;

  BEGIN
    DROP TABLE IF EXISTS temp_table_columns_filled;
    CREATE TEMPORARY TABLE temp_table_columns_filled ( t_column TEXT, t_filled_percent DECIMAL(5,1) );

    v_final_query:= $$ SELECT count(*) AS total $$;
    v_final_query_end:= $$ FROM $$ || v_table_schema || $$.$$ || v_table_name || $$; $$;

    SELECT array_agg(column_name)
    INTO v_column_array
    FROM information_schema.columns WHERE table_name = v_table_name AND table_schema = v_table_schema;

    FOREACH v_each_column IN ARRAY v_column_array
      LOOP
        v_individual_query := '
    ,count(*) FILTER (WHERE ' || v_each_column || ' IS NOT NULL) AS ' || v_each_column || ' ';
        v_final_query := v_final_query || v_individual_query;
      END LOOP;

    EXECUTE $$
  DROP TABLE IF EXISTS temp_result;
  CREATE TEMPORARY TABLE temp_result AS
  $$ || v_final_query || v_final_query_end || $$
  $$;

    EXECUTE $$ SELECT total FROM temp_result; $$ INTO v_total_result;

    FOREACH v_each_column IN ARRAY v_column_array
      LOOP
        EXECUTE $$ SELECT $$ || v_each_column || $$ FROM temp_result; $$ INTO v_column_result;
        RAISE NOTICE '% : %', v_each_column, (100 / v_total_result * v_column_result)::DECIMAL(5,1)::TEXT || '%';
        INSERT INTO temp_table_columns_filled(t_column, t_filled_percent) VALUES (v_each_column, (100 / v_total_result * v_column_result)::DECIMAL(5,1));
      END LOOP;

  END
$sql$;
DROP TABLE IF EXISTS temp_table_columns_filled_2;
CREATE TEMPORARY TABLE temp_table_columns_filled_2 AS
SELECT * FROM temp_table_columns_filled;
-------------------------------------------------------------------------------------------------------------------------------------------------------
SELECT t1.t_column, t1.t_filled_percent, t2.t_filled_percent, t1.t_filled_percent - t2.t_filled_percent AS diff
FROM temp_table_columns_filled_1 t1 JOIN temp_table_columns_filled_2 t2 ON t1.t_column = t2.t_column
WHERE t1.t_filled_percent - t2.t_filled_percent > 1 OR t1.t_filled_percent - t2.t_filled_percent < -1
ORDER BY t1.t_filled_percent - t2.t_filled_percent;
```

# Analyze multiple tables with a loop
```
DO $$
  DECLARE
    tab RECORD;
  BEGIN
    for tab in (select t.relname::varchar AS table_name, n.nspname::varchar AS schema_name
                FROM pg_class t
                  JOIN pg_namespace n ON n.oid = t.relnamespace
                WHERE t.relkind = 'r' and n.nspname::varchar = 'SCHEMA' AND t.relname::varchar ILIKE '%TABLE_NAME%'
                order by 1)
      LOOP
        RAISE NOTICE 'ANALYZE %.%', tab.schema_name, tab.table_name;
        EXECUTE format('ANALYZE %I.%I', tab.schema_name, tab.table_name);
      end loop;
  end
$$;
```

# Check table counts after analysis
```
SELECT schemaname,relname,n_live_tup
FROM pg_stat_user_tables
WHERE schemaname = 'SCHEMA'
  AND relname ILIKE '%TABLE_NAME%'
ORDER BY n_live_tup DESC;
```

# Hour by hour received apps in stage:
```
SELECT
  received_timestamp::DATE,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '00' ) AS t0,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '01' ) AS t1,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '02' ) AS t2,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '03' ) AS t3,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '04' ) AS t4,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '05' ) AS t5,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '06' ) AS t6,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '07' ) AS t7,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '08' ) AS t8,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '09' ) AS t9,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '10' ) AS t10,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '11' ) AS t11,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '12' ) AS t12,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '13' ) AS t13,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '14' ) AS t14,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '15' ) AS t15,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '16' ) AS t16,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '17' ) AS t17,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '18' ) AS t18,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '19' ) AS t19,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '20' ) AS t20,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '21' ) AS t21,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '22' ) AS t22,
  count(*) FILTER ( WHERE TO_CHAR(received_timestamp::TIMESTAMP,'HH24') = '23' ) AS t23
FROM [STAGE_TABLE]
WHERE received_timestamp::DATE >= '2021-09-01'
GROUP BY received_timestamp::DATE
ORDER BY received_timestamp::DATE DESC;
```
