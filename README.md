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
