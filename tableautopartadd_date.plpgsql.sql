/*
Postgres Auto-Partition Creator
Ed Snajder
This function is required when a new partition is needed for inserted data. When the appropriate partition 
is not found for a given date, an exception will be raised in the trigger which calls this function, creates the 
new partition, and then re-attempts the insert. This function adds all check constraints for the appropriate
range so the tables continue benefitting from constraint exclusion. 
DROP FUNCTION tableautopartadd_date(VARCHAR,VARCHAR,VARCHAR,TIMESTAMP)
*/
CREATE OR REPLACE FUNCTION tableautopartadd_date(tblname VARCHAR,colname VARCHAR, partitioninterval VARCHAR, 
partdate TIMESTAMP WITHOUT TIME ZONE) RETURNS VARCHAR
AS $$
DECLARE copypart VARCHAR;
DECLARE highdate TIMESTAMP WITHOUT TIME ZONE;
DECLARE highdatepart TIMESTAMP WITHOUT TIME ZONE;
DECLARE lowdate TIMESTAMP WITHOUT TIME ZONE;
BEGIN
RAISE WARNING 'Partition Creation Requested';
copypart:=(SELECT trim(trailing '_part' FROM tblname) || '_p_' || lower(partitioninterval));
IF partitioninterval = 'YEAR' THEN
   EXECUTE 'CREATE TABLE ' || tblname || '_y' || EXTRACT(YEAR FROM partdate) || '(LIKE ' || copypart || ' INCLUDING ALL) INHERITS ('||tblname||');';
   lowdate:=(SELECT date_trunc('YEAR',partdate));
   highdate:=lowdate+interval '1 year';
   highdatepart:=(SELECT date_trunc('YEAR',highdate));
   EXECUTE 'ALTER TABLE ' || tblname || '_y' || EXTRACT(YEAR FROM partdate) ||  ' ADD CHECK (' || colname || '>= ' || quote_literal(lowdate) || ' AND ' || colname || '< ' || quote_literal(highdate) || ');';
ELSEIF partitioninterval = 'QUARTER' THEN
   lowdate:=(SELECT date_trunc('QUARTER',partdate));
   highdate:= lowdate + interval '3 months';
   highdatepart:=(SELECT date_trunc('QUARTER',highdate));
   RAISE NOTICE 'creating q part for table % for %',copypart,partdate;
   EXECUTE 'CREATE TABLE ' || tblname || '_y' || EXTRACT(YEAR FROM partdate) || 'q' || EXTRACT(QUARTER FROM partdate) || '(LIKE ' || copypart || ' INCLUDING ALL) INHERITS ('||tblname||');';
   EXECUTE 'ALTER TABLE ' || tblname || '_y' || EXTRACT(YEAR FROM partdate) || 'q' || EXTRACT(QUARTER FROM partdate) || ' ADD CHECK (' || colname || '>= ' || quote_literal(lowdate) || ' AND ' || colname || '< ' || quote_literal(highdate) || ');';
ELSEIF partitioninterval = 'MONTH' THEN
   lowdate:=(SELECT date_trunc('MONTH',partdate));
   highdate:=partdate+interval '1 month';
   highdatepart:=(SELECT date_trunc('MONTH',highdate));
   EXECUTE 'CREATE TABLE ' || tblname || '_y' || EXTRACT(YEAR FROM partdate) || 'm' || EXTRACT(MONTH FROM partdate) || '(LIKE ' || copypart || ' INCLUDING ALL) INHERITS ('||tblname||');';
   EXECUTE 'ALTER TABLE ' || tblname || '_y' || EXTRACT(YEAR FROM partdate) || 'm' || EXTRACT(QUARTER FROM partdate) || ' ADD CHECK (' || colname || '>= ' || quote_literal(lowdate) || ' AND ' || colname || '< ' || quote_literal(highdate) || ');';
ELSE
RAISE WARNING 'Auto-Partition: New % partition could not be created, partition interval was not understood', tblname;
END IF;
RAISE WARNING 'Auto-Partition: New % partition added', tblname;
RETURN tblname;
END;
$$
LANGUAGE plpgsql;