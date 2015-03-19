/*
PostgreSQL Auto-Partition Function
Ed Snajder
This function will take a table, examine its data, and create a copy of a partitioned table
on YEAR, QUARTER or MONTH. It will then generate a uniquely-named trigger and trigger function 
(in case you want to partition several tables in the same database). Finally, it will generate
functions to populate the new table.

Variables
tblname is the name of the table to be partitioned
colname is the column to build the partition on. Must be DATE or TIMESTAMP (with or without TIMEZONE)
partitioninterval will be YEAR, QUARTER or MONTH. Default is QUARTER
migratedata is the flag to generate data migration scripts. Default 0

Test runs for function against table named "fact_table" with TIMESTAMP column day_ts
      SELECT tableautopartcreate('fact_table','day_ts','QUARTER',0);
      DROP TABLE fact_table_summary_part CASCADE;
      DROP TABLE fact_table_summary_p_quarter CASCADE;
      INSERT INTO fact_table_part SELECT * FROM fact_table WHERE EXTRACT(YEAR FROM day_ts) = '2010' LIMIT 1000
      INSERT INTO fact_table_part SELECT * FROM fact_table WHERE EXTRACT(YEAR FROM day_ts) = '2012' LIMIT 5000
*/
CREATE OR REPLACE FUNCTION tableautopartcreate(tblname varchar(255),colname varchar(255), 
   partitioninterval varchar(40) DEFAULT 'QUARTER', migratedata INTEGER DEFAULT 0) RETURNS VOID
AS $$
DECLARE mindateint INTEGER;
DECLARE mindatedate TIMESTAMP WITHOUT TIME ZONE;
DECLARE maxdateint INTEGER;
DECLARE maxdatedate TIMESTAMP WITHOUT TIME ZONE;
DECLARE partitions INTEGER;
DECLARE quarters INTEGER;
DECLARE months INTEGER;
DECLARE minyear INTEGER;
DECLARE maxyear INTEGER;
DECLARE tabletemplate VARCHAR;
DECLARE tableparent VARCHAR;
DECLARE triggername VARCHAR;
DECLARE triggerqry VARCHAR;
DECLARE colcheck VARCHAR;
BEGIN
IF (partitioninterval!='QUARTER' AND partitioninterval != 'YEAR' AND partitioninterval !='MONTH') THEN
   RAISE EXCEPTION 'Interval must be YEAR, QUARTER or MONTH';
   END IF;
--Check if using a DATE or TIMESTAMP column
SELECT data_type FROM INFORMATION_SCHEMA.COLUMNS WHERE table_name = tblname AND column_name = colname INTO colcheck;
IF (colcheck !='date' and colcheck!='timestampe with time zone' and colcheck!='timestamp without time zone') THEN
   RAISE EXCEPTION 'This function is for creating date based partitions. Please choose a column with a DATE OR TIMESTAMP data type.';
   END IF;
tabletemplate:=tblname || '_p_' || lower(partitioninterval);
tableparent:=tblname || '_part';
RAISE NOTICE '% % %',tableparent, tblname, tabletemplate;
--Make an empty copy of the table, this will become the parent table to the partitions
EXECUTE 'CREATE TABLE ' ||  tableparent || '(LIKE ' || tblname || ' EXCLUDING ALL)';
--Make an empty copy of the table that will be used to create future partitions
EXECUTE 'CREATE TABLE ' ||  tabletemplate || '(LIKE ' || tblname || ' INCLUDING ALL)' ;
--Add comment to the partiontioned table\column for future reference and identification
EXECUTE 'COMMENT ON COLUMN ' || $1 || '_part.' || colname || ' IS ' || quote_literal(partitioninterval || ' PARTITION ENABLED');
--Determine what partiontioned tables will need to be created
EXECUTE 'SELECT MIN(' || colname || ') FROM ' || tblname INTO mindatedate;
   minyear:=EXTRACT(YEAR FROM mindatedate);
   maxyear:=EXTRACT(YEAR FROM now());
   RAISE NOTICE 'Years % to %',minyear,maxyear;
   CASE partitioninterval
   WHEN 'YEAR' THEN
      FOR partitions IN minyear..maxyear LOOP
            EXECUTE 'CREATE TABLE ' ||  tableparent || '_y' || partitions ||' (LIKE ' || tabletemplate || ' INCLUDING ALL) INHERITS (' || tableparent || ')';
            EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-01-01') || ' AND day_ts< ' 
               || quote_literal((partitions+1)::varchar || '-01-01') || ')';
      END LOOP;
   WHEN 'QUARTER' THEN
      FOR partitions IN minyear..maxyear LOOP
         FOR quarters IN 1..4 LOOP
            EXECUTE 'CREATE TABLE ' ||  tableparent || '_y' || partitions || 'q' || quarters || ' (LIKE ' || tabletemplate || ' INCLUDING ALL) INHERITS (' || tableparent || ')';
            CASE quarters
            WHEN 1 THEN
               EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'q' || quarters || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-01-01') || ' AND day_ts< ' 
               || quote_literal((partitions)::varchar || '-04-01') || ')'; 
            WHEN 2 THEN
               EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'q' || quarters || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-04-01') || ' AND day_ts< ' 
               || quote_literal((partitions)::varchar || '-07-01') || ')'; 
            WHEN 3 THEN
               EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'q' || quarters || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-07-01') || ' AND day_ts< ' 
               || quote_literal((partitions)::varchar || '-10-01') || ')'; 
            WHEN 4 THEN
               EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'q' || quarters || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-10-01') || ' AND day_ts< ' 
               || quote_literal((partitions+1)::varchar || '-01-01') || ')'; 
            END CASE;                  
      END LOOP;
      END LOOP;
   WHEN 'MONTH' THEN
      FOR partitions IN minyear..maxyear LOOP
         FOR months IN 1..12 LOOP
            EXECUTE 'CREATE TABLE ' ||  tableparent || '_y' || partitions || 'm' || months || ' (LIKE ' || tabletemplate || ' INCLUDING ALL) INHERITS (' || tableparent || ')'; 
            IF months < 10 THEN
            EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'm' || months || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-0' || months ||'-01') || ' AND day_ts< ' 
               || quote_literal((partitions+1)::varchar || '-01-01') || ')'; 
            ELSEIF months BETWEEN 10 AND 11 THEN
            EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'm' || months || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-' || months ||'-01') || ' AND day_ts< ' 
               || quote_literal((partitions+1)::varchar || '-01-01') || ')';   
            ELSE           
            EXECUTE 'ALTER TABLE ' || tableparent || '_y' || partitions || 'm' || months || ' ADD CHECK (' || colname || '>=' || quote_literal(partitions || '-12-01') || ' AND day_ts< ' 
               || quote_literal((partitions+1)::varchar || '-01-01') || ')';
            END IF;   
      END LOOP;
      END LOOP;
   END CASE;
      RAISE NOTICE 'Adding trigger function';
      EXECUTE 'SELECT tableautopart_triggercreator_date(' || quote_literal(tblname || '_part') || ',' || quote_literal(colname) || ',' || quote_literal(partitioninterval) || ')' INTO triggername;
      RAISE NOTICE 'Trigger function % Added, creating trigger',triggername ;
      triggerqry:='CREATE TRIGGER ' || triggername || ' BEFORE INSERT ON ' || $1 || '_part FOR EACH ROW EXECUTE PROCEDURE ' || triggername || '()';
      --RAISE NOTICE '%',triggerqry;
      EXECUTE triggerqry;
END;
$$
LANGUAGE plpgsql;