/*
Postgres Automatic Partition Creator
Ed Snajder
This function is called by function tableautopartcreatetbl(name,colname,partitioninterval,migratedata)
It creates the function called by the trigger which will manage the partition inserts. The function is designed to 
be table specific so that partitions on multiple tables can be created. The function generated includes
a call to tableautopartadd_date(tblname,colname,partitioninterval,partdate), which adds new partitions as needed.
This is part of the date-based auto-partition function, which works on partitioning DATE and TIMESTAMP columns.

Test Call
       SELECT tableautopart_triggercreator_date('fact_table','day_ts','QUARTER');
*/
CREATE OR REPLACE FUNCTION tableautopart_triggercreator_date(tblname VARCHAR, colname VARCHAR, partitioninterval VARCHAR)
RETURNS VARCHAR AS
$$
DECLARE triggername VARCHAR;
DECLARE triggerdef VARCHAR;
BEGIN
--Create function for managing INSERTS on trigger. 
triggername:=tblname || LOWER(partitioninterval) || '_ins_tr_' || LOWER(colname);
triggerdef:=  'CREATE FUNCTION ' || triggername || E'() RETURNS TRIGGER AS \$\$\n';
triggerdef:= triggerdef || E'DECLARE newdate TIMESTAMP WITHOUT TIME ZONE;\n';
triggerdef:= triggerdef || E'DECLARE inserttbl VARCHAR;\n';
triggerdef:= triggerdef || E'BEGIN\n';
triggerdef:= triggerdef || E'newdate:=NEW.' || colname || E';\ninserttbl:=';
IF partitioninterval = 'YEAR' THEN triggerdef:= triggerdef || quote_literal(tblname) || ' || ' || quote_literal('_y') || E' || EXTRACT(YEAR from newdate);\n';
ELSEIF partitioninterval =  'QUARTER' THEN triggerdef:= triggerdef || quote_literal(tblname) || ' || ' || quote_literal('_y') || E' || EXTRACT(YEAR from newdate) || ' || quote_literal('q') || E' || EXTRACT(QUARTER from newdate);\n';
ELSEIF partitioninterval =  'MONTH' THEN triggerdef:= triggerdef || quote_literal(tblname) || ' || ' || quote_literal('_y') || E' || EXTRACT(YEAR from newdate) || ' || quote_literal('m') || E' || EXTRACT(MONTH from newdate);\n';
END IF;
triggerdef:= triggerdef || E'EXECUTE ' || quote_literal('INSERT INTO ') || '|| inserttbl || ' || quote_literal(' SELECT ($1).*') || ' USING NEW' || ';';
triggerdef:= triggerdef || E'\nRETURN NULL;\n';
triggerdef:= triggerdef || E'EXCEPTION WHEN undefined_table THEN\nEXECUTE ' || quote_literal('SELECT tableautopartadd_date('|| quote_literal(tblname) ||','|| quote_literal(colname) || ',' ||quote_literal(partitioninterval) || ',$1.' || colname || E')') || E'USING NEW;\n';
triggerdef:= triggerdef || E'EXECUTE ' || quote_literal('INSERT INTO ') || '|| inserttbl || ' || quote_literal(' SELECT ($1).*') || ' USING NEW' || ';';
triggerdef:= triggerdef || E'\nRETURN NULL;\n';
triggerdef:= triggerdef || E'END;\n';
triggerdef:= triggerdef || E'\$\$\n';
triggerdef:= triggerdef || E'LANGUAGE' || quote_literal('plpgsql');
EXECUTE (triggerdef);
RETURN triggername;
END;
$$
LANGUAGE plpgsql;