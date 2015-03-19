# pg_autopart_simple
Simple PostgreSQL Auto-Partition Strategy

Version 0.1
This is a set of three functions which will copy a table's structure into partitions, using a DATE or TIMESTAMP data type, on Years, Quarters or Months. 

tableautopartcreate.plpgsql.sql is the main function. This function will take a table, column, interval, and a flag for creating migration functions. The fourth variable is reserved for the next version, which will include the capabiliy of creating copy and rename scripts. 

tableautopart_triggercreator_date.plpgsql.sql is the function which creates a uniquely-named, dynamic trigger function for the parent partitioned table.

Finally, tableautopartadd_date.plpgsql.sql is the function called when the applicable partition is not found. It will create a new partition to accomodate the new values, and then the trigger function will re-attempt the insert. 

TODO
* Create function which scripts out a migration function or script, as well as a table rename function. This will allow for finally replacing the original table with the partitioned one.
* Add functionality for range partitions.
* Improve Logging
