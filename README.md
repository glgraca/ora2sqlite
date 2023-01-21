# Export Oracle schema to SQLite

This is a command-line tool to export Oracle schemas to an SQLite database.

It does not automatically copy indices, foreign keys, or primary keys (use -F, -I, -P, or -A).

You can indicate a filter using an SQL clause in the filter option (-f --filter). 

```sh
Use: ora2sqlite -s oracle -u username -p password 

  -s --source Oracle Database 
  -u --username Oracle schema 
  -p --password Oracle password 
  -d --destination SQLite filename (defaults to the oracle schema name)
  -b --blobs Copy blobs 
  -c --clobs Copy clobs (LONG is treated as CLOB)
  -f --filter Filter tables by name
  -r --rows Max number of rows
  -I --indices Copy indices
  -F --fks Copy foreign keys
  -P --pks Copy primary keys
  -A Copy indices, fks, and pks (same as -PFI)

  Example: ora2sqlite -s server:1521/service -u data -p pass -f "table_name in ('TEST')" -r 100
```

TODO:

- Handle BLOBS, CLOBS, etc
- Test dates and timestamps
- Verify BFILEs (replace by filename)
- Print some indication of progress
- Optimise inserts
- Filter out views and give option to include them (probably as tables, not views - also requires new filter)