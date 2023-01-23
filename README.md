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
  -v --views Copy views
  -V --view-filter View filter
  -b --blobs Copy blobs (RAW is treated as BLOB) 
  -c --clobs Copy clobs
  -x --xml Copy XML (XMLTYPE is treated as text)
  -f --table-filter Filter tables by name
  -r --rows Max number of rows
  -I --indices Copy indices
  -F --fks Copy foreign keys
  -P --pks Copy primary keys
  -A Copy indices, fks, and pks (same as -PFI)

  LONGs cannot be retrieved, so they are always set to null.

  Examples: 
  
    # Copy 100 rows of table TEST from DATA schema
    ora2sqlite -s server:1521/service -u data -p pass -f "table_name in ('TEST')" -r 100

    # Copy all tables of DATA with 100 rows, primary keys, foreign keys, indices, clobs, blobs, and XMLType
    ora2sqlite -s server:1521/service -u data -p pass -r 100 -A -cbx

    # Select some views
    ora2sqlite -s server:1521/service -u data -p pass -v -V "view_name like '%HR%'"
```

TODO:

- ~~Handle BLOBS, CLOBS, etc~~
- Test dates and timestamps
- Verify BFILEs (replace by filename)
- ~~Print some indication of progress~~
- ~~Optimise inserts~~
- ~~Filter out views and give option to include them (probably as tables, not views - also requires new filter)~~
- ~~Add examples to the documentation~~