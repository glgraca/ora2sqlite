#!/usr/bin/perl
use Data::Dumper;
use DBI qw(:sql_types);
use DBD::Oracle;
use DBD::SQLite;
use Getopt::Long qw(:config no_ignore_case bundling);
use open ':std', ':encoding(utf8)';
use utf8;
use strict;

$Data::Dumper::Terse = 1;

{
  my $banner=<<EOD;
  Use: ora2sqlite -s oracle -u username -p password [-d sqlite] [-b] [-c] 

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
    -U --uks Copy unique keys
    -A Copy indices, fks, pks, and uks (same as -PFIK)

    LONGs and BFILEs cannot be retrieved, so they are always set to null.

    Example: ora2sqlite -s server:1521/service -u data -p pass -d data.db -f "table_name in ('TEST')" -r 100
EOD

  my ($oracle_database, $oracle_username, $oracle_password, $sqlite_filename, 
      $copy_views, $view_name_filter, $table_name_filter, $max_rows,
      $copy_foreign_keys, $copy_primary_keys, $copy_unique_keys, $copy_all_constraints,
      $copy_blobs, $copy_clobs, $copy_xml, $copy_indices);

  GetOptions(
    's|source=s'=>\$oracle_database,
    'u|username=s'=>\$oracle_username,
    'p|password=s'=>\$oracle_password,
    'd|destination=s'=>\$sqlite_filename,
    'v|views'=>\$copy_views,
    'V|view-filter=s'=>\$view_name_filter,
    'b|blobs'=>\$copy_blobs,
    'c|clobs'=>\$copy_clobs,
    'x|xml'=>\$copy_xml,
    'f|table-filter=s'=>\$table_name_filter,
    'r|rows=s'=>\$max_rows,
    'I|indices'=>\$copy_indices,
    'F|fks'=>\$copy_foreign_keys,
    'P|pks'=>\$copy_primary_keys,
    'U|uks'=>\$copy_unique_keys,
    'A'=>\$copy_all_constraints
  ) or die $banner;

  die $banner unless defined $oracle_database && defined $oracle_username && defined $oracle_password;

  # default filename is name of oracle schema
  $sqlite_filename="${oracle_username}.db" if !defined $sqlite_filename;

  # Copy all constraints if -A
  $copy_indices=1, $copy_foreign_keys=1, $copy_primary_keys=1, $copy_unique_keys=1 if $copy_all_constraints;

  $view_name_filter='1=2' if !$copy_views;

  unlink $sqlite_filename;

  my $oracle=DBI->connect("dbi:Oracle://$oracle_database", $oracle_username, $oracle_password, {
    ReadOnly=>1, LongReadLen=>1024*1024*1024, LongTruncOk=>1, PrintWarn=>1, RaiseError=>1
  });
  $oracle->do(q(alter session set nls_timestamp_tz_format = 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'));
  $oracle->do(q(alter session set nls_timestamp_format = 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'));
  $oracle->do(q(alter session set nls_date_format = 'YYYY-MM-DD"T"HH24:MI:SS'));

  my $sqlite=DBI->connect("dbi:SQLite:dbname=$sqlite_filename",'','', {PrintWarn=>1, RaiseError=>1});
  $sqlite->do('pragma journal_mode = MEMORY');
  $sqlite->do('pragma synchronous = off');
  $sqlite->do('pragma locking_mode = exclusive');
  $sqlite->do('pragma page_size = 4096');
  $sqlite->{AutoCommit} = 0;
  $sqlite->{sqlite_unicode} = 1;

  my $tables=get_oracle_tables($oracle, $table_name_filter, $view_name_filter);
  create_sqlite_tables($oracle, $sqlite, $tables, $copy_primary_keys, $copy_foreign_keys, $copy_unique_keys);
  copy_data($oracle, $sqlite, $tables, $copy_blobs, $copy_clobs, $copy_xml, $max_rows);
  create_sqlite_indices($oracle, $sqlite, $table_name_filter, $view_name_filter) if $copy_indices;
}

sub sqlite_table_name {
  my $table_name=shift;
  $table_name=~s/\$/_/g;
  return "[$table_name]";
}

# { table => [ { id=>123, name=>column_name, type=>data_type, nullable=>'Y|N', oracle_type=>oracle_data_type } ]... }
sub get_oracle_tables {
  my ($oracle, $filter, $view_filter)=@_;
  my $tables={};

  $filter="where $filter" if $filter;
  $view_filter="where $view_filter" if $view_filter;

  my $query=qq(
    select lower(table_name), 
      row_number() over (partition by table_name order by column_id) column_id,
      lower(column_name),
      case 
        when data_type like 'TIMESTAMP%' then 'DATETIME'
        when data_type like 'DATE' then 'DATETIME'
        when data_type='NUMBER' and data_scale=0 then 'INTEGER'
        when data_type in ('FLOAT', 'NUMBER') then 'REAL'
        when data_type in ('CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') then 'TEXT'
        when data_type in ('RAW', 'BLOB') then 'BLOB'
        when data_type in ('CLOB', 'LONG', 'LONG RAW') then 'TEXT'
        when data_type='XMLTYPE' then 'TEXT'
        else 'TEXT'
        end data_type,
        nullable,
        data_type oracle_data_type
    from user_tab_cols       
    where hidden_column='NO'
          and column_name not like 'SYS_NC%'
          and (table_name in (select table_name from user_tables $filter) 
              or table_name in (select view_name from user_views $view_filter))
    order by table_name, column_id
  );

  my $st=$oracle->prepare($query);
  $st->execute();
  while(my @row=$st->fetchrow_array()) {
    my ($table_name, $column_id, $column_name, $column_type, $nullable, $oracle_type)=@row;
    push @{$tables->{$table_name}}, {
      'id'=>$column_id,
      'name'=>"$column_name",
      'type'=>$column_type,
      'nullable'=>$nullable,
      'oracle_type'=>$oracle_type};
  };

  return $tables;
};

sub create_sqlite_tables {
  my ($oracle, $sqlite, $tables, $copy_primary_keys, $copy_foreign_keys, $copy_unique_keys)=@_;

  print "Creating tables\n";

  for my $table_name (keys %$tables) {
    my $columns=$tables->{$table_name};
    my $sqlite_table_name=sqlite_table_name($table_name);

    my $create_cmd="create table $sqlite_table_name (";
    $create_cmd.=join ',', map { '['.$_->{name}.'] '.$_->{type}.' '.($_->{nullable} eq 'Y'?'':'NOT NULL') } @$columns;
    if($copy_primary_keys) {
      my $pk=get_primary_key($oracle, $table_name);
      $create_cmd.=", primary key ($pk)" if $pk;
    }
    if($copy_foreign_keys) {
      my $fks=get_foreign_keys($oracle, $table_name);
      for my $fk (@$fks) {
        my ($columns, $referenced_table, $referenced_columns)=@$fk;
        $create_cmd.=", foreign key ($columns) references $referenced_table($referenced_columns)";
      }
    }
    if($copy_unique_keys) {
      my $uks=get_unique_keys($oracle, $table_name);
      for my $uk (@$uks) {
        $create_cmd.=", unique($uk)";
      }
    }
    $create_cmd.=')';
    print "  $table_name\n";
    $sqlite->do($create_cmd);
  }
}

sub copy_data {
  my ($oracle, $sqlite, $tables, $copy_blobs, $copy_clobs, $copy_xml, $max_rows)=@_;
  
  print "Copying data\n";

  $max_rows="where rownum<$max_rows+1" if $max_rows;

  my $table_count=scalar keys %$tables;
  my $current_table=1;

  for my $table_name (keys %$tables) {
    my $sqlite_table_name=sqlite_table_name($table_name);
    $oracle->{LongReadLen}=256*1024*1024;
    print "  $table_name ($current_table/$table_count)\n";
    my $columns=$tables->{$table_name};
    my $select_columns=join ',', map {
      if(!$copy_blobs and $_->{type} eq 'BLOB') {
        'null'
      } elsif(!$copy_clobs and $_->{oracle_type} eq 'CLOB') {
        'null'
      } elsif($_->{oracle_type} eq 'XMLTYPE') {
        if($copy_xml) {
         '('.$_->{name}.').getClobVal()'
        } else {
          'null'
        }
      } elsif($_->{oracle_type} eq 'BFILE') {
        'null'
      } elsif($_->{oracle_type} =~ /^LONG/) {
        $oracle->{LongReadLen}=10*1024*1024;
      } else {
        if($_->{oracle_type} eq 'RAW') {
          'to_blob('.$_->{name}.')';
          $_->{name}
        } else {
          $_->{name}
        }
      }
    } @$columns;
    my $select_cmd="select $select_columns from $table_name $max_rows";
    my $st=$oracle->prepare($select_cmd);
    $st->execute();
    my $insert_cmd="insert into $sqlite_table_name values (". (join ',', map { '?' } @$columns).')';
    my $insert_st=$sqlite->prepare($insert_cmd);
    while(my @row=$st->fetchrow_array()) {
      for my $column (@$columns) {
        my $id=$column->{id};
        # BLOB
        if($column->{type} eq 'BLOB') {
          if($copy_blobs) {
            $insert_st->bind_param($id, $row[$id-1], SQL_BLOB);
          } else {
            $insert_st->bind_param($id, undef);
          }
        # RAW and CLOB
        } elsif($column->{oracle_type} eq 'RAW' || $column->{oracle_type} eq 'CLOB') {
          if($copy_clobs) {
            $insert_st->bind_param($id, $row[$id-1]);
          } else {
            $insert_st->bind_param($id, undef);
          }
        # All other types
        } else {
          my $v=$row[$id-1];
          # This is for custom types
          if(ref($v) eq 'ARRAY') {
            $v=Dumper($v);
          }
          $insert_st->bind_param($id, $v);
        }
      }
      $insert_st->execute();
    } 
    $st->finish();
    $sqlite->commit();
    $current_table++;
  }
}

sub get_primary_key {
  my ($oracle, $table_name)=@_;

  my $query=qq(
    select listagg('['||cols.column_name||']',',') within group (order by cols.position)
    from user_constraints cons
         inner join user_cons_columns cols on cons.constraint_name = cols.constraint_name and cons.owner = cols.owner
    where cols.table_name = upper(?)
          and cons.constraint_type = 'P'
    order by cols.position
  );

  my $pk=$oracle->selectrow_array($query, undef, sqlite_table_name($table_name));
  return $pk;
}

# [['col1, col2', 'ref_table', 'ref_col1, 'ref_col2'], [...]]
sub get_foreign_keys {
  my ($oracle, $table_name)=@_;

  my $query=qq(
    select distinct a.cols, t.table_name, b.cols, f.constraint_name
    from (select owner, table_name, constraint_name, listagg('['||column_name||']', ',') within group (order by position) over (partition by owner, constraint_name) cols from user_cons_columns) a
         inner join user_constraints f on a.owner = f.owner and a.constraint_name = f.constraint_name
         inner join user_constraints t on f.r_owner = t.owner and f.r_constraint_name = t.constraint_name
         inner join (select owner, constraint_name, listagg('['||column_name||']', ',') within group (order by position) over (partition by owner, constraint_name) cols from user_cons_columns) b on  b.owner = t.owner and b.constraint_name = t.constraint_name
    where f.constraint_type = 'R' and a.table_name=upper(?)
  );

  my $st=$oracle->prepare($query);
  $st->bind_param(1, $table_name);
  $st->execute();
  my $fks=[];
  while(my @row=$st->fetchrow_array()) {
    my ($columns, $referenced_table, $referenced_columns)=@row;
    push @$fks, [$columns, sqlite_table_name($referenced_table), $referenced_columns];
  }
  return $fks;
}

# ['col1,col2', 'cols3']
sub get_unique_keys {
  my ($oracle, $table_name)=@_;

  my $query=qq(
    select distinct cols
    from (select owner, constraint_name, table_name, listagg('['||column_name||']',',') within group (order by position) over (partition by table_name, constraint_name) cols 
          from user_cons_columns 
          where column_name not like 'SYS_NC%') 
         natural join user_constraints  
    where  constraint_type='U' and table_name=upper(?)
  );

  my $st=$oracle->prepare($query);
  $st->bind_param(1, $table_name);
  $st->execute();
  my $uks=[];
  while(my @row=$st->fetchrow_array()) {
    push @$uks, $row[0];
  }
  return $uks;
}

sub create_sqlite_indices {
  my ($oracle, $sqlite, $filter)=@_;

  print "Creating indices\n";

  $filter="and $filter" if $filter;
  
  my $query=qq(
    select index_name, table_name, uniqueness, listagg('['||c.column_name||']',',') within group (order by column_position), count(1) over ()
    from user_indexes i
         natural join user_ind_columns c 
    where index_type='NORMAL' $filter
    group by index_name, table_name, uniqueness
  );

  my $st=$oracle->prepare($query);
  $st->execute();
  my $current_index=1;
  while(my @row=$st->fetchrow_array()) {
    my ($index_name, $table_name, $uniqueness, $column_names, $index_count)=@row;
    my $sqlite_table_name=sqlite_table_name($table_name);
    $uniqueness='' if $uniqueness ne 'UNIQUE';
    my $create_cmd="create $uniqueness index $index_name on $sqlite_table_name ($column_names)";

    print "  $index_name on $table_name ($current_index/$index_count)\n";
    $current_index++;
    eval {
      $sqlite->do($create_cmd);
    };
    warn $@ if $@;
  };
}

END {
  print 'Executed in '.(time - $^T)."s\n";
}
