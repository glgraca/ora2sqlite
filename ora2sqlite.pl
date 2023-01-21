#!/usr/bin/perl
use Data::Dumper;
use DBI;
use DBD::Oracle;
use DBD::SQLite;
use Getopt::Long;
use open ':std', ':encoding(utf8)';
use utf8;
use strict;

my $banner=<<EOD;
Use: ora2sqlite -s oracle -u username -p password [-d sqlite] [-b] [-c] 

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

  Example: ora2sqlite -s server:1521/service -u data -p pass -d data.db -f "table_name in ('TEST')" -r 100
EOD

my ($oracle_database, $oracle_username, $oracle_password);
my ($sqlite_filename, $copy_blobs, $copy_clobs);
my $table_name_filter;
my $max_rows;
my ($copy_indices, $copy_foreign_keys, $copy_primary_keys, $copy_all_constraints);

my $options={};

GetOptions(
  's|source=s'=>\$oracle_database,
  'u|username=s'=>\$oracle_username,
  'p|password=s'=>\$oracle_password,
  'd|destination=s'=>\$sqlite_filename,
  'b|blobs'=>\$copy_blobs,
  'c|clobs'=>\$copy_clobs,
  'f|filter=s'=>\$table_name_filter,
  'r|rows=s'=>\$max_rows,
  'I|indices'=>\$copy_indices,
  'F|fks'=>\$copy_foreign_keys,
  'P|pks'=>\$copy_primary_keys,
  'A'=>\$copy_all_constraints
) or die $banner;

die $banner unless defined $oracle_database && defined $oracle_username && defined $oracle_password;

$sqlite_filename="${oracle_username}.db" if !defined $sqlite_filename;

$copy_indices=1, $copy_foreign_keys=1, $copy_primary_keys=1 if $copy_all_constraints;

unlink $sqlite_filename;

my %datatype_map=(
    #Strings
    'CHAR'=>'TEXT',
    'NCHAR'=>'TEXT',
    'VARCHAR2'=>'TEXT',
    'NVARCHAR2'=>'TEXT',
    #NUMERIC
    'NUMBER'=>'REAL',
    'INTEGER'=>'INTEGER',
    #DATES
    'DATE'=>'DATETIME',
    'TIMESTAMP'=>'DATETIME',
    #BLOBS
    'BLOB'=>'BLOB',
    'CLOB'=>'TEXT',
    'LONG'=>'TEXT',
    #OTHER
    'XMLTYPE'=>'TEXT',
    #UNKNOWN
    'UNKOWN'=>'TEXT',
);

my $oracle=DBI->connect("dbi:Oracle://$oracle_database", $oracle_username, $oracle_password, {ReadOnly=>1, LongReadLen=>100*1024*1024});
$oracle->do(q(alter session set nls_timestamp_format = 'YYYY-MM-DD"T"HH24:MI:SS.ff3"Z"'));
$oracle->do(q(alter session set nls_date_format = 'YYYY-MM-DD"T"HH24:MI:SS'));

my $sqlite=DBI->connect("dbi:SQLite:dbname=$sqlite_filename",'','');
$sqlite->{sqlite_unicode} = 1;

my $tables=get_oracle_tables($oracle, $table_name_filter);
create_sqlite_tables($sqlite, $tables);
copy_data($oracle, $sqlite, $tables, $max_rows);
create_sqlite_indices($oracle, $sqlite, $table_name_filter) if $copy_indices;

# { table => [ { name=>column_name, type=>column_type } ]... }
sub get_oracle_tables {
  my $oracle=shift;
  my $filter=shift;
  my $tables={};

  $filter="and $filter" if $filter;

  my $query=qq(
    select lower(table_name), 
       lower(column_name),
       case 
         when data_type like 'TIMESTAMP%' then 'DATETIME'
         when data_type like 'DATE' then 'DATETIME'
         when data_type='NUMBER' and data_scale=0 then 'INTEGER'
         when data_type in ('FLOAT', 'NUMBER') then 'REAL'
         when data_type in ('CHAR', 'NCHAR', 'VARCHAR2', 'NVARCHAR2') then 'TEXT'
         when data_type='BLOB' then 'BLOB'
         when data_type in ('CLOB', 'LONG', 'XMLTYPE') then 'TEXT'
         else 'TEXT'
         end data_type,
         nullable,
         data_type oracle_data_type
    from user_tab_cols
    where VIRTUAL_COLUMN='NO' 
    $filter
    order by table_name, column_id
  );

  my $st=$oracle->prepare($query);
  $st->execute();
  while(my @row=$st->fetchrow_array()) {
      my ($table_name, $column_name, $column_type, $nullable, $oracle_type)=@row;
      push @{$tables->{$table_name}}, {'name'=>$column_name, 'type'=>$column_type, 'nullable'=>$nullable, 'oracle_type'=>$oracle_type};
  };

  return $tables;
};

sub create_sqlite_tables {
  my $sqlite=shift;
  my $tables=shift;

  for my $table_name (keys %$tables) {
    my $columns=$tables->{$table_name};

    my $create_cmd="create table $table_name (";
    $create_cmd.=join ',', map { $_->{name}.' '.$_->{type}.' '.($_->{nullable} eq 'Y'?'':'NOT NULL') } @$columns;
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
    $create_cmd.=')';
    $sqlite->do($create_cmd);
  }
}

sub copy_data {
  my $oracle=shift;
  my $sqlite=shift;
  my $tables=shift;
  my $max_rows=shift||'';

  $max_rows="where rownum<$max_rows+1" if $max_rows;

  for my $table_name (keys %$tables) {
    my $columns=$tables->{$table_name};
    my $st=$oracle->prepare("select * from $table_name $max_rows");
    $st->execute();
    my $insert_cmd="insert into $table_name values (". (join ',', map { '?' } @$columns).')';
    my $insert_st=$sqlite->prepare($insert_cmd);
    while(my @row=$st->fetchrow_array()) {
       $insert_st->execute(@row);
    } 
  }
}

sub get_primary_key {
  my $oracle=shift;
  my $table_name=shift;

  my $query=qq(
    select listagg(cols.column_name,',')
    from user_constraints cons
         inner join user_cons_columns cols on cons.constraint_name = cols.constraint_name and cons.owner = cols.owner
    where cols.table_name = upper(?)
          and cons.constraint_type = 'P'
    order by cols.position
  );

  my $pk=$oracle->selectrow_array($query, undef, $table_name);
  return $pk;
}

sub create_sqlite_indices {
  my $oracle=shift;
  my $sqlite=shift;
  my $filter=shift;

  $filter="and $filter" if $filter;
  
  my $query=qq(
    select index_name, table_name, uniqueness, listagg(c.column_name,',') within group (order by column_position)
    from user_indexes i
         natural join user_ind_columns c 
    where index_type='NORMAL' $filter
    group by index_name, table_name, uniqueness
  );

  my $st=$oracle->prepare($query);
  $st->execute();
  while(my @row=$st->fetchrow_array()) {
    my ($index_name, $table_name, $uniqueness, $column_names)=@row;
    $uniqueness='' if $uniqueness ne 'UNIQUE';
    my $create_cmd="create $uniqueness index $index_name on $table_name ($column_names)";

    $sqlite->do($create_cmd);
  };
}

sub get_foreign_keys {
  my $oracle=shift;
  my $table_name=shift;

  my $query=qq(
    select listagg(distinct a.column_name,',') within group (order by a.position), t.table_name, listagg(distinct b.column_name,',') within group (order by b.position)
    from user_cons_columns a
         inner join user_constraints f on a.owner = f.owner and a.constraint_name = f.constraint_name
         inner join user_constraints t on f.r_owner = t.owner and f.r_constraint_name = t.constraint_name
         inner join user_cons_columns b on  b.owner = t.owner and b.constraint_name = t.constraint_name
    where f.constraint_type = 'R' and a.table_name=upper(?)
    group by a.table_name, f.constraint_name, t.constraint_name, t.table_name
  );

  my $st=$oracle->prepare($query);
  $st->bind_param(1, $table_name);
  $st->execute();
  my $fks=[];
  while(my @row=$st->fetchrow_array()) {
    my ($columns, $referenced_table, $referenced_columns)=@row;
    push @$fks, [$columns, $referenced_table, $referenced_columns];
  }
  return $fks;
}


