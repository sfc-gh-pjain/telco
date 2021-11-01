use role sysadmin;
use schema demo.telco;
use warehouse lab_s_wh;

create or replace table feature_store as select * from services;

-- one hot encoding
// DBNAME,TBLNAME,SCHNAME,COLNAME
create or replace procedure oneHotEncSQL (DBNAME STRING,SCHNAME STRING,TBLNAME STRING,COLNAME STRING)
  returns string
  language javascript strict
  execute as owner
as
$$
    // Set context
  var tblName = DBNAME + "." + SCHNAME + "." + TBLNAME;
  var msg = "Info: OneHot encoding for column " + COLNAME + " completed";
  
  // list distinct column values
  var listSql = "select distinct " + COLNAME + " from " + tblName + " ;" ;
  var listDistinct = snowflake.execute({ sqlText: listSql });
  var counter = 0 ;
  
  while(listDistinct.next())
  {
      var col = listDistinct.getColumnValue(1);
      var colName = COLNAME + "_" + col.replace(/\((.*?)\)/g,'').replace(/ /g,'');

      //add encoded col
      var addColSql = "alter table " + tblName + " add column " + colName + " number;" ;
      try 
      {
          var addCol = snowflake.execute({ sqlText: addColSql });
          counter += 1;
      }
      catch(err)
      {
          msg += " \n Info: Column '" + colName + "' already exists ";
 
      }

      //update encoded col
      var updColSql = "update " + tblName + " set " + colName + "= (case lower(" + COLNAME + ") when lower('" + col +"') then 1 else 0 end);";

      try 
      {
          var updCol = snowflake.execute({ sqlText: updColSql });
          counter += 1;
      }
      catch(err)
      {
          msg += " \n Info: columns updated: " + counter + " /\n Warning: " + err ;
      }
  }

    return msg;

$$;

-- oneHotEncSQL (DBNAME STRING,SCHNAME STRING,TBLNAME STRING,COLNAME STRING)

CALL oneHotEncSQL('DEMO','TELCO','SERVICE_CP','internetservice');

select internetservice,internetservice_dsl,internetservice_no, internetservice_FiberNULLoptic from service_cp;


-- one hot encoding
// DBNAME,TBLNAME,SCHNAME,COLNAME
create or replace procedure ordinalEncSQL (DBNAME STRING,SCHNAME STRING,TBLNAME STRING,COLNAME STRING)
  returns string
  language javascript strict
  execute as owner
as
$$
  var counter = 0 ;
  var msg = "Ordinal Encoding completed Successfully ";
    // Set context
  var tblName = DBNAME + "." + SCHNAME + "." + TBLNAME;

  // Create new ordinal encoding column
  var colName = COLNAME + "_ordinal";

  //add encoded col
  var addColSql = "alter table " + tblName + " add column " + colName + " number;" ;
  try 
  {
      var addCol = snowflake.execute({ sqlText: addColSql });
  }
  catch(err)
  {
      counter = 0;
      msg += " , Info: Column '" + colName + "' already exists ";
  }

  // list distinct column values
  var listSql = "select distinct "+ COLNAME + " from " + tblName + " ;" ;
  var listDistinct = snowflake.execute({ sqlText: listSql });
  
  while(listDistinct.next())
  {
      var col = listDistinct.getColumnValue(1);
      counter += 1;
      
      //update ordinal encoded col
      var updColSql = "update " + tblName + " set " + colName + " = " + counter + " where " + COLNAME + " = '" + col +"' ;";

      try 
      {
          var updCol = snowflake.execute({ sqlText: updColSql });
      }
      catch(err)
      {
          msg = "Warning: " + updColSql + " : " + err;
      }
  }

    return msg;

$$;



alter table SERVICE_CP drop column if exists internetservice_ordinal;
CALL ordinalEncSQL('DEMO','TELCO','SERVICE_CP','internetservice');
select internetservice,internetservice_ordinal from service_cp;


select internetservice,internetservice_dsl,internetservice_no, internetservice_FiberNULLoptic from service_cp;
