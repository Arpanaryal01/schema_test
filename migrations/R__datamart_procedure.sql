USE SCHEMA SCHEMA_CHANGE_TEST;


create or replace procedure SP_CREATE_TEMPORARY_TABLE()
  returns integer
  language sql
  as
  $$
    declare
      copy_into_temporary_table := '
                    create or replace
                        temporary table "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM" as
                          select
                            DICTIONARY_ITEM_ID,
                            DICTIONARY_ID,
                            DP_STUDY_ID,
                            KEYWORD,
                            TYPE_ID,
                            PARENT_ID,
                            TEXT_ID,
                            DICTIONARY_ITEM_TYPE,
                            IS_Q_LEVEL,
                            ACCESS_CODE,
                            DISPLAY_TYPE,
                            DATA_TYPE_ID,
                            FIRST_WAVE,
                            IS_NEW,
                            MIN,
                            MAX,
                            CCP,
                            EXTERNAL_KEY,
                            PERSISTENT_ID,
                            MEMRI_DEFINITION,
                            SORT,
                            DELIVERABLE_ID,
                            ACTIVE,
                            CRT_TS,
                            CRT_BY,
                            UPD_BY,
                            UPD_TS,
                            LEAF_LEVEL,
                            DICTIONARY_ITEM_SORT,
                            FULL_ARRAY_SLICE,
                            HIERARCHY_TEXT,
                            TO_INSERT,
                            IS_LEAF_NODE,
                            IS_DEFINITION,
                            IS_STANDARD,
                            FULL_LABEL
                          from
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ITEM"';


    begin
      execute immediate :copy_into_temporary_table;

            return object_construct('status_code', '1',
                              'message','Temporary Tables Created'
                             );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
    end;

  $$;

create or replace procedure SP_CREATE_PERSISTENT_ID()
returns varchar
language sql
execute as caller
as
$$
  begin
    insert into "DEV_MDM_DB"."DICTIONARY"."MASTER_LOOKUP"(
      PERSISTENT_ID,HIERARCHY_TEXT,EXTERNAL_KEYWORD,IS_LEAF,IS_DEFINITION,IS_STANDARD,CRT_BY,CRT_TS
    )
    select
      PERSISTENT_ID,
      LOWER(HIERARCHY_TEXT),
      CASE WHEN EXTERNAL_KEY = 'U0' THEN NULL ELSE EXTERNAL_KEY END AS EXTERNAL_KEY,
      IS_LEAF_NODE,
      IS_DEFINITION,
      IS_STANDARD,
      VW_USER_INFO.ID as CRT_BY,
      CURRENT_TIMESTAMP AS CRT_TS
    from
      "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM"
      join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_USER_INFO" on VW_USER_INFO.FIRST_NAME='ETL_USER'
    where
      TO_INSERT = TRUE QUALIFY ROW_NUMBER() OVER (
        PARTITION BY HIERARCHY_TEXT,
        IS_LEAF_NODE
        ORDER BY
          HIERARCHY_TEXT
      ) = 1;



    return object_construct('status_code', 1,
                             'message', 'Persistent ID Generated'
                            );
  exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

  end;
$$;


create or replace procedure SP_DATAMART_VALIDATION()
  returns integer
  language sql
  as
  $$

    declare
      staging_item_count integer ;
      top_level_with_parent_count_insight integer ;
      orphan_items_count_insight integer ;
      answer_item_count_insight integer ;
      persistent_id_ccp_valiadation_insight boolean;

      top_level_with_parent_count_memri integer ;
      orphan_items_count_memri integer ;
      answer_item_count_memri integer ;
      persistent_id_ccp_valiadation_memri boolean;
      

      top_level_parent_cursor_insight cursor for
        select
          count(*)
        from
          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
        where
          TYPE_ID = 5
          and PARENT_ID is not null
          and deliverable_id = 1;

       top_level_parent_cursor_memri cursor for
          select
            count(*)
          from
            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
          where
            type_id = '6'
            and PARENT_ID is not null
            and deliverable_id = 2;


       orphan_item_cursor_insight cursor for
         select
            count(*)
          from
             "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
          where
            TYPE_ID != 5
            and PARENT_ID is null
            and deliverable_id = 1;

        orphan_item_cursor_memri cursor for
            select
              count(*)
            from
             "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
            where
              type_id != '6'
              and PARENT_ID is null
              and deliverable_id = 2;


        staging_item_count_cursor cursor for
              select
                count(*)
              from
                "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".DICTIONARY_ITEM_STG;

         answer_item_count_cursor_insight cursor for
              select
                count(*)
              from
                   "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
              where
                TYPE_ID = 4
                and deliverable_id = 1;


         answer_item_count_cursor_memri cursor for
            select
              count(*)
            from
                "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
            where
              leaf_level = True
              and deliverable_id = 2;
              
              
         persistent_id_ccp_valiation_cursor_insight cursor for
              select 
                case when count(*) = 0 then true else false end insights_persistent_id_ccp_validation 
              from 
                (
                  with orphan_item_insight as(
                    select 
                      * 
                    from 
                       "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
                    where 
                      type_id != 5 
                      and parent_id is null 
                      and deliverable_id = 1
                  ) 
                  select 
                    persistent_id, 
                    count(distinct ccp) as distinct_ccp 
                  from 
                    orphan_item_insight 
                  group by 
                    persistent_id 
                  having 
                    distinct_ccp > 1
                );
                  
                  
         persistent_id_ccp_valiation_cursor_memri cursor for
              select 
                case when count(*) = 0 then true else false end memri_persistent_id_ccp_validation 
              from 
                (
                  with orphan_item_memri as(
                    select 
                      * 
                    from 
                       "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".T_DICTIONARY_ITEM 
                    where 
                      type_id != '6' 
                      and parent_id is null 
                      and deliverable_id = 2
                  ) 
                  select 
                    persistent_id, 
                    count(distinct ccp) as distinct_ccp 
                  from 
                    orphan_item_memri 
                  group by 
                    persistent_id 
                  having 
                    distinct_ccp > 1
                );


    begin

      open staging_item_count_cursor ;
      fetch staging_item_count_cursor into staging_item_count;

      open top_level_parent_cursor_insight ;
      fetch top_level_parent_cursor_insight into top_level_with_parent_count_insight;

      open orphan_item_cursor_insight ;
      fetch orphan_item_cursor_insight into orphan_items_count_insight;

      open answer_item_count_cursor_insight ;
      fetch answer_item_count_cursor_insight into answer_item_count_insight;

      open top_level_parent_cursor_memri ;
      fetch top_level_parent_cursor_memri into top_level_with_parent_count_memri;

      open orphan_item_cursor_memri ;
      fetch orphan_item_cursor_memri into orphan_items_count_memri;

      open answer_item_count_cursor_memri ;
      fetch answer_item_count_cursor_memri into answer_item_count_memri;
                  
      open persistent_id_ccp_valiation_cursor_insight ;
      fetch persistent_id_ccp_valiation_cursor_insight into persistent_id_ccp_valiadation_insight;
                  
      open persistent_id_ccp_valiation_cursor_memri;
      fetch persistent_id_ccp_valiation_cursor_memri into persistent_id_ccp_valiadation_memri;           
                  
       if (answer_item_count_insight = staging_item_count
              and top_level_with_parent_count_insight = 0
              and orphan_items_count_insight = 0
              and persistent_id_ccp_valiadation_insight = true

              and answer_item_count_memri = staging_item_count
              and top_level_with_parent_count_memri = 0
              and orphan_items_count_memri = 0
              and persistent_id_ccp_valiadation_memri = true
            ) then

              return object_construct('status_code', 1,
                                      'message','Datamart Validation Passed'
                                     );

       end if;

       return object_construct('status_code', 0,
                               'message','Datamart Validation Failed'
                              );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;


  $$;



create or replace procedure SP_GET_STUDY_DATA()
  returns integer
  language sql
  as
  $$
    
    declare
      
      study_name varchar;
      study_code varchar;
      version varchar;
     
    begin
       
        select STUDY_NAME into :study_name from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG;
        select STUDY_CODE into :study_code from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG;
        select DP_VERSION_ID into :version from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG;
        
        return object_construct('status_code', '1',
                                'message', object_construct('study_name', study_name,
                                                            'study_code', study_code,
                                                            'version', version,
                                                            'is_historic', 'True'
                                                            )
                               );
          
  
    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
    
    
    end;

  $$;

  create or replace procedure SP_INSERT_DICTIONARY()
  returns integer
  language sql
  as
  $$

    declare

      dictionary_id integer ;
      cursor_dictionary_id cursor for select ID from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG";

      count_dictionary_in_datamart integer ;
      dictionary_in_datamart_cursor cursor for select count(*) from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" where id = ?;

      count_value_changed integer;
      count_value_changed_cursor cursor for select count(*) from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_COMPARE";

      insert_into_dictionary := '
            INSERT INTO "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY"
                  SELECT
                    ID,
                    STUDY_ID,
                    VERSION,
                    STATUS_ID,
                    COMMENT,
                    SOURCE_PARENT_ID,
                    SOURCE_ID,
                    VERSION_RELEASE_DATE,
                    CRT_TS,
                    CRT_BY,
                    NULL,
                    NULL,
                    FALSE as IS_CURRENT_VERSION,
                    DB_VERSIONID
                  FROM
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"';

      merge_into_dictionary := '
         MERGE INTO "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" AS MART_DICTIONARY USING (
            SELECT
              ID,
              STUDY_ID,
              VERSION,
              STATUS_ID,
              COMMENT,
              SOURCE_PARENT_ID,
              SOURCE_ID,
              VERSION_RELEASE_DATE,
              CRT_TS,
              CRT_BY,
              UPD_BY,
              UPD_TS,
              DB_VERSIONID,
              FALSE AS IS_CURRENT_VERSION
            FROM
              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"
          ) AS STG_DICTIONARY ON MART_DICTIONARY.ID = STG_DICTIONARY.ID
          WHEN MATCHED
          THEN
            UPDATE
            SET
              MART_DICTIONARY.VERSION = STG_DICTIONARY.VERSION,
              MART_DICTIONARY.STATUS_ID = STG_DICTIONARY.STATUS_ID,
              MART_DICTIONARY.COMMENT = STG_DICTIONARY.COMMENT,
              MART_DICTIONARY.SOURCE_PARENT_ID = STG_DICTIONARY.SOURCE_PARENT_ID,
              MART_DICTIONARY.SOURCE_ID = STG_DICTIONARY.SOURCE_ID,
              MART_DICTIONARY.VERSION_RELEASE_DATE = STG_DICTIONARY.VERSION_RELEASE_DATE,
              MART_DICTIONARY.UPD_BY = STG_DICTIONARY.UPD_BY,
              MART_DICTIONARY.UPD_TS = STG_DICTIONARY.UPD_TS,
              MART_DICTIONARY.IS_CURRENT_VERSION = STG_DICTIONARY.IS_CURRENT_VERSION;';

         insert_into_dictionary_log :='
                insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_LOG"(
                        STUDY_ID,
                        ACTION_TYPE_ID,
                        MESSAGE,
                        OLD_VALUE,
                        NEW_VALUE,
                        CRT_TS,
                        UPD_BY,
                        UPD_TS,
                        FIELD,
                        CRT_BY,
                        DICTIONARY_ID
                    )
                    select
                        STUDY_ID,
                        ACTION_TYPE_ID,
                        MESSAGE,
                        OLD_VALUE,
                        NEW_VALUE,
                        CRT_TS,
                        UPD_BY,
                        UPD_TS,
                        FIELD,
                        CRT_BY,
                        DICTIONARY_ID
                    from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_COMPARE";';


    begin

      open cursor_dictionary_id ;
      fetch cursor_dictionary_id into dictionary_id;

      open dictionary_in_datamart_cursor using (dictionary_id);
      fetch dictionary_in_datamart_cursor into count_dictionary_in_datamart;


      open count_value_changed_cursor ;
      fetch count_value_changed_cursor into count_value_changed;

      if (count_dictionary_in_datamart = 0) then
          execute immediate : insert_into_dictionary;
          return object_construct('status_code', 1,
                                  'message','New Dictionary inserted.'
                                  );

      elseif (count_dictionary_in_datamart = 1) then
           if (count_value_changed=0) then
                   return object_construct('status_code', 1,
                                           'message','Nothing to update.'
                                          );
           else
                   execute immediate : insert_into_dictionary_log;
                   execute immediate : merge_into_dictionary;
                   return object_construct('status_code', 1,
                                           'message','Dictionary updated.'
                                          );
           end if;

      end if;

      return object_construct('status_code', 1);

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;


  $$;

  create or replace procedure"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".SP_INSERT_ITEM_TEXT()
  returns integer
  language sql
  as
  $$

        declare
            etl_user varchar;
            item_text_insert := '
                   insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".ITEM_TEXT(
                          text, active, crt_by, is_timeperiod
                        )
                        SELECT
                            TEXT,
                            TRUE AS ACTIVE,
                            VW_USER_INFO.ID AS CRT_BY,
                            FALSE AS TIMEPERIOD
                        FROM
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_ITEM_TEXT
                            left join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".VW_USER_INFO ON VW_USER_INFO.FIRST_NAME = \'ETL_USER\'
                        WHERE
                            VW_ITEM_TEXT.TEXT_ID IS NULL';

        begin
            execute immediate :item_text_insert;
            return object_construct('status_code', '1',
                                    'message','Item texts inserted.'
                                   );

        exception
            when statement_error then
                return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
                return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
                return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
        end;
  $$;


 create or replace procedure "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".SP_INSERT_STUDY()
   returns integer
  language sql
  as
  $$

    declare
      study_id integer ;
      study_id_cursor cursor for select STUDY_ID from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG";

      count_study_in_datamart integer ;
      study_in_datamart_cursor cursor for select count(*) from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" where id = ?;

      count_value_changed integer;
      count_value_changed_cursor cursor for select count(*) from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_COMPARE";

      insert_into_study := '
            INSERT INTO "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY"
                  SELECT
                    ID,
                    NAME,
                    STUDY_FAMILY_ID,
                    STUDY_RELEASE_ID,
                    STUDY_TYPE_ID,
                    STUDY_CODE,
                    SECURITYCODE,
                    RELEASE_DATE,
                    YEAR,
                    TREND_FAMILY_ID,
                    MINWAVE,
                    MAXWAVE,
                    STARTFIELDDATE,
                    ENDFIELDDATE,
                    USEEXTERNALKEY,
                    ACTIVE,
                    PARENT_STUDY_ID,
                    DP_STUDYID,
                    CRT_TS,
                    CRT_BY,
                    NULL as UPD_BY,
                    NULL as UPD_TS,
                    HIDE
                  FROM
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_STG"';

      merge_into_study := '
            MERGE INTO "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" AS MART_STUDY USING (
                        SELECT
                          ID,
                          NAME,
                          STUDY_FAMILY_ID,
                          STUDY_RELEASE_ID,
                          STUDY_TYPE_ID,
                          STUDY_CODE,
                          SECURITYCODE,
                          RELEASE_DATE,
                          YEAR,
                          TREND_FAMILY_ID,
                          MINWAVE,
                          MAXWAVE,
                          STARTFIELDDATE,
                          ENDFIELDDATE,
                          USEEXTERNALKEY,
                          ACTIVE,
                          PARENT_STUDY_ID,
                          DP_STUDYID,
                          CRT_TS,
                          CRT_BY,
                          UPD_BY,
                          UPD_TS,
                          HIDE
                        FROM
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_STG"
                      ) AS STG_STUDY ON MART_STUDY.ID = STG_STUDY.ID
                      WHEN MATCHED
                        THEN
                          UPDATE
                            SET
                              MART_STUDY.NAME = STG_STUDY.NAME,
                              MART_STUDY.STUDY_FAMILY_ID = STG_STUDY.STUDY_FAMILY_ID,
                              MART_STUDY.STUDY_RELEASE_ID = STG_STUDY.STUDY_RELEASE_ID,
                              MART_STUDY.STUDY_TYPE_ID = STG_STUDY.STUDY_TYPE_ID,
                              MART_STUDY.STUDY_CODE = STG_STUDY.STUDY_CODE,
                              MART_STUDY.SECURITYCODE = STG_STUDY.SECURITYCODE,
                              MART_STUDY.RELEASE_DATE = STG_STUDY.RELEASE_DATE,
                              MART_STUDY.YEAR = STG_STUDY.YEAR,
                              MART_STUDY.TREND_FAMILY_ID = STG_STUDY.TREND_FAMILY_ID,
                              MART_STUDY.MINWAVE = STG_STUDY.MINWAVE,
                              MART_STUDY.MAXWAVE = STG_STUDY.MAXWAVE,
                              MART_STUDY.STARTFIELDDATE = STG_STUDY.STARTFIELDDATE,
                              MART_STUDY.ENDFIELDDATE = STG_STUDY.ENDFIELDDATE,
                              MART_STUDY.USEEXTERNALKEY = STG_STUDY.USEEXTERNALKEY,
                              MART_STUDY.ACTIVE = STG_STUDY.ACTIVE,
                              MART_STUDY.PARENT_STUDY_ID = STG_STUDY.PARENT_STUDY_ID,
                              MART_STUDY.DP_STUDYID = STG_STUDY.DP_STUDYID,
                              MART_STUDY.UPD_BY = STG_STUDY.UPD_BY,
                              MART_STUDY.UPD_TS = STG_STUDY.UPD_TS,
                              MART_STUDY.HIDE = STG_STUDY.HIDE;';

       insert_into_dictionary_log :='
                insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_LOG"(
                        STUDY_ID,
                        ACTION_TYPE_ID,
                        MESSAGE,
                        OLD_VALUE,
                        NEW_VALUE,
                        CRT_TS,
                        UPD_BY,
                        UPD_TS,
                        FIELD,
                        CRT_BY,
                        DICTIONARY_ID
                    )
                    select
                        STUDY_ID,
                        ACTION_TYPE_ID,
                        MESSAGE,
                        OLD_VALUE,
                        NEW_VALUE,
                        CRT_TS,
                        UPD_BY,
                        UPD_TS,
                        FIELD,
                        CRT_BY,
                        DICTIONARY_ID
                    from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_COMPARE";';

    begin

      open study_id_cursor ;
      fetch study_id_cursor into study_id;

      open study_in_datamart_cursor using (study_id);
      fetch study_in_datamart_cursor into count_study_in_datamart;

      open count_value_changed_cursor ;
      fetch count_value_changed_cursor into count_value_changed;

      if (count_study_in_datamart = 0) then
          execute immediate : insert_into_study;
          return object_construct('status_code', 1,
                                  'message','New study inserted.'
                                  );

      elseif (count_study_in_datamart = 1) then
           if (count_value_changed=0) then
                   return object_construct('status_code', 1,
                                           'message','Nothing to update.'
                                          );
           else
                   execute immediate : insert_into_dictionary_log;
                   execute immediate : merge_into_study;
                   return object_construct('status_code', 1,
                                           'message','Study updated.'
                                          );
           end if;

      end if;

      return object_construct('status_code', 1);

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end
 $$;

 create or replace procedure SP_INSERT_SUPPRESSION()
  returns integer
  language sql
  as
  $$
   declare
     supression_count integer;
     cursor_supression_count cursor for select
                                          count(*)
                                        from
                                          "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_SUPPRESSION"
                                        where
                                          DICTIONARY_ID = (
                                            select
                                              distinct DICTIONARY_ID
                                            from
                                              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_SUPRESSION_STG"
                                             );
     suppression_present boolean;
     cursor_suppression_present
            cursor
                for
                    select
                        exists(
                          select
                              *
                          from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_SUPRESSION_STG"
                        ) as SUPPRESSION_PRESENT;


     suppression_changed_reverse boolean;
     cursor_suppression_changed_reverse
            cursor
                for
                    select
                        exists(
                          select
                            DICTIONARY_ID,
                            PERSISTENT_ID,
                            DELIVERABLE_ID,
                            ACTIVE
                          from
                          "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".DICTIONARY_SUPPRESSION
                          EXCEPT
                          select
                            DICTIONARY_ID,
                            KEYWORD,
                            DELIVERABLE_ID,
                            ACTIVE
                          from
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_SUPRESSION_STG
                        ) as SUPPRESSION_EXISTS;

     suppression_changed boolean;
     cursor_suppression_changed
            cursor
                for
                    select
                      exists(
                         select
                            DICTIONARY_ID,
                            KEYWORD,
                            DELIVERABLE_ID,
                            ACTIVE
                          from
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_SUPRESSION_STG
                          EXCEPT
                          select
                            DICTIONARY_ID,
                            PERSISTENT_ID,
                            DELIVERABLE_ID,
                            ACTIVE
                          from
                          "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".DICTIONARY_SUPPRESSION
                      ) as SUPPRESSION_EXISTS;


     insert_into_suppression := 'insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_SUPPRESSION"
                                select
                                  *
                                from
                                  "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_SUPRESSION_STG"';

     delete_from_suppression := 'delete
                                  from
                                     "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_SUPPRESSION"
                                  where
                                      DICTIONARY_ID = (
                                            select
                                              distinct DICTIONARY_ID
                                            from
                                              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_SUPRESSION_STG"
                                             );';

   begin

      open cursor_supression_count ;
      fetch cursor_supression_count into supression_count;

      open cursor_suppression_changed_reverse ;
      fetch cursor_suppression_changed_reverse into suppression_changed_reverse;

      open cursor_suppression_changed ;
      fetch cursor_suppression_changed into suppression_changed;

      open cursor_suppression_present ;
      fetch cursor_suppression_present into suppression_present;

      if (suppression_present = false) then

           return object_construct('status_code', 1,
                                    'message', 'Staging Suppression table is empty. No Suppression inserted'
                                   );
      end if;

      if (supression_count = 0) then
          execute immediate : insert_into_suppression;
          return object_construct('status_code', 1,
                                  'message', 'Suppression Inserted'
                                 );
      else
        if (suppression_changed_reverse = true or suppression_changed = true) then

           execute immediate : delete_from_suppression;
           execute immediate : insert_into_suppression;

           return object_construct('status_code', 1,
                                    'message', 'Suppression Updated'
                                   );
           end if;
      end if;

   return object_construct('status_code', 1,
                                    'message', 'Suppression already inserted. No changes made.'
                                   );

   exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end
 $$;

 create or replace procedure SP_LOAD_ATTRIBUTE_AND_ATTRIBUTE_TYPE()
  returns integer
  language sql
  execute as caller
  as
  $$
    declare

      new_attribute_exists integer;
      new_attribute_type_exists integer;

    begin

     select count(*) into :new_attribute_exists from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_ATTRIBUTE";
     select count(*) into :new_attribute_type_exists from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_ATTRIBUTE_TYPE";

     if (new_attribute_exists != 0 or new_attribute_type_exists != 0) then

                  if (new_attribute_exists != 0) then
                     insert into "DEV_MDM_DB"."DICTIONARY"."ATTRIBUTE"(NAME,CRT_TS,CRT_BY,UPD_BY,UPD_TS)
                        select NAME,CRT_TS,CRT_BY,UPD_BY,UPD_TS from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_ATTRIBUTE";
                  end if;

                  if (new_attribute_type_exists != 0) then
                     insert into "DEV_MDM_DB"."DICTIONARY"."ATTRIBUTE_TYPE"(NAME,CRT_TS,CRT_BY,UPD_BY,UPD_TS)
                        select ATTRIBUTE_TYPE,CRT_TS,CRT_BY,UPD_BY,UPD_TS from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_ATTRIBUTE_TYPE";
                  end if;

                  return object_construct('status_code', '1',
                                          'message', 'New Attribute and Attribute Type  Inserted.'
                                         );

       else
                  return object_construct('status_code', '1',
                                          'message', 'No New Attribute and Attribute Type Found.'
                                         );
      end if;



     return object_construct('status_code', '1',
                              'message','Attribute and Attribute Type  Inserted.'
                             );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;

  $$;


  create or replace procedure SP_LOAD_ATTRIBUTE_MASTER()
  returns integer
  language sql
  as
  $$
    declare
        new_attribute_exists integer;
    begin
        select count(*) into :new_attribute_exists from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_ATTRIBUTE_MASTER";

        if (new_attribute_exists != 0) then

            insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ATTRIBUTE_MASTER"  (ATTRIBUTE_TYPE_ID,ATTRIBUTE_VALUE,
                ATTRIBUTE_KEY,ACTIVE,CRT_TS,CRT_BY,UPD_BY,UPD_TS,ATTRIBUTE_ID)
                    select ATTRIBUTE_TYPE_ID,ATTRIBUTE_VALUE,
                    ATTRIBUTE_KEY,ACTIVE,CRT_TS,CRT_BY,UPD_BY,UPD_TS,ATTRIBUTE_ID from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_ATTRIBUTE_MASTER";

            return object_construct('status_code', '1',
                                    'message', 'New Attribute Inserted.'
                                   );

        else
            return object_construct('status_code', '1',
                                     'message', 'No New Attribute Found.'
                                    );
        end if;


      return object_construct('status_code', '1',
                              'message','Attribute Inserted.'
                             );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;

  $$;
  

  create or replace procedure SP_LOAD_DICTIONARY_ATTRIBUTES()
  returns integer
  language sql
  as
  $$
    declare

      new_dictionary_item_attribute_exists integer;
      new_dictionary_item_attribute_reverse_exists integer;
      new_dictionary_attribute_exists integer;
      new_dictionary_attribute_reverse_exists integer;

    begin

     select count(*) into :new_dictionary_item_attribute_exists from(
       select DICTIONARY_ID,DICTIONARY_ITEM_ID,STUDY_FAMILY_ID,ATTRIBUTE_MASTER_ID,PERSISTENT_ID
          from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ATTRIBUTE" where IS_STUDYATTRIBUTE = 0

       except

       select DICTIONARY_ID,DICTIONARY_ITEM_ID,STUDY_FAMILY_ID,ATTRIBUTE_MASTER_ID,DICTIONARY_ITEM_ATTRIBUTE.PERSISTENT_ID
          from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_ATTRIBUTE"
                join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" on DICTIONARY_ITEM.ID = DICTIONARY_ITEM_ATTRIBUTE.DICTIONARY_ITEM_ID
                    where DICTIONARY_ID=(select dictionary_id from study_stg)
     );

      select count(*) into :new_dictionary_item_attribute_reverse_exists from(

       select DICTIONARY_ID,DICTIONARY_ITEM_ID,STUDY_FAMILY_ID,ATTRIBUTE_MASTER_ID,DICTIONARY_ITEM_ATTRIBUTE.PERSISTENT_ID
          from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_ATTRIBUTE"
                join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" on DICTIONARY_ITEM.ID = DICTIONARY_ITEM_ATTRIBUTE.DICTIONARY_ITEM_ID
                    where DICTIONARY_ID=(select dictionary_id from study_stg)
       except

       select DICTIONARY_ID,DICTIONARY_ITEM_ID,STUDY_FAMILY_ID,ATTRIBUTE_MASTER_ID,PERSISTENT_ID
          from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ATTRIBUTE" where IS_STUDYATTRIBUTE = 0

     );

     select count(*) into :new_dictionary_attribute_exists from(
       select DICTIONARY_ID,ATTRIBUTE_MASTER_ID
          from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ATTRIBUTE" where IS_STUDYATTRIBUTE = 1

       except

       select DICTIONARY_ID,ATTRIBUTE_MASTER_ID from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ATTRIBUTE"
          where DICTIONARY_ID=(select dictionary_id from study_stg)
     );



     select count(*) into :new_dictionary_attribute_reverse_exists from(

       select DICTIONARY_ID,ATTRIBUTE_MASTER_ID from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ATTRIBUTE"
            where DICTIONARY_ID=(select dictionary_id from study_stg)

       except

       select DICTIONARY_ID,ATTRIBUTE_MASTER_ID
          from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ATTRIBUTE" where IS_STUDYATTRIBUTE = 1
     );



     if (new_dictionary_item_attribute_exists != 0 or new_dictionary_attribute_exists != 0 or
         new_dictionary_item_attribute_reverse_exists != 0 or new_dictionary_attribute_reverse_exists!=0

        ) then

         if (new_dictionary_item_attribute_exists != 0 or new_dictionary_item_attribute_reverse_exists != 0) then
            delete from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_ATTRIBUTE" where DICTIONARY_ITEM_ID in(
            select ID from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" where DICTIONARY_ID=(
                select dictionary_id from study_stg ) and deliverable_id=1
                );

            insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_ATTRIBUTE"(DICTIONARY_ITEM_ID,STUDY_FAMILY_ID,
                ATTRIBUTE_MASTER_ID,PERSISTENT_ID, CRT_TS,CRT_BY,UPD_BY,UPD_TS)
                select DICTIONARY_ITEM_ID,STUDY_FAMILY_ID,ATTRIBUTE_MASTER_ID,PERSISTENT_ID,
                    CRT_TS,CRT_BY,UPD_BY,UPD_TS from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ATTRIBUTE"
                        where IS_STUDYATTRIBUTE = 0;
          end if;

         if (new_dictionary_attribute_exists != 0 or new_dictionary_attribute_reverse_exists!=0 ) then
            delete from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ATTRIBUTE" where DICTIONARY_ID=(
                select dictionary_id from study_stg
                );

            insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ATTRIBUTE"(DICTIONARY_ID,ATTRIBUTE_MASTER_ID,
                ACTIVE,CRT_TS,CRT_BY,UPD_BY,UPD_TS)
                select DICTIONARY_ID,ATTRIBUTE_MASTER_ID,ACTIVE,
                    CRT_TS,CRT_BY,UPD_BY,UPD_TS from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_ATTRIBUTE"
                        where IS_STUDYATTRIBUTE = 1;

        end if;


        return object_construct('status_code', '1',
                                'message', 'Dictionary Item Attribute Inserted.'
                               );
     else
              return object_construct('status_code', '1',
                                      'message', 'No New Attribute Found.'
                                      );
     end if;


    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;

  $$;


  create or replace procedure SP_LOAD_DICTIONARY_ITEMS()
  returns integer
  language sql
  as
  $$
    declare

      insert_into_dictionary_item := '
            insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" (
                SELECT
                  DICTIONARY_ITEM_ID,
                  DICTIONARY_ID,
                  PERSISTENT_ID,
                  KEYWORD,
                  TYPE_ID,
                  PARENT_ID,
                  TEXT_ID,
                  IS_Q_LEVEL,
                  ACCESS_CODE,
                  FIRST_WAVE,
                  IS_NEW,
                  DICTIONARY_ITEM_SORT as SORT,
                  DELIVERABLE_ID,
                  ACTIVE,
                  CRT_TS,
                  CRT_BY,
                  UPD_BY,
                  UPD_TS,
                  DISPLAY_TYPE,
                  FALSE as IS_ANS_FLIPPED
                FROM
                  "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM"
                ORDER BY
                  SORT
              )';


    insert_into_item_range := '
              insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_RANGE"
                SELECT
                    *
                  FROM
                    (
                      select
                      DICTIONARY_ITEM_ID,
                      MIN,
                      MAX,
                      0 AS MIDPOINT,
                      0 as FEMALE_MIDPOINT,
                      0 as MALE_MIDPOINT,
                      ACTIVE,
                      CRT_TS,
                      CRT_BY,
                      UPD_BY,
                      UPD_TS
                    from
                      "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM"
                    where
                      DICTIONARY_ITEM_TYPE = \'a\'
                      or LEAF_LEVEL = TRUE
                    )
                   WHERE
                      MAX != 0
                      OR MIN != 0
                      OR MIDPOINT != 0
                      OR MALE_MIDPOINT != 0
                      OR FEMALE_MIDPOINT != 0';

      insert_into_datapoint :=  '
            insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_DATAPOINT"(
                  DICTIONARY_ITEM_ID, DATA_TYPE_ID,
                  CCP_EXPRESSION, EXTERNALKEY, MEMRI_DEFINITION,ACTIVE,
                  CRT_TS, CRT_BY, UPD_BY,
                  UPD_TS,SORT
                )
                select
                  DICTIONARY_ITEM_ID,
                  DATA_TYPE_ID,
                  CCP,
                  EXTERNAL_KEY,
                  MEMRI_DEFINITION,
                  ACTIVE,
                  CRT_TS,
                  CRT_BY,
                  UPD_BY,
                  UPD_TS,
                  SORT
                from
                  "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM"
                where
                  DICTIONARY_ITEM_TYPE = \'a\'
                  or LEAF_LEVEL = TRUE'
                  ;


    begin
      execute immediate :insert_into_dictionary_item;
      execute immediate :insert_into_item_range;
      execute immediate :insert_into_datapoint;

      return object_construct('status_code', '1',
                              'message','Dictionary Item Data Loaded.'
                             );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
    end;

  $$;


  create or replace procedure SP_LOAD_STUDY_WEIGHT()
returns integer
  language sql
  as
  $$

    declare

      study_weight_changed_reverse boolean;
      cursor_study_weight_changed_reverse
            cursor
                for
                    select
                      exists(
                        select
                          WEIGHT_TYPE_ID,
                          WEIGHT_EXTENSION_ID,
                          MULTIPLIER,
                          DIVISOR,
                          CCP,
                          PERSISTENT_ID
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".STUDY_WEIGHT
                        where
                          study_id = (
                            select
                              study_id
                            from
                              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG
                          )
                        except
                        select
                          WEIGHT_TYPE_ID,
                          WEIGHT_EXTENSION_ID,
                          MULTIPLIER,
                          DIVISOR,
                          CCP,
                          PERSISTENT_ID
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_WEIGHT"
                      ) as STUDY_WEIGHT_CHANGE;

      study_weight_changed boolean;
      cursor_study_weight_changed
            cursor
                for
                    select
                      exists(
                        select
                          WEIGHT_TYPE_ID,
                          WEIGHT_EXTENSION_ID,
                          MULTIPLIER,
                          DIVISOR,
                          CCP,
                          PERSISTENT_ID
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_WEIGHT"
                        except
                        select
                          WEIGHT_TYPE_ID,
                          WEIGHT_EXTENSION_ID,
                          MULTIPLIER,
                          DIVISOR,
                          CCP,
                          PERSISTENT_ID
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".STUDY_WEIGHT
                        where
                          study_id = (
                            select
                              study_id
                            from
                              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG
                          )
                      ) as STUDY_WEIGHT_CHANGE;

      delete_study_weight := '
                delete from
                  "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".STUDY_WEIGHT
                where
                  STUDY_ID = (
                        select
                          study_id
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG
                        )';

      insert_into_study_weight := '
              insert into "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART".STUDY_WEIGHT (
                STUDY_ID,
                WEIGHT_TYPE_ID,
                WEIGHT_EXTENSION_ID,
                MULTIPLIER,
                DIVISOR,
                CCP,
                PERSISTENT_ID,
                CRT_TS,
                CRT_BY,
                UPD_BY,
                UPD_TS
                )
                  SELECT
                    STUDY_ID,
                    WEIGHT_TYPE_ID,
                    WEIGHT_EXTENSION_ID,
                    MULTIPLIER,
                    DIVISOR,
                    CCP,
                    PERSISTENT_ID,
                    CRT_TS,
                    CRT_BY,
                    UPD_BY,
                    UPD_TS
                  FROM
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_WEIGHT"';

      study_weight_present boolean;
      cursor_study_weight_present
            cursor
                for
                    select
                        exists(
                          select
                              *
                          from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_WEIGHT"
                        ) as STUDY_WEIGHT_PRESENT;


    begin

      open cursor_study_weight_changed_reverse ;
      fetch cursor_study_weight_changed_reverse into study_weight_changed_reverse;

      open cursor_study_weight_changed ;
      fetch cursor_study_weight_changed into study_weight_changed;

      open cursor_study_weight_present ;
      fetch cursor_study_weight_present into study_weight_present;

      if (study_weight_present = false) then
         return object_construct('status_code', '0',
                                 'message', 'Study Weight Table is empty'
                                 );

      end if;



      if (study_weight_changed = true or study_weight_changed_reverse = true) then

                  execute immediate : delete_study_weight;
                  execute immediate : insert_into_study_weight;

                  return object_construct('status_code', '1',
                                          'message', 'Study weight cleared. New study weight inserted'
                                         );

           else
                  return object_construct('status_code', '1',
                                          'message', 'Study weight unchanged.'
                                         );
      end if;


    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;

  $$;

 
create or replace procedure SP_LOAD_STUDY_WEIGHT_TYPE_AND_EXTENSION()
  returns integer
  language sql
  execute as caller
  as
  $$
    declare

      new_weight_extension_available boolean;
      cursor_new_weight_extension_available
            cursor
                for
                    select
                      exists(
                        SELECT
                            *
                        FROM
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_WEIGHT_EXTENSION"

                      ) as STUDY_WEIGHT_EXTENSION_AVAILABLE;

      new_weight_type_available boolean;
      cursor_new_weight_type_available
            cursor
                for
                    select
                      exists(
                        SELECT
                            *
                        FROM
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_WEIGHT_TYPE"

                      ) as STUDY_WEIGHT_TYPE_AVAILABLE;

      insert_into_weight_type := '
            insert into "DEV_MDM_DB"."DICTIONARY"."WEIGHT_TYPE" (
                WEIGHT_TYPE,
                ACTIVE,
                CRT_TS,
                CRT_BY,
                UPD_BY,
                UPD_TS
                )
                  SELECT
                    WEIGHT_TYPE,
                    ACTIVE,
                    CRT_TS,
                    CRT_BY,
                    UPD_BY,
                    UPD_TS
                  FROM
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_WEIGHT_TYPE"';


    insert_into_weight_extension := '
              insert into "DEV_MDM_DB"."DICTIONARY"."WEIGHT_EXTENSION" (
                WEIGHT_EXTENSION,
                ACTIVE,
                CRT_TS,
                CRT_BY,
                UPD_BY,
                UPD_TS
                )
                  SELECT
                    WEIGHT_EXTENSION,
                    ACTIVE,
                    CRT_TS,
                    CRT_BY,
                    UPD_BY,
                    UPD_TS
                  FROM
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_WEIGHT_EXTENSION"';



    begin

      open cursor_new_weight_extension_available ;
      fetch cursor_new_weight_extension_available into new_weight_extension_available;

      open cursor_new_weight_type_available ;
      fetch cursor_new_weight_type_available into new_weight_type_available;

      if (new_weight_extension_available = true or new_weight_type_available = true) then

                  if (new_weight_extension_available = true) then
                     execute immediate :insert_into_weight_extension;
                  end if;


                  if (new_weight_type_available = true) then
                     execute immediate :insert_into_weight_type;
                  end if;




                  return object_construct('status_code', '1',
                                          'message', 'New Weight Type and Extension Inserted.'
                                         );

       else
                  return object_construct('status_code', '1',
                                          'message', 'No New Weight Type and Extension Found.'
                                         );
      end if;



      return object_construct('status_code', '1',
                              'message','Study Weight and Type Inserted'
                             );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
    end;

  $$;


  create or replace procedure SP_UPDATE_IS_CURRENT_VERSION()
  returns integer
  language sql
  as
  $$


    declare
      study_id integer ;
      dictionary_id integer ;

      study_id_cursor cursor for select STUDY_ID from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG";

      dictionary_id_cursor cursor for select ID from "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" where STUDY_ID = ? order by DP_VERSIONID desc limit 1;
      update_is_current_version:='update "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" set is_current_version = False where STUDY_ID = ';

    begin

      open study_id_cursor ;
      fetch study_id_cursor into study_id;

      update_is_current_version := update_is_current_version || study_id ||';';
      execute immediate :update_is_current_version;

      open dictionary_id_cursor using(study_id);
      fetch dictionary_id_cursor into dictionary_id;

      update_is_current_version:='update "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" set is_current_version = TRUE where ID =';
      update_is_current_version := update_is_current_version || dictionary_id ||';';
      execute immediate :update_is_current_version;


      return object_construct('status_code', 1,
                              'message','IS_CURRENT_FLAG Updated.'
                              );

    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;


  $$;


create or replace procedure SP_VALIDATE_DICTIONARY()
  returns integer
  language sql
  as
  $$

    declare

      datamart_dictionary_id integer ;
      cursor_datamart_dictionary_id
        cursor for select
                      ID
                    from
                      "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY"
                    where
                      STUDY_ID =(
                        select
                          STUDY_ID
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_DICTIONARY_STG
                      )
                      and DP_VERSIONID = (
                        select
                          DB_VERSIONID
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_DICTIONARY_STG
                      );


        dictionary_id integer;
        cursor_dictionary_id cursor for select id from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG";

    begin

      open cursor_datamart_dictionary_id ;
      fetch cursor_datamart_dictionary_id into datamart_dictionary_id;

      open cursor_dictionary_id ;
      fetch cursor_dictionary_id into dictionary_id;


      if (dictionary_id = datamart_dictionary_id or datamart_dictionary_id is NULL) then
          return object_construct('status_code', 1,
                         'message','Dictionary Valid'
                   );
      end if;

      return object_construct('status_code', 0,
                                    'message','Dictionary Invalid'
                                   );
    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;


  $$;


  create or replace procedure SP_VALIDATE_STUDY()
   returns integer
  language sql
  as
  $$

    declare

      datamart_study_id integer ;
      cursor_datamart_study_id
        cursor for select
                    ID
                  from
                    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY"
                  where
                    name =(
                      select
                        NAME
                      from
                        "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_STUDY_STG
                    );

       datamart_study_id_code integer ;
       cursor_datamart_study_id_code
        cursor for select
                    ID
                  from
                    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY"
                  where
                    STUDY_CODE =(
                      select
                        STUDY_CODE
                      from
                        "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_STUDY_STG
                    );

        study_id integer;
        cursor_study_id cursor for select id from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_STUDY_STG";

    begin

      open cursor_datamart_study_id ;
      fetch cursor_datamart_study_id into datamart_study_id;

      open cursor_study_id ;
      fetch cursor_study_id into study_id;

      open cursor_datamart_study_id_code ;
      fetch cursor_datamart_study_id_code into datamart_study_id_code;

      if (study_id = datamart_study_id or datamart_study_id is NULL) then
            if (study_id = datamart_study_id_code or datamart_study_id_code is NULL) then
                  return object_construct('status_code', 1,
                                          'message','Study Valid'
                                         );
             end if;
      end if;

      return object_construct('status_code', 0,
                                    'message','Study Invalid'
                                   );
    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;


  $$;

  create or replace procedure SP_CLEAR_DICTIONARY_TABLES()
  returns integer
  language sql
  as
  $$

    declare

      dictionary_item_changed_reverse boolean;
      cursor_dictionary_item_changed_reverse
            cursor
                for
                    select
                      exists(
                        select
                          DICTIONARY_ID,
                          KEYWORD,
                          TYPE_ID,
                          TEXT_ID,
                          IS_Q_LEVEL,
                          DICTIONARY_DATAPOINT.SORT,
                          IS_NEW,
	                      FIRST_WAVE,
                          CCP_EXPRESSION,
                          EXTERNALKEY,
                          MEMRI_DEFINITION,
                          PERSISTENT_ID,
                          ACCESS_CODE,
                          DISPLAY_TYPE,
                          DELIVERABLE_ID,
                          CASE WHEN MIN IS NULL THEN '0' ELSE MIN END AS MIN,
                          CASE WHEN MAX IS NULL THEN '0' ELSE MAX END AS MAX
                        from
                          "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM"
                           left join "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_DATAPOINT" on DICTIONARY_ITEM.ID = DICTIONARY_DATAPOINT.DICTIONARY_ITEM_ID
                           left join "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM_RANGE" on DICTIONARY_ITEM.ID = DICTIONARY_ITEM_RANGE.DICTIONARY_ITEM_ID
                        where
                          DICTIONARY_ITEM.DICTIONARY_ID = (
                            select
                              ID
                            from
                              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"
                          )
                        except
                        select
                          DICTIONARY_ID,
                          KEYWORD,
                          TYPE_ID,
                          TEXT_ID,
                          IS_Q_LEVEL,
                          SORT,
                          IS_NEW,
	                      FIRST_WAVE,
                          CCP,
                          EXTERNAL_KEY,
                          MEMRI_DEFINITION,
                          PERSISTENT_ID,
                          ACCESS_CODE,
                          DISPLAY_TYPE,
                          DELIVERABLE_ID,
                          CASE WHEN MIN IS NULL THEN '0' ELSE MIN END AS MIN,
                          CASE WHEN MAX IS NULL THEN '0' ELSE MAX END AS MAX
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM"
                      ) as DICTIONARY_ITEM_CHANGED;

      dictionary_item_changed boolean;
      cursor_dictionary_item_changed
            cursor
                for
                    select
                      exists(
                       select
                          DICTIONARY_ID,
                          KEYWORD,
                          TYPE_ID,
                          TEXT_ID,
                          IS_Q_LEVEL,
                          SORT,
                          IS_NEW::BOOLEAN as IS_NEW,
	                      FIRST_WAVE,
                          CCP,
                          EXTERNAL_KEY,
                          MEMRI_DEFINITION,
                          PERSISTENT_ID,
                          ACCESS_CODE,
                          DISPLAY_TYPE,
                          DELIVERABLE_ID,
                          CASE WHEN MIN IS NULL THEN '0' ELSE MIN END AS MIN,
                          CASE WHEN MAX IS NULL THEN '0' ELSE MAX END AS MAX
                        from
                          "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."T_DICTIONARY_ITEM"
                        except
                        select
                          DICTIONARY_ID,
                          KEYWORD,
                          TYPE_ID,
                          TEXT_ID,
                          IS_Q_LEVEL,
                          DICTIONARY_DATAPOINT.SORT,
                          IS_NEW,
	                      FIRST_WAVE,
                          CCP_EXPRESSION,
                          EXTERNALKEY,
                          MEMRI_DEFINITION,
                          PERSISTENT_ID,
                          ACCESS_CODE,
                          DISPLAY_TYPE,
                          DELIVERABLE_ID,
                          CASE WHEN MIN IS NULL THEN '0' ELSE MIN END AS MIN,
                          CASE WHEN MAX IS NULL THEN '0' ELSE MAX END AS MAX
                        from
                          "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM"
                           left join "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_DATAPOINT" on DICTIONARY_ITEM.ID = DICTIONARY_DATAPOINT.DICTIONARY_ITEM_ID
                           left join "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM_RANGE" on DICTIONARY_ITEM.ID = DICTIONARY_ITEM_RANGE.DICTIONARY_ITEM_ID
                        where
                          DICTIONARY_ITEM.DICTIONARY_ID = (
                            select
                              ID
                            from
                              "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"
                          )
                      ) as DICTIONARY_ITEM_CHANGED;

      dictionary_exists boolean;
      cursor_dictionary_exists
            cursor
                for
                  select
                    exists(
                      select
                        *
                      from
                        "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY"
                      where
                        ID =(
                          select
                            ID
                          from
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"
                        )
                        and STUDY_ID = (
                          select
                            STUDY_ID
                          from
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"
                        )
                        and DP_VERSIONID = (
                          select
                            DB_VERSIONID
                          from
                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."VW_DICTIONARY_STG"
                        )
                    ) as DICTIONARY_EXISTS;

      dictionary_range := '
                delete from
                  "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM_RANGE"
                where
                  dictionary_item_id in(
                    select
                      id
                    from
                      "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM"
                    where
                      DICTIONARY_ID =';
      dictionary_datapoint := '
                delete from
                  "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_DATAPOINT"
                where
                  dictionary_item_id in(
                    select
                      id
                    from
                      "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM"
                    where
                      DICTIONARY_ID =';

       dictionary_item := '
                delete from
                  "DEV_DATAMART_DB"."DEV_DICTIONARY"."DICTIONARY_ITEM"
                where
                   DICTIONARY_ID =';
     dictionary_id integer ;
     dictionary_id_cursor cursor for select ID from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VW_DICTIONARY_STG;

    begin
      open cursor_dictionary_exists ;
      fetch cursor_dictionary_exists into dictionary_exists;

      open cursor_dictionary_item_changed ;
      fetch cursor_dictionary_item_changed into dictionary_item_changed;

      open cursor_dictionary_item_changed_reverse ;
      fetch cursor_dictionary_item_changed into dictionary_item_changed_reverse;

      open dictionary_id_cursor;
      fetch dictionary_id_cursor into dictionary_id;

      dictionary_range := dictionary_range || dictionary_id ||');';
      dictionary_datapoint := dictionary_datapoint || dictionary_id ||');';
      dictionary_item := dictionary_item || dictionary_id || ';';

      if (dictionary_exists = true) then
           if (dictionary_item_changed = true or dictionary_item_changed_reverse=true) then

                  execute immediate :dictionary_range;
                  execute immediate :dictionary_datapoint;
                  execute immediate :dictionary_item;

                  return object_construct('status_code', 1,
                                          'message','Dictionary Exists. Dictionary Tables Cleared'
                                          );
           else
                  return object_construct('status_code', 0,
                                          'message','Dictionary Exists. Dictionary Tables unchanged'
                                          );


            end if;
      else
            return object_construct('status_code', 1,
                                          'message','Dictionary Doesn\'t Exists.'
                                          );


      end if;



    exception
          when statement_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when expression_error then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);
          when other then
            return object_construct('status_code', '-1',
                                    'sql_code', sqlcode,
                                    'sql_err_message', sqlerrm,
                                    'sql_state', sqlstate);

    end;


  $$;














