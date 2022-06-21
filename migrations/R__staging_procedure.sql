USE SCHEMA SCHEMA_CHANGE_TEST;

create or replace procedure SP_DATA_CLEANING()
   returns varchar
   language sql
   as
   $$
      declare

            trim_label_leading_trailing_spaces := '
                UPDATE 
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                SET 
                    FULL_LABEL = TRIM(FULL_LABEL), 
                    SUPER = TRIM(SUPER), 
                    CATEGORY = TRIM(CATEGORY), 
                    DETAIL1 = TRIM(DETAIL1), 
                    DETAIL2 = TRIM(DETAIL2), 
                    DETAIL3 = TRIM(DETAIL3), 
                    DETAIL4 = TRIM(DETAIL4), 
                    TIMEPERIOD = TRIM(TIMEPERIOD)';


            replace_label_apostrophe_with_correct_one := '
                UPDATE 
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                SET 
                    FULL_LABEL = REGEXP_REPLACE(FULL_LABEL,\'[‘’]\',\'\'\'\'),
                    SUPER = REGEXP_REPLACE(SUPER,\'[‘’]\',\'\'\'\'), 
                    CATEGORY = REGEXP_REPLACE(CATEGORY,\'[‘’]\',\'\'\'\'), 
                    DETAIL1 = REGEXP_REPLACE(DETAIL1,\'[‘’]\',\'\'\'\'), 
                    DETAIL2 = REGEXP_REPLACE(DETAIL2,\'[‘’]\',\'\'\'\'), 
                    DETAIL3 = REGEXP_REPLACE(DETAIL3,\'[‘’]\',\'\'\'\'), 
                    DETAIL4 = REGEXP_REPLACE(DETAIL4,\'[‘’]\',\'\'\'\'), 
                    TIMEPERIOD = REGEXP_REPLACE(TIMEPERIOD,\'[‘’]\',\'\'\'\')';

                    
            replace_label_hypen_with_correct_one :=  '
                UPDATE 
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG"
                SET 
                    FULL_LABEL = REPLACE(FULL_LABEL,\'—\',\'-\'),
                    SUPER =  REPLACE(SUPER,\'—\',\'-\'),
                    CATEGORY =  REPLACE(CATEGORY,\'—\',\'-\'),
                    DETAIL1 =  REPLACE(DETAIL1,\'—\',\'-\'),
                    DETAIL2 =  REPLACE(DETAIL2,\'—\',\'-\'), 
                    DETAIL3 =  REPLACE(DETAIL3,\'—\',\'-\'), 
                    DETAIL4 =  REPLACE(DETAIL4,\'—\',\'-\'),
                    TIMEPERIOD =  REPLACE(TIMEPERIOD,\'—\',\'-\')';

    
            remove_spaces_between_label_text :=  '             
                UPDATE 
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                SET 
                    FULL_LABEL = REGEXP_REPLACE(FULL_LABEL, \' +\'\, \' \'),
                    SUPER = REGEXP_REPLACE(SUPER, \' +\'\, \' \'),
                    CATEGORY = REGEXP_REPLACE(CATEGORY, \' +\'\, \' \'),
                    DETAIL1 = REGEXP_REPLACE(DETAIL1, \' +\'\, \' \'),
                    DETAIL2 = REGEXP_REPLACE(DETAIL2, \' +\'\, \' \'),
                    DETAIL3 = REGEXP_REPLACE(DETAIL3, \' +\'\, \' \'),
                    DETAIL4 = REGEXP_REPLACE(DETAIL4, \' +\'\, \' \'),
                    TIMEPERIOD = REGEXP_REPLACE(TIMEPERIOD, \' +\'\, \' \')';


            remove_break_line :=  '
                UPDATE 
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                SET 
                    FULL_LABEL = REPLACE(FULL_LABEL,\'\n\',\'\'),
                    SUPER = REPLACE(SUPER,\'\n\',\'\'), 
                    CATEGORY = REPLACE(CATEGORY,\'\n\',\'\'),
                    DETAIL1 = REPLACE(DETAIL1,\'\n\',\'\'), 
                    DETAIL2 = REPLACE(DETAIL2,\'\n\',\'\'),
                    DETAIL3 = REPLACE(DETAIL3,\'\n\',\'\'),
                    DETAIL4 = REPLACE(DETAIL4,\'\n\',\'\'), 
                    TIMEPERIOD = REPLACE(TIMEPERIOD,\'\n\',\'\')';
                    
           
            remove_copyright_record_without_full_label := '
                DELETE FROM 
                    "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."COPYRIGHT_STG" 
                WHERE 
                    TRIM(VALUE) = \'\' OR VALUE IS NULL';   
                  
                   
            remove_footnote_datapoint_based_on_existence_on_dictionary_table:= '
                DELETE FROM 
                     "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."FOOTNOTE_STG" AS FOOTNOTE_STAGE 
                WHERE 
                     FOOTNOTE_STAGE.CCP NOT IN (
                        SELECT 
                            DATAMART_DATAPOINT.CCP_EXPRESSION 
                        FROM 
                            "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_DATAPOINT" AS DATAMART_DATAPOINT
                        )'; 
                  
             replace_double_quotation :='
                    UPDATE "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG"
                SET 
                    FULL_LABEL = REGEXP_REPLACE(FULL_LABEL,\'[”“]\',\'"\'),
                    SUPER = REGEXP_REPLACE(SUPER,\'[”“]\',\'"\'), 
                    CATEGORY = REGEXP_REPLACE(CATEGORY,\'[”“]\',\'"\'),
                    DETAIL1 = REGEXP_REPLACE(DETAIL1,\'[”“]\',\'"\'), 
                    DETAIL2 = REGEXP_REPLACE(DETAIL2,\'[”“]\',\'"\'),
                    DETAIL3 = REGEXP_REPLACE(DETAIL3,\'[”“]\',\'"\'),
                    DETAIL4 = REGEXP_REPLACE(DETAIL4,\'[”“]\',\'"\'), 
                    TIMEPERIOD = REGEXP_REPLACE(TIMEPERIOD,\'[”“]\',\'"\')';

       
      begin

            execute immediate :trim_label_leading_trailing_spaces;
            execute immediate :replace_label_apostrophe_with_correct_one;
            execute immediate :replace_label_hypen_with_correct_one;
            execute immediate :remove_spaces_between_label_text;
            execute immediate :remove_break_line;
            execute immediate :remove_copyright_record_without_full_label;
            execute immediate :remove_footnote_datapoint_based_on_existence_on_dictionary_table;
            execute immediate :replace_double_quotation;  
                  
            return object_construct('status_code', '1',
                                    'message','All the data cleaing is done sucessfully.'
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
   
   
create or replace procedure SP_TRUNCATE_STAGING_TABLES()
  returns integer
  language sql
  as
  $$

    declare
        truncate_study := 'truncate table "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG"';
        truncate_dictionary := 'truncate table "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG"';
        truncate_suppression := 'truncate table "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."SUPPRESSION_STG"';
        truncate_weight := 'truncate table "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."WEIGHT_STG"';
        truncate_attribute := 'truncate table "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."ATTRIBUTE_STG"';

    begin
     execute immediate :truncate_study;
     execute immediate :truncate_dictionary;
     execute immediate :truncate_suppression;
     execute immediate :truncate_weight;
     execute immediate :truncate_attribute;

     return object_construct('status_code', '1',
                             'message', 'Tables truncated'
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
  
  
  create 
or replace procedure SP_VALIDATE_STAGING_DB(date_partition varchar,date_time_partition varchar) 
  returns varchar 
  language sql 
  as 
  $$ 
      declare 
          is_unqiue_full_label boolean;
          is_ccp_keyword_validation integer;
          is_immediate_parent_display_type_validation integer;
          is_dictionary_required_field integer;
          is_fusion_study_required_field integer;
          is_all_study_required_field integer;
          is_suppression_required_field integer;
          is_study_weight_required_field integer;
          is_memri_label_ccp_validation integer;
          is_full_label_ccp_validation integer; 
          is_attribute_required_field integer;
          is_both_persistentid_hierarchialtext_empty integer;
          parent_is_empty boolean;
          parent_exists integer;
          study_type_id integer;
          study_code varchar;
          
          copy_into_query varchar;
          validation_query varchar;

           begin
           
        -----get study_code-----
            
            select study_code into : study_code from (select study_code from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG");
           
        -----no duplicate full_label(replacing spaces)-----

            select is_unique_full_label into : is_unqiue_full_label from (select (select (select count(distinct(replace(full_label, ' '))) as full_label_excluding_space from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG")
                = (select count(*) as filerecordcount from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG")) as is_unique_full_label);


        -----one ccp can have multiple keyword but one keyword should only have one ccp------  
            
            select count(*) into :is_ccp_keyword_validation from (select keyword, count(distinct ccp) as distinct_ccp from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                group by keyword having distinct_ccp > 1);
    


        -----immediate parent level should not have more than one display_type.----


            select 
                display_type_count into : is_immediate_parent_display_type_validation 
                from 
                (
                    with arr_full_label as (
                    select 
                        full_label, 
                        cast(qlevel as int) as qlevel, 
                        display_type, 
                        split(full_label, '|') as arr_full_label, 
                        array_slice(arr_full_label, 0, qlevel) as arr_full_label_upto_qlevel, 
                        array_to_string(
                        arr_full_label_upto_qlevel, '| '
                        ) as full_label_upto_qlevel 
                    from 
                        "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG"
                    ), 
                    more_than_ond_display_type as(
                    select 
                        full_label_upto_qlevel, 
                        count(full_label_upto_qlevel) as full_label_upto_qlevel_count 
                    from 
                        (
                        select 
                            distinct full_label_upto_qlevel, 
                            qlevel, 
                            display_type 
                        from 
                            arr_full_label
                        ) 
                    group by 
                        full_label_upto_qlevel 
                    having 
                        full_label_upto_qlevel_count > 1
                    ) 
                    select 
                    count(*) as display_type_count 
                    from 
                    more_than_ond_display_type
                );


        ------dictionary_required_field------  

            select count(*) into : is_dictionary_required_field from (select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" where
                d_study_id is null or d_version_id is null or display_type is null or ccp is null or keyword is null or sort is null or qlevel is null or memri_definition is null or full_label is null);



        ---------------study_required_field---------------
          
           ------get_study_type_id----

            select look_up_view_study_type.id as study_type_id into : study_type_id from DEV_DATAMART_DB.SCHEMA_CHNAGE_DATAMART.VW_STUDY_TYPE as look_up_view_study_type 
                join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as stage_study on stage_study.study_type = look_up_view_study_type.study_type;
                    
           -------fusion_study_validation-----

            select count(*) into : is_fusion_study_required_field from (select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" where 
                study_id is null or study_type is null or study_release is null or study_name is null or study_code is null or year is null or dp_version_id is null or study_family is null or source_provider is null or trend_family is null or parent_study_id is null );


           -------all_study_validation-----

            select count(*) into : is_all_study_required_field from ( select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" where 
                study_id is null or study_type is null or study_release is null or study_name is null or study_code is null or year is null or dp_version_id is null or study_family is null or source_provider is null or trend_family is null);


        ---------suppression_file_validation-----------      
              
            select count(*) into : is_suppression_required_field from (select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."SUPPRESSION_STG" where keyword is null);  

        ---------study_weight_file_validation-----------      

            select count(*) into : is_study_weight_required_field from (select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."WEIGHT_STG" where 
                dp_study_id is null or weight_type is null or extension is null or multiplier is null or divisor is null);

        ---------duplicate_memri_label_based_on_ccp-----------      

              select count(*) into : is_memri_label_ccp_validation from (select super, category, detail1, detail2, detail3, detail4, timeperiod, count(distinct ccp) as ccp_distinct_count, count(ccp) as ccp_count from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                group by super, category, detail1, detail2, detail3, detail4, timeperiod having ccp_count <> ccp_distinct_count);

                                
        ---------duplicate_full_label_based_on_ccp-----------      

            select count(*) into : is_full_label_ccp_validation from (select full_label, count(distinct ccp) as ccp_distinct_count, count(ccp) as ccp_count from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" 
                group by full_label having ccp_count <> ccp_distinct_count);


                    
        ---------attribute_required_field-----------      
                  
            select count(*) into : is_attribute_required_field from (select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."ATTRIBUTE_STG" where 
                attribute_type is null or attribute_value is null);
                       
                           
        ---------persistent_id_hierarcy_text_validation-----------      
                  
            select count(*) into : is_both_persistentid_hierarchialtext_empty from (select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."ATTRIBUTE_STG" where 
                hierarchy_text is null and persistent_id is null);
                  
                  
            select equal_null(PARENT_STUDY_ID, null) into : parent_is_empty from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG;
                if (parent_is_empty = False) then 
                    select count(*) into : parent_exists from (select ID from DEV_DATAMART_DB.SCHEMA_CHNAGE_DATAMART.STUDY where 
                    ID in (select PARENT_STUDY_ID from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".STUDY_STG));
                    if (parent_exists = 0) then 
                        return object_construct('status_code', '1', 'message', 'Validation failed for fusion_parent_exist.');
                    end if;
                end if;
                  
            if (is_unqiue_full_label = false) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_duplicate_full_label';
                set validation_query := 'select STUDY_STG.study_name as study_name, STUDY_STG.Study_code as study_code,STUDY_STG.dp_version_id as dp_version_id,full_label, count(replace(full_label, \' \')) as full_label_count  from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" as DICTIONARY_ITEM_STG 
                                            join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on DICTIONARY_ITEM_STG.D_STUDY_ID = STUDY_STG.DP_STUDY_ID  group by full_label, study_name, study_code, dp_version_id having full_label_count > 1';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                    ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for unique_full_label.');
            end if;

            if (is_ccp_keyword_validation != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_keyword_ccp_validation';
                set validation_query := 'with output_keyword_ccp as (select STUDY_STG.study_name as study_name, STUDY_STG.Study_code as study_code,STUDY_STG.dp_version_id as dp_version_id,keyword,count(distinct ccp) as distinct_ccp from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" as DICTIONARY_ITEM_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on DICTIONARY_ITEM_STG.D_STUDY_ID = STUDY_STG.DP_STUDY_ID group by keyword,study_name, study_code, dp_version_id having distinct_ccp > 1)
                                            select study_name,study_code, dp_version_id,full_label,output_keyword_ccp.keyword,distinct_ccp from output_keyword_ccp join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" on output_keyword_ccp.keyword = DICTIONARY_ITEM_STG.keyword';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                                        ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for ccp_keyword.');
            end if;

            if (is_immediate_parent_display_type_validation != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_immediate_parent_display_type_validation';
                set validation_query := 'with arr_full_label as (select STUDY_STG.study_name as study_name, STUDY_STG.Study_code as study_code,STUDY_STG.dp_version_id as dp_version_id ,full_label, cast(qlevel as int) as qlevel, display_type, 
                                            split(full_label, \'|\') as arr_full_label, array_slice(arr_full_label, 0, qlevel) as arr_full_label_upto_qlevel, array_to_string(arr_full_label_upto_qlevel, \'| \') as full_label_upto_qlevel from 
                                            "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" as DICTIONARY_ITEM_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on DICTIONARY_ITEM_STG.D_STUDY_ID = STUDY_STG.DP_STUDY_ID), 
                                             more_than_ond_display_type as(select study_name,study_code,dp_version_id,full_label_upto_qlevel, count(full_label_upto_qlevel) as full_label_upto_qlevel_count from (
                                             select distinct full_label_upto_qlevel,qlevel,display_type,study_name,study_code,dp_version_id from arr_full_label) group by full_label_upto_qlevel,study_name,study_code,dp_version_id having full_label_upto_qlevel_count > 1) 
                                             select * from more_than_ond_display_type';

                set copy_into_query :='COPY INTO' || copy_into_query ||
                  ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';

                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for immediate_parent_display_type.');
            end if;

            if (is_dictionary_required_field != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_dictionary_required_field_validation';
                set validation_query := 'select STUDY_STG.study_name, STUDY_STG.Study_code ,DICTIONARY_ITEM_STG.* from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" as DICTIONARY_ITEM_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on DICTIONARY_ITEM_STG.D_STUDY_ID = STUDY_STG.DP_STUDY_ID where
                                            d_study_id is null or d_version_id is null or display_type is null or ccp is null or keyword is null or sort is null or qlevel is null or memri_definition is null or full_label is null';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                    ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';

                execute immediate :copy_into_query;          
                return object_construct('status_code', 0,'message', 'Validation failed for dictionary_required_field.');
            end if;
                    
            if (is_study_weight_required_field != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_study_weight_required_field_validation';
                set validation_query := 'select STUDY_STG.study_name, STUDY_STG.Study_code ,STUDY_STG.dp_version_id,WEIGHT_STG.* from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."WEIGHT_STG" as WEIGHT_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on WEIGHT_STG.DP_STUDY_ID = STUDY_STG.DP_STUDY_ID where 
                                            WEIGHT_STG.dp_study_id is null or weight_type is null or extension is null or multiplier is null or divisor is null';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                    ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for study_weight_required_field.');
            end if;

            if (is_memri_label_ccp_validation != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_duplicate_memri_label__based_on_ccp';
                set validation_query := 'select STUDY_STG.study_name as study_name, STUDY_STG.Study_code as study_code,STUDY_STG.dp_version_id as dp_version_id , super, category, detail1, detail2, detail3, detail4, timeperiod, count(distinct ccp) as ccp_distinct_count, count(ccp) as ccp_count from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" as DICTIONARY_ITEM_STG  
                                            join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on DICTIONARY_ITEM_STG.D_STUDY_ID = STUDY_STG.DP_STUDY_ID group by super, category, detail1, detail2, detail3, detail4, timeperiod, study_name, study_code, dp_version_id having ccp_count <> ccp_distinct_count';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                    ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for duplicate_memri_label_vs_ccp.');
            end if;

            if (is_full_label_ccp_validation != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_duplicate_fulllabel_based_on_ccp';
                set validation_query := 'select STUDY_STG.study_name as study_name, STUDY_STG.Study_code as study_code,STUDY_STG.dp_version_id as dp_version_id, full_label, count(distinct ccp) as ccp_distinct_count, count(ccp) as ccp_count from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."DICTIONARY_ITEM_STG" as DICTIONARY_ITEM_STG
                                            join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on DICTIONARY_ITEM_STG.D_STUDY_ID = STUDY_STG.DP_STUDY_ID group by full_label, study_name, study_code, dp_version_id having ccp_count <> ccp_distinct_count';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                    ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for duplicate_full_label_vs_ccp.');
            end if; 
                        
            if (is_attribute_required_field != 0) then
     
                set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_attribute_required_field_validation';
                set validation_query := 'select STUDY_STG.study_name, STUDY_STG.Study_code,ATTRIBUTE_STG.*  from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."ATTRIBUTE_STG" as ATTRIBUTE_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on ATTRIBUTE_STG.DP_STUDY_ID = STUDY_STG.DP_STUDY_ID where 
                                            attribute_type is null or attribute_value is null';
     
                set copy_into_query :='COPY INTO' || copy_into_query ||
                     ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                execute immediate :copy_into_query;
                return object_construct('status_code', 0,'message', 'Validation failed for attribute_required_field.');
            end if;

            if (is_both_persistentid_hierarchialtext_empty != 0) then
     
                    set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_both_persistentid_hierarchytext_null_validation';
                    set validation_query := 'select  STUDY_STG.study_name, STUDY_STG.Study_code,ATTRIBUTE_STG.* from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."ATTRIBUTE_STG" as ATTRIBUTE_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on ATTRIBUTE_STG.DP_STUDY_ID = STUDY_STG.DP_STUDY_ID where 
                                                hierarchy_text is null and persistent_id is null';
     
                    set copy_into_query :='COPY INTO' || copy_into_query ||
                        ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                    execute immediate :copy_into_query;
                    return object_construct('status_code', 0,'message', 'Validation failed for both_persistentid_hierarchy_field_empty.');
            end if;

            if (study_type_id = 51) then

                    if (is_fusion_study_required_field != 0) then
     
                        set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_fusion_study_required_field_validation';
                        set validation_query := ' select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" where 
                                                    study_id is null or study_type is null or study_release is null or study_name is null or study_code is null or year is null or dp_version_id is null or study_family is null or source_provider is null or trend_family is null or parent_study_id is null';
     
                        set copy_into_query :='COPY INTO' || copy_into_query ||
                            ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';

                        execute immediate :copy_into_query;
                        return object_construct('status_code', 0,'message', 'Validation failed for fusion_study_required_field.');
                    end if;

                    if (is_suppression_required_field != 0) then
     
                        set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_suppression_required_field_validation';
                        set validation_query := 'select STUDY_STG.study_name, STUDY_STG.Study_code ,STUDY_STG.dp_version_id, SUPPRESSION_STG.* from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."SUPPRESSION_STG" as SUPPRESSION_STG join "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" as STUDY_STG on SUPPRESSION_STG.DICTIONARY_ID = STUDY_STG.DICTIONARY_ID where keyword is null  ';
     
                        set copy_into_query :='COPY INTO' || copy_into_query ||
                            ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                        execute immediate :copy_into_query;
                        return object_construct('status_code', 0,'message', 'Validation failed for suppression_required_field.');
                    end if;
    
                    return object_construct('status_code', 1,'message', 'Validation for fusion dictionary, fusion study and fusion suppression has passed successfully.');

            end if;        

            if (study_type_id = 50 or study_type_id = 52 or study_type_id = 54) then

                if (is_all_study_required_field != 0) then
     
                        set copy_into_query := '@"DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".VALIDATION_EXCEPTION_STAGE/'||:date_partition||'/'||:date_time_partition||'/'||:study_code||'_all_study_required_field_validation';
                        set validation_query := ' select * from "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST"."STUDY_STG" where 
                                                       study_id is null or study_type is null or study_release is null or study_name is null or study_code is null or year is null or dp_version_id is null or study_family is null or source_provider is null or trend_family is null';
     
                        set copy_into_query :='COPY INTO' || copy_into_query ||
                            ' FROM (' || validation_query ||') file_format = "DEV_DATAMART_DB"."SCHEMA_CHANGE_TEST".MDM_CSV HEADER = TRUE DETAILED_OUTPUT = TRUE overwrite = true';
    
                        execute immediate :copy_into_query;
                        return object_construct('status_code', 0,'message', 'Validation failed for study_required_field.');
                end if; 
                          
                return object_construct('status_code', 1,'message', 'All the validation for dictionary and study has passed successfully.');

            end if;

            return object_construct('status_code', 0,'message', 'No rows are present in the tables for validation.');
            
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
            

