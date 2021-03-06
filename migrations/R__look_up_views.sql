USE SCHEMA SCHEMA_CHNAGE_DATAMART;

create or replace view VW_ACTION_TYPE(
	ID,
	NAME,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  NAME, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."ACTION_TYPE";
  
create or replace view VW_ATTRIBUTE(
	ID,
	NAME,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  NAME, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."ATTRIBUTE";
  
  
create or replace view VW_ATTRIBUTE_TYPE(
	ATTRIBUTE_TYPE_ID,
	ATTRIBUTE_TYPE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  NAME, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."ATTRIBUTE_TYPE";
  
 
create or replace view VW_DATA_GROUP_TYPE(
	DATA_GROUP_TYPE_ID,
	DATA_GROUP_TYPE,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  DATA_GROUP_TYPE_ID, 
  DATA_GROUP_TYPE, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS from "DEV_MDM_DB"."DICTIONARY"."DATA_GROUP_TYPE";
  
  
create or replace view VW_DATA_TYPE(
	DATA_TYPE_ID,
	DATA_TYPE,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  DATA_TYPE_ID, 
  DATA_TYPE, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."DATA_TYPE";
  
  
create or replace view VW_DELIVERABLE(
	DELIVERABLE_ID,
	DELIVERABLE,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  NAME, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."DELIVERABLE";
  

create or replace view VW_DICTIONARY_ITEM_TYPE(
	ID,
	TYPE,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS,
	DELIVERABLE_ID,
	DELIVERABLE
) as 
select 
  DICTIONARY_ITEM_TYPE.ID, 
  DICTIONARY_ITEM_TYPE.TYPE, 
  DICTIONARY_ITEM_TYPE.ACTIVE, 
  DICTIONARY_ITEM_TYPE.CRT_TS, 
  DICTIONARY_ITEM_TYPE.CRT_BY, 
  DICTIONARY_ITEM_TYPE.UPD_BY, 
  DICTIONARY_ITEM_TYPE.UPD_TS, 
  DICTIONARY_ITEM_TYPE.DELIVERABLE_ID, 
  DELIVERABLE.NAME 
from 
  "DEV_MDM_DB"."DICTIONARY"."DICTIONARY_ITEM_TYPE" 
  join "DEV_MDM_DB"."DICTIONARY"."DELIVERABLE" on DELIVERABLE.ID = DICTIONARY_ITEM_TYPE.DELIVERABLE_ID;
  
  
create or replace view VW_DICTIONARY_SOURCE(
	ID,
	SOURCE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  SOURCE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."DICTIONARY_SOURCE";
 

create or replace view VW_MASTER_LOOKUP(
	ID,
	PERSISTENT_ID,
	HIERARCHY_TEXT,
	EXTERNAL_KEYWORD,
	IS_LEAF,
	IS_DEFINITION,
	IS_STANDARD
) as 
SELECT 
  ID, 
  PERSISTENT_ID, 
  HIERARCHY_TEXT, 
  EXTERNAL_KEYWORD, 
  IS_LEAF, 
  IS_DEFINITION, 
  IS_STANDARD 
FROM 
  DEV_MDM_DB.DICTIONARY.MASTER_LOOKUP;
  
  
create or replace view VW_OBJECT_TYPE(
	ID,
	NAME,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS,
	OBJECT_TABLE
) as 
select 
  ID, 
  NAME, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS,
  OBJECT_TABLE
from 
  "DEV_MDM_DB"."DICTIONARY"."OBJECT_TYPE";
 
 
 
create or replace view VW_SOURCE(
	SOURCE_ID,
	SOURCE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as
SELECT 
ID AS SOURCE_ID,
	SOURCE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
    FROM DEV_MDM_DB.DICTIONARY.DICTIONARY_SOURCE;
    

create or replace view VW_SOURCE_PROVIDER(
	ID,
	SOURCE_PROVIDER,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  SOURCE_PROVIDER, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."SOURCE_PROVIDER";    
  
  
create or replace view VW_STATUS(
	ID,
	STATUS,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS,
	DESCRIPTION
) as 
SELECT 
  ID, 
  STATUS, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS, 
  DESCRIPTION 
FROM 
  DEV_MDM_DB.DICTIONARY.DICTIONARY_STATUS;
  

create or replace view VW_STUDY_FAMILY(
	ID,
	STUDY_FAMILY,
	SOURCE_PROVIDER_ID,
	SORT,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS,
	IS_CUSTOMQUESTION
) as 
select 
  ID, 
  STUDY_FAMILY, 
  SOURCE_PROVIDER_ID, 
  SORT, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS, 
  IS_CUSTOMQUESTION 
from 
  "DEV_MDM_DB"."DICTIONARY"."STUDY_FAMILY";   
  
 

create or replace view VW_STUDY_RELEASE(
	ID,
	STUDY_RELEASE,
	SORT,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  STUDY_RELEASE, 
  SORT, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."STUDY_RELEASE";
  
  
create or replace view VW_STUDY_TYPE(
	ID,
	STUDY_TYPE,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS,
	SORT
) as 
select 
  ID, 
  STUDY_TYPE, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS, 
  SORT 
from 
  "DEV_MDM_DB"."DICTIONARY"."STUDY_TYPE";
  

create or replace view VW_TREND_FAMILY(
	ID,
	TREND_FAMILY,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  TREND_FAMILY, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."TREND_FAMILY";
  
  
create or replace view VW_USER_INFO(
	ID,
	FIRST_NAME,
	LAST_NAME,
	EMAIL,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  FIRST_NAME, 
  LAST_NAME, 
  EMAIL, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."USER_INFO";
  
  
create or replace view VW_WEIGHT_EXTENSION(
	ID,
	WEIGHT_EXTENSION,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  WEIGHT_EXTENSION, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."WEIGHT_EXTENSION"; 
  
  
create or replace view VW_WEIGHT_TYPE(
	ID,
	WEIGHT_TYPE,
	ACTIVE,
	CRT_TS,
	CRT_BY,
	UPD_BY,
	UPD_TS
) as 
select 
  ID, 
  WEIGHT_TYPE, 
  ACTIVE, 
  CRT_TS, 
  CRT_BY, 
  UPD_BY, 
  UPD_TS 
from 
  "DEV_MDM_DB"."DICTIONARY"."WEIGHT_TYPE";
  
 
 
  
  
  


    
    
  
  
  
 
  
