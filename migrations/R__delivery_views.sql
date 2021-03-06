USE SCHEMA SCHEMA_CHNAGE_DATAMART;

create or replace view VW_SUPPRESSION(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	PARENT_STUDY_ID,
	PARENT_STUDY_CODE,
	DICTIONARY_ID,
	DP_VERSION_ID,
	PERSISTENT_ID,
	DELIVERABLE_ID
) as

select 
  DICTIONARY.STUDY_ID,
  STUDY.DP_STUDYID,
  STUDY.STUDY_CODE,
  STUDY.PARENT_STUDY_ID,
  PARENT_STUDY.STUDY_CODE as PARENT_STUDY_CODE,
  DICTIONARY_ID, 
  DICTIONARY.DP_VERSIONID as DP_VERSION_ID,
  PERSISTENT_ID, 
  DELIVERABLE_ID 
from 
  "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_SUPPRESSION" 
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" on DICTIONARY_SUPPRESSION.DICTIONARY_ID = DICTIONARY.ID 
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" on STUDY.ID = DICTIONARY.STUDY_ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" as PARENT_STUDY on PARENT_STUDY.ID = STUDY.PARENT_STUDY_ID;
  
  
create or replace view VW_INSIGHT_DATA(
	DICTIONARY_ID,
	STUDY_ID,
	STUDY_CODE,
	DP_STUDYID,
	DP_VERSIONID,
	VERSION,
	DICTIONARY_ITEM_ID,
	FULL_LABEL,
	DELIVERABLE_ID,
	DELIVERABLE,
	ACCESS_CODE,
	DISPLAY_TYPE,
	QLEVEL,
	FIRST_WAVE,
	IS_NEW,
	MIN,
	MAX,
	MIDPOINT,
	MALE_MIDPOINT,
	FEMALE_MIDPOINT,
	KEYWORD,
	PERSISTENT_ID,
	CCP,
	EXTERNAL_KEY,
	SORT,
	DATA_TYPE_ID,
	DATA_TYPE,
	MEMRI_DEFINITION
) as(

WITH RECURSIVE DICTIONARY_ITEM_PARENT(
  INDENT, INDENT_TYPE_ID, INDENT_DISPLAY_TYPE, 
  ID, PARENT_ITEM_ID, TEXT, 
  TYPE_ID, TYPE, DELIVERABLE_ID, ACCESS_CODE, 
  DISPLAY_TYPE, FIRST_WAVE, IS_NEW, 
  DICTIONARY_ID, KEYWORD,PERSISTENT_ID
) AS (
  SELECT 
    '' AS INDENT, 
    '' AS INDENT_TYPE_ID, 
    '' AS INDENT_DISPLAY_TYPE, 
    DICTIONARY_ITEM.ID, 
    TO_NUMBER(PARENT_ITEM_ID) AS PARENT_ITEM_ID, 
    TEXT, 
    TO_NUMBER(TYPE_ID) AS TYPE_ID, 
    TYPE, 
    DICTIONARY_ITEM_TYPE.DELIVERABLE_ID, 
    ACCESS_CODE, 
    DISPLAY_TYPE, 
    FIRST_WAVE, 
    IS_NEW, 
    DICTIONARY_ID, 
    KEYWORD,
    PERSISTENT_ID
  FROM 
    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ITEM_TEXT" AS ITEM_TEXT ON DICTIONARY_ITEM.TEXT_ID = ITEM_TEXT.ID 
    JOIN DEV_DATAMART_DB.DEV_DICTIONARY.VW_DICTIONARY_ITEM_TYPE as DICTIONARY_ITEM_TYPE ON DICTIONARY_ITEM.TYPE_ID = DICTIONARY_ITEM_TYPE.ID 
  WHERE 
    PARENT_ITEM_ID IS NULL 
  UNION ALL 
  SELECT 
    INDENT || DICTIONARY_ITEM_PARENT.TEXT || '|', 
    INDENT_TYPE_ID || DICTIONARY_ITEM_PARENT.TYPE || '|', 
    INDENT_DISPLAY_TYPE || DICTIONARY_ITEM_PARENT.DISPLAY_TYPE || '|', 
    DICTIONARY_ITEM.ID, 
    DICTIONARY_ITEM.PARENT_ITEM_ID, 
    DICTIONARY_ITEM.TEXT, 
    DICTIONARY_ITEM.TYPE_ID, 
    DICTIONARY_ITEM.TYPE, 
    DICTIONARY_ITEM.DELIVERABLE_ID, 
    DICTIONARY_ITEM.ACCESS_CODE, 
    DICTIONARY_ITEM.DISPLAY_TYPE, 
    DICTIONARY_ITEM.FIRST_WAVE, 
    DICTIONARY_ITEM.IS_NEW, 
    DICTIONARY_ITEM.DICTIONARY_ID, 
    DICTIONARY_ITEM.KEYWORD,
    DICTIONARY_ITEM.PERSISTENT_ID
  FROM 
    (
      SELECT 
        DICTIONARY_ITEM.ID, 
        TO_NUMBER(PARENT_ITEM_ID) AS PARENT_ITEM_ID, 
        TEXT, 
        TO_NUMBER(TYPE_ID) AS TYPE_ID, 
        TYPE, 
        DICTIONARY_ITEM_TYPE.DELIVERABLE_ID, 
        ACCESS_CODE, 
        DISPLAY_TYPE, 
        FIRST_WAVE, 
        IS_NEW, 
        DICTIONARY_ID, 
        KEYWORD,
        PERSISTENT_ID
      FROM 
        "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" 
        JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ITEM_TEXT" AS ITEM_TEXT ON DICTIONARY_ITEM.TEXT_ID = ITEM_TEXT.ID 
        JOIN DEV_DATAMART_DB.DEV_DICTIONARY.VW_DICTIONARY_ITEM_TYPE as DICTIONARY_ITEM_TYPE ON DICTIONARY_ITEM.TYPE_ID = DICTIONARY_ITEM_TYPE.ID
    ) AS DICTIONARY_ITEM 
    JOIN DICTIONARY_ITEM_PARENT ON DICTIONARY_ITEM_PARENT.ID = DICTIONARY_ITEM.PARENT_ITEM_ID
), 
INSIGHT AS (
  SELECT 
    DICTIONARY_ID, 
    DICTIONARY.STUDY_ID,
    STUDY.STUDY_CODE,
    DP_STUDYID, 
    DP_VERSIONID AS DP_VERSIONID,
    DICTIONARY.VERSION as VERSION,
    DICTIONARY_ITEM_PARENT.ID as DICTIONARY_ITEM_ID, 
    INDENT || TEXT AS FULL_LABEL, 
    DICTIONARY_ITEM_PARENT.DELIVERABLE_ID,
    VW_DELIVERABLE.DELIVERABLE,
    ACCESS_CODE, 
    INDENT_DISPLAY_TYPE || DISPLAY_TYPE AS DISPLAY_TYPE, 
    FIRST_WAVE, 
    IS_NEW, 
    DICTIONARY_ITEM_RANGE.MIN, 
    DICTIONARY_ITEM_RANGE.MAX, 
    DICTIONARY_ITEM_RANGE.MIDPOINT, 
    DICTIONARY_ITEM_RANGE.MALE_MIDPOINT, 
    DICTIONARY_ITEM_RANGE.FEMALE_MIDPOINT, 
    KEYWORD,
    PERSISTENT_ID,
    DICTIONARY_DATAPOINT.CCP_EXPRESSION, 
    DICTIONARY_DATAPOINT.EXTERNALKEY, 
    DICTIONARY_DATAPOINT.SORT, 
    DICTIONARY_DATAPOINT.DATA_TYPE_ID,
    VW_DATA_TYPE.DATA_TYPE,
    DICTIONARY_DATAPOINT.MEMRI_DEFINITION 
  FROM 
    DICTIONARY_ITEM_PARENT 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_DATAPOINT" ON DICTIONARY_ITEM_PARENT.ID = DICTIONARY_DATAPOINT.DICTIONARY_ITEM_ID 
    LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_RANGE" ON DICTIONARY_ITEM_PARENT.ID = DICTIONARY_ITEM_RANGE.DICTIONARY_ITEM_ID 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" ON DICTIONARY_ITEM_PARENT.DICTIONARY_ID = DICTIONARY.ID 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON DICTIONARY.STUDY_ID = STUDY.ID
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DATA_TYPE" on DICTIONARY_DATAPOINT.DATA_TYPE_ID = VW_DATA_TYPE.DATA_TYPE_ID
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DELIVERABLE" on DICTIONARY_ITEM_PARENT.DELIVERABLE_ID = VW_DELIVERABLE.DELIVERABLE_ID
  WHERE 
    TYPE_ID = '4' 
    AND DICTIONARY_ITEM_PARENT.DELIVERABLE_ID = 1 
) 
SELECT 
  DICTIONARY_ID, 
  STUDY_ID,
  STUDY_CODE,
  DP_STUDYID, 
  DP_VERSIONID,
  VERSION,
  DICTIONARY_ITEM_ID, 
  FULL_LABEL, 
  DELIVERABLE_ID,
  DELIVERABLE,
  ACCESS_CODE, 
  FLATTEN_DISPLAY_TYPE.VALUE :: STRING AS DISPLAY_TYPE, 
  FLATTEN_DISPLAY_TYPE.INDEX + 1 AS QLEVEL, 
  FIRST_WAVE, 
  IS_NEW, 
  MIN, 
  MAX, 
  MIDPOINT, 
  MALE_MIDPOINT, 
  FEMALE_MIDPOINT, 
  KEYWORD,
  PERSISTENT_ID,
  CCP_EXPRESSION, 
  EXTERNALKEY, 
  SORT, 
  DATA_TYPE_ID,
  INITCAP(DATA_TYPE) as DATA_TYPE,
  MEMRI_DEFINITION 
FROM 
  INSIGHT, 
  LATERAL FLATTEN(
    INPUT => SPLIT(DISPLAY_TYPE, '|')
  ) FLATTEN_DISPLAY_TYPE 
WHERE 
  VALUE :: STRING != '0' QUALIFY ROW_NUMBER() OVER (
    PARTITION BY DICTIONARY_ITEM_ID 
    ORDER BY 
      DICTIONARY_ITEM_ID, 
      INDEX DESC
  ) = 1

);


create or replace view VW_MEMRI_DATA(
	DICTIONARY_ID,
	STUDY_ID,
	STUDY_CODE,
	DP_STUDY_ID,
	DP_VERSION_ID,
	DICTIONARY_ITEM_ID,
	DELIVERABLE_ID,
	DELIVERABLE,
	ACCESS_CODE,
	DISPLAY_TYPE,
	QLEVEL,
	FIRST_WAVE,
	IS_NEW,
	MIN,
	MAX,
	MIDPOINT,
	MALE_MIDPOINT,
	FEMALE_MIDPOINT,
	KEYWORD,
	PERSISTENT_ID,
	CCP,
	EXTERNAL_KEY,
	SORT,
	DATA_TYPE_ID,
	DATA_TYPE,
	MEMRI_DEFINITION,
	SUPER,
	CATEGORY,
	DETAIL1,
	DETAIL2,
	DETAIL3,
	DETAIL4,
	TIME_PERIOD
) as(

WITH RECURSIVE DICTIONARY_ITEM_PARENT(
  INDENT, INDENT_TYPE_ID, INDENT_DISPLAY_TYPE,
  ID, PARENT_ITEM_ID, TEXT,
  TYPE_ID, TYPE, DELIVERABLE_ID, ACCESS_CODE,
  DISPLAY_TYPE, FIRST_WAVE, IS_NEW,
  DICTIONARY_ID, KEYWORD, PERSISTENT_ID
) AS (
  SELECT
    '' AS INDENT,
    '' AS INDENT_TYPE_ID,
    '' AS INDENT_DISPLAY_TYPE,
    DICTIONARY_ITEM.ID,
    TO_NUMBER(PARENT_ITEM_ID) AS PARENT_ITEM_ID,
    TEXT,
    TO_NUMBER(TYPE_ID) AS TYPE_ID,
    TYPE,
    DICTIONARY_ITEM_TYPE.DELIVERABLE_ID,
    ACCESS_CODE,
    DISPLAY_TYPE,
    FIRST_WAVE,
    IS_NEW,
    DICTIONARY_ID,
    KEYWORD,
    PERSISTENT_ID
  FROM
    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM"
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ITEM_TEXT" AS ITEM_TEXT ON DICTIONARY_ITEM.TEXT_ID = ITEM_TEXT.ID
    JOIN DEV_DATAMART_DB.DEV_DICTIONARY.VW_DICTIONARY_ITEM_TYPE as DICTIONARY_ITEM_TYPE ON DICTIONARY_ITEM.TYPE_ID = DICTIONARY_ITEM_TYPE.ID
  WHERE
    PARENT_ITEM_ID IS NULL
  UNION ALL
  SELECT
    INDENT || DICTIONARY_ITEM_PARENT.TEXT || '|',
    INDENT_TYPE_ID || DICTIONARY_ITEM_PARENT.TYPE || '|',
    INDENT_DISPLAY_TYPE || DICTIONARY_ITEM_PARENT.DISPLAY_TYPE || '|',
    DICTIONARY_ITEM.ID,
    DICTIONARY_ITEM.PARENT_ITEM_ID,
    DICTIONARY_ITEM.TEXT,
    DICTIONARY_ITEM.TYPE_ID,
    DICTIONARY_ITEM.TYPE,
    DICTIONARY_ITEM.DELIVERABLE_ID,
    DICTIONARY_ITEM.ACCESS_CODE,
    DICTIONARY_ITEM.DISPLAY_TYPE,
    DICTIONARY_ITEM.FIRST_WAVE,
    DICTIONARY_ITEM.IS_NEW,
    DICTIONARY_ITEM.DICTIONARY_ID,
    DICTIONARY_ITEM.KEYWORD,
    DICTIONARY_ITEM.PERSISTENT_ID
  FROM
    (
      SELECT
        DICTIONARY_ITEM.ID,
        TO_NUMBER(PARENT_ITEM_ID) AS PARENT_ITEM_ID,
        TEXT,
        TO_NUMBER(TYPE_ID) AS TYPE_ID,
        TYPE,
        DICTIONARY_ITEM_TYPE.DELIVERABLE_ID,
        ACCESS_CODE,
        DISPLAY_TYPE,
        FIRST_WAVE,
        IS_NEW,
        DICTIONARY_ID,
        KEYWORD,
        PERSISTENT_ID
      FROM
        "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM"
        JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ITEM_TEXT" AS ITEM_TEXT ON DICTIONARY_ITEM.TEXT_ID = ITEM_TEXT.ID
        JOIN DEV_DATAMART_DB.DEV_DICTIONARY.VW_DICTIONARY_ITEM_TYPE as DICTIONARY_ITEM_TYPE ON DICTIONARY_ITEM.TYPE_ID = DICTIONARY_ITEM_TYPE.ID
    ) AS DICTIONARY_ITEM
    JOIN DICTIONARY_ITEM_PARENT ON DICTIONARY_ITEM_PARENT.ID = DICTIONARY_ITEM.PARENT_ITEM_ID
),
MEMRI AS(
  SELECT
    DICTIONARY_ID,
    DICTIONARY.STUDY_ID,
    STUDY.STUDY_CODE,
    DP_STUDYID AS DP_STUDY_ID,
    DP_VERSIONID AS DP_VERSION_ID,
    DICTIONARY_ITEM_PARENT.ID as DICTIONARY_ITEM_ID,
    TYPE_ID,
    INDENT || TEXT AS FULL_LABEL,
    INDENT_TYPE_ID || TYPE AS TYPE,
    DICTIONARY_ITEM_PARENT.DELIVERABLE_ID,
    VW_DELIVERABLE.DELIVERABLE,
    ACCESS_CODE,
    DISPLAY_TYPE,
    FIRST_WAVE,
    IS_NEW,
    DICTIONARY_ITEM_RANGE.MIN,
    DICTIONARY_ITEM_RANGE.MAX,
    DICTIONARY_ITEM_RANGE.MIDPOINT,
    DICTIONARY_ITEM_RANGE.MALE_MIDPOINT,
    DICTIONARY_ITEM_RANGE.FEMALE_MIDPOINT,
    DICTIONARY_DATAPOINT.CCP_EXPRESSION as CCP,
    KEYWORD,
    PERSISTENT_ID,
    DICTIONARY_DATAPOINT.EXTERNALKEY as EXTERNAL_KEY,
    DICTIONARY_DATAPOINT.SORT,
    DICTIONARY_DATAPOINT.DATA_TYPE_ID,
    VW_DATA_TYPE.DATA_TYPE,
    DICTIONARY_DATAPOINT.MEMRI_DEFINITION
  FROM
    DICTIONARY_ITEM_PARENT
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_DATAPOINT" ON DICTIONARY_ITEM_PARENT.ID = DICTIONARY_DATAPOINT.DICTIONARY_ITEM_ID
    LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_RANGE" ON DICTIONARY_ITEM_PARENT.ID = DICTIONARY_ITEM_RANGE.DICTIONARY_ITEM_ID
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" ON DICTIONARY_ITEM_PARENT.DICTIONARY_ID = DICTIONARY.ID
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON DICTIONARY.STUDY_ID = STUDY.ID
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DATA_TYPE" on DICTIONARY_DATAPOINT.DATA_TYPE_ID = VW_DATA_TYPE.DATA_TYPE_ID
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DELIVERABLE" on DICTIONARY_ITEM_PARENT.DELIVERABLE_ID = VW_DELIVERABLE.DELIVERABLE_ID
  WHERE
    DICTIONARY_ITEM_PARENT.DELIVERABLE_ID = 2
  ORDER BY
    DP_STUDY_ID,
    DP_VERSION_ID,
    FULL_LABEL
)



SELECT
  *
FROM
  (
    SELECT
      DICTIONARY_ID,
      STUDY_ID,
      STUDY_CODE,
      DP_STUDY_ID,
      DP_VERSION_ID,
      DICTIONARY_ITEM_ID,
      DELIVERABLE_ID,
      DELIVERABLE,
      ACCESS_CODE,
      NULL AS DISPLAY_TYPE,
      NULL AS QLEVEL,
      FIRST_WAVE,
      IS_NEW,
      MIN,
      MAX,
      MIDPOINT,
      MALE_MIDPOINT,
      FEMALE_MIDPOINT,
      KEYWORD,
      PERSISTENT_ID,
      CCP,
      EXTERNAL_KEY,
      SORT,
      DATA_TYPE_ID,
      INITCAP(DATA_TYPE),
      MEMRI_DEFINITION,
      FLATTEN_TYPE.VALUE :: STRING AS TYPE,
      FLATTEN_LABEL.VALUE :: STRING AS TEXT
    FROM
      MEMRI,
      LATERAL FLATTEN(
        INPUT => SPLIT(FULL_LABEL, '|')
      ) FLATTEN_LABEL,
      LATERAL FLATTEN(
        INPUT => SPLIT(TYPE, '|')
      ) FLATTEN_TYPE
    WHERE
      FLATTEN_LABEL.INDEX = FLATTEN_TYPE.INDEX
  ) PIVOT(
    MAX(TEXT) FOR TYPE IN (
      'Super', 'Category', 'Detail1', 'Detail2',
      'Detail3', 'Detail4', 'TimePeriod'
    )
  ) AS PIVOTED_MEMRI(
    DICTIONARY_ID, STUDY_ID,STUDY_CODE,DP_STUDY_ID,
    DP_VERSION_ID, DICTIONARY_ITEM_ID,
    DELIVERABLE_ID, DELIVERABLE,ACCESS_CODE, DISPLAY_TYPE,
    QLEVEL, FIRST_WAVE, IS_NEW, MIN, MAX,
    MIDPOINT, MALE_MIDPOINT, FEMALE_MIDPOINT,
    KEYWORD, PERSISTENT_ID, CCP, EXTERNAL_KEY, SORT,
    DATA_TYPE_ID, DATA_TYPE,MEMRI_DEFINITION,
    SUPER, CATEGORY, DETAIL1, DETAIL2,
    DETAIL3, DETAIL4, TIME_PERIOD
  )


);

create or replace view VW_FUSION_STUDY(
	DICTIONARY_ID,
	STUDY_ID,
	NAME,
	STUDY_CODE,
	PARENT_STUDY_ID,
	DP_STUDYID,
	STUDY_TYPE_ID,
	IS_PARENT,
	IS_SUPPRESSED,
	FULL_LABEL,
	DELIVERABLE_ID,
	DELIVERABLE,
	ACCESS_CODE,
	DISPLAY_TYPE,
	QLEVEL,
	FIRST_WAVE,
	IS_NEW,
	MIN,
	MAX,
	MIDPOINT,
	MALE_MIDPOINT,
	FEMALE_MIDPOINT,
	KEYWORD,
	CCP,
	EXTERNAL_KEY,
	SORT,
	DATA_TYPE_ID,
	DATA_TYPE,
	MEMRI_DEFINITION
) as  


WITH LATEST_DICTIONARY AS(
  SELECT 
    LATEST_DICTIONARY.ID AS DICTIONARY_ID, 
    STUDY.ID AS STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID 
  FROM 
    (
      SELECT 
        * 
      FROM 
        "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" QUALIFY ROW_NUMBER() OVER (
          PARTITION BY STUDY_ID 
          ORDER BY 
            DP_VERSIONID DESC
        ) = 1
    ) AS LATEST_DICTIONARY 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON STUDY.ID = LATEST_DICTIONARY.STUDY_ID
), 
FUSION_STUDY AS(
  SELECT 
    *,
    LATEST_DICTIONARY.DICTIONARY_ID as FUSION_DICTIONARY_ID,
    FALSE AS IS_PARENT 
  FROM 
    LATEST_DICTIONARY 
  WHERE 
    STUDY_TYPE_ID = 51
), 
FUSION_STUDY_PARENT_DICTIONARY AS (
  SELECT 
    LATEST_DICTIONARY.DICTIONARY_ID,
    FUSION_STUDY.STUDY_ID, 
    FUSION_STUDY.NAME, 
    FUSION_STUDY.STUDY_CODE, 
    FUSION_STUDY.PARENT_STUDY_ID, 
    FUSION_STUDY.DP_STUDYID, 
    FUSION_STUDY.STUDY_TYPE_ID,
    FUSION_STUDY.DICTIONARY_ID as FUSION_DICTIONARY_ID,    
    TRUE AS IS_PARENT 
  FROM 
    FUSION_STUDY 
    JOIN LATEST_DICTIONARY ON FUSION_STUDY.PARENT_STUDY_ID = LATEST_DICTIONARY.STUDY_ID
), 
ALL_FUSION_STUDY AS (
  SELECT 
    * 
  FROM 
    FUSION_STUDY_PARENT_DICTIONARY 
  UNION ALL 
  SELECT 
    * 
  FROM 
    FUSION_STUDY
) 





SELECT
  ALL_FUSION_STUDY.FUSION_DICTIONARY_ID as DICTIONARY_ID,
  ALL_FUSION_STUDY.STUDY_ID,
  ALL_FUSION_STUDY.NAME,
  ALL_FUSION_STUDY.STUDY_CODE,
  ALL_FUSION_STUDY.PARENT_STUDY_ID,
  ALL_FUSION_STUDY.DP_STUDYID,
  ALL_FUSION_STUDY.STUDY_TYPE_ID,
  ALL_FUSION_STUDY.IS_PARENT,
  CASE WHEN VW_SUPPRESSION.PERSISTENT_ID IS NULL THEN FALSE ELSE TRUE END AS IS_SUPPRESSED, 
  VW_INSIGHT_DATA.FULL_LABEL, 
  VW_INSIGHT_DATA.DELIVERABLE_ID, 
  VW_INSIGHT_DATA.DELIVERABLE, 
  VW_INSIGHT_DATA.ACCESS_CODE, 
  VW_INSIGHT_DATA.DISPLAY_TYPE, 
  VW_INSIGHT_DATA.QLEVEL, 
  VW_INSIGHT_DATA.FIRST_WAVE, 
  VW_INSIGHT_DATA.IS_NEW, 
  VW_INSIGHT_DATA.MIN, 
  VW_INSIGHT_DATA.MAX, 
  VW_INSIGHT_DATA.MIDPOINT, 
  VW_INSIGHT_DATA.MALE_MIDPOINT, 
  VW_INSIGHT_DATA.FEMALE_MIDPOINT, 
  VW_INSIGHT_DATA.KEYWORD, 
  VW_INSIGHT_DATA.CCP, 
  VW_INSIGHT_DATA.EXTERNAL_KEY, 
  VW_INSIGHT_DATA.SORT :: NUMBER, 
  VW_INSIGHT_DATA.DATA_TYPE_ID, 
  VW_INSIGHT_DATA.DATA_TYPE, 
  VW_INSIGHT_DATA.MEMRI_DEFINITION 
FROM 
  ALL_FUSION_STUDY 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_INSIGHT_DATA" ON ALL_FUSION_STUDY.DICTIONARY_ID = VW_INSIGHT_DATA.DICTIONARY_ID 
  LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_SUPPRESSION" ON VW_SUPPRESSION.STUDY_ID = ALL_FUSION_STUDY.STUDY_ID 
  AND VW_SUPPRESSION.PERSISTENT_ID = VW_INSIGHT_DATA.KEYWORD 
ORDER BY 
  TO_NUMBER(SORT);  
  

create or replace view VW_STUDY_DICTIONARY(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	RECORDNO,
	PARENT_RECORDNO,
	DATA_TYPE,
	RECORD_TAG,
	ACCESS_CODE,
	TEXT,
	KEYWORD,
	CCP,
	DISPLAY_TYPE,
	IS_Q_LEVEL,
	MIN,
	MAX,
	MIDPOINT,
	IS_NEW,
	PERSISTENT_ID,
	DICTIONARY_ID,
	DELIVERABLE_ID,
	DELIVERABLE,
	DP_VERSIONID,
	VERSION,
	SORT,
	IS_PARENT,
	IS_SUPPRESSED,
	PARENT_STUDY_ID,
	PARENT_STUDY_CODE
) as 


WITH STUDY AS(
  SELECT 
    DICTIONARY.ID AS DICTIONARY_ID, 
    STUDY.ID AS STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID, 
    DP_VERSIONID 
  FROM 
    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON STUDY.ID = DICTIONARY.STUDY_ID
), 
LATEST_STUDY AS(
  SELECT 
    DICTIONARY_ID, 
    STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID 
  FROM 
    STUDY QUALIFY ROW_NUMBER() OVER (
      PARTITION BY STUDY_ID 
      ORDER BY 
        DP_VERSIONID DESC
    ) = 1
), 
FUSION_STUDY AS(
  SELECT 
    DICTIONARY_ID, 
    STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID, 
    DICTIONARY_ID AS FUSION_DICTIONARY_ID, 
    FALSE AS IS_PARENT 
  FROM 
    STUDY 
  WHERE 
    PARENT_STUDY_ID IS NOT NULL
), 
NON_FUSION_STUDY AS(
  SELECT 
    DICTIONARY_ID, 
    STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID, 
    DICTIONARY_ID AS FUSION_DICTIONARY_ID, 
    FALSE AS IS_PARENT 
  FROM 
    STUDY 
  WHERE 
    PARENT_STUDY_ID IS NULL
), 
FUSION_STUDY_PARENT AS (
  SELECT 
    LATEST_STUDY.DICTIONARY_ID, 
    FUSION_STUDY.STUDY_ID, 
    FUSION_STUDY.NAME, 
    FUSION_STUDY.STUDY_CODE, 
    FUSION_STUDY.PARENT_STUDY_ID, 
    FUSION_STUDY.DP_STUDYID, 
    FUSION_STUDY.STUDY_TYPE_ID, 
    FUSION_STUDY.DICTIONARY_ID AS FUSION_DICTIONARY_ID, 
    TRUE AS IS_PARENT 
  FROM 
    FUSION_STUDY 
    JOIN LATEST_STUDY ON FUSION_STUDY.PARENT_STUDY_ID = LATEST_STUDY.STUDY_ID
), 
ALL_FUSION_STUDY AS (
  SELECT 
    * 
  FROM 
    FUSION_STUDY_PARENT 
  UNION ALL 
  SELECT 
    * 
  FROM 
    FUSION_STUDY
), 
DICTIONARY AS(
  SELECT 
    DICTIONARY_ITEM.ID, 
    DICTIONARY_ITEM.PARENT_ITEM_ID, 
    VW_DATA_TYPE.DATA_TYPE, 
    VW_DICTIONARY_ITEM_TYPE.TYPE AS RECORD_TAG, 
    ACCESS_CODE, 
    ITEM_TEXT.TEXT, 
    KEYWORD, 
    CCP_EXPRESSION, 
    DICTIONARY_ITEM.DISPLAY_TYPE, 
    IS_Q_LEVEL, 
    MIN, 
    MAX, 
    MIDPOINT, 
    IS_NEW, 
    PERSISTENT_ID, 
    DICTIONARY.ID AS DICTIONARY_ID, 
    DICTIONARY_ITEM.DELIVERABLE_ID, 
    DICTIONARY.DP_VERSIONID,
    DICTIONARY.VERSION,
    DICTIONARY_ITEM.SORT 
  FROM 
    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" 
    LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_RANGE" ON DICTIONARY_ITEM_RANGE.DICTIONARY_ITEM_ID = DICTIONARY_ITEM.ID 
    LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_DATAPOINT" ON DICTIONARY_DATAPOINT.DICTIONARY_ITEM_ID = DICTIONARY_ITEM.ID 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ITEM_TEXT" ON DICTIONARY_ITEM.TEXT_ID = ITEM_TEXT.ID 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" ON DICTIONARY.ID = DICTIONARY_ITEM.DICTIONARY_ID 
    LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DATA_TYPE" ON VW_DATA_TYPE.DATA_TYPE_ID = DICTIONARY_DATAPOINT.DATA_TYPE_ID 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DICTIONARY_ITEM_TYPE" ON VW_DICTIONARY_ITEM_TYPE.ID = DICTIONARY_ITEM.TYPE_ID 
) 
SELECT 
  NON_FUSION_STUDY.STUDY_ID, 
  NON_FUSION_STUDY.DP_STUDYID, 
  NON_FUSION_STUDY.STUDY_CODE, 
  DICTIONARY.ID AS RECORDNO, 
  DICTIONARY.PARENT_ITEM_ID AS PARENT_RECORDNO, 
  DICTIONARY.DATA_TYPE, 
  DICTIONARY.RECORD_TAG, 
  DICTIONARY.ACCESS_CODE, 
  DICTIONARY.TEXT, 
  DICTIONARY.KEYWORD, 
  DICTIONARY.CCP_EXPRESSION, 
  DICTIONARY.DISPLAY_TYPE, 
  DICTIONARY.IS_Q_LEVEL, 
  DICTIONARY.MIN, 
  DICTIONARY.MAX, 
  DICTIONARY.MIDPOINT, 
  DICTIONARY.IS_NEW, 
  DICTIONARY.PERSISTENT_ID, 
  NON_FUSION_STUDY.FUSION_DICTIONARY_ID AS DICTIONARY_ID, 
  DICTIONARY.DELIVERABLE_ID,
  VW_DELIVERABLE.DELIVERABLE,
  DICTIONARY.DP_VERSIONID,
  DICTIONARY.VERSION, 
  DICTIONARY.SORT, 
  NON_FUSION_STUDY.IS_PARENT, 
  FALSE AS IS_SUPPRESSED, 
  NULL AS PARENT_STUDY_ID, 
  NULL AS PARENT_STUDY_CODE 
FROM 
  NON_FUSION_STUDY 
  JOIN DICTIONARY ON NON_FUSION_STUDY.DICTIONARY_ID = DICTIONARY.DICTIONARY_ID 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DELIVERABLE" on VW_DELIVERABLE.DELIVERABLE_ID = DICTIONARY.DELIVERABLE_ID
  
UNION ALL 
SELECT 
  ALL_FUSION_STUDY.STUDY_ID, 
  ALL_FUSION_STUDY.DP_STUDYID, 
  ALL_FUSION_STUDY.STUDY_CODE, 
  DICTIONARY.ID AS RECORDNO, 
  DICTIONARY.PARENT_ITEM_ID AS PARENT_RECORDNO, 
  DICTIONARY.DATA_TYPE, 
  DICTIONARY.RECORD_TAG, 
  DICTIONARY.ACCESS_CODE, 
  DICTIONARY.TEXT, 
  DICTIONARY.KEYWORD, 
  DICTIONARY.CCP_EXPRESSION, 
  DICTIONARY.DISPLAY_TYPE, 
  DICTIONARY.IS_Q_LEVEL, 
  DICTIONARY.MIN, 
  DICTIONARY.MAX, 
  DICTIONARY.MIDPOINT, 
  DICTIONARY.IS_NEW, 
  DICTIONARY.PERSISTENT_ID, 
  ALL_FUSION_STUDY.FUSION_DICTIONARY_ID AS DICTIONARY_ID, 
  DICTIONARY.DELIVERABLE_ID,
  VW_DELIVERABLE.DELIVERABLE,
  DICTIONARY.DP_VERSIONID,
  DICTIONARY.VERSION, 
  DICTIONARY.SORT, 
  ALL_FUSION_STUDY.IS_PARENT, 
  CASE WHEN VW_SUPPRESSION.PERSISTENT_ID IS NULL THEN FALSE ELSE TRUE END AS IS_SUPPRESSED, 
  ALL_FUSION_STUDY.PARENT_STUDY_ID, 
  STUDY.STUDY_CODE AS PARENT_STUDY_CODE 
FROM 
  ALL_FUSION_STUDY 
  JOIN DICTIONARY ON ALL_FUSION_STUDY.DICTIONARY_ID = DICTIONARY.DICTIONARY_ID 
  LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_SUPPRESSION" ON VW_SUPPRESSION.DICTIONARY_ID = ALL_FUSION_STUDY.FUSION_DICTIONARY_ID 
  AND VW_SUPPRESSION.PERSISTENT_ID = DICTIONARY.KEYWORD 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON STUDY.ID = ALL_FUSION_STUDY.PARENT_STUDY_ID
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_DELIVERABLE" on VW_DELIVERABLE.DELIVERABLE_ID = DICTIONARY.DELIVERABLE_ID;


create or replace view VW_STUDY_DICTIONARY_FLAT(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	FULL_LABEL,
	ACCESS_CODE,
	KEYWORD,
	PERSISTENT_ID,
	EXTERNAL_KEY,
	CCP,
	DISPLAY_TYPE,
	QLEVEL,
	MIN,
	MAX,
	MIDPOINT,
	IS_NEW,
	SORT,
	DP_VERSIONID,
	VERSION,
	DICTIONARY_ID,
	DELIVERABLE,
	DELIVERABLE_ID,
	IS_PARENT,
	IS_SUPPRESSED,
	PARENT_STUDY_ID,
	PARENT_STUDY_CODE
) as 

WITH STUDY AS(
  SELECT 
    DICTIONARY.ID AS DICTIONARY_ID, 
    STUDY.ID AS STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID, 
    DP_VERSIONID 
  FROM 
    "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" 
    JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON STUDY.ID = DICTIONARY.STUDY_ID
), 
LATEST_STUDY AS(
  SELECT 
    DICTIONARY_ID, 
    STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID 
  FROM 
    STUDY QUALIFY ROW_NUMBER() OVER (
      PARTITION BY STUDY_ID 
      ORDER BY 
        DP_VERSIONID DESC
    ) = 1
), 
FUSION_STUDY AS(
  SELECT 
    DICTIONARY_ID, 
    STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID, 
    DICTIONARY_ID AS FUSION_DICTIONARY_ID, 
    FALSE AS IS_PARENT 
  FROM 
    STUDY 
  WHERE 
    PARENT_STUDY_ID IS NOT NULL
), 
NON_FUSION_STUDY AS(
  SELECT 
    DICTIONARY_ID, 
    STUDY_ID, 
    NAME, 
    STUDY_CODE, 
    PARENT_STUDY_ID, 
    DP_STUDYID, 
    STUDY_TYPE_ID, 
    DICTIONARY_ID AS FUSION_DICTIONARY_ID, 
    FALSE AS IS_PARENT 
  FROM 
    STUDY 
  WHERE 
    PARENT_STUDY_ID IS NULL
), 
FUSION_STUDY_PARENT AS (
  SELECT 
    LATEST_STUDY.DICTIONARY_ID, 
    FUSION_STUDY.STUDY_ID, 
    FUSION_STUDY.NAME, 
    FUSION_STUDY.STUDY_CODE, 
    FUSION_STUDY.PARENT_STUDY_ID, 
    FUSION_STUDY.DP_STUDYID, 
    FUSION_STUDY.STUDY_TYPE_ID, 
    FUSION_STUDY.DICTIONARY_ID AS FUSION_DICTIONARY_ID, 
    TRUE AS IS_PARENT 
  FROM 
    FUSION_STUDY 
    JOIN LATEST_STUDY ON FUSION_STUDY.PARENT_STUDY_ID = LATEST_STUDY.STUDY_ID
), 
ALL_FUSION_STUDY AS (
  SELECT 
    * 
  FROM 
    FUSION_STUDY_PARENT 
  UNION ALL 
  SELECT 
    * 
  FROM 
    FUSION_STUDY
) 
SELECT 
  ALL_FUSION_STUDY.STUDY_ID, 
  ALL_FUSION_STUDY.DP_STUDYID, 
  ALL_FUSION_STUDY.STUDY_CODE, 
  VW_INSIGHT_DATA.FULL_LABEL, 
  VW_INSIGHT_DATA.ACCESS_CODE, 
  VW_INSIGHT_DATA.KEYWORD,
  VW_INSIGHT_DATA.PERSISTENT_ID,
  VW_INSIGHT_DATA.EXTERNAL_KEY, 
  VW_INSIGHT_DATA.CCP, 
  VW_INSIGHT_DATA.DISPLAY_TYPE, 
  VW_INSIGHT_DATA.QLEVEL, 
  VW_INSIGHT_DATA.MIN, 
  VW_INSIGHT_DATA.MAX, 
  VW_INSIGHT_DATA.MIDPOINT, 
  VW_INSIGHT_DATA.IS_NEW, 
  VW_INSIGHT_DATA.SORT :: NUMBER AS SORT, 
  VW_INSIGHT_DATA.DP_VERSIONID,
  VW_INSIGHT_DATA.VERSION,
  ALL_FUSION_STUDY.FUSION_DICTIONARY_ID AS DICTIONARY_ID, 
  VW_INSIGHT_DATA.DELIVERABLE, 
  VW_INSIGHT_DATA.DELIVERABLE_ID, 
  ALL_FUSION_STUDY.IS_PARENT, 
  CASE WHEN VW_SUPPRESSION.PERSISTENT_ID IS NULL THEN FALSE ELSE TRUE END AS IS_SUPPRESSED, 
  ALL_FUSION_STUDY.PARENT_STUDY_ID, 
  STUDY.STUDY_CODE AS PARENT_STUDY_CODE 
FROM 
  ALL_FUSION_STUDY 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_INSIGHT_DATA" ON ALL_FUSION_STUDY.DICTIONARY_ID = VW_INSIGHT_DATA.DICTIONARY_ID 
  LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_SUPPRESSION" ON VW_SUPPRESSION.DICTIONARY_ID = ALL_FUSION_STUDY.FUSION_DICTIONARY_ID 
  AND VW_SUPPRESSION.PERSISTENT_ID = VW_INSIGHT_DATA.KEYWORD 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" ON STUDY.ID = ALL_FUSION_STUDY.PARENT_STUDY_ID 
UNION ALL 
SELECT 
  NON_FUSION_STUDY.STUDY_ID, 
  NON_FUSION_STUDY.DP_STUDYID, 
  NON_FUSION_STUDY.STUDY_CODE, 
  VW_INSIGHT_DATA.FULL_LABEL, 
  VW_INSIGHT_DATA.ACCESS_CODE, 
  VW_INSIGHT_DATA.KEYWORD,
  VW_INSIGHT_DATA.PERSISTENT_ID,
  VW_INSIGHT_DATA.EXTERNAL_KEY, 
  VW_INSIGHT_DATA.CCP, 
  VW_INSIGHT_DATA.DISPLAY_TYPE, 
  VW_INSIGHT_DATA.QLEVEL, 
  VW_INSIGHT_DATA.MIN, 
  VW_INSIGHT_DATA.MAX, 
  VW_INSIGHT_DATA.MIDPOINT, 
  VW_INSIGHT_DATA.IS_NEW, 
  VW_INSIGHT_DATA.SORT :: NUMBER AS SORT, 
  VW_INSIGHT_DATA.DP_VERSIONID,
  VW_INSIGHT_DATA.VERSION,
  NON_FUSION_STUDY.FUSION_DICTIONARY_ID AS DICTIONARY_ID, 
  VW_INSIGHT_DATA.DELIVERABLE, 
  VW_INSIGHT_DATA.DELIVERABLE_ID, 
  NON_FUSION_STUDY.IS_PARENT, 
  FALSE AS IS_SUPPRESSED, 
  NULL as PARENT_STUDY_ID, 
  NULL AS PARENT_STUDY_CODE 
FROM 
  NON_FUSION_STUDY 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_INSIGHT_DATA" ON NON_FUSION_STUDY.DICTIONARY_ID = VW_INSIGHT_DATA.DICTIONARY_ID;

create or replace view VW_STUDY_INFO(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	SECURITYCODE,
	YEAR,
	RELEASE_PERIOD,
	RELEASE_PERIOD_SORT,
	STUDY_NAME,
	STUDY_TYPE_ID,
	STUDY_TYPE,
	STUDY_FAMILY_ID,
	STUDY_FAMILY,
	STUDY_FAMILY_SORT,
	MINWAVE,
	MAXWAVE,
	TREND_FAMILY_ID,
	TREND_FAMILY,
	STARTFIELDDATE,
	ENDFIELDDATE,
	RELEASE_DATE,
	REISSUEDATE,
	PARENT_STUDY_ID,
	PARENT_STUDY_CODE,
	PRODUCT_CODE
) as 


SELECT 
  STUDY.ID AS STUDY_ID, 
  STUDY.DP_STUDYID, 
  STUDY.STUDY_CODE, 
  STUDY.SECURITYCODE, 
  STUDY.YEAR, 
  STUDY_RELEASE AS RELEASE_PERIOD, 
  VW_STUDY_RELEASE.SORT AS RELEASE_PERIOD_SORT, 
  STUDY.NAME AS STUDY_NAME, 
  STUDY.STUDY_TYPE_ID, 
  STUDY_TYPE, 
  STUDY.STUDY_FAMILY_ID, 
  STUDY_FAMILY, 
  VW_STUDY_FAMILY.SORT AS STUDY_FAMILY_SORT, 
  STUDY.MINWAVE, 
  STUDY.MAXWAVE, 
  STUDY.TREND_FAMILY_ID, 
  TREND_FAMILY, 
  STUDY.STARTFIELDDATE, 
  STUDY.ENDFIELDDATE, 
  STUDY.RELEASE_DATE, 
  NULL AS REISSUEDATE, 
  STUDY.PARENT_STUDY_ID, 
  PARENT_STUDY.STUDY_CODE AS PARENT_STUDY_CODE,
  SUBSTR(STUDY.SECURITYCODE, 0, 2) as PRODUCT_CODE
FROM 
  "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_STUDY_RELEASE" ON VW_STUDY_RELEASE.ID = STUDY.STUDY_RELEASE_ID 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_TREND_FAMILY" ON VW_TREND_FAMILY.ID = STUDY.TREND_FAMILY_ID 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_STUDY_TYPE" ON STUDY.STUDY_TYPE_ID = VW_STUDY_TYPE.ID 
  JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_STUDY_FAMILY" ON STUDY.STUDY_FAMILY_ID = VW_STUDY_FAMILY.ID 
  LEFT JOIN "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" AS PARENT_STUDY ON STUDY.PARENT_STUDY_ID = PARENT_STUDY.ID; 
  

create or replace view VW_STUDY_WEIGHT(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	WEIGHT_TYPE,
	WEIGHT_EXTENSION,
	MULTIPLIER,
	DIVISOR,
	CCP,
	PERSISTENT_ID
) as  

select 
  STUDY.ID as STUDY_ID,
  STUDY.DP_STUDYID,
  STUDY_CODE,
  WEIGHT_TYPE,
  WEIGHT_EXTENSION,
  MULTIPLIER,
  DIVISOR,
  CCP,
  PERSISTENT_ID  
from 
  "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY_WEIGHT" 
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_WEIGHT_TYPE" on STUDY_WEIGHT.WEIGHT_TYPE_ID = VW_WEIGHT_TYPE.ID 
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_WEIGHT_EXTENSION" on STUDY_WEIGHT.WEIGHT_EXTENSION_ID = VW_WEIGHT_EXTENSION.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" on STUDY_WEIGHT.STUDY_ID = STUDY.ID; 
  
  
create or replace view VW_DICTIONARY_ITEM_ATTRIBUTE(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	PERSISTENT_ID,
	ATTRIBUTE_TYPE_ID,
	ATTRIBUTE_TYPE,
	ATTRIBUTE_ID,
	ATTRIBUTE,
	ATTRIBUTE_VALUE,
	ATTRIBUTE_KEY,
	DP_VERSIONID,
	DICTIONARY_ID
) as


select 
  STUDY.ID as STUDY_ID, 
  STUDY.DP_STUDYID, 
  STUDY.STUDY_CODE,
  DICTIONARY_ITEM_ATTRIBUTE.PERSISTENT_ID,
  VW_ATTRIBUTE_TYPE.ATTRIBUTE_TYPE_ID,
  VW_ATTRIBUTE_TYPE.ATTRIBUTE_TYPE,
  VW_ATTRIBUTE.ID as ATTRIBUTE_ID,
  VW_ATTRIBUTE.NAME as ATTRIBUTE,
  ATTRIBUTE_MASTER.ATTRIBUTE_VALUE,
  ATTRIBUTE_MASTER.ATTRIBUTE_KEY,
  DICTIONARY.DP_VERSIONID,
  DICTIONARY.ID as DICTIONARY_ID
from 
  "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" 
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" on DICTIONARY.STUDY_ID = STUDY.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM" on DICTIONARY_ITEM.DICTIONARY_ID = DICTIONARY.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ITEM_ATTRIBUTE" on DICTIONARY_ITEM_ATTRIBUTE.DICTIONARY_ITEM_ID = DICTIONARY_ITEM.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ATTRIBUTE_MASTER" on DICTIONARY_ITEM_ATTRIBUTE.ATTRIBUTE_MASTER_ID = ATTRIBUTE_MASTER.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_ATTRIBUTE" on ATTRIBUTE_MASTER.ATTRIBUTE_ID = VW_ATTRIBUTE.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_ATTRIBUTE_TYPE" on VW_ATTRIBUTE_TYPE.ATTRIBUTE_TYPE_ID = ATTRIBUTE_MASTER.ATTRIBUTE_TYPE_ID;


create or replace view VW_STUDY_ATTRIBUTE(
	STUDY_ID,
	DP_STUDYID,
	STUDY_CODE,
	ATTRIBUTE_TYPE_ID,
	ATTRIBUTE_TYPE,
	ATTRIBUTE_ID,
	ATTRIBUTE,
	ATTRIBUTE_VALUE,
	ATTRIBUTE_KEY
) as  
select 
  STUDY.ID as STUDY_ID, 
  STUDY.DP_STUDYID, 
  STUDY.STUDY_CODE,
  VW_ATTRIBUTE_TYPE.ATTRIBUTE_TYPE_ID,
  VW_ATTRIBUTE_TYPE.ATTRIBUTE_TYPE,
  VW_ATTRIBUTE.ID as ATTRIBUTE_ID,
  VW_ATTRIBUTE.NAME as ATTRIBUTE,
  ATTRIBUTE_MASTER.ATTRIBUTE_VALUE,
  ATTRIBUTE_MASTER.ATTRIBUTE_KEY

from 
  "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY" 
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."STUDY" on DICTIONARY.STUDY_ID = STUDY.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."DICTIONARY_ATTRIBUTE" on DICTIONARY_ATTRIBUTE.DICTIONARY_ID = DICTIONARY.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."ATTRIBUTE_MASTER" on DICTIONARY_ATTRIBUTE.ATTRIBUTE_MASTER_ID = ATTRIBUTE_MASTER.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_ATTRIBUTE" on ATTRIBUTE_MASTER.ATTRIBUTE_ID = VW_ATTRIBUTE.ID
  join "DEV_DATAMART_DB"."SCHEMA_CHNAGE_DATAMART"."VW_ATTRIBUTE_TYPE" on VW_ATTRIBUTE_TYPE.ATTRIBUTE_TYPE_ID = ATTRIBUTE_MASTER.ATTRIBUTE_TYPE_ID;
  
  
