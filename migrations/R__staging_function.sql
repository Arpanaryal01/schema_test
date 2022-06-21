USE SCHEMA SCHEMA_CHANGE_TEST;

create or replace function FN_CLEAN_HIERARCHY_TEXT(HIERARCHY_TEXT STRING, STUDY_TYPE STRING)
 returns string
 language javascript
AS
$$
let text = HIERARCHY_TEXT
let type = STUDY_TYPE
let national_study_type = '52'

const remove_square_braces_regex = /\[(.*?)\]/ig
const remove_curly_braces_regex = /\{(.*?)\}/ig

const clean_label_regex = /[!,*,.,:,;,_,\,]/g


const clean_leading_symbol = /^[@#%+]{1,}/g

let full_label = text.split('|');
let clean_full_label = [];

if (type === national_study_type){
    for(let index in full_label){
        label = full_label[index];
        label = label.replaceAll(clean_leading_symbol,'');
        label = label.replaceAll(remove_square_braces_regex,'');
        label = label.replaceAll(remove_curly_braces_regex,'');
        clean_full_label.push(label);
    }
    clean_full_label = clean_full_label.join('|');
    return clean_full_label
}
return text;
$$
;
