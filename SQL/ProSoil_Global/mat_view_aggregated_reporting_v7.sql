
-- 08.03.2021 (MT) 
-- Added values for Global agg reporting_term (to fix related issues in Superset)

-- 06.03.2021 (MT) 
-- Added M4 roster vars for list + text description extraction (Q v12)

-- UPDATED 03.03.2021 (MT) based on v7.1.1 (ref. email 11.01.2021)
-- Modifications (ref. email 03.03.2021):
-- 1. Reordered according to variables codes (IDS)
-- 2. Added array for o3_1_3_lessons_sel, o3_1_4_lessons_txt and o3_2_3_exchanges_txt (last 2 are descriptions for each associated roster entry)

-- UPDATED 10.01.2021 (MT) based on v7 (ref. email 15.12.2020)
-- Modifications (ref. email 10.01.2020):
-- 1. Changed CAST on some vars 
-- 2. Added conditional logic (CASE WHEN) to remove NULLs on some vars; remove -99999999 on others 
-- 3. Added UNION for 'Global' aggregations

-- UPDATED 24.05.2021 (MT) based on new questionnaire PSG-ADR v7
-- Modifications (ref. email 24.05.2021)
-- 1. Added qualitative fields for M4, O1.3, O2.1, O2.2, O3.3
-- 2. (26.05.2021) Added greatest to GLOBAL o2_2_0_courses_n to fix an issue with new questionnaire (v7) where I forgot to put adequate controls on a calculated variable. Fixed in questionnaire v8.

-- UPDATED 18.06.2021 (MT) based on questionnaire PSG-ADR v8
-- Modifications (ref. email 21.06.2021)
-- 1. Added CASE WHEN logic to remove Afar from global average aggregation: o1_2_3_steps_impl_pc
-- 2. Added subquery in GLOBAL values computation to allow for accurate average calculations (e.g. o3_3_1_knowledge_female_pc). Grouped all applicable variables
--> together in both main queries to facilitate UNION (i.e. moved all relevant globally averaged variables to the end of the SELECT)
-- 3. Added o1_1_5_adopting_trg_n to allow proper computation of global avg. for Adopting farmers when exceptions to z1_1 values
-- 4. Added filter for duplicate India 2020 target interview (to be removed later)

drop materialized view aggregated_reporting;
create materialized view aggregated_reporting as
SELECT 
    i.key,
    i.interview_id, 
    (i.payload ->> 'date_time'::text)::timestamp without time zone AS date_time,
    i.payload ->> 'entered_by'::text AS entered_by, 
    cp.label AS cp_name,
    (i.payload ->> 'cp_name'::text)::integer AS cp_name_id,
    (i.payload ->> 'reporting_year'::text)::integer AS reporting_year,
    (i.payload ->> 'reporting_term'::text)::integer AS reporting_term_id,
    rt.label AS reporting_term,
    (i.payload ->> 'data_type'::text)::boolean AS data_type,
    --------------------------------------------------------------------------------------------
    -- OUTCOME INDICATORS
    --------------------------------------------------------------------------------------------
    -- 1. REHAB LAND
    (i.payload ->> 'm1_0_0_rehabilitated_ha'::text)::double precision AS m1_0_0_rehabilitated_ha,
    coalesce((i.payload ->> 'm1_0_1_rehabilitated_onfarm_ha'::text)::double precision, 0) AS m1_0_1_rehabilitated_onfarm_ha,
    coalesce((i.payload ->> 'm1_0_2_rehabilitated_offfarm_ha'::text)::double precision, 0) AS m1_0_2_rehabilitated_offfarm_ha,
    -- 2. WOMEN
    --coalesce(((i.payload ->> 'm2_0_0_women_improved_pc'::text)::double precision) / 100::double precision, 0) AS m2_0_0_women_improved_pc,
    coalesce((i.payload ->> 'm2_0_1_women_improved_hh_n'::text)::integer, 0) AS m2_0_1_women_improved_hh_n,
    coalesce((i.payload ->> 'm2_0_2_women_improved_n'::text)::integer, 0) AS m2_0_2_women_improved_n,
    -- 3. CROPS
    ( SELECT string_agg(crop_type.label, ', '::text) AS string_agg
           FROM jsonb_array_elements(i.payload -> 'm3_0_0_crops_types'::text) crops(value)
             JOIN analytics.codes crop_type ON crop_type.code = crops.value::integer AND crop_type.variable = 'm3_0_0_crops_types'::text AND crop_type.questionnaire_id = i.questionnaire_id AND crop_type.questionnaire_version = i.questionnaire_version) AS m3_0_0_crops_types_all,
    ( SELECT array_agg(crop_type.label) AS array_agg
           FROM jsonb_array_elements(i.payload -> 'm3_0_0_crops_types'::text) crops(value)
             JOIN analytics.codes crop_type ON crop_type.code = crops.value::integer AND crop_type.variable = 'm3_0_0_crops_types'::text AND crop_type.questionnaire_id = i.questionnaire_id AND crop_type.questionnaire_version = i.questionnaire_version) AS m3_0_0_crops_types,
    ( SELECT array_agg(crops.value::integer) AS array_agg
           FROM jsonb_array_elements(i.payload -> 'm3_0_0_crops_types'::text) crops(value)) AS crop_type_ids,   
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 101)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_beans,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 102)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_cotton,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 103)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_feed_biomass,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 104)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_maize,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 105)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_niebe,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 106)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_red_gram,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 107)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_rice,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 108)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_sorghum,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 109)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_tef,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 110)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_wheat,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 111)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_yams,
    jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 112)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision AS m3_0_2_crops_increase_pc_maniok,
    greatest((i.payload ->> 'm3_0_3_crops_increase_avg'::text)::double precision, 0) AS m3_0_3_crops_increase_avg,
    -- 4. INCENTIVES
    ( SELECT string_agg(lessons.value::text, ', '::text) AS string_agg
          FROM jsonb_array_elements(i.payload -> 'm4_0_5_incentives_ls'::text) lessons(value)) AS m4_0_5_incentives_ls_all,
    ( SELECT array_agg(lessons.value::text) AS array_agg
          FROM jsonb_array_elements(i.payload -> 'm4_0_5_incentives_ls'::text) lessons(value)) AS m4_0_5_incentives_ls_ls,
    coalesce((i.payload ->> 'm4_0_0_incentives_n'::text)::integer, 0) AS m4_0_0_incentives_n,
    (i.payload ->> 'm4_0_1_incentives_cp_n'::text)::integer AS m4_0_1_incentives_cp_n,
    jsonb_path_query_array(i.payload, '$."m4_0_2_incentives_rst"[*]."m4_0_3_incentives_txt"'::jsonpath) AS m4_0_3_incentives_txt,
    --------------------------------------------------------------------------------------------
    -- OUTPUT INDICATORS
    --------------------------------------------------------------------------------------------
    -- 1.1 ADOPTING
    --coalesce(((i.payload ->> 'o1_1_0_adopting_farm_pc'::text)::double precision) / 100::double precision, 0) AS o1_1_0_adopting_farm_pc,
    coalesce((i.payload ->> 'o1_1_1_adopting_farm_n'::text)::integer, 0) AS o1_1_1_adopting_farm_n,
    --coalesce(((i.payload ->> 'o1_1_2_adopting_farm_female_pc'::text)::double precision) / 100::double precision, 0) AS o1_1_2_adopting_farm_female_pc,
    coalesce((i.payload ->> 'o1_1_3_adopting_farm_female_n'::text)::integer, 0) AS o1_1_3_adopting_farm_female_n,
    -- New variable used for GLOBAL AVG % adopting farmers (o1_1_0_adopting_farm_pc) calculation. Allows for India (cp 106) exception - all other CPs use z1_1_target_pop_n to compute o1_1_0_adopting_farm_pc
    coalesce((i.payload ->> 'o1_1_5_adopting_trg_n'::text)::integer, 0) AS o1_1_5_adopting_trg_n,
    -- -- 1.2 STEPS
    coalesce((i.payload ->> 'o1_2_1_steps_total_n'::text)::integer, 0) AS o1_2_1_steps_total_n,
    coalesce((i.payload ->> 'o1_2_2_steps_impl_n'::text)::integer, 0) AS o1_2_2_steps_impl_n,
    coalesce((i.payload ->> 'o1_2_3_steps_impl_pc'::text)::double precision, 0) AS o1_2_3_steps_impl_pc,
    -- 1.3 MEASURES
    coalesce((i.payload ->> 'o1_3_0_measures_n'::text)::integer, 0) AS o1_3_0_measures_n,
    coalesce((i.payload ->> 'o1_3_1_measures_women_n'::text)::double precision, 0) AS o1_3_1_measures_women_n,
    (i.payload ->> 'o1_3_2_measures_cp_n'::text)::integer AS o1_3_2_measures_cp_n,
    ( SELECT string_agg(lessons.value::text, ', '::text) AS string_agg
          FROM jsonb_array_elements(i.payload -> 'o1_3_6_measures_ls'::text) lessons(value)) AS o1_3_6_measures_ls_all,
    ( SELECT array_agg(lessons.value::text) AS array_agg
          FROM jsonb_array_elements(i.payload -> 'o1_3_6_measures_ls'::text) lessons(value)) AS o1_3_6_measures_ls,
    jsonb_path_query_array(i.payload, '$."o1_3_3_measures_rst"[*]."o1_3_4_measures_txt"'::jsonpath) AS o1_3_4_measures_txt,
    -- 2.1 STRATEGIES
    coalesce((i.payload ->> 'o2_1_0_strategies_n'::text)::integer, 0) AS o2_1_0_strategies_n,
    (i.payload ->> 'o2_1_1_strategies_cp_n'::text)::integer AS o2_1_1_strategies_cp_n,
    ( SELECT string_agg(lessons.value::text, ', '::text) AS string_agg
          FROM jsonb_array_elements(i.payload -> 'o2_1_5_strategies_ls'::text) lessons(value)) AS o2_1_5_strategies_ls_all,
    ( SELECT array_agg(lessons.value::text) AS array_agg
          FROM jsonb_array_elements(i.payload -> 'o2_1_5_strategies_ls'::text) lessons(value)) AS o2_1_5_strategies_ls,
    jsonb_path_query_array(i.payload, '$."o2_1_2_strategies_rst"[*]."o2_1_3_strategies_txt"'::jsonpath) AS o2_1_3_strategies_txt,
    -- 2.2 COURSES
    coalesce((i.payload ->> 'o2_2_0_courses_n'::text)::integer, 0) AS o2_2_0_courses_n,
    (i.payload ->> 'o2_2_1_courses_cp_n'::text)::integer AS o2_2_1_courses_cp_n,
    ( SELECT string_agg(lessons.value::text, ', '::text) AS string_agg
          FROM jsonb_array_elements(i.payload -> 'o2_2_5_courses_ls'::text) lessons(value)) AS o2_2_5_courses_ls_all,
    ( SELECT array_agg(lessons.value::text) AS array_agg
          FROM jsonb_array_elements(i.payload -> 'o2_2_5_courses_ls'::text) lessons(value)) AS o2_2_5_courses_ls,
    jsonb_path_query_array(i.payload, '$."o2_2_2_courses_rst"[*]."o2_2_3_courses_txt"'::jsonpath) AS o2_2_3_courses_txt,
    -- 3.1 LESSONS
    ( SELECT string_agg(lessons.value::text, ', '::text) AS string_agg
          FROM jsonb_array_elements(i.payload -> 'o3_1_0_lessons_ls'::text) lessons(value)) AS o3_1_0_lessons_ls_all,
    ( SELECT array_agg(lessons.value::text) AS array_agg
          FROM jsonb_array_elements(i.payload -> 'o3_1_0_lessons_ls'::text) lessons(value)) AS o3_1_0_lessons_ls,
    greatest((i.payload ->> 'o3_1_1_lessons_n'::text)::integer, 0) AS o3_1_1_lessons_n,
    jsonb_path_query_array(i.payload, '$."o3_1_2_lessons_rst"[*]."o3_1_3_lessons_sel"'::jsonpath) AS o3_1_3_lessons_sel,
    jsonb_path_query_array(i.payload, '$."o3_1_2_lessons_rst"[*]."o3_1_4_lessons_txt"'::jsonpath) AS o3_1_4_lessons_txt,
    -- 3.2 EXCHANGES
    ( SELECT string_agg(lessons.value::text, ', '::text) AS string_agg
          FROM jsonb_array_elements(i.payload -> 'o3_2_0_exchanges_ls'::text) lessons(value)) AS o3_2_0_exchanges_ls_all,
    ( SELECT array_agg(lessons.value::text) AS array_agg
          FROM jsonb_array_elements(i.payload -> 'o3_2_0_exchanges_ls'::text) lessons(value)) AS o3_2_0_exchanges_ls_ls,
    greatest((i.payload ->> 'o3_2_1_exchanges_n'::text)::integer, 0) AS o3_2_1_exchanges_n,
    jsonb_path_query_array(i.payload, '$."o3_2_2_exchanges_rst"[*]."o3_2_3_exchanges_txt"'::jsonpath) AS o3_2_3_exchanges_txt,
    -- -- 3.3 KNOWLEDGE
    -- -- o3_3_1_knowledge_female_pc moved to end of variables to accomodate global average calculation (Union variable order alignment) 
    coalesce((i.payload ->> 'o3_3_0_knowledge_n'::text)::integer, 0) AS o3_3_0_knowledge_n,
    coalesce((i.payload ->> 'o3_3_2_knowledge_female_n'::text)::integer, 0) AS o3_3_2_knowledge_female_n,
    jsonb_path_query_array(i.payload, '$."o3_3_3_knowledge_rst"[*]."o3_3_4_knowledge_txt"'::jsonpath) AS o3_3_4_knowledge_txt,
    -- 4.1 CLIMATE
    (i.payload ->> 'o4_1_0_climate_ops_sel'::text)::boolean AS o4_1_0_climate_ops_sel,
    -- NEW VARIABLES as per questionnaire v9
    (i.payload ->> 'o4_1_0_climate_act'::text)::integer AS o4_1_0_climate_act,
    (i.payload ->> 'o4_1_0_climate_n'::text)::integer AS o4_1_0_climate_n,
    (i.payload ->> 'o4_1_0_climate_ghg'::text)::double precision AS o4_1_0_climate_ghg,
    --------------------------------------------------------------------------------------------
    -- ACTIVITY INDICATORS
    --------------------------------------------------------------------------------------------
    -- TARGET --
    -------------------------
    (i.payload ->> 'z1_1_target_pop_n'::text)::integer AS z1_1_target_pop_n,
    (i.payload ->> 'z1_2_target_pop_female_n'::text)::double precision AS z1_2_target_pop_female_n,
    greatest(((i.payload ->> 'z1_3_target_pop_female_pc'::text)::double precision) / 100::double precision, 0) AS z1_3_target_pop_female_pc,
    -- There are now 2 vars for Avg HH n: one for target (z1_4_target_avg_hh_n) and one for actual (z2_14_target_avg_hh_n). 
    -- They are the same values, but must be declared separately on either target or actual data entry - so we need 2 variables
    (i.payload ->> 'z1_4_target_avg_hh_n'::text)::double precision AS z1_4_target_avg_hh_n,
    -- There are now 2 vars for HH avg beneficiaries: one for target (z1_5_target_avg_ben_hh_n) and one for actual (z2_15_target_avg_ben_hh_n). 
    -- They are the same values, but must be declared separately on either target or actual data entry - so we need 2 variables
    (i.payload ->> 'z1_5_target_avg_ben_hh_n'::text)::double precision AS z1_5_target_avg_ben_hh_n,
    -- There are now 2 vars for HH Calculation factor: one for target (z1_6_target_hh_calc_factor) and one for actual (z2_16_target_hh_calc_factor). 
    -- They are the same values, but must be declared separately on either target or actual data entry - so we need 2 variables
    greatest((i.payload ->> 'z1_6_target_hh_calc_factor'::text)::double precision, 0) AS z1_6_target_hh_calc_factor,
    greatest((i.payload ->> 'z1_7_target_dir_ben_hh'::text)::double precision, 0) AS z1_7_target_dir_ben_hh,
    greatest((i.payload ->> 'z1_8_target_dir_ben_n'::text)::double precision, 0) AS z1_8_target_dir_ben_n,
    ((i.payload ->> 'z1_9_target_dir_female_pc'::text)::double precision) / 100::double precision AS z1_9_target_dir_female_pc,
    greatest((i.payload ->> 'z1_10_target_dir_female_n'::text)::double precision, 0) AS z1_10_target_dir_female_n,
    (i.payload ->> 'z1_11_target_ind_ben_n'::text)::integer AS z1_11_target_ind_ben_n,
    greatest((i.payload ->> 'z1_12_target_tot_ben_n'::text)::double precision, 0) AS z1_12_target_tot_ben_n,
    (i.payload ->> 'z1_13_target_ind_tot_pop'::text)::integer AS z1_13_target_ind_tot_pop,
    -------------------------
    -- ACTUAL --
    -------------------------
    (i.payload ->> 'z2_1_train_target_n'::text)::integer AS z2_1_train_target_n,
    (i.payload ->> 'z2_2_train_target_female_n'::text)::integer AS z2_2_train_target_female_n,
    greatest(((i.payload ->> 'z2_3_train_target_female_pc'::text)::double precision) / 100::double precision, 0) AS z2_3_train_target_female_pc,
    (i.payload ->> 'z2_4_train_target_young_n'::text)::integer AS z2_4_train_target_young_n,
    greatest(((i.payload ->> 'z2_5_train_target_young_pc'::text)::double precision) / 100::double precision, 0) AS z2_5_train_target_young_pc,
    (i.payload ->> 'z2_6_train_other_n'::text)::integer AS z2_6_train_other_n,
    (i.payload ->> 'z2_7_train_other_female_n'::text)::integer AS z2_7_train_other_female_n,
    greatest(((i.payload ->> 'z2_8_train_other_female_pc'::text)::double precision) / 100::double precision, 0) AS z2_8_train_other_female_pc,
    (i.payload ->> 'z2_9_train_other_young_n'::text)::integer AS z2_9_train_other_young_n,
    greatest(((i.payload ->> 'z2_10_train_other_young_pc'::text)::double precision) / 100::double precision, 0) AS z2_10_train_other_young_pc,
    greatest((i.payload ->> 'z2_11_train_total_n'::text)::integer, 0) AS z2_11_train_total_n,
    greatest((i.payload ->> 'z2_12_train_total_female_n'::text)::integer, 0) AS z2_12_train_total_female_n,
    greatest(((i.payload ->> 'z2_13_train_total_female_pc'::text)::double precision) / 100::double precision, 0) AS z2_13_train_total_female_pc,
    (i.payload ->> 'z2_14_target_avg_hh_n'::text)::double precision AS z2_14_target_avg_hh_n,
    (i.payload ->> 'z2_15_target_avg_ben_hh_n'::text)::double precision AS z2_15_target_avg_ben_hh_n,
    greatest((i.payload ->> 'z2_16_target_hh_calc_factor'::text)::double precision, 0)  AS z2_16_target_hh_calc_factor,
    greatest((i.payload ->> 'z2_17_train_direct_ben_n'::text)::double precision, 0) AS z2_17_train_direct_ben_n,
    (i.payload ->> 'z2_18_reached_ict_n'::text)::integer AS z2_18_reached_ict_n,
    (i.payload ->> 'z2_19_sponsored_n'::text)::integer AS z2_19_sponsored_n,
    -------------------------
    -- AVERAGED VARIABLES FOR GLOBAL
    -- moved to end of variables selection to accomodate global average calculation (UNION variable order alignment). This was the easiest method. 
    -------------------------
    coalesce(((i.payload ->> 'm2_0_0_women_improved_pc'::text)::double precision) / 100::double precision, 0) AS m2_0_0_women_improved_pc,
    coalesce(((i.payload ->> 'o1_1_0_adopting_farm_pc'::text)::double precision) / 100::double precision, 0) AS o1_1_0_adopting_farm_pc,
    coalesce(((i.payload ->> 'o1_1_2_adopting_farm_female_pc'::text)::double precision) / 100::double precision, 0) AS o1_1_2_adopting_farm_female_pc,
    coalesce(((i.payload ->> 'o3_3_1_knowledge_female_pc'::text)::double precision) / 100::double precision, 0) AS o3_3_1_knowledge_female_pc
FROM analytics.interviews i
LEFT JOIN analytics.codes cp ON ((i.payload ->> 'cp_name'::text)::integer) = cp.code AND cp.variable = 'cp_name'::text AND cp.questionnaire_id = i.questionnaire_id AND cp.questionnaire_version = i.questionnaire_version
LEFT JOIN analytics.codes rt ON ((i.payload ->> 'reporting_term'::text)::integer) = rt.code AND rt.variable = 'reporting_term'::text AND rt.questionnaire_id = i.questionnaire_id AND rt.questionnaire_version = i.questionnaire_version
LEFT JOIN analytics.codes lesson ON ((i.payload ->> 'o3_1_3_lessons_sel'::text)::integer) = rt.code and rt.variable = 'o3_1_3_lessons_sel'::text AND rt.questionnaire_id = i.questionnaire_id AND rt.questionnaire_version = i.questionnaire_version
WHERE i.questionnaire_id IN ('3d540184-7e28-4e95-84b4-51ff9a15f266'::uuid)
-- remove duplicate 2020 India Target interview
AND key != '29-48-53-96'
    --------------------------------------------------------------------------------------------
    -- UNION: Run query again with aggregations for GLOBAL (by year and data_type only - not interested in terms right now, but could add later)
    --------------------------------------------------------------------------------------------
UNION
SELECT 
-- GLOBAL SUBQUERY allows for accurate computation of averages (i.e. global_x/global_y rather than AVG(averages))
-- Select * from subquery and perform AVG operations (e.g. o3_3_2_knowledge_female_n/o3_3_0_knowledge_n) and generate new variables
*,
m2_0_2_women_improved_n/NULLIF(m2_0_1_women_improved_hh_n,0)::double precision AS o3_3_1_knowledge_female_pc,
-- Is TARGET data? Use o1_1_5_adopting_trg_n (which = z1_1_target_pop_n EXCEPT when the CP is India); Is ACTUAL data? use z2_1_train_target_n
o1_1_1_adopting_farm_n/NULLIF((CASE WHEN data_type THEN o1_1_5_adopting_trg_n ELSE z2_1_train_target_n END),0)::double precision AS o1_1_0_adopting_farm_pc,
o1_1_3_adopting_farm_female_n/NULLIF(o1_1_1_adopting_farm_n,0)::double precision AS o1_1_2_adopting_farm_female_pc,
o3_3_2_knowledge_female_n/NULLIF(o3_3_0_knowledge_n,0)::double precision AS m2_0_0_women_improved_pc
FROM(
-- Explicit casting in subquery necessary because of postgresql subquery evaluation process
SELECT 
    '10-00-00-00' AS key,
    '10000000-0000-0000-0000-000000000000'::uuid AS interview_id,
    '2020-12-30T12:00:00'::timestamp without time zone AS date_time,
    'Automatic aggregation' AS entered_by,
    'Global' AS cp_name,
    200 AS cp_name_id,
    (i.payload ->> 'reporting_year'::text)::integer AS reporting_year,
    2 AS reporting_term_id,
    'December (end of term)' AS reporting_term,
    (i.payload ->> 'data_type'::text)::boolean AS data_type,
    --------------------------------------------------------------------------------------------
    -- OUTCOME INDICATORS
    --------------------------------------------------------------------------------------------
    -- 1. REHAB LAND
    SUM((i.payload ->> 'm1_0_0_rehabilitated_ha'::text)::double precision) AS m1_0_0_rehabilitated_ha,
    SUM(coalesce((i.payload ->> 'm1_0_1_rehabilitated_onfarm_ha'::text)::double precision, 0)) AS m1_0_1_rehabilitated_onfarm_ha,
    SUM(coalesce((i.payload ->> 'm1_0_2_rehabilitated_offfarm_ha'::text)::double precision, 0)) AS m1_0_2_rehabilitated_offfarm_ha,
    -- 2. WOMEN
    -- AVG(averages) correct? Need to check on how m2_0_0_women_improved_pc is computed in mastertabelle
    -- ASSUMING NOT AoA
    --AVG(coalesce(((i.payload ->> 'm2_0_0_women_improved_pc'::text)::double precision) / 100::double precision, 0)) AS m2_0_0_women_improved_pc,
    SUM(coalesce((i.payload ->> 'm2_0_1_women_improved_hh_n'::text)::integer, 0)) AS m2_0_1_women_improved_hh_n,
    SUM(coalesce((i.payload ->> 'm2_0_2_women_improved_n'::text)::integer, 0)) AS m2_0_2_women_improved_n,
    -- 3. CROPS
    NULL AS m3_0_0_crops_types_all,
    CAST(NULL AS text[]) m3_0_0_crops_types,
    CAST(NULL AS int[]) crop_type_ids,
    -- AVG(averages) for all of these as they are computed originally by the CPs using some special sauce
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 101)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_beans,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 102)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_cotton,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 103)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_feed_biomass,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 104)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_maize,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 105)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_niebe,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 106)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_red_gram,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 107)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_rice,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 108)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_sorghum,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 109)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_tef,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 110)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_wheat,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 111)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_yams,
    AVG(jsonb_path_query_first(i.payload, '$."m3_0_1_crops_increase_rst"?(@."m3_0_1_crops_increase_rst__id" == 112)."m3_0_2_crops_increase_pc"'::jsonpath)::double precision) AS m3_0_2_crops_increase_pc_maniok,
    AVG(CASE WHEN greatest((i.payload ->> 'm3_0_3_crops_increase_avg'::text)::double precision, 0) != 0 THEN coalesce((i.payload ->> 'm3_0_3_crops_increase_avg'::text)::double precision, 0) END) AS m3_0_3_crops_increase_avg,
    -- 4. INCENTIVES
    NULL AS m4_0_5_incentives_ls_all,
    CAST(NULL AS text[]) m4_0_5_incentives_ls,
    SUM(coalesce((i.payload ->> 'm4_0_0_incentives_n'::text)::integer, 0)) AS m4_0_0_incentives_n,
    SUM((i.payload ->> 'm4_0_1_incentives_cp_n'::text)::integer) AS m4_0_1_incentives_cp_n,
    CAST(NULL AS jsonb) m4_0_3_incentives_txt,
    --------------------------------------------------------------------------------------------
    -- OUTPUT INDICATORS
    --------------------------------------------------------------------------------------------
    -- 1.1 ADOPTING
    SUM(coalesce((i.payload ->> 'o1_1_1_adopting_farm_n'::text)::integer, 0)) AS o1_1_1_adopting_farm_n,
    --AVG(coalesce(((i.payload ->> 'o1_1_2_adopting_farm_female_pc'::text)::double precision) / 100::double precision, 0)) AS o1_1_2_adopting_farm_female_pc,
    SUM(coalesce((i.payload ->> 'o1_1_3_adopting_farm_female_n'::text)::integer, 0)) AS o1_1_3_adopting_farm_female_n,
    -- New variable used for GLOBAL AVG % adopting farmers (o1_1_0_adopting_farm_pc) calculation. Allows for India (cp 106) exception - all other CPs use z1_1_target_pop_n to compute o1_1_0_adopting_farm_pc
    SUM(CASE WHEN (i.payload ->> 'cp_name'::text)::integer != 106 THEN (i.payload ->> 'z1_1_target_pop_n'::text)::integer ELSE coalesce((i.payload ->> 'o1_1_5_adopting_trg_n'::text)::integer, 0) END) AS o1_1_5_adopting_trg_n,
    -- 1.2 STEPS
    SUM(coalesce((i.payload ->> 'o1_2_1_steps_total_n'::text)::integer, 0)) AS o1_2_1_steps_total_n,
    SUM(coalesce((i.payload ->> 'o1_2_2_steps_impl_n'::text)::integer, 0)) AS o1_2_2_steps_impl_n,
    -- AVG(averages): and remove Afar (103) from aggregate calculation as it is not applicable for that CP
    AVG(CASE WHEN (i.payload ->> 'cp_name'::text)::integer != 103 THEN coalesce((i.payload ->> 'o1_2_3_steps_impl_pc'::text)::double precision, 0) END) AS o1_2_3_steps_impl_pc,
    -- 1.3 MEASURES
    SUM(coalesce((i.payload ->> 'o1_3_0_measures_n'::text)::integer, 0)) AS o1_3_0_measures_n,
    SUM(coalesce((i.payload ->> 'o1_3_1_measures_women_n'::text)::double precision, 0)) AS o1_3_1_measures_women_n,
    SUM((i.payload ->> 'o1_3_2_measures_cp_n'::text)::integer) AS o1_3_2_measures_cp_n,
    NULL AS o1_3_6_measures_ls_all,
    CAST(NULL AS text[]) o1_3_6_measures_ls,
    CAST(NULL AS jsonb) o1_3_4_measures_txt,
    -- 2.1 STRATEGIES
    SUM(coalesce((i.payload ->> 'o2_1_0_strategies_n'::text)::integer, 0)) AS o2_1_0_strategies_n,
    SUM((i.payload ->> 'o2_1_1_strategies_cp_n'::text)::integer) AS o2_1_1_strategies_cp_n,
    NULL AS o2_1_5_strategies_ls_all,
    CAST(NULL AS text[]) o2_1_5_strategies_ls,
    CAST(NULL AS jsonb) o2_1_3_strategies_txt,
    -- 2.2 COURSES
    -- Added greatest here to fix an issue with new questionnaire (v7) where I forgot to put adequate controls on a calculated variable. Fixed in questionnaire v8.
    SUM(greatest(coalesce((i.payload ->> 'o2_2_0_courses_n'::text)::integer, 0),0)) AS o2_2_0_courses_n,
    SUM((i.payload ->> 'o2_2_1_courses_cp_n'::text)::integer) AS o2_2_1_courses_cp_n,
    NULL AS o2_2_5_courses_ls_all,
    CAST(NULL AS text[]) o2_2_5_courses_ls,
    CAST(NULL AS jsonb) o2_2_3_courses_txt,
    -- 3.1 LESSONS
    NULL AS o3_1_0_lessons_ls_all,
    CAST(NULL AS text[]) o3_1_0_lessons_ls,
    SUM(greatest((i.payload ->> 'o3_1_1_lessons_n'::text)::integer, 0)) AS o3_1_1_lessons_n,
    CAST(NULL AS jsonb) o3_1_3_lessons_sel,
    CAST(NULL AS jsonb) o3_1_4_lessons_txt,
    -- -- 3.2 EXCHANGES
    NULL AS o3_2_0_exchanges_ls_all,
    CAST(NULL AS text[]) o3_2_0_exchanges_ls_ls,
    SUM(greatest((i.payload ->> 'o3_2_1_exchanges_n'::text)::integer, 0)) AS o3_2_1_exchanges_n,
    CAST(NULL AS jsonb) o3_2_3_exchanges_txt,
    -- -- 3.3 KNOWLEDGE
    SUM(coalesce((i.payload ->> 'o3_3_0_knowledge_n'::text)::integer, 0)) AS o3_3_0_knowledge_n,
    SUM(coalesce((i.payload ->> 'o3_3_2_knowledge_female_n'::text)::integer, 0)) AS o3_3_2_knowledge_female_n,
    CAST(NULL AS jsonb) o3_3_4_knowledge_txt,
    -- 4.1 CLIMATE
    CAST(NULL AS boolean) o4_1_0_climate_ops_sel,
    -- NEW VARIABLES as per questionnaire v9
    SUM((i.payload ->> 'o4_1_0_climate_act'::text)::integer) AS o4_1_0_climate_act,
    SUM((i.payload ->> 'o4_1_0_climate_n'::text)::integer) AS o4_1_0_climate_n,
    SUM((i.payload ->> 'o4_1_0_climate_ghg'::text)::double precision) AS o4_1_0_climate_ghg,
    --------------------------------------------------------------------------------------------
    -- ACTIVITY INDICATORS
    --------------------------------------------------------------------------------------------
    -- TARGET --
    -------------------------
    SUM((i.payload ->> 'z1_1_target_pop_n'::text)::integer) AS z1_1_target_pop_n,
    SUM((i.payload ->> 'z1_2_target_pop_female_n'::text)::double precision) AS z1_2_target_pop_female_n,
    AVG(greatest(((i.payload ->> 'z1_3_target_pop_female_pc'::text)::double precision) / 100::double precision, 0)) AS z1_3_target_pop_female_pc,
    AVG((i.payload ->> 'z1_4_target_avg_hh_n'::text)::double precision) AS z1_4_target_avg_hh_n,
    AVG((i.payload ->> 'z1_5_target_avg_ben_hh_n'::text)::double precision) AS z1_5_target_avg_ben_hh_n,
    AVG(greatest((i.payload ->> 'z1_6_target_hh_calc_factor'::text)::double precision, 0)) AS z1_6_target_hh_calc_factor,
    SUM(greatest((i.payload ->> 'z1_7_target_dir_ben_hh'::text)::double precision, 0)) AS z1_7_target_dir_ben_hh,
    SUM(greatest((i.payload ->> 'z1_8_target_dir_ben_n'::text)::double precision, 0)) AS z1_8_target_dir_ben_n,
    AVG(((i.payload ->> 'z1_9_target_dir_female_pc'::text)::double precision) / 100::double precision) AS z1_9_target_dir_female_pc,
    SUM(greatest((i.payload ->> 'z1_10_target_dir_female_n'::text)::double precision, 0)) AS z1_10_target_dir_female_n,
    SUM((i.payload ->> 'z1_11_target_ind_ben_n'::text)::integer) AS z1_11_target_ind_ben_n,
    SUM(greatest((i.payload ->> 'z1_12_target_tot_ben_n'::text)::double precision, 0)) AS z1_12_target_tot_ben_n,
    SUM((i.payload ->> 'z1_13_target_ind_tot_pop'::text)::integer) AS z1_13_target_ind_tot_pop,
    -------------------------
    -- ACTUAL --
    -------------------------
    SUM((i.payload ->> 'z2_1_train_target_n'::text)::integer) AS z2_1_train_target_n,
    SUM((i.payload ->> 'z2_2_train_target_female_n'::text)::integer) AS z2_2_train_target_female_n,
    AVG(greatest(((i.payload ->> 'z2_3_train_target_female_pc'::text)::double precision) / 100::double precision, 0)) AS z2_3_train_target_female_pc,
    SUM((i.payload ->> 'z2_4_train_target_young_n'::text)::integer) AS z2_4_train_target_young_n,
    AVG(greatest(((i.payload ->> 'z2_5_train_target_young_pc'::text)::double precision) / 100::double precision, 0)) AS z2_5_train_target_young_pc,
    SUM((i.payload ->> 'z2_6_train_other_n'::text)::integer) AS z2_6_train_other_n,
    SUM((i.payload ->> 'z2_7_train_other_female_n'::text)::integer) AS z2_7_train_other_female_n,
    AVG(greatest(((i.payload ->> 'z2_8_train_other_female_pc'::text)::double precision) / 100::double precision, 0)) AS z2_8_train_other_female_pc,
    SUM((i.payload ->> 'z2_9_train_other_young_n'::text)::integer) AS z2_9_train_other_young_n,
    AVG(greatest(((i.payload ->> 'z2_10_train_other_young_pc'::text)::double precision) / 100::double precision, 0)) AS z2_10_train_other_young_pc,
    SUM(greatest((i.payload ->> 'z2_11_train_total_n'::text)::integer, 0)) AS z2_11_train_total_n,
    SUM(greatest((i.payload ->> 'z2_12_train_total_female_n'::text)::integer, 0)) AS z2_12_train_total_female_n,
    AVG(greatest(((i.payload ->> 'z2_13_train_total_female_pc'::text)::double precision) / 100::double precision, 0)) AS z2_13_train_total_female_pc,
    AVG((i.payload ->> 'z2_14_target_avg_hh_n'::text)::double precision) AS z2_14_target_avg_hh_n,
    AVG((i.payload ->> 'z2_15_target_avg_ben_hh_n'::text)::double precision) AS z2_15_target_avg_ben_hh_n,
    AVG(greatest((i.payload ->> 'z2_16_target_hh_calc_factor'::text)::double precision, 0)) AS z2_16_target_hh_calc_factor,
    SUM(greatest((i.payload ->> 'z2_17_train_direct_ben_n'::text)::double precision, 0)) AS z2_17_train_direct_ben_n,
    SUM((i.payload ->> 'z2_18_reached_ict_n'::text)::integer) AS z2_18_reached_ict_n,
    SUM((i.payload ->> 'z2_19_sponsored_n'::text)::integer) AS z2_19_sponsored_n
FROM analytics.interviews i
LEFT JOIN analytics.codes cp ON ((i.payload ->> 'cp_name'::text)::integer) = cp.code AND cp.variable = 'cp_name'::text AND cp.questionnaire_id = i.questionnaire_id AND cp.questionnaire_version = i.questionnaire_version
LEFT JOIN analytics.codes rt ON ((i.payload ->> 'reporting_term'::text)::integer) = rt.code AND rt.variable = 'reporting_term'::text AND rt.questionnaire_id = i.questionnaire_id AND rt.questionnaire_version = i.questionnaire_version
LEFT JOIN analytics.codes lesson ON ((i.payload ->> 'o3_1_3_lessons_sel'::text)::integer) = rt.code and rt.variable = 'o3_1_3_lessons_sel'::text AND rt.questionnaire_id = i.questionnaire_id AND rt.questionnaire_version = i.questionnaire_version
WHERE i.questionnaire_id IN ('3d540184-7e28-4e95-84b4-51ff9a15f266'::uuid)
-- remove duplicate 2020 India Target interview
AND key != '29-48-53-96'
GROUP BY reporting_year, data_type) AS subquery;
GRANT SELECT ON aggregated_reporting to analytics;

        
