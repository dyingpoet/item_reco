

set mapred.task.timeout 1800000
set hive.exec.reducers.bytes.per.reducer=81920000;
set mapred.reduce.tasks=4;
--SET mapred.output.compress 'true';
--SET mapred.output.compression.codec 'org.apache.hadoop.io.compress.GzipCodec';
set hive.exec.compress.output=false;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.mapred.supports.subdirectories=true;

use jli21;

--create table trans_sample as SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}' and membership_nbr = 799306444;
-- create table trans_sample2 as SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}' and membership_nbr = 695086207;

            -- partition_system_item_nbr is RECO item
            --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM ${hiveconf:seasonality_tbl} WHERE start_of_month='${hiveconf:seasonality_month}') a
-- drop table if exists reco_cob_season;
-- create table reco_cob_season as
--         SELECT
--             a.system_item_nbr
--             , a.seasonality_vst * b.score as score
--             , b.partition_system_item_nbr
--         FROM
--             --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.sams_combined_global_seasonality_by_month_item WHERE start_of_month='2015-11-01') a
--             --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.sams_combined_global_seasonality_by_month_item WHERE start_of_month='${hiveconf:seasonality_month}') a
--             (SELECT system_item_nbr, seasonality_vst, start_of_month FROM ${hiveconf:seasonality_tbl} WHERE start_of_month='${hiveconf:seasonality_month}') a
--         JOIN
--             --(SELECT system_item_nbr, partition_system_item_nbr, score FROM pythia.item_level_cobought_cosine_scores_yearly WHERE ds='2015-12-01' AND membership_type_code='all' AND region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
--             (SELECT system_item_nbr, partition_system_item_nbr, score FROM ${hiveconf:cobought_tbl} WHERE ds='${hiveconf:cobought_dt}' AND membership_type_code='${hiveconf:member_type}' AND region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
--         ON a.system_item_nbr = b.system_item_nbr
-- ;
-- 
-- 
-- 
-- drop table if exists reco_sample_stage1;
-- create table reco_sample_stage1 as
--     SELECT 
--         b.membership_nbr
--         , b.cardholder_nbr
--         , b.membership_create_date
--         , a.partition_system_item_nbr
--         , a.score * b.score  as score
--         , a.system_item_nbr
--     FROM 
--     jli21.reco_cob_season a
--     JOIN 
--     jli21.trans_sample b
--     ON a.system_item_nbr = b.system_item_nbr
-- ;

-- --empty
-- drop table if exists reco_sample_stage1;
-- create table reco_sample_stage1 as
--     SELECT 
--         b.membership_nbr
--         , b.cardholder_nbr
--         , b.membership_create_date
--         , a.partition_system_item_nbr
--         , a.score * b.score  as score
--     FROM 
--     (
--         SELECT
--             a.system_item_nbr
--             , a.seasonality_vst * b.score as score
--             , b.partition_system_item_nbr
--         FROM
--             (SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.sams_combined_global_seasonality_by_month_item WHERE start_of_month='${hiveconf:seasonality_month}') a
--             --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM ${hiveconf:seasonality_tbl} WHERE start_of_month='${hiveconf:seasonality_month}') a
--         JOIN
--             (SELECT system_item_nbr, partition_system_item_nbr, score FROM pythia.item_level_cobought_cosine_scores_yearly WHERE membership_type_code='all' and region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
--             -- partition_system_item_nbr is RECO item
--             --(SELECT system_item_nbr, partition_system_item_nbr, score FROM ${hiveconf:cobought_tbl} WHERE ds='${hiveconf:cobought_dt}' AND membership_type_code='${hiveconf:member_type}' AND region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
--         ON a.system_item_nbr = b.system_item_nbr
--     ) a
--     JOIN 
--     --(SELECT * FROM pythia.pis_member_item_trans_score_partition WHERE campaign_month='${hiveconf:trans_partition}') b
--     --(SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
--     jli21.trans_sample b
--     ON a.system_item_nbr = b.system_item_nbr
-- ;

-- drop table if exists reco_sample_stage2;
-- create table reco_sample_stage2 as
-- SELECT
-- a.*
-- FROM
-- (
-- SELECT
--     a.membership_nbr
--     , a.cardholder_nbr
--     , a.membership_create_date
--     , a.partition_system_item_nbr
--     , MAX(a.score) as score
-- FROM
-- jli21.reco_sample_stage1 a
-- GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
-- ) a
-- LEFT OUTER JOIN
-- (SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
-- ON a.partition_system_item_nbr = b.system_item_nbr
-- AND a.membership_nbr = b.membership_nbr
-- AND a.cardholder_nbr = b.cardholder_nbr
-- AND a.membership_create_date = b.membership_create_date
-- WHERE b.system_item_nbr IS NULL
-- ;



-- drop table if exists reco_sample_stage2c;
-- create table reco_sample_stage2c as
-- SELECT
-- a.*
-- FROM
--     (
--     SELECT
--         a.membership_nbr
--         , a.cardholder_nbr
--         , a.membership_create_date
--         , a.partition_system_item_nbr
--         , MAX(a.score) as score
--     FROM
--         (SELECT membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr, rank() over (PARTITION BY membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr ORDER BY score DESC) as rank, partition_system_item_nbr, score  FROM jli21.reco_sample_stage1) a
--     WHERE a.rank <= 2
--     GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
--     ) a
-- LEFT OUTER JOIN
--     --(SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
--     jli21.trans_sample b
--     ON a.partition_system_item_nbr = b.system_item_nbr
--         AND a.membership_nbr = b.membership_nbr
--         AND a.cardholder_nbr = b.cardholder_nbr
--         AND a.membership_create_date = b.membership_create_date
--     WHERE b.system_item_nbr IS NULL 
-- ;







-- drop table if exists reco_sample_stage21;
-- create table reco_sample_stage21 as
--     SELECT 
--         b.membership_nbr
--         , b.cardholder_nbr
--         , b.membership_create_date
--         , a.partition_system_item_nbr
--         , a.score * b.score  as score
--         , a.system_item_nbr
--     FROM 
--     jli21.reco_cob_season a
--     JOIN 
--     jli21.trans_sample2 b
--     ON a.system_item_nbr = b.system_item_nbr
-- ;




-- drop table if exists reco_sample_stage22b;
-- create table reco_sample_stage22b as
-- SELECT
-- a.*
-- FROM
--     (
--     SELECT
--         a.membership_nbr
--         , a.cardholder_nbr
--         , a.membership_create_date
--         , a.partition_system_item_nbr
--         , MAX(a.score) as score
--     FROM
--         (SELECT membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr, rank() over (PARTITION BY membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr ORDER BY score DESC) as rank, partition_system_item_nbr, score  FROM jli21.reco_sample_stage21) a
--     WHERE a.rank <= 1
--     GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
--     ) a
-- LEFT OUTER JOIN
--     --(SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
--     jli21.trans_sample2 b
--     ON a.partition_system_item_nbr = b.system_item_nbr
--         AND a.membership_nbr = b.membership_nbr
--         AND a.cardholder_nbr = b.cardholder_nbr
--         AND a.membership_create_date = b.membership_create_date
--     WHERE b.system_item_nbr IS NULL 
-- ;



-- join with cat_score and item_info
drop table if exists reco_sample_stage23b;
create table reco_sample_stage23b as
SELECT
a.*
FROM
    (
    SELECT
        a.membership_nbr
        , a.cardholder_nbr
        , a.membership_create_date
        , a.partition_system_item_nbr
        , CASE WHEN b.num_items IS NULL THEN a.score ELSE a.score * (1+num_items) END as score
    FROM
        (
        SELECT
            a.*, b.category_nbr, b.sub_category_nbr 
        FROM
            (
            SELECT
                a.membership_nbr
                , a.cardholder_nbr
                , a.membership_create_date
                , a.partition_system_item_nbr
                , MAX(a.score) as score
            FROM
                (SELECT membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr, rank() over (PARTITION BY membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr ORDER BY score DESC) as rank, partition_system_item_nbr, score FROM jli21.reco_sample_stage21) a
            WHERE a.rank <= 1
            GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
            ) a
        JOIN
            sams_us_clubs.item_info b
        ON a.partition_system_item_nbr = b.system_item_nbr
        ) a
    LEFT OUTER JOIN
        (
        SELECT 
            membership_nbr, cardholder_nbr, membership_create_date, category_nbr, count(distinct system_item_nbr) AS num_items
        FROM
            (
            SELECT a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.system_item_nbr, b.category_nbr, b.sub_category_nbr FROM jli21.trans_sample2 a JOIN sams_us_clubs.item_info b ON a.system_item_nbr = b.system_item_nbr
            ) a
        GROUP BY membership_nbr, cardholder_nbr, membership_create_date,  category_nbr
        ) b
    ON a.category_nbr = b.category_nbr
    ) a
LEFT OUTER JOIN
    --(SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
    jli21.trans_sample2 b
    ON a.partition_system_item_nbr = b.system_item_nbr
        AND a.membership_nbr = b.membership_nbr
        AND a.cardholder_nbr = b.cardholder_nbr
        AND a.membership_create_date = b.membership_create_date
    WHERE b.system_item_nbr IS NULL 
;




-- -- -- join with cat_score and item_info
-- drop table if exists reco_sample_stage13b;
-- create table reco_sample_stage13b as
-- SELECT
-- a.*
-- FROM
--     (
--     SELECT
--         a.membership_nbr
--         , a.cardholder_nbr
--         , a.membership_create_date
--         , a.partition_system_item_nbr
--         , CASE WHEN b.num_items IS NULL THEN a.score ELSE a.score * (1+num_items) END as score
--     FROM
--         (
--         SELECT
--             a.*, b.category_nbr, b.sub_category_nbr 
--         FROM
--             (
--             SELECT
--                 a.membership_nbr
--                 , a.cardholder_nbr
--                 , a.membership_create_date
--                 , a.partition_system_item_nbr
--                 , MAX(a.score) as score
--             FROM
--                 (SELECT membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr, rank() over (PARTITION BY membership_nbr, cardholder_nbr, membership_create_date, system_item_nbr ORDER BY score DESC) as rank, partition_system_item_nbr, score FROM jli21.reco_sample_stage1) a
--             WHERE a.rank <= 1
--             GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
--             ) a
--         JOIN
--             sams_us_clubs.item_info b
--         ON a.partition_system_item_nbr = b.system_item_nbr
--         ) a
--     LEFT OUTER JOIN
--         (
--         SELECT 
--             membership_nbr, cardholder_nbr, membership_create_date, category_nbr, count(distinct system_item_nbr) AS num_items
--         FROM
--             (
--             SELECT a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.system_item_nbr, b.category_nbr, b.sub_category_nbr FROM jli21.trans_sample a JOIN sams_us_clubs.item_info b ON a.system_item_nbr = b.system_item_nbr
--             ) a
--         GROUP BY membership_nbr, cardholder_nbr, membership_create_date,  category_nbr
--         ) b
--     ON a.category_nbr = b.category_nbr
--     ) a
-- LEFT OUTER JOIN
--     --(SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
--     jli21.trans_sample b
--     ON a.partition_system_item_nbr = b.system_item_nbr
--         AND a.membership_nbr = b.membership_nbr
--         AND a.cardholder_nbr = b.cardholder_nbr
--         AND a.membership_create_date = b.membership_create_date
--     WHERE b.system_item_nbr IS NULL 
-- ;




