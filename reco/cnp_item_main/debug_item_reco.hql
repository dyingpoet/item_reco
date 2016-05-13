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


--USE ${hiveconf:Database};

drop table if exists jli21.debug_item_reco;
create table jli21.debug_item_reco as
--INSERT OVERWRITE DIRECTORY '${hiveconf:item_reco_landing_dir}'
--
--SELECT
--a.*
--FROM
--(
--SELECT 
--    a.membership_nbr
--    , a.cardholder_nbr
--    , a.membership_create_date
--    , a.partition_system_item_nbr
--    , MAX(a.score) as score
--FROM
--( 
--    SELECT 
--        b.membership_nbr
--        , b.cardholder_nbr
--        , b.membership_create_date
--        , a.partition_system_item_nbr
--        , a.score * b.score  as score
--    FROM 
--    (
        SELECT
            a.system_item_nbr
            , a.seasonality_vst * b.score as score
            , b.partition_system_item_nbr
        FROM
            --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.sams_combined_global_seasonality_by_month_item WHERE start_of_month='${hiveconf:seasonality_month}') a
            (SELECT system_item_nbr, seasonality_vst, start_of_month FROM ${hiveconf:seasonality_tbl} WHERE start_of_month='${hiveconf:seasonality_month}') a
            --(SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.item_specific_seasonality_data_month WHERE start_of_month='2015-11-01') a
        JOIN
            (SELECT system_item_nbr, partition_system_item_nbr, score FROM ${hiveconf:cobought_tbl} WHERE ds='${hiveconf:cobought_dt}' AND membership_type_code='${hiveconf:member_type}' AND region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
            --(SELECT system_item_nbr, partition_system_item_nbr, score FROM pythia.item_level_cobought_cosine_scores_yearly WHERE membership_type_code='all' and region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
            --(SELECT system_item_nbr, partition_system_item_nbr, score FROM ${hiveconf:cobought_tbl} WHERE ds='${hiveconf:cobought_dt}' AND membership_type_code='${hiveconf:member_type}' AND region_nbr='all') b
            -- partition_system_item_nbr is RECO item
            --(SELECT system_item_nbr, partition_system_item_nbr, score FROM pythia.item_level_cobought_cosine_scores_yearly WHERE ds='2015-12-01' AND membership_type_code='all' AND region_nbr='all') b
            --(SELECT system_item_nbr, partition_system_item_nbr, score FROM ${hiveconf:cobought_tbl} WHERE ds='${hiveconf:cobought_dt}' AND membership_type_code='${hiveconf:member_type}' AND region_nbr='all' AND system_item_nbr NOT IN (${hiveconf:anchor_blacklist}) AND partition_system_item_nbr NOT IN (${hiveconf:reco_blacklist}) ) b
        ON a.system_item_nbr = b.system_item_nbr
--    ) a
--    JOIN 
--    --(SELECT * FROM pythia.pis_member_item_trans_score_partition WHERE campaign_month='${hiveconf:trans_partition}') b
--    (SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
--    ON a.system_item_nbr = b.system_item_nbr
--
--) a
--GROUP BY a.membership_nbr, a.cardholder_nbr, a.membership_create_date, a.partition_system_item_nbr
--) a
--LEFT OUTER JOIN
--(SELECT * FROM ${hiveconf:trans_tbl} WHERE campaign_month='${hiveconf:trans_partition}') b
--ON a.partition_system_item_nbr = b.system_item_nbr
--AND a.membership_nbr = b.membership_nbr
--AND a.cardholder_nbr = b.cardholder_nbr
--AND a.membership_create_date = b.membership_create_date
--WHERE b.system_item_nbr IS NULL
---- filter out purchased items
;

--FAILED: ParseException line 14:75 cannot recognize input near '$' '{' 'hiveconf' in join source


--hive -hiveconf Database=pythia -hiveconf seasonality_tbl=pythia.item_specific_seasonality_data_month -hiveconf cobought_dt=2015-12-01 -hiveconf cobought_tbl=pythia.item_level_cobought_cosine_scores_yearly -hiveconf reco_blacklist=/home/jli21/project/reco/src/production/item_reco/src/config/system.87 -hiveconf anchor_blacklist=/home/jli21/project/reco/src/production/item_reco/src/config/system.87 -hiveconf seasonality_month=2015-11-01 -hiveconf trans_tbl=pythia.pis_member_item_trans_score_partition -hiveconf trans_partition=2015-12-03 -hiveconf item_reco_landing_dir=/user/pythia/Workspaces/SamsMEP/Recommend/Scoring/cnp_member/2015-12-03/Decay/recommend_score_cobought_no_smoothing_max_cosine_cobought_transaction_debug -hiveconf member_type=all -f /home/jli21/project/reco/src/production/item_reco/src/core/CoboughtModelItem.hql



--SELECT system_item_nbr, seasonality_vst, start_of_month FROM pythia.item_specific_seasonality_data_month WHERE start_of_month='2015-11-01';
--SELECT system_item_nbr, partition_system_item_nbr, score FROM pythia.item_level_cobought_cosine_scores_yearly WHERE ds='2015-12-01' AND membership_type_code='all' AND region_nbr='all' AND system_item_nbr=10474 limit 3;
--
--
--10004   1.4589928057553958      2015-11-01
--10044   1.2934924257821931      2015-11-01
--10474   1.1577828894363813      2015-11-01
--
--10474   52979   0.1952922822799054
--10474   20875136        0.08904092547262676
--10474   10465   0.06077630330480875






