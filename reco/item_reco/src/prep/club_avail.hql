SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;


INSERT OVERWRITE DIRECTORY '${hiveconf:club_item_landing_dir}/special/'
SELECT
    --a.cat_subcat_nbr as cat_subcat_nbr
    a.system_item_nbr as system_item_nbr
    --, a.value_coupon_nbr as value_coupon_nbr
    , b.club_nbr as club_nbr
FROM 
    --sams_us_clubs.club_item_inventory b
    --sams_us_clubs.club_item_inventory_history b
    ${hiveconf:club_item_inventory_history_table} b
JOIN 
    ${hiveconf:item_info} a
ON (a.system_item_nbr = b.system_item_nbr and b.ds='${hiveconf:inventory_ds}' and a.ds='${hiveconf:item_info_ds}'
and b.status in ('A', 'S', 'C', 'O')
and a.category_nbr in (79,76,72,77,93,84,56,48,85,97,91,27,88))
--and cast(substr(a.cat_subcat_nbr,1,2) as int) in (79,76,72,77,93,84,56,48,85,97,91,27,88)
;

--INSERT INTO TABLE fia_subcat_item_club PARTITION(campaign_month='${hiveconf:campaign_month_type}')
INSERT OVERWRITE DIRECTORY '${hiveconf:club_item_landing_dir}/general/'
SELECT
    --a.cat_subcat_nbr as cat_subcat_nbr
    a.system_item_nbr as system_item_nbr
    --, a.value_coupon_nbr as value_coupon_nbr
    , b.club_nbr as club_nbr
FROM 
    --sams_us_clubs.club_item_inventory b
    --sams_us_clubs.club_item_inventory_history b
    ${hiveconf:club_item_inventory_history_table} b
JOIN 
    ${hiveconf:item_info} a
ON (a.system_item_nbr = b.system_item_nbr and b.ds='${hiveconf:inventory_ds}' and a.ds='${hiveconf:item_info_ds}'
and b.onsite_onhand_qty >0 and b.status in ('A', 'S', 'C', 'O')
and a.category_nbr not in (79,76,72,77,93,84,56,48,85,97,91,27,88))
--and cast(substr(a.cat_subcat_nbr,1,2) as int) not in (79,76,72,77,93,84,56,48,85,97,91,27,88)
;


