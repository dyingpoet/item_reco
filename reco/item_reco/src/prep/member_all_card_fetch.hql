SET hive.exec.compress.output=false;
SET mapred.output.compression.codec=org.apache.hadoop.io.compress.BZip2Codec;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;

USE ${hiveconf:Database};

--INSERT OVERWRITE TABLE ${hiveconf:member_club_pref_table} PARTITION(campaign_month='${hiveconf:campaign_month}')
INSERT OVERWRITE DIRECTORY '${hiveconf:all_card_landing_dir}'
            SELECT 
                DISTINCT
                b.membership_nbr as membership_nbr
                , c.cardholder_nbr as cardholder_nbr
                , b.membership_create_date
                --, b.membership_create_date as membership_create_date
                --, IF(c.preferred_club_nbr IS NULL and b.assigned_club_nbr IS NULL, b.issuing_club_nbr, c.preferred_club_nbr) as preferred_club_nbr
                , IF(c.preferred_club_nbr IS NULL and b.assigned_club_nbr IS NULL, b.issuing_club_nbr, IF(c.preferred_club_nbr IS NULL, b.assigned_club_nbr, c.preferred_club_nbr)) as preferred_club_nbr
                , b.issuing_club_nbr as issuing_club_nbr
                , b.assigned_club_nbr as assigned_club_nbr
                , b.snapshot_begin_date
                , b.snapshot_end_date
                , b.member_status_code
            FROM 
                --customers.sams_us_clubs_sams_membership_dim b
                ${hiveconf:sams_membership_dim_table} b
            JOIN 
                --customers.sams_us_clubs_sams_mbr_cardholder_dim c
                ${hiveconf:sams_cardholder_dim_table} c
            --ON (b.membership_nbr = c.membership_nbr and c.cardholder_status_code in ('A','P','F') and c.current_ind='Y' and b.current_ind = 'Y' and b.membership_obsolete_date>='${hiveconf:ds_obsolete_date_lb}')  
            ON (b.membership_nbr = c.membership_nbr and b.member_status_code in ('A','E','P') and c.current_ind='Y' and b.current_ind = 'Y' and b.membership_obsolete_date>='${hiveconf:ds_obsolete_date_lb}')  
            WHERE b.business_club_type_cd = 0 and (b.issuing_country_code IS NULL OR b.issuing_country_code = 'US')
;


 

