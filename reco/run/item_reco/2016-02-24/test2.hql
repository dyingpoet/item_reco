USE jli21;
drop table if exists tmpItemRecoStep1;
create table tmpItemRecoStep1 as
select * FROM pythia.item_level_cobought_cosine_scores_yearly WHERE ds='2016-01-12' AND membership_type_code='all' AND region_nbr='all' limit 10 
;

