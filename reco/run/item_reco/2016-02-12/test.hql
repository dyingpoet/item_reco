set mapred.task.timeout 1800000
set hive.exec.reducers.bytes.per.reducer=81920000;
set mapreduce.job.reduces=1000;
set mapred.output.compress=true;
set hive.exec.compress.output=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.parallel=true;
set hive.auto.convert.join=true;
set hive.mapred.supports.subdirectories=true;


USE jli21;
drop table if exists tmpItemRecoStep1;
create table tmpItemRecoStep1 as
--SELECT system_item_nbr, partition_system_item_nbr, score, rank() over (PARTITION BY system_item_nbr ORDER BY score DESC) as rank FROM pythia.item_level_cobought_cosine_scores_yearly WHERE ds='2016-01-12' AND membership_type_code='all' AND region_nbr='all' 
select * FROM pythia.item_level_cobought_cosine_scores_yearly WHERE ds='2016-01-12' AND membership_type_code='all' AND region_nbr='all' limit 10 
;

set mapred.output.compression.codec=org.apache.hadoop.io.compress.GzipCodec;
set io.compression.codecs=org.apache.hadoop.io.compress.GzipCodec;
