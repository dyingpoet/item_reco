hive -e "select * from jli21.reco_sample_stage2 order by score desc" > sample_item_reco_prasanna
hive -e "select * from jli21.trans_sample order by score desc" > sample_reorder_prasanna


