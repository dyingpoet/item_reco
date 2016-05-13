SET mapred.task.timeout 1800000
set default_parallel 500


--define capStreamPy `/usr/bin/python qtlScore.py $nQtl`
--define capStreamPy `/usr/bin/Rscript qtlScore.r $nQtl`
define capStreamPy `/usr/bin/python qtlScore_self.py $nQtl`
    input (stdin using PigStreaming('|'))
    output (stdout using PigStreaming('|'))
    ship('$TaskDir/qtlScore_self.py')
;


transData = LOAD '$inputScore' USING PigStorage('\u0001') AS (user:chararray,system_item_nbr:chararray,val:double);

transDataGrp = GROUP transData BY system_item_nbr;

grpData = FOREACH transDataGrp GENERATE FLATTEN(transData);

outputScore = STREAM grpData THROUGH capStreamPy  as (user:chararray,system_item_nbr:chararray,val:double);

scoreConvert = FOREACH outputScore { 
        userSplit = STRSPLIT(user,'_',3);  
        GENERATE (chararray) userSplit.$0 AS membership_nbr, (chararray) userSplit.$1 AS pCardholder_nbr, (chararray) userSplit.$2 AS createDate, system_item_nbr, val;
}

STORE scoreConvert INTO '$outputScore' USING PigStorage('\u0001');


