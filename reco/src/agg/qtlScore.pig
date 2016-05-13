SET mapred.task.timeout 1800000
set default_parallel 500


--define capStreamPy `/usr/bin/python qtlScore.py $nQtl`
--define capStreamPy `/usr/bin/Rscript qtlScore.r $nQtl`
define capStreamPy `/usr/bin/python qtlScore_self.py $nQtl`
    input (stdin using PigStreaming('|'))
    output (stdout using PigStreaming('|'))
    ship('$TaskDir/qtlScore_self.py')
;


transData = LOAD '$inputScore' USING PigStorage('\u0001') AS (user:chararray,catsubcat:chararray,val:double);

transDataGrp = GROUP transData BY catsubcat;

grpData = FOREACH transDataGrp GENERATE FLATTEN(transData);

outputScore = STREAM grpData THROUGH capStreamPy  as (user:chararray,catsubcat:chararray,val:double);

STORE outputScore INTO '$outputScore' USING PigStorage('\u0001');


