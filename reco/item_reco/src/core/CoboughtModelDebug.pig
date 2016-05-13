SET mapred.task.timeout 1800000
set default_parallel 400

define scoreStreamPy `/usr/bin/python CoboughtModelMain.py subcat.txt cobought.txt seasonality.txt idf.txt $targetMonth $targetNumSubcats $method $targetIdf`
    input (stdin using PigStreaming('|'))
    output (stdout using PigStreaming('|'))
    ship('$core_dir/CoboughtModelBackendCalcEngine.py', '$core_dir/CoboughtModelConfig.py', '$core_dir/CoboughtModelInput.py', '$core_dir/CoboughtModelMain.py', '$core_dir/CoboughtModelUserHist.py', '$core_dir/CoboughtModelUtil.py')
    cache('$subcat#subcat.txt','$cobought#cobought.txt','$seasonality#seasonality.txt','$idf#idf.txt')
;
    --ship('/home/jli21/sams/offer/2014dec/scoring/src/core/CoboughtModelBackendCalcEngine.py', '/home/jli21/sams/offer/2014dec/scoring/src/core/CoboughtModelConfig.py', '/home/jli21/sams/offer/2014dec/scoring/src/core/CoboughtModelInput.py', '/home/jli21/sams/offer/2014dec/scoring/src/core/CoboughtModelMain.py', '/home/jli21/sams/offer/2014dec/scoring/src/core/CoboughtModelUserHist.py', '/home/jli21/sams/offer/2014dec/scoring/src/core/CoboughtModelUtil.py')
    --ship('$core_dir/CoboughtModelBackendCalcEngine.py', '$core_dir/CoboughtModelConfig.py', '$core_dir/CoboughtModelInput.py', '$core_dir/CoboughtModelMain.py', '$core_dir/CoboughtModelUserHist.py', '$core_dir/CoboughtModelUtil.py')
    --ship('./CoboughtModelBackendCalcEngine.py', './CoboughtModelConfig.py', './CoboughtModelInput.py', './CoboughtModelMain.py', './CoboughtModelUserHist.py', './CoboughtModelUtil.py')


transData = LOAD '$transData' USING PigStorage('\u0001') AS (user:chararray, subcat:chararray, value:double);
--transData = LOAD '$transData' USING PigStorage('\u0001') AS (user:chararray, subcat:chararray, value:double, memberType:chararray, memberRegion:chararray);
--transData = load '$transData' using PigStorage('\u0001') as (membership_nbr:chararray, paid_cardholder_nbr:chararray, create_date:chararray, subcat:chararray, value:double);
--transData = foreach transData generate CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(paid_cardholder_nbr,'_')),create_date) as user, subcat, value;

--transData = foreach transData generate *, 'a' as memberType, 'a' as memberRegion;

transDataGrp = GROUP transData BY user;

grpData = FOREACH transDataGrp GENERATE FLATTEN(transData);

score = STREAM grpData THROUGH scoreStreamPy AS (user:chararray,subcat:chararray,score:double, anchorSubcat:chararray);

-- convert to the same scale as reward-type score
scoreConvert = FOREACH score { 
	userSplit = STRSPLIT(user,'_',3);  
	GENERATE (chararray) userSplit.$0 AS membership_nbr, (chararray) userSplit.$1 AS pCardholder_nbr, (chararray) userSplit.$2 AS createDate, subcat, score, anchorSubcat;}

STORE scoreConvert INTO '$output' USING PigStorage('\u0001');
