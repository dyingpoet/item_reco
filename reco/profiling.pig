set default_parallel 1000
register 'datafu-0.0.11-SNAPSHOT.jar';

define Median datafu.pig.stats.StreamingMedian();
define Quantile datafu.pig.stats.StreamingQuantile('0.0','0.25','0.5','0.75','1.0');
define VAR datafu.pig.stats.VAR();



A = load '$input' using PigStorage('\u0001') as (
membership_nbr:chararray 
--, pCardholder_nbr:chararray
, createDate:chararray
, subcat:chararray
, score:double
);

B = foreach A generate score as val;

grouped = GROUP B ALL;

--medians = FOREACH grouped {sorted = order B by val; GENERATE Median(sorted.val);}

--quantiles = FOREACH grouped {sorted = order B by val; GENERATE Quantile(sorted.val);}

mean  = FOREACH grouped generate AVG(B.val) as mean;

--describe medians;
--describe quantiles;
--describe mean;
--dump medians;
--dump quantiles;
--dump mean;
--
--store medians into '$medians' using PigStorage('\t');
--store quantiles into '$quantiles' using PigStorage('\t');
store mean into '$mean' using PigStorage('\t');

