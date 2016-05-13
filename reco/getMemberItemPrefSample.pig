set default_parallel 1000

score = LOAD '$ScoreInput' USING PigStorage('\u0001');

describe score;
/*
--scoreSample = FILTER score BY membershipNumber in ('1130848','764075008','180711'); --pig 0.12
*/

scoreSample = FILTER score BY ($0 == '1130848') OR ($0 == '764075008') OR ($0=='180711'); 

STORE scoreSample INTO '$ScoreSample' USING PigStorage();

 
