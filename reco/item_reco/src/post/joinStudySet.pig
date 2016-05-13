set default_parallel 1000

recoScore = LOAD '$RecoScore' USING PigStorage('\t') AS (
                membership_nbr: int
                , cardholder_nbr: int
                , membership_create_date: chararray
                , cat_subcat_nbr: chararray
                , score: float
);

studySet = LOAD '$StudySet' USING PigStorage('\t') AS (
                cat_subcat_nbr: chararray
);

recoScoreJoin = JOIN recoScore BY cat_subcat_nbr, studySet BY cat_subcat_nbr using 'replicated';

recoScore2 = FOREACH recoScoreJoin GENERATE 
        recoScore::membership_nbr AS membership_nbr
        , recoScore::cardholder_nbr AS cardholder_nbr
        , recoScore::membership_create_date AS membership_create_date
        , recoScore::cat_subcat_nbr AS cat_subcat_nbr
        , recoScore::score AS score
;

recoScoreGrp = GROUP recoScore2 ALL;

recoScoreMax = FOREACH recoScoreGrp GENERATE MAX(recoScore2.score);

STORE recoScore2 INTO '$recoStudy' USING PigStorage('\t');
STORE recoScoreMax INTO '$recoStudyMax' USING PigStorage('\t');

