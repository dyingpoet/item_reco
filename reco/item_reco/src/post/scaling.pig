set default_parallel 1000

recoScore = LOAD '$RecoScore' USING PigStorage('\t') AS (
                membership_nbr: int
                , cardholder_nbr: int
                , membership_create_date: chararray
                , cat_subcat_nbr: chararray
                , score: float
);

--1.1202174
recoScoreScaled = FOREACH recoScore GENERATE membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr, score *0.3 / ((float)$max);

STORE recoScoreScaled INTO '$recoStudyScaled' USING PigStorage('\t');

