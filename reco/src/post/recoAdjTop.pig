set default_parallel 1000



recoScore0 = LOAD '$RecoScore' USING PigStorage('\u0001') AS (
                membership_nbr: int
                , cardholder_nbr: int
                , membership_create_date: chararray
                , cat_subcat_nbr: chararray
--                , system_item_nbr: int
--                , value_coupon_nbr: int
                , score: float
                , anchor_cat_subcat_nbr: chararray
);

recoScore = FILTER recoScore0 BY score > 1e-4;
STORE recoScore INTO '$RecoTop' USING PigStorage('\u0001');
