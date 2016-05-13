set default_parallel 1000



recoScore = LOAD '$RecoScore' USING PigStorage('\u0001') AS (
                membership_nbr: int
                , cardholder_nbr: int
                , membership_create_date: chararray
                , cat_subcat_nbr: chararray
--                , system_item_nbr: int
--                , value_coupon_nbr: int
                , score: float
                , anchor_cat_subcat_nbr: chararray
);


remindSet = LOAD '$RemindSet' USING PigStorage('\u0001') AS 
(
        membership_nbr: int,
        pdcardholder_nbr: int,
        membership_create_date: chararray,
        cat_subcat_nbr: chararray,
        item_list_before:bag{(system_item_nbr: int, total_visit_before: int, total_qty_before: float, total_retail_before: float)},
        item_list_after:bag{(system_item_nbr: int, total_visit_after: int, total_qty_after: float, total_retail_after: float)}
);


remindSetFlt = FILTER remindSet BY SIZE(item_list_before) > 0;

remindWindow = FOREACH remindSetFlt GENERATE membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr;

describe remindWindow;

scoreJoin = JOIN recoScore BY (membership_nbr, cardholder_nbr, membership_create_date, cat_subcat_nbr) LEFT, remindWindow BY (membership_nbr, pdcardholder_nbr, membership_create_date,cat_subcat_nbr);

--scoreNew = FOREACH scoreJoin GENERATE recoScore::membership_nbr, recoScore::cardholder_nbr, recoScore::membership_create_date, recoScore::cat_subcat_nbr, ((remindWindow::cat_subcat_nbr IS NULL)?(recoScore::score):(recoScore::score+0.2)) AS score, recoScore::anchor_cat_subcat_nbr;
scoreNew = FOREACH scoreJoin GENERATE recoScore::membership_nbr, recoScore::cardholder_nbr, recoScore::membership_create_date, recoScore::cat_subcat_nbr, ((remindWindow::cat_subcat_nbr IS NULL)?(recoScore::score):(recoScore::score+0.2)) AS score;

STORE scoreNew INTO '$RecoRemind' USING PigStorage('\t');


