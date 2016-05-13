set default_parallel 1000

/* input: 1) club_item output from club_avail.pig <itemclub> 10172^A8224	2) popularity output from item_club_preference.pig  delim
   output: <club,subcat,item>
*/


clubAvail = LOAD '$clubAvail' Using PigStorage('\u0001') AS (item:int,club:chararray);

--4876^A4920^A23811729^A7
itemClubPopularity = LOAD '$itemClubPopularity' USING PigStorage('\u0001') AS (
    club: chararray
    , subcat: chararray
    , system_item_nbr: int
    , rank: float
);

popularityJoin = JOIN clubAvail BY (item,club), itemClubPopularity BY (system_item_nbr, club);

popularAvail = FOREACH popularityJoin GENERATE 
    itemClubPopularity::club AS club
    , itemClubPopularity::subcat AS subcat
    , itemClubPopularity::system_item_nbr AS system_item_nbr
    , itemClubPopularity::rank
;

popularGrp = GROUP popularAvail BY (club,subcat);

popularTop1 = FOREACH popularGrp {
                popularOrder = ORDER popularAvail BY itemClubPopularity::rank;
                popularTop = LIMIT popularOrder 1;
                GENERATE FLATTEN(group) AS (club,subcat), FLATTEN(popularTop.system_item_nbr);
}

STORE popularTop1 INTO '$popularSubcat2Item' Using PigStorage('\u0001');
/*
*/





