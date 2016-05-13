set default_parallel 1000

/*
Mem0 = LOAD '$MemCombined' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        membership_create_date: chararray,
        membership_obsolete_date: chararray,
        membership_type_code: int,
        member_status_code: chararray,
        plus_membership_ind: chararray,
        pdcardholder_nbr: chararray,
        pdcardholder_type_code: chararray,
        pdcardholder_status_code: chararray,
        pdcardholder_issuing_club_nbr: chararray,
        pdcardholder_assigned_club_nbr: chararray,
        pdcardholder_preferred_club_nbr: chararray,
        cardholders:bag{(cardholder_nbr: chararray, cardholder_type_code: chararray, cardholder_status_code: chararray, issuing_club_nbr: chararray, assigned_club_nbr: chararray, preferred_club_nbr: chararray)}
);


Mem1 = FOREACH Mem0 GENERATE membership_nbr,
                             pdcardholder_nbr,
                             membership_create_date,
                             membership_obsolete_date;
Mem2 = DISTINCT Mem1;

Mem3 = FOREACH Mem0 GENERATE membership_nbr,
			     membership_create_date,
			     membership_obsolete_date,
			     pdcardholder_nbr,
                             FLATTEN(cardholders) AS (cardholder_nbr, cardholder_type_code, cardholder_status_code, issuing_club_nbr, assigned_club_nbr, preferred_club_nbr);
			     --FLATTEN(cardholders) AS (cardholder_nbr, cardholder_type_code, cardholder_status_code, preferred_club_nbr);

Mem4 = FOREACH Mem3 GENERATE membership_nbr,
                             membership_create_date,
                             membership_obsolete_date,
			     pdcardholder_nbr,
			     cardholder_nbr; 
*/


-- Load the raw transaction data
TransOnline00 = LOAD '$TransOnline' USING PigStorage('\u0001') AS (
         order_nbr: chararray,
         system_item_nbr: long,
         membership_nbr: chararray,
         card_holder_nbr: chararray,
         category_nbr: int,
         sub_category_nbr: int,
         unit_retail_amt: float,
         ordered_qty: float,
         order_date: chararray
);

TransOffline00 = LOAD '$TransOffline' USING PigStorage('\u0001') AS (
         visit_nbr: int,
         club_nbr: int,
         system_item_nbr: long,
         household_id: long,
         membership_nbr: chararray,
         card_holder_nbr: chararray,
         category_nbr: int,
         sub_category_nbr: int,
         retail_all: float,
         unit_qty: double,
         retail_per_item: float,
         cost_per_item: double,
         visit_date: chararray
);

TransOffline0 = FILTER TransOffline00 BY unit_qty > 0 and retail_all > 0 and sub_category_nbr!=91 and sub_category_nbr!=97;

--TransOnline0 = FOREACH TransOnline00 GENERATE (int) order_nbr AS visit_nbr, (int) 0 AS club_nbr, (long) system_item_nbr, (long) 0 AS household_id, (chararray) membership_nbr, (chararray) card_holder_nbr, (int) category_nbr, (int) sub_category_nbr, (float) unit_retail_amt*ordered_qty AS retail_all, (double) ordered_qty AS unit_qty, (float) unit_retail_amt AS retail_per_item, (double) 0.0 AS cost_per_item, (chararray) order_date AS visit_date;


TransOnline0a = FOREACH TransOnline00 GENERATE (int) order_nbr AS visit_nbr, (int) 6279 AS club_nbr, (long) system_item_nbr, (long) 0 AS household_id, (chararray) membership_nbr, (chararray) card_holder_nbr, (int) category_nbr, (int) sub_category_nbr, (float) unit_retail_amt*ordered_qty AS retail_all, (double) ordered_qty AS unit_qty, (float) unit_retail_amt AS retail_per_item, (double) 0.0 AS cost_per_item, (chararray) order_date AS visit_date;
TransOnline0b = FOREACH TransOnline00 GENERATE (int) order_nbr AS visit_nbr, (int) 4753 AS club_nbr, (long) system_item_nbr, (long) 0 AS household_id, (chararray) membership_nbr, (chararray) card_holder_nbr, (int) category_nbr, (int) sub_category_nbr, (float) unit_retail_amt*ordered_qty AS retail_all, (double) ordered_qty AS unit_qty, (float) unit_retail_amt AS retail_per_item, (double) 0.0 AS cost_per_item, (chararray) order_date AS visit_date;

TransOnline0 = UNION TransOnline0a, TransOnline0b;


TransData00 = UNION TransOnline0, TransOffline0;


-- time decayed transactions
TransData00 = foreach TransData00 generate
	 visit_nbr,
         club_nbr,
         system_item_nbr,
         household_id,
         membership_nbr,
         card_holder_nbr,
         category_nbr,
         sub_category_nbr,
         (double) 1.0 / EXP( ( DaysBetween(ToDate('2014-12-30','yyyy-MM-dd'),ToDate(visit_date,'yyyy-MM-dd')))/365.0) as retail_all,
         (double) 1.0 as unit_qty,
         retail_per_item,
         cost_per_item,
         visit_date
;

/*
filterClub = LOAD '$FilterClub' USING PigStorage('\u0001') AS (club_nbr:int);

TransData56 = JOIN TransData00 by club_nbr, filterClub by club_nbr using 'replicated'; 

TransData01 = FOREACH TransData56 GENERATE 
         TransData00::visit_nbr AS          visit_nbr
         , TransData00::club_nbr AS          club_nbr
         , TransData00::system_item_nbr AS          system_item_nbr
         , TransData00::household_id AS          household_id
         , TransData00::membership_nbr AS          membership_nbr
         , TransData00::card_holder_nbr AS          card_holder_nbr
         , TransData00::category_nbr AS          category_nbr
         , TransData00::sub_category_nbr AS          sub_category_nbr
         , TransData00::retail_all AS          retail_all
         , TransData00::unit_qty AS          unit_qty
         , TransData00::retail_per_item AS          retail_per_item
         , TransData00::cost_per_item AS          cost_per_item
         , TransData00::visit_date AS          visit_date
;
*/
TransData01 = FOREACH TransData00 GENERATE *;


ItemSubcat = LOAD '$ItemSubCatMapping' USING PigStorage('\u0001') AS (
        system_item_nbr: long,
        category_nbr: int,
        sub_category_nbr: int
);

TransData02 = JOIN TransData01 BY system_item_nbr, ItemSubcat BY system_item_nbr USING 'replicated';

TransData0 = FOREACH TransData02 GENERATE TransData01::visit_nbr AS visit_nbr,
                                         TransData01::club_nbr AS club_nbr,
                                         TransData01::system_item_nbr AS system_item_nbr,
                                         TransData01::household_id AS household_id,
                                         TransData01::membership_nbr AS membership_nbr,
                                         TransData01::card_holder_nbr AS card_holder_nbr,
                                         ItemSubcat::category_nbr AS category_nbr,
                                         ItemSubcat::sub_category_nbr AS sub_category_nbr,
                                         TransData01::retail_all AS retail_all,
                                         TransData01::unit_qty AS unit_qty,
                                         TransData01::retail_per_item AS retail_per_item,
                                         TransData01::cost_per_item AS cost_per_item,
                                         TransData01::visit_date AS visit_date;


TransData2 = FOREACH TransData0 GENERATE membership_nbr AS membership_nbr,
					 card_holder_nbr AS card_holder_nbr,
					 category_nbr AS category_nbr,
					 sub_category_nbr AS sub_category_nbr,
                                         system_item_nbr AS system_item_nbr,
                                         club_nbr AS club_nbr,
					 unit_qty AS unit_qty,
					 visit_date AS visit_date;

-- keep only transactions 1) with positive quantities, 2) visit date within a certain range, 3) valid cat, sub cat nbr, 4) non-null membership_nbr, card_holder_nbr
TransData3 = FILTER TransData2 BY (unit_qty > 0.0 AND 
				   visit_date >= '$DataStartT' AND visit_date <= '$DataEndT' AND 
				   category_nbr IS NOT NULL AND SIZE((chararray)category_nbr) <= 2 AND
				   sub_category_nbr IS NOT NULL AND SIZE((chararray)sub_category_nbr) <= 2 AND
				   membership_nbr IS NOT NULL AND card_holder_nbr IS NOT NULL);

TransData4 = FOREACH TransData3 GENERATE membership_nbr,
					 card_holder_nbr,
					 CONCAT((SIZE((chararray)category_nbr) == 1 ? CONCAT('0', (chararray)category_nbr) : (chararray)category_nbr),
                        		     (SIZE((chararray)sub_category_nbr) == 1 ? CONCAT('0', (chararray)sub_category_nbr) : (chararray)sub_category_nbr)) AS cat_subcat_nbr,
                                         system_item_nbr,
                                         club_nbr AS club_nbr,
					 unit_qty,
					 visit_date;

--TransData5 = GROUP TransData4 BY (membership_nbr, card_holder_nbr, cat_subcat_nbr, system_item_nbr, visit_date);

--TransData6 = FOREACH TransData5 GENERATE FLATTEN(group) AS (membership_nbr, card_holder_nbr, cat_subcat_nbr, system_item_nbr, visit_date),
--							MAX(TransData4.unit_qty) AS unit_qty;

--TransData7 = JOIN Mem4 BY (membership_nbr, cardholder_nbr), TransData4 BY (membership_nbr, card_holder_nbr); 
--
--
--TransData8 = FOREACH TransData7 GENERATE Mem4::membership_nbr AS membership_nbr,
--					 Mem4::membership_create_date AS membership_create_date,
--					 Mem4::membership_obsolete_date AS membership_obsolete_date,
--					 Mem4::pdcardholder_nbr AS pdcardholder_nbr, 
--					 TransData4::cat_subcat_nbr AS cat_subcat_nbr,
--					 TransData4::system_item_nbr AS system_item_nbr,
--					 TransData4::unit_qty AS unit_qty,
--					 TransData4::visit_date AS visit_date;

TransData8 = FOREACH TransData4 GENERATE membership_nbr AS membership_nbr,
					 card_holder_nbr AS cardholder_nbr, 
					 cat_subcat_nbr AS cat_subcat_nbr,
					 system_item_nbr AS system_item_nbr,
                                         club_nbr AS club_nbr,
					 unit_qty AS unit_qty,
					 visit_date AS visit_date;

-- keep transactions that are during the period of [EligibleStartT, EligibleEndT] and truly belong to this member-cardholder

-- apply eligibility rules to get users qualified for reward
--TransData9 = FILTER TransData83 BY (visit_date >= '$EligibleStartT' AND visit_date <= '$EligibleEndT');

-- group data to get the study set
TransData10 = GROUP TransData8 BY (club_nbr,cat_subcat_nbr, system_item_nbr);

TransData11 = FOREACH TransData10 GENERATE group.club_nbr AS club_nbr, group.cat_subcat_nbr AS cat_subcat_nbr, group.system_item_nbr AS system_item_nbr, COUNT(TransData8) AS countVisits;
--STORE TransData11 INTO '$OutIntermediate' Using PigStorage('\t');


--TransData11Join = JOIN TransData11 BY system_item_nbr, itemDesc BY system_item_nbr;

--TransData12 = FOREACH TransData11Join GENERATE TransData11::cat_subcat_nbr AS cat_subcat_nbr, TransData11::system_item_nbr AS system_item_nbr, TransData11::countVisits AS countVisits, itemDesc::item_desc AS item_desc;
--TransData12 = FOREACH TransData11Join GENERATE TransData11::cat_subcat_nbr AS cat_subcat_nbr, TransData11::system_item_nbr AS system_item_nbr, TransData11::countVisits AS countVisits, itemDesc::customer_item_nbr as customer_item_nbr, itemDesc::item1_desc AS item1_desc, itemDesc::item2_desc AS item2_desc, itemDesc::base_unit_retail_amt AS base_unit_retail_amt;


TransData13 = GROUP TransData11 BY (club_nbr,cat_subcat_nbr);

TransData14 = FOREACH TransData13 {
    transOrder = ORDER TransData11 BY countVisits DESC;
    --transLimit = LIMIT transOrder 5;
    --GENERATE group AS cat_subcat_nbr, transLimit;
    --GENERATE group AS cat_subcat_nbr, transOrder;
    GENERATE FLATTEN(group) AS (club_nbr,cat_subat_nbr),transOrder;
}

define add_rank `/usr/bin/python add_rank_club.py`
    input (stdin using PigStreaming('|'))
    output (stdout using PigStreaming('|'))
    ship('$TaskDir/add_rank_club.py','$TaskDir/bag.py');

StudySet = STREAM TransData14 THROUGH add_rank AS (club_nbr:chararray,cat_subcat_nbr:chararray, system_item_nbr:int, rank:int);


STORE StudySet INTO '$OutputGlobalPref' Using PigStorage('\u0001');


