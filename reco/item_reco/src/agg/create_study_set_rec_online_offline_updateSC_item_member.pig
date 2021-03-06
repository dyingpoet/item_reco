set default_parallel 500


--Mem0 = LOAD '$MemCombined' USING PigStorage('\u0001') AS (
--        membership_nbr: chararray,
--        membership_create_date: chararray,
--        membership_obsolete_date: chararray,
--        membership_type_code: int,
--        member_status_code: chararray,
--        plus_membership_ind: chararray,
--        pdcardholder_nbr: chararray,
--        pdcardholder_type_code: chararray,
--        pdcardholder_status_code: chararray,
--        pdcardholder_preferred_club_nbr: chararray,
--        cardholders:bag{(cardholder_nbr: chararray, cardholder_type_code: chararray, cardholder_status_code: chararray, preferred_club_nbr: int)}
--);

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

Mem4 = FOREACH Mem3 GENERATE membership_nbr,
                             membership_create_date,
                             membership_obsolete_date,
			     pdcardholder_nbr,
			     cardholder_nbr; 
*/

Mem4 = LOAD '$MemCombined' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        pdcardholder_nbr: chararray,
        membership_create_date: chararray,
        preferred_club: chararray,
        assigned_club: chararray,
        issued_club: chararray
);



-- ItemSubcat = LOAD '$ItemSubCatMapping' USING PigStorage('\u0001') AS (
--         system_item_nbr: long,
--         category_nbr: int,
--         sub_category_nbr: int
-- );


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



TransOnline00 = FILTER TransOnline00 BY unit_retail_amt > 0 and ordered_qty > 0;

-- TransOnline01 = JOIN TransOnline00 BY system_item_nbr, ItemSubcat BY system_item_nbr USING 'replicated';
-- 
-- TransOnline0 = FOREACH TransOnline01 GENERATE TransOnline00::order_nbr AS order_nbr,
--         TransOnline00::system_item_nbr AS system_item_nbr,
--         TransOnline00::membership_nbr AS membership_nbr,
--         TransOnline00::card_holder_nbr AS card_holder_nbr,
--         ItemSubcat::category_nbr AS category_nbr,
--         ItemSubcat::sub_category_nbr AS sub_category_nbr,
--         TransOnline00::unit_retail_amt AS unit_retail_amt,
--         TransOnline00::ordered_qty AS unit_qty,
--         TransOnline00::order_date AS visit_date;

--TransOnline2 = FOREACH TransOnline0 GENERATE membership_nbr AS membership_nbr,
TransOnline2 = FOREACH TransOnline00 GENERATE membership_nbr AS membership_nbr,
					 card_holder_nbr AS card_holder_nbr,
					 --category_nbr AS category_nbr,
					 --sub_category_nbr AS sub_category_nbr,
                                         system_item_nbr AS system_item_nbr,
					 --unit_qty AS unit_qty,
                                         --(double) 1.0 / EXP( ( DaysBetween(ToDate('$today','yyyy-MM-dd'),ToDate(visit_date,'yyyy-MM-dd')))/180.0) as unit_qty,
                                         (double) 1.0 / EXP( ( DaysBetween(ToDate('$today','yyyy-MM-dd'),ToDate(order_date,'yyyy-MM-dd')))/180.0) as unit_qty,
					 order_date AS visit_date;
					 --visit_date AS visit_date;

-- keep only transactions 1) with positive quantities, 2) visit date within a certain range, 3) valid cat, sub cat nbr, 4) non-null membership_nbr, card_holder_nbr
TransOnline3 = FILTER TransOnline2 BY (unit_qty > 0.0 AND 
				   visit_date >= '$DataStartT' AND visit_date <= '$DataEndT' AND 
				   --category_nbr IS NOT NULL AND SIZE((chararray)category_nbr) <= 2 AND
				   --sub_category_nbr IS NOT NULL AND SIZE((chararray)sub_category_nbr) <= 2 AND
                                   system_item_nbr IS NOT NULL AND
				   membership_nbr IS NOT NULL AND card_holder_nbr IS NOT NULL);

TransOnline4 = FOREACH TransOnline3 GENERATE * ; 

TransOnline5 = GROUP TransOnline4 BY (membership_nbr, system_item_nbr, visit_date);

TransOnline6 = FOREACH TransOnline5 GENERATE FLATTEN(group) AS (membership_nbr, system_item_nbr, visit_date),
							MAX(TransOnline4.unit_qty) AS unit_qty;
							--SUM(TransOnline4.unit_qty) AS unit_qty;

TransOnline7 = JOIN Mem4 BY (membership_nbr), TransOnline6 BY (membership_nbr); 


TransOnline8 = FOREACH TransOnline7 GENERATE Mem4::membership_nbr AS membership_nbr,
					 Mem4::membership_create_date AS membership_create_date,
					 --Mem4::membership_obsolete_date AS membership_obsolete_date,
					 Mem4::pdcardholder_nbr AS pdcardholder_nbr, 
					 TransOnline6::system_item_nbr AS system_item_nbr,
					 TransOnline6::unit_qty AS unit_qty,
					 TransOnline6::visit_date AS visit_date;

-- keep transactions that are during the period of [EligibleStartT, EligibleEndT] and truly belong to this member-cardholder
--TransOnline81 = FILTER TransOnline8 BY (visit_date >= membership_create_date AND visit_date < membership_obsolete_date);
TransOnline81 = FILTER TransOnline8 BY (visit_date >= membership_create_date);

TransOnline82 = GROUP TransOnline81 BY (membership_nbr, membership_create_date, pdcardholder_nbr, system_item_nbr, visit_date);

TransOnline83 = FOREACH TransOnline82 GENERATE FLATTEN(group) AS (membership_nbr, membership_create_date, pdcardholder_nbr, system_item_nbr, visit_date),
					   MAX(TransOnline81.unit_qty) AS unit_qty;
--					   SUM(TransData81.unit_qty) AS unit_qty;





TransOffline00 = FILTER TransOffline00 BY unit_qty > 0 and retail_all > 0 and sub_category_nbr!=91 and sub_category_nbr!=97;

-- TransOffline01 = JOIN TransOffline00 BY (system_item_nbr), ItemSubcat BY (system_item_nbr) USING 'replicated';
-- 
-- TransOffline0 = FOREACH TransOffline01 GENERATE TransOffline00::visit_nbr AS visit_nbr,
--                                           TransOffline00::club_nbr AS club_nbr,
--                                           TransOffline00::system_item_nbr AS system_item_nbr,
--                                           TransOffline00::household_id AS household_id,
--                                           TransOffline00::membership_nbr AS membership_nbr,
--                                           TransOffline00::card_holder_nbr AS card_holder_nbr,
--                                           ItemSubcat::category_nbr AS category_nbr,
--                                           ItemSubcat::sub_category_nbr AS sub_category_nbr,
--                                           TransOffline00::retail_all AS retail_all,
--                                           TransOffline00::unit_qty AS unit_qty,
--                                           TransOffline00::retail_per_item AS retail_per_item,
--                                           TransOffline00::cost_per_item AS cost_per_item,
--                                           TransOffline00::visit_date AS visit_date;


-- time decayed transactions
TransOffline0 = foreach TransOffline00 generate
	 visit_nbr,
         club_nbr,
         system_item_nbr,
         --household_id,
         membership_nbr,
         card_holder_nbr,
         category_nbr,
         sub_category_nbr,
         (double) retail_all / EXP( ( DaysBetween(ToDate('$today','yyyy-MM-dd'),ToDate(visit_date,'yyyy-MM-dd')))/365.0) as retail_all,
         (double) 1.0 / EXP( ( DaysBetween(ToDate('$today','yyyy-MM-dd'),ToDate(visit_date,'yyyy-MM-dd')))/180.0) as unit_qty,
         --retail_per_item,
         --cost_per_item,
         visit_date
;


-- keep only transactions within studycats
--TransOffline1 = JOIN TransOffline0 By (category_nbr, sub_category_nbr), StudyCats2 By (cat_nbr, subcat_nbr) USING 'replicated';

--TransOffline2 = FOREACH TransOffline1 GENERATE TransOffline0::membership_nbr AS membership_nbr,
--					 TransOffline0::card_holder_nbr AS card_holder_nbr,
--					 TransOffline0::category_nbr AS category_nbr,
--					 TransOffline0::sub_category_nbr AS sub_category_nbr,
--					 TransOffline0::unit_qty AS unit_qty,
--					 TransOffline0::visit_date AS visit_date;

TransOffline2 = FOREACH TransOffline0 GENERATE membership_nbr AS membership_nbr,
					 card_holder_nbr AS card_holder_nbr,
					 --category_nbr AS category_nbr,
					 --sub_category_nbr AS sub_category_nbr,
                                         system_item_nbr AS system_item_nbr,
					 unit_qty AS unit_qty,
					 visit_date AS visit_date;

-- keep only transactions 1) with positive quantities, 2) visit date within a certain range, 3) valid cat, sub cat nbr, 4) non-null membership_nbr, card_holder_nbr
TransOffline3 = FILTER TransOffline2 BY (unit_qty > 0.0 AND 
				   visit_date >= '$DataStartT' AND visit_date <= '$DataEndT' AND 
				   --category_nbr IS NOT NULL AND SIZE((chararray)category_nbr) <= 2 AND
				   --sub_category_nbr IS NOT NULL AND SIZE((chararray)sub_category_nbr) <= 2 AND
                                   system_item_nbr IS NOT NULL AND
				   membership_nbr IS NOT NULL AND card_holder_nbr IS NOT NULL);

TransOffline4 = FOREACH TransOffline3 GENERATE * ; 

TransOffline5 = GROUP TransOffline4 BY (membership_nbr, system_item_nbr, visit_date);

TransOffline6 = FOREACH TransOffline5 GENERATE FLATTEN(group) AS (membership_nbr, system_item_nbr, visit_date),
							MAX(TransOffline4.unit_qty) AS unit_qty;
							--SUM(TransOffline4.unit_qty) AS unit_qty;

TransOffline7 = JOIN Mem4 BY (membership_nbr), TransOffline6 BY (membership_nbr); 


TransOffline8 = FOREACH TransOffline7 GENERATE Mem4::membership_nbr AS membership_nbr,
					 Mem4::membership_create_date AS membership_create_date,
					 --Mem4::membership_obsolete_date AS membership_obsolete_date,
					 Mem4::pdcardholder_nbr AS pdcardholder_nbr, 
					 TransOffline6::system_item_nbr AS system_item_nbr,
					 TransOffline6::unit_qty AS unit_qty,
					 TransOffline6::visit_date AS visit_date;

-- keep transactions that are during the period of [EligibleStartT, EligibleEndT] and truly belong to this member-cardholder
--TransOffline81 = FILTER TransOffline8 BY (visit_date >= membership_create_date AND visit_date < membership_obsolete_date);
TransOffline81 = FILTER TransOffline8 BY (visit_date >= membership_create_date);

TransOffline82 = GROUP TransOffline81 BY (membership_nbr, membership_create_date, pdcardholder_nbr, system_item_nbr, visit_date);

TransOffline83 = FOREACH TransOffline82 GENERATE FLATTEN(group) AS (membership_nbr, membership_create_date, pdcardholder_nbr, system_item_nbr, visit_date),
					   MAX(TransOffline81.unit_qty) AS unit_qty;
--					   SUM(TransData81.unit_qty) AS unit_qty;


TransData83 = UNION TransOnline83, TransOffline83;





-- apply eligibility rules to get users qualified for reward
TransData9 = FILTER TransData83 BY (visit_date >= '$EligibleStartT' AND visit_date <= '$EligibleEndT');

-- group data to get the study set
TransData10 = GROUP TransData9 BY (membership_nbr, pdcardholder_nbr, membership_create_date, system_item_nbr);

StudySet = FOREACH TransData10 GENERATE FLATTEN(group) AS (membership_nbr, pdcardholder_nbr, membership_create_date, system_item_nbr), SUM(TransData9.unit_qty) AS total_qty;

StudySet = FOREACH StudySet GENERATE CONCAT(CONCAT(CONCAT(membership_nbr,'_'),CONCAT(pdcardholder_nbr,'_')),membership_create_date) as user, system_item_nbr, total_qty; 

STORE StudySet INTO '$OutStudySet' Using PigStorage('\u0001');
