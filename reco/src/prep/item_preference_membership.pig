set default_parallel 800

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
        membership_create_date,
        membership_obsolete_date,
        pdcardholder_nbr,
        FLATTEN(cardholders) AS (cardholder_nbr, cardholder_type_code, cardholder_status_code, issuing_club_nbr, assigned_club_nbr, preferred_club_nbr);

Mem2 = FOREACH Mem1 GENERATE membership_nbr,
        membership_create_date,
        membership_obsolete_date,
        pdcardholder_nbr,
        cardholder_nbr;
*/

Mem2 = LOAD '$MemCombined' USING PigStorage('\u0001') AS (
        membership_nbr: chararray,
        pdcardholder_nbr: chararray,
        membership_create_date: chararray,
        preferred_club: chararray,
        assigned_club: chararray,
        issued_club: chararray
);

/*
TransData0 = LOAD '$TransData' USING PigStorage('\u0001') AS (
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
*/

ItemSubcat = LOAD '$ItemSubCatMapping' USING PigStorage('\u0001') AS (
        system_item_nbr: long,
        category_nbr: int,
        sub_category_nbr: int
);


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

TransOnline0 = FOREACH TransOnline00 GENERATE (int) order_nbr AS visit_nbr, (int) 0 AS club_nbr, (long) system_item_nbr, (long) 0 AS household_id, (chararray) membership_nbr, (chararray) card_holder_nbr, (int) category_nbr, (int) sub_category_nbr, (float) unit_retail_amt*ordered_qty AS retail_all, (double) ordered_qty AS unit_qty, (float) unit_retail_amt AS retail_per_item, (double) 0.0 AS cost_per_item, (chararray) order_date AS visit_date;

TransData0 = UNION TransOnline0, TransOffline0;



TransData1 = JOIN TransData0 BY system_item_nbr, ItemSubcat BY system_item_nbr USING 'replicated';

TransData2 = FOREACH TransData1 GENERATE TransData0::visit_nbr AS visit_nbr,
                                         TransData0::club_nbr AS club_nbr,
                                         TransData0::system_item_nbr AS system_item_nbr,
                                         TransData0::household_id AS household_id,
                                         TransData0::membership_nbr AS membership_nbr,
                                         TransData0::card_holder_nbr AS card_holder_nbr,
                                         ItemSubcat::category_nbr AS category_nbr,
                                         ItemSubcat::sub_category_nbr AS sub_category_nbr,
                                         TransData0::retail_all AS retail_all,
                                         TransData0::unit_qty AS unit_qty,
                                         TransData0::retail_per_item AS retail_per_item,
                                         TransData0::cost_per_item AS cost_per_item,
                                         TransData0::visit_date AS visit_date;

TransData3 = FILTER TransData2 BY (unit_qty > 0.0 AND retail_all > 0.0 AND
                                   --(visit_date >= '$DateLB' AND visit_date <= '$DateUB') AND
                                   ((visit_date >= '$DateLB' AND visit_date <= '$DateUB') OR (visit_date >= '$SPLB' AND visit_date <= '$SPUB')) AND
                                   category_nbr IS NOT NULL AND SIZE((chararray)category_nbr) <= 2 AND
                                   sub_category_nbr IS NOT NULL AND SIZE((chararray)sub_category_nbr) <= 2 AND
                                   membership_nbr IS NOT NULL AND card_holder_nbr IS NOT NULL);

TransData4 = FOREACH TransData3 GENERATE membership_nbr,
                                         card_holder_nbr,
                                        CONCAT((SIZE((chararray)category_nbr) == 1 ? CONCAT('0', (chararray)category_nbr) : (chararray)category_nbr),
                                        (SIZE((chararray)sub_category_nbr) == 1 ? CONCAT('0', (chararray)sub_category_nbr) : (chararray)sub_category_nbr)) AS cat_subcat_nbr,
                                        system_item_nbr,
                                        retail_all,
                                        unit_qty,
                                        visit_date;


TransData5 = GROUP TransData4 BY (membership_nbr, card_holder_nbr, cat_subcat_nbr, system_item_nbr, visit_date) PARALLEL 500;

TransData6 = FOREACH TransData5 GENERATE FLATTEN(group) AS (membership_nbr, card_holder_nbr, cat_subcat_nbr, system_item_nbr, visit_date),
                                                        SUM(TransData4.retail_all) AS retail_all,
                                                        SUM(TransData4.unit_qty) AS unit_qty;

TransData7 = JOIN Mem2 BY (membership_nbr), TransData6 BY (membership_nbr) PARALLEL 500;

TransData8 = FOREACH TransData7 GENERATE Mem2::membership_nbr AS membership_nbr,
        Mem2::pdcardholder_nbr AS pdcardholder_nbr,
        --Mem2::cardholder_nbr AS cardholder_nbr,
        Mem2::membership_create_date AS membership_create_date,
        TransData6::cat_subcat_nbr AS cat_subcat_nbr,
        TransData6::system_item_nbr AS system_item_nbr,
        TransData6::visit_date AS visit_date,
        TransData6::unit_qty AS unit_qty,
        TransData6::retail_all AS retail_all;

TransData9 = FILTER TransData8 BY (visit_date >= membership_create_date);

TransData10 = GROUP TransData9 BY (membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr) PARALLEL 500;

TransData11 = FOREACH TransData10 GENERATE FLATTEN(group) AS (membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr, system_item_nbr),
TransData9.(visit_date, unit_qty, retail_all) AS trans;

--STORE TransData11 INTO '$OutStage' USING PigStorage('\u0001');


TransData12 = FOREACH TransData11 {
                trans_after = FILTER trans BY (visit_date >= '$SplitDate');
                trans_before = FILTER trans BY (visit_date < '$SplitDate');
                GENERATE membership_nbr,
                         pdcardholder_nbr,
                         membership_create_date,
                         cat_subcat_nbr,
                         system_item_nbr,
                         (trans_after IS NULL ? 0.0 : SUM(trans_after.unit_qty)) AS total_qty_after,
                         (trans_after IS NULL ? 0.0 : SUM(trans_after.retail_all)) AS total_retail_after,
                         (trans_after IS NULL ? 0 : COUNT(trans_after)) AS total_visit_after,
                         (trans_before IS NULL ? 0.0 : SUM(trans_before.unit_qty)) AS total_qty_before,
                         (trans_before IS NULL ? 0.0 : SUM(trans_before.retail_all)) AS total_retail_before,
                         (trans_before IS NULL ? 0 : COUNT(trans_before)) AS total_visit_before;
}

TransData13 = GROUP TransData12 BY (membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr);

TransData14 = FOREACH TransData13 {
                item_list = TransData12.(system_item_nbr, total_qty_after, total_retail_after, total_visit_after, total_qty_before, total_retail_before, total_visit_before);
                item_list_before = FILTER item_list BY (total_visit_before > 0);
                item_list_after = FILTER item_list BY (total_visit_after > 0);
                ordered_item_list_before = ORDER item_list_before BY total_visit_before DESC;
                ordered_item_list_after = ORDER item_list_after BY total_visit_after DESC;
                GENERATE FLATTEN(group) AS (membership_nbr, pdcardholder_nbr, membership_create_date, cat_subcat_nbr),
                         ordered_item_list_before.(system_item_nbr, total_visit_before, total_qty_before, total_retail_before) AS item_list_before,
                         ordered_item_list_after.(system_item_nbr, total_visit_after, total_qty_after, total_retail_after) AS item_list_after;
}

STORE TransData14 INTO '$OutputMemberPref' USING PigStorage('\u0001');


