/*
SET mapred.job.name '$JOB_NAME';
SET mapred.max.split.size '$MAX_SPLIT_SIZE'; 
SET mapred.job.queue.name '$QUEUE_NAME';
*/
-- LOAD input data files

input_rp = LOAD '$INPUT_RP_EXTRACT_PATH' USING PigStorage(',', '-schema'); 

input_prism  = LOAD '$INPUT_PRISM_EXTRACT_PATH' USING PigStorage(',', '-schema');

input_promo = LOAD '$INPUT_PROMO_EXTRACT_PATH' USING PigStorage(',', '-schema');

input_sbt = LOAD '$INPUT_SBT_EXTRACT_PATH' USING PigStorage(',', '-schema');

project_sbt = FOREACH input_sbt GENERATE
  departmentId,
  classId,
  itemId,
  sbt_item;

distinct_sbt = DISTINCT project_sbt PARALLEL 20;

input_sales_restricted = LOAD '$INPUT_SALES_RESTRICTED_EXTRACT_PATH' USING PigStorage(',', '-schema');
 
/* 
[-] Possible options 
-- COGROUP the aliases and Flatten  
-- Union on schema  and remove duplicates and filter 
-- Leftouter join for each feed 
    as PIG OUTER JOIN does not work in a single line for multiple Aliases [Currently used to keep the logic simple]
*/

joined_rp_promo = JOIN input_rp BY (departmentId, classId, itemId) LEFT OUTER, input_promo BY (departmentId,classId, itemId) PARALLEL 20;

project_rp_promo = FOREACH joined_rp_promo GENERATE
    input_rp::departmentId AS departmentId:int,
    input_rp::classId AS classId:int,
    input_rp::itemId AS itemId:int,
    input_rp::height AS height:double,
    input_rp::width AS width:double,
    input_rp::depth AS depth:double,
    input_rp::max_dim AS max_dim:double,
    input_rp::med_dim AS med_dim:double,
    input_rp::min_dim AS min_dim:double,
    input_rp::entertainmentsegmentationstartingsalesdate AS entertainmentsegmentationstartingsalesdate:chararray,
    input_rp::prepaidownercode AS prepaidownercode:chararray,
    input_rp::isflammable AS isflammable:boolean,
    input_rp::ishazardousmaterial AS ishazardousmaterial:boolean,
    input_rp::prev_isHold AS prev_isHold:int,
    input_rp::prev_isShip AS prev_isShip:int,
    input_rp::prev_isRush AS prev_isRush:int,
    input_rp::prev_isShipToStore AS prev_isShipToStore:int,
    input_rp::prev_isHoldMarkets AS prev_isHoldMarkets:chararray,
    input_rp::prev_isShipMarkets AS prev_isShipMarkets:chararray,
    input_rp::prev_isRushMarkets AS prev_isRushMarkets:chararray,
    input_rp::prev_isShipToStoreMarkets AS prev_isShipToStoreMarkets:chararray,
    input_rp::mca_isHold AS mca_isHold:boolean,
    input_rp::mca_isShip AS mca_isShip:boolean,
    input_rp::mca_isRush AS mca_isRush:boolean,
    input_rp::mca_isShipToStore AS mca_isShipToStore:boolean,
    input_rp::mca_isHoldMarkets AS mca_isHoldMarkets:chararray,
    input_rp::mca_isShipMarkets AS mca_isShipMarkets:chararray,
    input_rp::mca_isRushMarkets AS mca_isRushMarkets:chararray,
    input_rp::mca_isShipToStoreMarkets AS mca_isShipToStoreMarkets:chararray,
    input_rp::isHoldExcluded AS isHoldExcluded:boolean,
    input_rp::isShipExcluded AS isShipExcluded:boolean,
    input_rp::isRushExcluded AS isRushExcluded:boolean,
    input_rp::isShipToStoreExcluded AS isShipToStoreExcluded:boolean,
    input_rp::future_street_date AS future_street_date:int,
    input_rp::prepaid_item AS prepaid_item:int,
    input_rp::hazardous_flammable AS hazardous_flammable:int,
    input_promo::offer_type_code AS offer_type_code:chararray,
    input_promo::promo_start_date AS promo_start_date:chararray,
    input_promo::promo_end_date AS promo_end_date:chararray,
    input_promo::promo_item AS promo_item:int;

joined_rp_promo_sbt = JOIN project_rp_promo BY (departmentId, classId, itemId) LEFT OUTER, distinct_sbt  BY (departmentId, classId, itemId) PARALLEL 20;

project_rp_promo_sbt = FOREACH joined_rp_promo_sbt GENERATE
    project_rp_promo::departmentId AS departmentId:int,
    project_rp_promo::classId AS classId:int,
    project_rp_promo::itemId AS itemId:int,
    project_rp_promo::height AS height:double,
    project_rp_promo::width AS width:double,
    project_rp_promo::depth AS depth:double,
    project_rp_promo::max_dim AS max_dim:double,
    project_rp_promo::med_dim AS med_dim:double,
    project_rp_promo::min_dim AS min_dim:double,
    project_rp_promo::entertainmentsegmentationstartingsalesdate AS entertainmentsegmentationstartingsalesdate: chararray,
    project_rp_promo::prepaidownercode AS prepaidownercode: chararray,
    project_rp_promo::isflammable AS isflammable:boolean,
    project_rp_promo::ishazardousmaterial AS ishazardousmaterial:boolean,
    project_rp_promo::prev_isHold AS prev_isHold:int,
    project_rp_promo::prev_isShip AS prev_isShip:int,
    project_rp_promo::prev_isRush AS prev_isRush:int,
    project_rp_promo::prev_isShipToStore AS prev_isShipToStore:int,
    project_rp_promo::prev_isHoldMarkets AS prev_isHoldMarkets:chararray,
    project_rp_promo::prev_isShipMarkets AS prev_isShipMarkets:chararray,
    project_rp_promo::prev_isRushMarkets AS prev_isRushMarkets:chararray,
    project_rp_promo::prev_isShipToStoreMarkets AS prev_isShipToStoreMarkets:chararray,
    project_rp_promo::mca_isHold AS mca_isHold:boolean,
    project_rp_promo::mca_isShip AS mca_isShip:boolean,
    project_rp_promo::mca_isRush AS mca_isRush:boolean,
    project_rp_promo::mca_isShipToStore AS mca_isShipToStore:boolean,
    project_rp_promo::mca_isHoldMarkets AS mca_isHoldMarkets:chararray,
    project_rp_promo::mca_isShipMarkets AS mca_isShipMarkets:chararray,
    project_rp_promo::mca_isRushMarkets AS mca_isRushMarkets:chararray,
    project_rp_promo::mca_isShipToStoreMarkets AS mca_isShipToStoreMarkets:chararray,
    project_rp_promo::isHoldExcluded AS isHoldExcluded:boolean,
    project_rp_promo::isShipExcluded AS isShipExcluded:boolean,
    project_rp_promo::isRushExcluded AS isRushExcluded:boolean,
    project_rp_promo::isShipToStoreExcluded AS isShipToStoreExcluded:boolean,
    project_rp_promo::future_street_date AS future_street_date: int,
    project_rp_promo::prepaid_item AS prepaid_item: int,
    project_rp_promo::hazardous_flammable AS hazardous_flammable:int,
    project_rp_promo::offer_type_code AS offer_type_code: chararray,
    project_rp_promo::promo_start_date AS promo_start_date: chararray,
    project_rp_promo::promo_end_date AS promo_end_date: chararray,
    project_rp_promo::promo_item AS promo_item:int,
    distinct_sbt::sbt_item AS sbt_item:int;
      
joined_rp_promo_sbt_sr = JOIN project_rp_promo_sbt BY (departmentId, classId, itemId) LEFT OUTER, input_sales_restricted BY (departmentId, classId, itemId) PARALLEL 20;

project_rp_promo_sbt_sr = FOREACH joined_rp_promo_sbt_sr GENERATE
    project_rp_promo_sbt::departmentId AS departmentId:int,
    project_rp_promo_sbt::classId AS classId:int,
    project_rp_promo_sbt::itemId AS itemId:int,
    project_rp_promo_sbt::height AS height:double,
    project_rp_promo_sbt::width AS width:double,
    project_rp_promo_sbt::depth AS depth:double,
    project_rp_promo_sbt::max_dim AS max_dim:double,
    project_rp_promo_sbt::med_dim AS med_dim:double,
    project_rp_promo_sbt::min_dim AS min_dim:double,
    project_rp_promo_sbt::entertainmentsegmentationstartingsalesdate AS entertainmentsegmentationstartingsalesdate: chararray,
    project_rp_promo_sbt::prepaidownercode AS prepaidownercode: chararray,
    project_rp_promo_sbt::isflammable AS isflammable:boolean,
    project_rp_promo_sbt::ishazardousmaterial AS ishazardousmaterial:boolean,
    project_rp_promo_sbt::prev_isHold AS prev_isHold:int,
    project_rp_promo_sbt::prev_isShip AS prev_isShip:int,
    project_rp_promo_sbt::prev_isRush AS prev_isRush:int,
    project_rp_promo_sbt::prev_isShipToStore AS prev_isShipToStore:int,
    project_rp_promo_sbt::prev_isHoldMarkets AS prev_isHoldMarkets:chararray,
    project_rp_promo_sbt::prev_isShipMarkets AS prev_isShipMarkets:chararray,
    project_rp_promo_sbt::prev_isRushMarkets AS prev_isRushMarkets:chararray,
    project_rp_promo_sbt::prev_isShipToStoreMarkets AS prev_isShipToStoreMarkets:chararray,
    project_rp_promo_sbt::mca_isHold AS mca_isHold:boolean,
    project_rp_promo_sbt::mca_isShip AS mca_isShip:boolean,
    project_rp_promo_sbt::mca_isRush AS mca_isRush:boolean,
    project_rp_promo_sbt::mca_isShipToStore AS mca_isShipToStore:boolean,
    project_rp_promo_sbt::mca_isHoldMarkets AS mca_isHoldMarkets:chararray,
    project_rp_promo_sbt::mca_isShipMarkets AS mca_isShipMarkets:chararray,
    project_rp_promo_sbt::mca_isRushMarkets AS mca_isRushMarkets:chararray,
    project_rp_promo_sbt::mca_isShipToStoreMarkets AS mca_isShipToStoreMarkets:chararray,
    project_rp_promo_sbt::isHoldExcluded AS isHoldExcluded:boolean,
    project_rp_promo_sbt::isShipExcluded AS isShipExcluded:boolean,
    project_rp_promo_sbt::isRushExcluded AS isRushExcluded:boolean,
    project_rp_promo_sbt::isShipToStoreExcluded AS isShipToStoreExcluded:boolean,
    project_rp_promo_sbt::future_street_date AS future_street_date: int,
    project_rp_promo_sbt::prepaid_item AS prepaid_item: int,
    project_rp_promo_sbt::hazardous_flammable AS hazardous_flammable:int,
    project_rp_promo_sbt::offer_type_code AS offer_type_code: chararray,
    project_rp_promo_sbt::promo_start_date AS promo_start_date: chararray,
    project_rp_promo_sbt::promo_end_date AS promo_end_date: chararray,
    project_rp_promo_sbt::promo_item AS promo_item:int,
    project_rp_promo_sbt::sbt_item AS sbt_item:int,
    input_sales_restricted::sales_restricted AS sales_restricted:int;

joined_rp_fact = JOIN project_rp_promo_sbt_sr BY (departmentId, classId, itemId) LEFT OUTER, input_prism BY (departmentId, classId, itemId) PARALLEL 20;

project_rp_fact = FOREACH joined_rp_fact GENERATE
    project_rp_promo_sbt_sr::departmentId AS departmentId:int,
    project_rp_promo_sbt_sr::classId AS classId:int,
    project_rp_promo_sbt_sr::itemId AS itemId:int,
    project_rp_promo_sbt_sr::height AS height:double,
    project_rp_promo_sbt_sr::width AS width:double,
    project_rp_promo_sbt_sr::depth AS depth:double,
    project_rp_promo_sbt_sr::max_dim AS max_dim:double,
    project_rp_promo_sbt_sr::med_dim AS med_dim:double,
    project_rp_promo_sbt_sr::min_dim AS min_dim:double,
    input_prism::tcin AS tcin:int,
    input_prism::isSoldInStore AS isSoldInStore:chararray,
    input_prism::isSoldOnline AS isSoldOnline: chararray,
    input_prism::priceTypeCode AS priceTypeCode: chararray,
    input_prism::priceTypeStartDate AS priceTypeStartDate: chararray,
    input_prism::priceTypeEndDate AS priceTypeEndDate: chararray,
    input_prism::isSignatureRequired AS isSignatureRequired: chararray,
    input_prism::returnMethodCode AS returnMethodCode: int,
    input_prism::kitItem AS kitItem: int,
    input_prism::mozaratId AS mozaratId: int,
    input_prism::itemStatusCode AS itemStatusCode: chararray,
    input_prism::shipCarrierCode AS shipCarrierCode: chararray,
    input_prism::shipCarrierDesc AS shipCarrierDesc: chararray,
    input_prism::primeVendorId AS primeVendorId: int,
    input_prism::primaryFulfillLocationType AS primaryFulfillLocationType: chararray,
    input_prism::dvs_product AS dvs_product: int,
    input_prism::prism_kit_item AS prism_kit_item: int,
    input_prism::obselete AS obselete: int,
    input_prism::elig_prism_statuscode AS elig_prism_statuscode: int,
    input_prism::elig_prism_pricetype AS elig_prism_pricetype: int,
    input_prism::web_only AS web_only: int,
    input_prism::store_only AS store_only: int,
    input_prism::prism_sign_item AS prism_sign_item: int,
    input_prism::prism_ltl_item AS prism_ltl_item: int,
    input_prism::elig_prism_return_method AS elig_prism_return_method: int,
    input_prism::assorted_tcins_store_only AS assorted_tcins_store_only: int,
    input_prism::multi_tcins AS multi_tcins: int,
    project_rp_promo_sbt_sr::entertainmentsegmentationstartingsalesdate AS entertainmentsegmentationstartingsalesdate: chararray,
    project_rp_promo_sbt_sr::prepaidownercode AS prepaidownercode: chararray,
    project_rp_promo_sbt_sr::isflammable AS isflammable:boolean,
    project_rp_promo_sbt_sr::ishazardousmaterial AS ishazardousmaterial:boolean,
    project_rp_promo_sbt_sr::prev_isHold AS prev_isHold:int,
    project_rp_promo_sbt_sr::prev_isShip AS prev_isShip:int,
    project_rp_promo_sbt_sr::prev_isRush AS prev_isRush:int,
    project_rp_promo_sbt_sr::prev_isShipToStore AS prev_isShipToStore:int,
    project_rp_promo_sbt_sr::prev_isHoldMarkets AS prev_isHoldMarkets:chararray,
    project_rp_promo_sbt_sr::prev_isShipMarkets AS prev_isShipMarkets:chararray,
    project_rp_promo_sbt_sr::prev_isRushMarkets AS prev_isRushMarkets:chararray,
    project_rp_promo_sbt_sr::prev_isShipToStoreMarkets AS prev_isShipToStoreMarkets:chararray,
    project_rp_promo_sbt_sr::mca_isHold AS mca_isHold:boolean,
    project_rp_promo_sbt_sr::mca_isShip AS mca_isShip:boolean,
    project_rp_promo_sbt_sr::mca_isRush AS mca_isRush:boolean,
    project_rp_promo_sbt_sr::mca_isShipToStore AS mca_isShipToStore:boolean,
    project_rp_promo_sbt_sr::mca_isHoldMarkets AS mca_isHoldMarkets:chararray,
    project_rp_promo_sbt_sr::mca_isShipMarkets AS mca_isShipMarkets:chararray,
    project_rp_promo_sbt_sr::mca_isRushMarkets AS mca_isRushMarkets:chararray,
    project_rp_promo_sbt_sr::mca_isShipToStoreMarkets AS mca_isShipToStoreMarkets:chararray,
    project_rp_promo_sbt_sr::isHoldExcluded AS isHoldExcluded:boolean,
    project_rp_promo_sbt_sr::isShipExcluded AS isShipExcluded:boolean,
    project_rp_promo_sbt_sr::isRushExcluded AS isRushExcluded:boolean,
    project_rp_promo_sbt_sr::isShipToStoreExcluded AS isShipToStoreExcluded:boolean,
    project_rp_promo_sbt_sr::future_street_date AS future_street_date: int,
    project_rp_promo_sbt_sr::prepaid_item AS prepaid_item: int,
    project_rp_promo_sbt_sr::hazardous_flammable AS hazardous_flammable:int,
    project_rp_promo_sbt_sr::offer_type_code AS offer_type_code: chararray,
    project_rp_promo_sbt_sr::promo_start_date AS promo_start_date: chararray,
    project_rp_promo_sbt_sr::promo_end_date AS promo_end_date: chararray,
    project_rp_promo_sbt_sr::promo_item AS promo_item:int,
    project_rp_promo_sbt_sr::sbt_item AS sbt_item : int,
    project_rp_promo_sbt_sr::sales_restricted AS sales_restricted: int,
    (input_prism::departmentId IS NOT NULL AND input_prism::tcin IS NOT NULL ? 1 : 0) AS tcin_item: int;

rp_ff_eligibility = FOREACH project_rp_fact {
    default_ineligibility = (tcin_item == 0 ?
        TRUE : ((obselete IS NOT NULL AND obselete == 1) ? 
        TRUE : ((promo_item IS NOT NULL AND promo_item == 1) ? 
        TRUE : ((future_street_date IS NOT NULL AND future_street_date == 1) ? 
        TRUE : ((prepaid_item IS NOT NULL AND  prepaid_item  == 1) ? 
        TRUE : ((prism_kit_item IS NOT NULL AND  prism_kit_item == 0) ? 
        TRUE : ((sbt_item IS NOT NULL AND  sbt_item == 1) ?
        TRUE : FALSE)))))));
    ineligible_sfs_and_rush = ((multi_tcins IS NOT NULL AND multi_tcins == 1) ?
        TRUE : ((hazardous_flammable IS NOT NULL AND hazardous_flammable == 1) ?
        TRUE : ((prism_sign_item IS NOT NULL AND prism_sign_item == 1) ?
        TRUE : ((prism_ltl_item IS NOT NULL AND prism_ltl_item == 0) ?
        TRUE : ((elig_prism_statuscode IS NOT NULL AND elig_prism_statuscode == 0) ?
        TRUE : ((elig_prism_pricetype IS NOT NULL AND  elig_prism_pricetype == 0) ?
        TRUE : FALSE)))))); 
    GENERATE
    ..tcin_item,
    ((isSoldOnline IS NOT NULL AND isSoldOnline == 'Y') AND (isSoldInStore IS NULL OR isSoldInStore == 'N') ? 
        0 : 1 
    ) AS esfs_enabled:int,
    default_ineligibility AS default_ineligibility:boolean,
    ineligible_sfs_and_rush AS ineligible_sfs_and_rush:boolean, 
    (isHoldExcluded IS NOT NULL AND isHoldExcluded == TRUE ?
        FALSE : (default_ineligibility == TRUE ? 
        FALSE : ((multi_tcins IS NOT NULL AND multi_tcins == 1) ?
        FALSE : ((elig_prism_statuscode IS NOT NULL AND elig_prism_statuscode == 0) ?
        FALSE : ((elig_prism_pricetype IS NOT NULL AND elig_prism_pricetype == 0) ?
        FALSE : ((web_only IS NOT NULL AND web_only == 1) ?
        FALSE : mca_isHold)))))
    ) AS cur_isHold:boolean,
    (isShipExcluded IS NOT NULL AND isShipExcluded == TRUE ?
        FALSE : (default_ineligibility == TRUE ?
        FALSE : (ineligible_sfs_and_rush == TRUE ?
        FALSE : mca_isShip))
    ) AS cur_isShip:boolean,
    (isRushExcluded IS NOT NULL AND isRushExcluded == TRUE ?
        FALSE : (default_ineligibility == TRUE ?
        FALSE : (ineligible_sfs_and_rush == TRUE ? 
        FALSE : ((web_only IS NOT NULL AND  web_only == 1) ?
        FALSE : mca_isRush)))
    ) AS cur_isRush:boolean,
    (isShipToStoreExcluded IS NOT NULL AND isShipToStoreExcluded == TRUE ?
        FALSE : (default_ineligibility == TRUE ?
        FALSE : ((prism_ltl_item IS NOT NULL AND prism_ltl_item == 0) ?
        FALSE : (((store_only IS NOT NULL AND store_only == 1) OR (assorted_tcins_store_only IS NOT NULL AND assorted_tcins_store_only == 1)) ? 
        FALSE : ((elig_prism_return_method IS NOT NULL AND elig_prism_return_method == 0) ?
        FALSE : ((sales_restricted IS NOT NULL AND sales_restricted == 1) ?
        FALSE : ((dvs_product IS NOT NULL AND dvs_product == 1) ?
        FALSE : mca_isShipToStore)))))) 
    ) AS cur_isShipToStore:boolean,
    mca_isHoldMarkets AS cur_isHoldMarkets:chararray,
    mca_isShipMarkets AS cur_isShipMarkets:chararray,
    mca_isRushMarkets AS cur_isRushMarkets:chararray,
    mca_isShipToStoreMarkets AS cur_isShipToStoreMarkets:chararray;
}

--Store to hive external path with partition 
STORE rp_ff_eligibility INTO '$OUTPUT_PATH' USING PigStorage(',', '-schema');
