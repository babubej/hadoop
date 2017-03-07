SET mapred.job.queue.name '$QUEUE_NAME';
SET mapred.job.name '$JOB_NAME';
SET mapred.max.split.size '$MAX_SPLIT_SIZE';
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

input_loc_item_comb = LOAD '$INPUT_RP_FACT_PATH' USING PigStorage(',', '-schema');

project_loc_item_comb = FOREACH input_loc_item_comb GENERATE
    departmentId AS departmentId:int,
    classId AS classId:int,
    itemId AS itemId:int,
    locationId AS locationId:int,
    ine_air AS ine_air:int,
    ine_dsd  AS ine_dsd:int,
    statusCode AS statusCode:int,
    isDeleted AS isDeleted:boolean,
    loc_isShip AS loc_isShip:boolean,
    loc_isRush AS loc_isRush:boolean,
    loc_isHold AS loc_isHold:boolean,
    loc_isShipToStore AS loc_isShipToStore:boolean,
    loc_markets AS loc_markets:chararray,
    box_max AS box_max:float,
    box_min AS box_min:float,
    box_med AS box_med:float,
    tcin AS tcin:int,
    isSoldInStore AS isSoldInStore:chararray,
    isSoldOnline AS isSoldOnline:chararray,
    priceTypeCode AS priceTypeCode:chararray,
    priceTypeStartDate AS priceTypeStartDate:chararray,
    priceTypeEndDate AS priceTypeEndDate:chararray,
    isSignatureRequired AS isSignatureRequired:chararray,
    returnMethodCode AS returnMethodCode:int,
    kitItem AS kitItem:int,
    mozaratId AS mozaratId:int,
    itemStatusCode AS itemStatusCode:chararray,
    shipCarrierCode AS shipCarrierCode:chararray,
    shipCarrierDesc AS shipCarrierDesc:chararray,
    primeVendorId AS primeVendorId:int,
    primaryFulfillLocationType AS primaryFulfillLocationType:chararray,
    ine_dvs_product AS ine_dvs_product:int,
    elig_prism_kit AS elig_prism_kit:int,
    obselete AS obselete:int,
    elig_prism_statuscode AS elig_prism_statuscode:int,
    elig_prism_pricetype AS elig_prism_pricetype:int,
    web_only AS web_only:int,
    store_only AS store_only:int,
    prism_sign AS prism_sign:int,
    elig_prism_ltl AS elig_prism_ltl:int,
    elig_prism_return_method AS elig_prism_return_method:int,
    multi_tcins AS multi_tcins:int,
    entertainmentsegmentationstartingsalesdate AS entertainment_segmentation_starting_salesdate:chararray,
    prepaidownercode AS prepaidownercode:chararray,
    isflammable AS isflammable:boolean,
    ishazardousmaterial AS ishazardousmaterial:boolean,
    isHold AS isHold:boolean,
    isShip AS isShip:boolean,
    isRush AS isRush:boolean,
    isShipToStore AS isShipToStore:boolean,
    isHoldMarkets AS isHoldMarkets,
    isShipMarkets AS isShipMarkets,
    isRushMarkets AS isRushMarkets,
    isShipToStoreMarkets AS isShipToStoreMarkets,
    isHoldExcluded AS isHoldExcluded:boolean,
    isShipExcluded AS isShipExcluded:boolean,
    isRushExcluded AS isRushExcluded:boolean,
    isShipToStoreExcluded AS isShipToStoreExcluded:boolean,
    ine_future_street_date AS ine_future_street_date:int,
    ine_prepaid_item AS ine_prepaid_item:int,
    ine_hazardous_flammable AS ine_hazardous_flammable:int,
    offer_type_code AS offer_type_code:chararray,
    promo_start_date AS promo_start_date:chararray,
    promo_end_date AS promo_end_date:chararray,
    ine_promo AS ine_promo:long,
    ine_sbt AS ine_sbt:int,
    ine_sales_restricted AS ine_sales_restricted:int;

eligibility_rules  = FOREACH project_item_loc_comb {
    default_ff_ineligiblity = (obselete IS NOT NULL AND obselete == 1 OR 
        ine_promo IS NOT NULL AND ine_promo == 1 OR
        ine_future_street_date IS NOT NULL AND ine_future_street_date == 1 OR 
        ine_prepaid_item IS NOT NULL AND  ine_prepaid_item  == 1 OR 
        elig_prism_kit IS NOT NULL AND  elig_prism_kit == 0  OR  
        ine_sbt IS NOT NULL AND  ine_sbt == 1 
    );
    default_sfs_and_hold_ineligibility = (multi_tcins IS NOT NULL AND multi_tcins == 1 OR
        ine_hazardous_flammable IS NOT NULL AND ine_hazardous_flammable == 1 OR
        prism_sign IS NOT NULL AND prism_sign == 1 OR
        elig_prism_ltl IS NOT NULL AND elig_prism_ltl == 0 OR
        elig_prism_statuscode IS NOT NULL AND elig_prism_statuscode == 0 OR
        elig_prism_pricetype IS NOT NULL AND  elig_prism_pricetype == 0 
    );
    GENERATE
        ..ine_sales_restricted,
        (   default_ineligibility == 1 ? 
            false : multi_tcins == 1 ? 
            false : elig_prism_statuscode == 0 ? 
            false : elig_prism_pricetype == 0 ? 
            fasle : web_only == 1 ? 
            false : true
         ) AS isHold:boolean,
         (  default_ineligibility == 1 ? 
            false : default_sfs_and_hold_ineligibility == 1 ? 
            false : true
         ) AS isShip:boolean,
         (  default_ineligibility == 1 ?
            false : default_sfs_and_hold_ineligibility == 1 ?  
            false : web_only == 1 ? 
            false : true 
         ) AS isShip:boolean,
         (  default_ineligibility == 1 ? 
            false : elig_prism_ltl == 0 ? 
            false : inelig_prism_channel_ship_store_esfs
            false : elig_prism_return_method == 0 ? 
            false : ine_sales_restricted == 1 ?
            false : ine_dvs_product == 1 ? 
            false : true
          ) AS isShipToStore:boolean;
}
