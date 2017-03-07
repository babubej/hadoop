/*SET mapred.job.queue.name '$QUEUE_NAME';
SET mapred.job.name '$JOB_NAME';
SET mapred.max.split.size '$MAX_SPLIT_SIZE';
--SET pig.splitCombination 'true';
--SET pig.exec.reducers.max '999';
--SET pig.exec.reducers.bytes.per.reducer '3221225472';
*/

input_air  = LOAD '$INPUT_AIR_PATH' USING PigStorage(',') AS (
    departmentid:int,
    classid:int,
    itemid:int,
    locationid:int
);

input_dsd = LOAD '$INPUT_DSD_PATH' USING PigStorage(',') AS (
    departmentid:int,
    classid:int,
    itemid:int
);

input_location_detail = LOAD '$INPUT_LOC_DETAIL_PATH' USING PigStorage(',', '-schema'); 

input_location_bxz = LOAD '$INPUT_LOC_BXZ_PATH' USING PigStorage(',', '-schema');

input_rpl = LOAD '$INPUT_RPL_PATH' USING PigStorage(',', '-schema');

input_sioc = LOAD '$INPUT_SIOC_PATH' USING PigStorage(',') AS (departmentId:int, 
    classId:int, 
    itemId:int
);
 
/* 
*  [-] Possible options 
*  -- COGROUP the aliases and Flatten  
*  -- Union on schema  and remove duplicates and filter 
*  -- Leftouter join for each feed 
*      as PIG OUTER JOIN does not work in a single line for multiple Aliases [Currently used here, to keep the logic simple]
*/

joined_rpl_air = JOIN input_rpl BY (departmentId, classId, itemId, locationId) LEFT OUTER, input_air BY (departmentid,classid, itemid, locationid) PARALLEL 500;

project_rpl_air = FOREACH joined_rpl_air GENERATE
    input_rpl::departmentId AS departmentId:int,
    input_rpl::classId AS classId:int,
    input_rpl::itemId AS itemId:int,
    input_rpl::locationId AS locationId:int,
    input_rpl::inventorydeterminationindicator AS inventorydeterminationindicator:int,
    input_rpl::prev_isHold AS prev_isHold:int,
    input_rpl::prev_isRush AS prev_isRush:int,
    input_rpl::prev_isShip AS prev_isShip:int,
    input_rpl::prev_isShipToStore AS prev_isShipToStore:int,
    input_rpl::statusCode AS rplStatusCode:chararray,
    input_rpl::isDeleted AS isDeleted:boolean,
    input_rpl::ine_status_code AS ine_status_code:int,
    ((input_air::departmentid IS NULL OR input_air::classid IS NULL OR input_air::itemid IS NULL ) ? 0 :1)  AS air_item:int;

joined_rpl_air_dsd = JOIN project_rpl_air BY (departmentId, classId, itemId) LEFT OUTER, input_dsd  BY (departmentid, classid, itemid) PARALLEL 500;

project_rpl_air_dsd = FOREACH joined_rpl_air_dsd GENERATE
    project_rpl_air::departmentId AS departmentId:int,
    project_rpl_air::classId AS classId:int,
    project_rpl_air::itemId AS itemId:int,
    project_rpl_air::locationId AS locationId:int,
    project_rpl_air::inventorydeterminationindicator AS inventorydeterminationindicator:int,
    project_rpl_air::prev_isHold AS prev_isHold:int,
    project_rpl_air::prev_isRush AS prev_isRush:int,
    project_rpl_air::prev_isShip AS prev_isShip:int,
    project_rpl_air::prev_isShipToStore AS prev_isShipToStore:int,
    project_rpl_air::rplStatusCode AS rplStatusCode:chararray,
    project_rpl_air::isDeleted AS isDeleted:boolean,
    project_rpl_air::ine_status_code AS ine_status_code:int,
    project_rpl_air::air_item AS air_item:int,
    ((input_dsd::departmentid IS NULL OR input_dsd::classid IS NULL OR input_dsd::itemid IS NULL ) ? 0 :1)  AS dsd_item:int;
     
joined_rpl_loc_detail = JOIN project_rpl_air_dsd BY (locationId) LEFT OUTER, input_location_detail BY (locationId) USING 'replicated' PARALLEL 500;

project_rpl_loc_detail = FOREACH joined_rpl_loc_detail GENERATE
    project_rpl_air_dsd::departmentId AS departmentId:int,
    project_rpl_air_dsd::classId AS classId:int,
    project_rpl_air_dsd::itemId AS itemId:int,
    project_rpl_air_dsd::locationId AS locationId:int,
    project_rpl_air_dsd::inventorydeterminationindicator AS inventorydeterminationindicator:int,
    project_rpl_air_dsd::prev_isHold AS prev_isHold:int,
    project_rpl_air_dsd::prev_isRush AS prev_isRush:int,
    project_rpl_air_dsd::prev_isShip AS prev_isShip:int,
    project_rpl_air_dsd::prev_isShipToStore AS prev_isShipToStore:int,
    project_rpl_air_dsd::air_item AS air_item:int,
    project_rpl_air_dsd::dsd_item  AS dsd_item:int,
    project_rpl_air_dsd::rplStatusCode AS rplStatusCode:chararray,
    project_rpl_air_dsd::isDeleted AS isDeleted:boolean,
    project_rpl_air_dsd::ine_status_code AS ine_status_code:int,
    input_location_detail::locationId AS loc_detail_locationId:int,
    input_location_detail::isShip AS loc_isShip:int,
    input_location_detail::isRush AS loc_isRush:int,
    input_location_detail::isHold AS loc_isHold:int,
    input_location_detail::isShipToStore AS loc_isShipToStore:int,
    input_location_detail::markets AS loc_markets;

filter_rpl_loc_detail = FILTER project_rpl_loc_detail BY (loc_detail_locationId IS NOT NULL);

joined_rpl_loc_bxz = JOIN filter_rpl_loc_detail BY (locationId) LEFT OUTER, input_location_bxz BY (locationId) USING 'replicated' PARALLEL 500;

project_rpl_loc_bxz = FOREACH joined_rpl_loc_bxz GENERATE
    filter_rpl_loc_detail::departmentId AS departmentId:int,
    filter_rpl_loc_detail::classId AS classId:int,
    filter_rpl_loc_detail::itemId AS itemId:int,
    filter_rpl_loc_detail::locationId AS locationId:int,
    filter_rpl_loc_detail::inventorydeterminationindicator AS inventorydeterminationindicator:int,
    filter_rpl_loc_detail::prev_isHold AS prev_isHold:int,
    filter_rpl_loc_detail::prev_isRush AS prev_isRush:int,
    filter_rpl_loc_detail::prev_isShip AS prev_isShip:int,
    filter_rpl_loc_detail::prev_isShipToStore AS prev_isShipToStore:int,
    filter_rpl_loc_detail::air_item AS air_item:int,
    filter_rpl_loc_detail::dsd_item AS dsd_item:int,
    filter_rpl_loc_detail::rplStatusCode AS rplStatusCode:chararray,
    filter_rpl_loc_detail::isDeleted AS isDeleted:boolean,
    filter_rpl_loc_detail::ine_status_code AS ine_status_code:int,
    filter_rpl_loc_detail::loc_isShip AS loc_isShip:int,
    filter_rpl_loc_detail::loc_isRush AS loc_isRush:int,
    filter_rpl_loc_detail::loc_isHold AS loc_isHold:int,
    filter_rpl_loc_detail::loc_isShipToStore AS loc_isShipToStore:int,
    filter_rpl_loc_detail::loc_markets AS loc_markets:chararray,
    input_location_bxz::maxdim AS box_max:bigdecimal,
    input_location_bxz::mindim AS box_min:bigdecimal,
    input_location_bxz::meddim AS box_med:bigdecimal,
    input_location_bxz::online_enabled AS online_enabled:int;

joined_rpl_sioc = JOIN project_rpl_loc_bxz BY (departmentId, classId, itemId) LEFT OUTER, input_sioc BY (departmentId, classId, itemId) USING 'replicated' PARALLEL 500;

project_rpl_fact = FOREACH joined_rpl_sioc GENERATE
    project_rpl_loc_bxz::departmentId AS departmentId:int,
    project_rpl_loc_bxz::classId AS classId:int,
    project_rpl_loc_bxz::itemId AS itemId:int,
    project_rpl_loc_bxz::locationId AS locationId:int,
    project_rpl_loc_bxz::inventorydeterminationindicator AS prev_inventorydeterminationindicator:int,
    project_rpl_loc_bxz::prev_isHold AS prev_isHold:int,
    project_rpl_loc_bxz::prev_isRush AS prev_isRush:int,
    project_rpl_loc_bxz::prev_isShip AS prev_isShip:int,
    project_rpl_loc_bxz::prev_isShipToStore AS prev_isShipToStore:int,
    project_rpl_loc_bxz::air_item AS air_item:int,
    project_rpl_loc_bxz::dsd_item AS dsd_item:int,
    project_rpl_loc_bxz::rplStatusCode AS rplStatusCode:chararray,
    project_rpl_loc_bxz::isDeleted AS isDeleted:boolean,
    project_rpl_loc_bxz::ine_status_code AS ine_status_code:int,
    project_rpl_loc_bxz::loc_isShip AS loc_isShip:int,
    project_rpl_loc_bxz::loc_isRush AS loc_isRush:int,
    project_rpl_loc_bxz::loc_isHold AS loc_isHold:int,
    project_rpl_loc_bxz::loc_isShipToStore AS loc_isShipToStore:int,
    project_rpl_loc_bxz::loc_markets AS loc_markets:chararray,
    project_rpl_loc_bxz::box_max AS box_max:bigdecimal,
    project_rpl_loc_bxz::box_min AS box_min:bigdecimal,
    project_rpl_loc_bxz::box_med AS box_med:bigdecimal,
    project_rpl_loc_bxz::online_enabled AS online_enabled:int,
    (input_sioc::departmentId IS NULL OR input_sioc::classId IS NULL OR input_sioc::itemId IS NULL ? 0 : 1) AS sioc_item:int,
    (((isDeleted IS NOT NULL AND isDeleted == TRUE) OR
      (air_item IS NOT NULL AND air_item == 0 AND 
        dsd_item IS NOT NULL AND dsd_item == 0 AND 
        rplStatusCode IS NOT NULL AND rplStatusCode != 'D'
      ) OR
      (project_rpl_loc_bxz::ine_status_code IS NOT NULL AND project_rpl_loc_bxz::ine_status_code == 1)
      ) ? 1 : 0
    ) AS default_rpl_ineligibility:int;
 
--Store to hive external path with partition 
STORE project_rpl_fact INTO '$OUTPUT_PATH' USING PigStorage(',', '-schema');
