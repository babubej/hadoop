REGISTER $ACE_CORE_UDFS;

/*
SET mapred.job.queue.name '$QUEUE_NAME';
SET mapred.job.name '$JOB_NAME';
SET mapred.max.split.size '$MAX_SPLIT_SIZE';
*/
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

DEFINE LookupRplStatusCode com.target.ace.core_udfs.LookUp('$LOOKUP_PATH', '$RPL_ITEM_STATUS_CODE');

input_rpl = LOAD '$INPUT_PATH' USING PigStorage(',', '-schema');

filter_rpl = FILTER input_rpl BY (locationId IS NOT NULL);

project_rpl = FOREACH filter_rpl GENERATE
    departmentId AS departmentId:int,
    classId AS classId:int,
    itemId AS itemId:int,
    locationId AS locationId:int,
    rplStatusCode AS rplStatusCode:chararray,
    isDeleted AS isDeleted:boolean,
    future_street_date AS future_street_date:int,
    prev_inventorydeterminationindicator AS prev_inventorydeterminationindicator:int,
    prev_item_loc_isShip AS prev_isShip:int,
    prev_item_loc_isRush AS prev_isRush:int,
    prev_item_loc_isHold AS prev_isHold:int,
    prev_item_loc_isShipToStore AS prev_isShipToStore:int,
    cur_item_loc_isShip AS cur_isShip:int,
    cur_item_loc_isRush AS cur_isRush:int,
    cur_item_loc_isHold AS cur_isHold:int,
    cur_item_loc_isShipToStore AS cur_isShipToStore:int;


distinct_rpl = DISTINCT project_rpl PARALLEL 500;

input_sbt = LOAD '$INPUT_SBT_PATH' USING PigStorage(',', '-schema');

project_sbt = FOREACH input_sbt GENERATE
    departmentId,
    classId,
    itemId,
    locationId,
    sbt_indicator;

joined_rpl = JOIN distinct_rpl BY (departmentId, classId, itemId, locationId) LEFT OUTER, project_sbt BY (departmentId, classId, itemId, locationId) PARALLEL 500;

rpl_eligibility = FOREACH joined_rpl GENERATE
    distinct_rpl::departmentId AS departmentId,
    distinct_rpl::classId AS classId,
    distinct_rpl::itemId AS itemId,
    distinct_rpl::rplStatusCode AS rplStatusCode,
    distinct_rpl::isDeleted AS isDeleted,
    distinct_rpl::locationId AS locationId,
    distinct_rpl::prev_isShip AS prev_isShip,
    distinct_rpl::prev_isRush AS prev_isRush,
    distinct_rpl::prev_isHold AS prev_isHold,
    distinct_rpl::prev_isShipToStore AS prev_isShipToStore,
    distinct_rpl::prev_inventorydeterminationindicator AS prev_inventorydeterminationindicator,
    distinct_rpl::cur_isShip AS cur_isShip,
    distinct_rpl::cur_isRush AS cur_isRush,
    distinct_rpl::cur_isHold AS cur_isHold,
    distinct_rpl::cur_isShipToStore AS cur_isShipToStore,
    (distinct_rpl::future_street_date IS NOT NULL AND distinct_rpl::future_street_date == 1 ? 1 :
        (project_sbt::sbt_indicator IS NULL ? 0 : 
            (project_sbt::sbt_indicator != 2  ? project_sbt::sbt_indicator : 
                ((rplStatusCode IS NOT NULL AND LookupRplStatusCode(rplStatusCode)) ? 
                  2 : 1) 
            )
        )
    ) AS cur_inventorydeterminationindicator:int,
    ToUnixTime(CurrentTime()) AS updated_on:long;

STORE rpl_eligibility INTO '$OUTPUT_PATH' USING PigStorage(',', '-schema');
