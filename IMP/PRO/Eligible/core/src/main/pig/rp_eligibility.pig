/*
SET mapred.job.queue.name '$QUEUE_NAME';
SET mapred.job.name '$JOB_NAME';
SET mapred.max.split.size '$MAX_SPLIT_SIZE';
*/
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

input_combined = LOAD '$INPUT_PATH' USING PigStorage(',', '-schema');

project_rp = FOREACH input_combined GENERATE
    departmentId,
    classId,
    itemId,
    entertainmentsegmentationstartingsalesdate,
    prepaidownercode,
    isflammable,
    ishazardousmaterial,
    height,
    width,
    depth,
    prev_item_isHoldMarkets,
    prev_item_isShipMarkets,
    prev_item_isRushMarkets,
    prev_item_isShipToStoreMarkets,
    prev_item_isShip,
    prev_item_isRush,
    prev_item_isHold,
    prev_item_isShipToStore,
    cur_item_isHoldMarkets,
    cur_item_isShipMarkets,
    cur_item_isRushMarkets,
    cur_item_isShipToStoreMarkets,
    cur_item_isShip,
    cur_item_isRush,
    cur_item_isHold,
    cur_item_isShipToStore,
    cur_item_loc_isShip,
    cur_item_loc_isRush,
    cur_item_loc_isHold,
    cur_item_loc_isShipToStore;

grouped_rp = GROUP project_rp BY (departmentId, classId, itemId);

rp_eligibility_one_loc = FOREACH grouped_rp {
    prev_item_isHold = LIMIT project_rp.prev_item_isHold 1;
    prev_item_isShip = LIMIT project_rp.prev_item_isShip 1;
    prev_item_isRush = LIMIT project_rp.prev_item_isRush 1;
    prev_item_isShipToStore = LIMIT project_rp.prev_item_isShipToStore 1;
    prev_item_isHoldMarkets = LIMIT project_rp.prev_item_isHoldMarkets 1;
    prev_item_isShipMarkets = LIMIT project_rp.prev_item_isShipMarkets 1;
    prev_item_isRushMarkets = LIMIT project_rp.prev_item_isRushMarkets 1;
    prev_item_isShipToStoreMarkets = LIMIT project_rp.prev_item_isShipToStoreMarkets 1;
    cur_item_isHold = (SUM(project_rp.cur_item_isHold) > 0 ? 1 : 0);
    cur_item_isShip = (SUM(project_rp.cur_item_isShip) > 0 ? 1: 0);
    cur_item_isRush = (SUM( project_rp.cur_item_isRush) > 0 ? 1 : 0);
    cur_item_isShipToStore = (SUM(project_rp.cur_item_isShipToStore) > 0 ? 1 :0);
    cur_item_isHoldMarkets = LIMIT project_rp.cur_item_isHoldMarkets 1;
    cur_item_isShipMarkets = LIMIT project_rp.cur_item_isShipMarkets 1;
    cur_item_isRushMarkets = LIMIT project_rp.cur_item_isRushMarkets 1;
    entertainmentsegmentationstartingsalesdate = LIMIT project_rp.entertainmentsegmentationstartingsalesdate 1;
    prepaidownercode = LIMIT project_rp.prepaidownercode 1;
    isflammable = LIMIT project_rp.isflammable 1;
    ishazardousmaterial = LIMIT project_rp.ishazardousmaterial 1;
    height = LIMIT project_rp.height 1;
    width = LIMIT project_rp.width 1;
    depth = LIMIT project_rp.depth 1;
    cur_item_isShipToStoreMarkets = LIMIT project_rp.cur_item_isShipToStoreMarkets 1;
    one_loc_isHold = (SUM(project_rp.cur_item_loc_isHold) > 0 ? 1 : 0);
    one_loc_isShip = (SUM(project_rp.cur_item_loc_isShip) > 0 ? 1 : 0);
    one_loc_isRush = (SUM(project_rp.cur_item_loc_isRush) > 0 ? 1 : 0);
    GENERATE
        FLATTEN(group) AS (departmentId:int, classId:int, itemId:int),
        FLATTEN(entertainmentsegmentationstartingsalesdate) AS entertainmentsegmentationstartingsalesdate:chararray,
        FLATTEN(prepaidownercode) AS prepaidownercode:chararray,
        FLATTEN(isflammable) AS isflammable:boolean,
        FLATTEN(ishazardousmaterial) AS ishazardousmaterial:boolean,
        FLATTEN(height) AS height:bigdecimal,
        FLATTEN(width) AS width:bigdecimal,
        FLATTEN(depth) AS depth:bigdecimal,
        FLATTEN(prev_item_isHoldMarkets) AS prev_item_isHoldMarkets:chararray,
        FLATTEN(prev_item_isShipMarkets) AS prev_item_isShipMarkets:chararray,
        FLATTEN(prev_item_isRushMarkets) AS prev_item_isRushMarkets:chararray,
        FLATTEN(prev_item_isShipToStoreMarkets) AS prev_item_isShipToStoreMarkets:chararray,
        FLATTEN(prev_item_isShip) AS prev_item_isShip:int,
        FLATTEN(prev_item_isRush) AS prev_item_isRush:int,
        FLATTEN(prev_item_isHold) AS prev_item_isHold:int,
        FLATTEN(prev_item_isShipToStore) AS prev_item_isShipToStore:int,
        FLATTEN(cur_item_isHoldMarkets) AS cur_item_isHoldMarkets:chararray,
        FLATTEN(cur_item_isShipMarkets) AS cur_item_isShipMarkets:chararray,
        FLATTEN(cur_item_isRushMarkets) AS cur_item_isRushMarkets:chararray,
        FLATTEN(cur_item_isShipToStoreMarkets) AS cur_item_isShipToStoreMarkets:chararray,
        cur_item_isShip AS cur_item_isShip:int,
        cur_item_isRush AS cur_item_isRush:int,
        cur_item_isHold AS cur_item_isHold:int,
        cur_item_isShipToStore AS cur_item_isShipToStore:int,
        one_loc_isHold AS one_loc_isHold:int,
        one_loc_isShip AS one_loc_isShip:int,
        one_loc_isRush AS one_loc_isRush:int;
}

rp_eligibility = FOREACH rp_eligibility_one_loc GENERATE
        departmentId, 
        classId, 
        itemId,
        entertainmentsegmentationstartingsalesdate,
        prepaidownercode,
        isflammable,
        ishazardousmaterial,
        height,
        width,
        depth,
        one_loc_isHold,
        one_loc_isShip,
        one_loc_isRush,
        prev_item_isHoldMarkets AS prev_isHoldMarkets,
        prev_item_isShipMarkets AS prev_isShipMarkets,
        prev_item_isRushMarkets AS prev_isRushMarkets,
        prev_item_isShipToStoreMarkets AS prev_isShipToStoreMarkets,
        prev_item_isShip AS prev_isShip,
        prev_item_isRush AS prev_isRush,
        prev_item_isHold AS prev_isHold,
        prev_item_isShipToStore AS prev_isShipToStore,
        cur_item_isHoldMarkets AS cur_isHoldMarkets,
        cur_item_isShipMarkets AS cur_isShipMarkets,
        cur_item_isRushMarkets AS cur_isRushMarkets,
        cur_item_isShipToStoreMarkets AS cur_isShipToStoreMarkets,
        ((one_loc_isShip IS NOT NULL AND one_loc_isShip > 0) ? cur_item_isShip : 0) AS cur_isShip:int,
        ((one_loc_isHold IS NOT NULL AND one_loc_isHold > 0) ? cur_item_isHold : 0) AS cur_isHold:int,
        ((one_loc_isRush IS NOT NULL AND one_loc_isRush > 0) ? cur_item_isRush : 0) AS cur_isRush:int,
        cur_item_isShipToStore  AS cur_isShipToStore:int,
        ToUnixTime(CurrentTime()) AS updated_on:long;

STORE rp_eligibility INTO '$OUTPUT_PATH' USING PigStorage(',', '-schema');
