/*
SET mapred.job.queue.name '$QUEUE_NAME';
SET mapred.job.name '$JOB_NAME';
SET mapred.max.split.size '$MAX_SPLIT_SIZE';
*/
SET mapred.compress.map.output true;
SET mapred.map.output.compression.codec org.apache.hadoop.io.compress.SnappyCodec;

REGISTER '$ACE_CORE_JAR';

DEFINE CHECK_MARKETS com.target.ace.core_udfs.CheckValueInDelimitedString('\\|');

input_rp_fact = LOAD '$INPUT_RP_FACT_PATH' USING PigStorage(',', '-schema');

--filter_rp_fact = FILTER input_rp_fact BY (obselete IS NULL OR obselete != 1);

project_rp_fact = FOREACH input_rp_fact GENERATE
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
    (double)max_dim AS max_dim:double,
    (double)min_dim AS min_dim:double,
    (double)med_dim AS med_dim:double,
    esfs_enabled,
    future_street_date,
    prev_isHoldMarkets,
    prev_isShipMarkets,
    prev_isRushMarkets,
    prev_isShipToStoreMarkets,
    prev_isShip AS prev_isShip:int,
    prev_isRush AS prev_isRush:int,
    prev_isHold AS prev_isHold:int,
    prev_isShipToStore AS prev_isShipToStore:int,
    cur_isHoldMarkets,
    cur_isShipMarkets,
    cur_isRushMarkets,
    cur_isShipToStoreMarkets,
    (cur_isShip IS NOT NULL AND cur_isShip == TRUE ? 1 : 0) AS cur_isShip:int,
    (cur_isRush IS NOT NULL AND cur_isRush == TRUE ? 1 : 0) AS cur_isRush:int,
    (cur_isHold IS NOT NULL AND cur_isHold == TRUE ? 1 : 0)AS cur_isHold:int,
    (cur_isShipToStore IS NOT NULL AND cur_isShipToStore == TRUE ? 1 :0) AS cur_isShipToStore:int;
    
input_rpl_fact = LOAD '$INPUT_RPL_FACT_PATH' USING PigStorage(',', '-schema');

--filter_rpl_fact = FILTER input_rpl_fact BY ( isDeleted IS NULL OR isDeleted == FALSE);

project_rpl_fact = FOREACH input_rpl_fact GENERATE
    departmentId AS departmentId:int,
    classId AS classId:int,
    itemId AS itemId:int,
    locationId AS locationId:int,
    rplStatusCode AS rplStatusCode:chararray,
    isDeleted AS isDeleted:boolean,
    prev_inventorydeterminationindicator AS prev_inventorydeterminationindicator:int,
    prev_isShip AS prev_isShip:int,
    prev_isRush AS prev_isRush:int,
    prev_isHold AS prev_isHold:int,
    prev_isShipToStore AS prev_isShipToStore:int,
    loc_isShip AS loc_isShip:int,
    loc_isRush AS loc_isRush:int,
    loc_isHold AS loc_isHold:int,
    loc_isShipToStore AS loc_isShipToStore:int,
    loc_markets AS loc_markets:chararray,
    (double)box_max AS box_max:double,
    (double)box_min AS box_min:double,
    (double)box_med AS box_med:double,
    online_enabled AS online_enabled:int,
    sioc_item AS sioc_item:int,
    default_rpl_ineligibility AS default_rpl_ineligibility:int;

/*
*  Item present in RP will have a record in RPL
*/
join_rp_loc_combined_fact = JOIN project_rp_fact BY (departmentId, classId, itemId) LEFT OUTER, project_rpl_fact BY (departmentId, classId, itemId) PARALLEL 800;

project_rp_loc_combined_fact = FOREACH join_rp_loc_combined_fact GENERATE
    project_rp_fact::departmentId AS departmentId:int,
    project_rp_fact::classId AS classId:int,
    project_rp_fact::itemId AS itemId:int,
    project_rpl_fact::locationId AS locationId:int,
    project_rpl_fact::rplStatusCode AS rplStatusCode:chararray,
    project_rpl_fact::isDeleted AS isDeleted:boolean,
    project_rpl_fact::prev_inventorydeterminationindicator AS prev_inventorydeterminationindicator:int,
    project_rpl_fact::prev_isShip AS prev_item_loc_isShip:int,
    project_rpl_fact::prev_isRush AS prev_item_loc_isRush:int,
    project_rpl_fact::prev_isHold AS prev_item_loc_isHold:int,
    project_rpl_fact::prev_isShipToStore AS prev_item_loc_isShipToStore:int,
    project_rpl_fact::loc_isShip AS loc_isShip:int,
    project_rpl_fact::loc_isRush AS loc_isRush:int,
    project_rpl_fact::loc_isHold AS loc_isHold:int,
    project_rpl_fact::loc_isShipToStore AS loc_isShipToStore:int,
    project_rpl_fact::loc_markets AS loc_markets:chararray,
    project_rpl_fact::box_max AS loc_box_max:double,
    project_rpl_fact::box_min AS loc_box_min:double,
    project_rpl_fact::box_med AS loc_box_med:double,
    project_rpl_fact::online_enabled AS online_enabled:int,
    project_rpl_fact::sioc_item AS sioc_item:int,
    project_rpl_fact::default_rpl_ineligibility AS default_rpl_ineligibility:int,
    project_rp_fact::entertainmentsegmentationstartingsalesdate AS entertainmentsegmentationstartingsalesdate:chararray,
    project_rp_fact::prepaidownercode AS prepaidownercode:chararray,
    project_rp_fact::isflammable AS isflammable:boolean,
    project_rp_fact::ishazardousmaterial AS ishazardousmaterial:boolean,
    project_rp_fact::height AS height:double,
    project_rp_fact::width AS width:double,
    project_rp_fact::depth AS depth:double,
    project_rp_fact::max_dim AS item_max_dim:double,
    project_rp_fact::min_dim AS item_min_dim:double,
    project_rp_fact::med_dim AS item_med_dim:double,
    project_rp_fact::future_street_date AS future_street_date:int,
    project_rp_fact::esfs_enabled AS esfs_enabled:int,
    project_rp_fact::prev_isShipMarkets AS prev_item_isShipMarkets:chararray,
    project_rp_fact::prev_isRushMarkets AS prev_item_isRushMarkets:chararray,
    project_rp_fact::prev_isHoldMarkets AS prev_item_isHoldMarkets:chararray,
    project_rp_fact::prev_isShipToStoreMarkets AS prev_item_isShipToStoreMarkets:chararray,
    project_rp_fact::prev_isShip AS prev_item_isShip:int,
    project_rp_fact::prev_isRush AS prev_item_isRush:int,
    project_rp_fact::prev_isHold AS prev_item_isHold:int,
    project_rp_fact::prev_isShipToStore AS prev_item_isShipToStore:int,
    project_rp_fact::cur_isShipMarkets AS cur_item_isShipMarkets:chararray,
    project_rp_fact::cur_isRushMarkets AS cur_item_isRushMarkets:chararray,
    project_rp_fact::cur_isHoldMarkets AS cur_item_isHoldMarkets:chararray,
    project_rp_fact::cur_isShipToStoreMarkets AS cur_item_isShipToStoreMarkets:chararray,
    project_rp_fact::cur_isShip AS cur_item_isShip:int,
    project_rp_fact::cur_isRush AS cur_item_isRush:int,
    project_rp_fact::cur_isHold AS cur_item_isHold:int,
    project_rp_fact::cur_isShipToStore AS cur_item_isShipToStore:int;

/*
* location level FF options and item level FF options are TRUE ? then check markets
*    - itemLevelMarkets is NULL/Empty value means ALL MARKETS enable it to all Location default.
*    - if itemLevelMarkets is NOT NULL/Empty 
*        -> check if Location level market  matches the itemMarkets values ? TRUE : FALSE
*
* default_ineligibility:
*     if an item not present in DSD and not statuscode 'D', and not present in AIR then it is Ineligibile.
*         Reason:
*         -- DSD items will be removed from AIR File
*         -- 'D'-coded items will be removed from AIR File
*/

location_markets_eligibility = FOREACH project_rp_loc_combined_fact {
    itemLocWithMarkets_isHold = (cur_item_isHold == 1 AND (loc_isHold IS NOT NULL AND loc_isHold == 1) AND 
      (cur_item_isHoldMarkets IS NULL OR SIZE(TRIM(cur_item_isHoldMarkets)) == 0 OR CHECK_MARKETS(cur_item_isHoldMarkets, loc_markets)) ? 
      1 : 0
    );
    itemLocWithMarkets_isShip = (cur_item_isShip == 1 AND (loc_isShip IS NOT NULL AND loc_isShip == 1) AND 
      (cur_item_isShipMarkets IS NULL OR SIZE(TRIM(cur_item_isShipMarkets)) == 0 OR CHECK_MARKETS(cur_item_isShipMarkets, loc_markets)) ?
      1 : 0 
    );
    itemLocWithMarkets_isRush = (cur_item_isRush == 1 AND (loc_isRush IS NOT NULL AND loc_isRush == 1) AND 
      (cur_item_isRushMarkets IS NULL OR SIZE(TRIM(cur_item_isRushMarkets)) == 0 OR CHECK_MARKETS(cur_item_isRushMarkets, loc_markets)) ? 
      1 : 0 
    );
    itemLocWithMarkets_isShipToStore = (cur_item_isShipToStore == 1 AND (loc_isShipToStore IS NOT NULL AND loc_isShipToStore == 1) AND 
      (cur_item_isShipToStoreMarkets IS NULL OR SIZE(TRIM(cur_item_isShipToStoreMarkets)) == 0 OR CHECK_MARKETS(cur_item_isShipToStoreMarkets, loc_markets)) ? 
      1 : 0 
    );
    fit_in_box = (NOT (item_max_dim == 0.0 AND item_min_dim == 0.0 AND item_med_dim == 0.0) AND
                    (loc_box_max IS NOT NULL AND loc_box_min IS NOT NULL AND loc_box_med IS NOT NULL) AND
                    (item_max_dim <= loc_box_max AND item_min_dim <= loc_box_min AND item_med_dim <= loc_box_med) ? 1 : 0 );
    item_loc_isHold = ((itemLocWithMarkets_isHold == 1 AND default_rpl_ineligibility == 0) ? 1 : 0); 
    item_loc_isShip = (((itemLocWithMarkets_isShip == 0) OR (default_rpl_ineligibility == 1) OR
                          ((sioc_item IS NULL OR sioc_item == 0) AND  ((esfs_enabled IS NOT NULL AND esfs_enabled == 0) AND (online_enabled IS NOT NULL AND online_enabled == 0)))
                      ) ? 0 : 1
    ); 
    item_loc_isRush = ((itemLocWithMarkets_isRush == 1 AND default_rpl_ineligibility == 0) ? 1 : 0); 
    item_loc_isShipToStore = ((itemLocWithMarkets_isShipToStore == 1 AND default_rpl_ineligibility == 0) ? 1 : 0); 
    GENERATE
        ..cur_item_isShipToStore,
        (sioc_item IS NOT NULL AND sioc_item == 1 ? 1 : fit_in_box) AS fit_in_box:int,
        itemLocWithMarkets_isHold AS itemLocWithMarkets_isHold:int,
        itemLocWithMarkets_isShip AS itemLocWithMarkets_isShip:int,
        itemLocWithMarkets_isRush AS itemLocWithMarkets_isRush:int,
        itemLocWithMarkets_isShipToStore AS itemLocWithMarkets_isShipToStore:int,
        item_loc_isHold AS item_loc_isHold:int,
        item_loc_isShip AS item_loc_isShip:int,
        item_loc_isRush AS item_loc_isRush:int,
        item_loc_isShipToStore AS item_loc_isShipToStore:int;
}

grouped_loc_eligibility = GROUP location_markets_eligibility BY (departmentId, classId, itemId, locationId);

project_loc_eligibility = FOREACH grouped_loc_eligibility {
    fit_in_atleast_one_box = (SUM(location_markets_eligibility.fit_in_box) > 0 ? 1 : 0);
    cur_item_loc_isHold = (SUM(location_markets_eligibility.item_loc_isHold) > 0 ? 1 : 0);
    cur_item_loc_isShip = (SUM(location_markets_eligibility.item_loc_isShip) > 0 ? 1 : 0);
    cur_item_loc_isRush = (SUM(location_markets_eligibility.item_loc_isRush) > 0 ? 1 : 0);
    cur_item_loc_isShipToStore = (SUM(location_markets_eligibility.item_loc_isShipToStore) > 0 ? 1 :0);
    GENERATE
        FLATTEN(location_markets_eligibility),
        fit_in_atleast_one_box,
        cur_item_loc_isHold AS cur_item_loc_isHold:int,
        (fit_in_atleast_one_box == 1 AND cur_item_loc_isShip == 1 ? 1 : 0 ) AS cur_item_loc_isShip:int,
        (fit_in_atleast_one_box == 1 AND cur_item_loc_isRush == 1 ? 1 : 0 ) AS cur_item_loc_isRush:int,
        cur_item_loc_isShipToStore AS cur_item_loc_isShipToStore:int;
};

--final_loc_eligibility = DISTINCT project_loc_eligibility;

project_item_loc_eligibility = FOREACH project_loc_eligibility GENERATE
    location_markets_eligibility::departmentId AS departmentId,
    location_markets_eligibility::classId AS classId,
    location_markets_eligibility::itemId AS itemId,
    location_markets_eligibility::locationId AS locationId,
    location_markets_eligibility::rplStatusCode AS rplStatusCode,
    location_markets_eligibility::isDeleted AS isDeleted,
    location_markets_eligibility::prev_inventorydeterminationindicator AS prev_inventorydeterminationindicator,
    location_markets_eligibility::prev_item_loc_isShip AS prev_item_loc_isShip,
    location_markets_eligibility::prev_item_loc_isRush AS prev_item_loc_isRush,
    location_markets_eligibility::prev_item_loc_isHold AS prev_item_loc_isHold,
    location_markets_eligibility::prev_item_loc_isShipToStore AS prev_item_loc_isShipToStore,
    location_markets_eligibility::loc_isShip AS loc_isShip,
    location_markets_eligibility::loc_isRush AS loc_isRush,
    location_markets_eligibility::loc_isHold AS loc_isHold,
    location_markets_eligibility::loc_isShipToStore AS loc_isShipToStore,
    location_markets_eligibility::loc_markets AS loc_markets,
    location_markets_eligibility::loc_box_max AS loc_box_max,
    location_markets_eligibility::loc_box_min AS loc_box_min,
    location_markets_eligibility::loc_box_med AS loc_box_med,
    location_markets_eligibility::online_enabled AS online_enabled,
    location_markets_eligibility::sioc_item AS sioc_item,
    location_markets_eligibility::default_rpl_ineligibility AS default_rpl_ineligibility,
    location_markets_eligibility::entertainmentsegmentationstartingsalesdate AS entertainmentsegmentationstartingsalesdate,
    location_markets_eligibility::prepaidownercode AS prepaidownercode,
    location_markets_eligibility::isflammable AS isflammable,
    location_markets_eligibility::ishazardousmaterial AS ishazardousmaterial,
    location_markets_eligibility::height AS height,
    location_markets_eligibility::width AS width,
    location_markets_eligibility::depth AS depth,
    location_markets_eligibility::item_max_dim AS item_max_dim,
    location_markets_eligibility::item_min_dim AS item_min_dim,
    location_markets_eligibility::item_med_dim AS item_med_dim,
    location_markets_eligibility::future_street_date AS future_street_date,
    location_markets_eligibility::esfs_enabled AS esfs_enabled,
    location_markets_eligibility::prev_item_isShipMarkets AS prev_item_isShipMarkets,
    location_markets_eligibility::prev_item_isRushMarkets AS prev_item_isRushMarkets,
    location_markets_eligibility::prev_item_isHoldMarkets AS prev_item_isHoldMarkets,
    location_markets_eligibility::prev_item_isShipToStoreMarkets AS prev_item_isShipToStoreMarkets,
    location_markets_eligibility::prev_item_isShip AS prev_item_isShip,
    location_markets_eligibility::prev_item_isRush AS prev_item_isRush,
    location_markets_eligibility::prev_item_isHold AS prev_item_isHold,
    location_markets_eligibility::prev_item_isShipToStore AS prev_item_isShipToStore,
    location_markets_eligibility::cur_item_isShipMarkets AS cur_item_isShipMarkets,
    location_markets_eligibility::cur_item_isRushMarkets AS cur_item_isRushMarkets,
    location_markets_eligibility::cur_item_isHoldMarkets AS cur_item_isHoldMarkets,
    location_markets_eligibility::cur_item_isShipToStoreMarkets AS cur_item_isShipToStoreMarkets,
    location_markets_eligibility::cur_item_isShip AS cur_item_isShip,
    location_markets_eligibility::cur_item_isRush AS cur_item_isRush,
    location_markets_eligibility::cur_item_isHold AS cur_item_isHold,
    location_markets_eligibility::cur_item_isShipToStore AS cur_item_isShipToStore,
    location_markets_eligibility::fit_in_box AS fit_in_box,
    location_markets_eligibility::itemLocWithMarkets_isHold AS itemLocWithMarkets_isHold,
    location_markets_eligibility::itemLocWithMarkets_isShip AS itemLocWithMarkets_isShip,
    location_markets_eligibility::itemLocWithMarkets_isRush AS itemLocWithMarkets_isRush,
    location_markets_eligibility::itemLocWithMarkets_isShipToStore AS itemLocWithMarkets_isShipToStore,
    location_markets_eligibility::item_loc_isHold AS item_loc_isHold,
    location_markets_eligibility::item_loc_isShip AS item_loc_isShip,
    location_markets_eligibility::item_loc_isRush AS item_loc_isRush,
    location_markets_eligibility::item_loc_isShipToStore AS item_loc_isShipToStore,
    fit_in_atleast_one_box,
    cur_item_loc_isHold,
    cur_item_loc_isShip,
    cur_item_loc_isRush,
    cur_item_loc_isShipToStore;
 
--Store to hive external path with partition 
STORE project_item_loc_eligibility INTO '$OUTPUT_PATH' USING PigStorage(',', '-schema');
