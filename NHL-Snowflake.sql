/*
Welcome Everyone
Need: 
    Standard Account
    Access to AccountAdmin
    Access to a warehouse
*/

CREATE DATABASE NHL;
CREATE SCHEMA STAGING;
CREATE SCHEMA EDW;

USE SCHEMA STAGING;

CREATE OR REPLACE NETWORK RULE NHL.STAGING.NHL_API_NETWORK_RULE
MODE = EGRESS
TYPE = HOST_PORT
 VALUE_LIST = ('api.nhle.com','api-web.nhle.com');

CREATE OR REPLACE EXTERNAL ACCESS INTEGRATION NHL_API_ACCESS_INTEGRATION
ALLOWED_NETWORK_RULES = (NHL.STAGING.NHL_API_NETWORK_RULE)
ENABLED=TRUE;

/* in order to get play by play info, we need all the games*/
CREATE OR REPLACE FUNCTION get_nhl_games()
    RETURNS variant
    LANGUAGE PYTHON
    RUNTIME_VERSION = 3.11
    packages=('requests==2.31.0')
    HANDLER = 'get_nhl_games'
    EXTERNAL_ACCESS_INTEGRATIONS = (NHL_API_ACCESS_INTEGRATION)
    AS
    $$
import requests
import json
def get_nhl_games():
    session = requests.Session()
    url = "https://api.nhle.com/stats/rest/en/game"
    response = session.get(url)
    return(response.json()['data'])
    $$;

    --let's see what's there
    select get_nhl_games();

    create or replace table staging.games(
    games_json variant,
    retrieve_dtm timestamp
    );

    /*IF YOU WANTED TO AUTOMATE, TURN THESE INTO A SP*/
    DELETE FROM STAGING.GAMES;
    insert into staging.games 
    select get_nhl_games(),current_timestamp;

    /* LET'S SEE THE DATA */
    SELECT TOP 1 GJ.* FROM STAGING.GAMES,
    LATERAL FLATTEN(INPUT=>GAMES_JSON) GJ;

    create OR REPLACE table edw.games (
    game_id integer,
    season_id integer,
    eastern_start_dtm timestamp,
    game_date date,
    game_num integer,
    game_state_id integer,
    game_scheduled_state_id integer,
    game_type integer,
    period integer,
    home_score integer,
    home_team_id integer,
    visiting_score integer,
    visiting_team_id integer,
    create_dtm timestamp,
    update_dtm timestamp 
    );


merge into edw.games AS games using (
SELECT 
f.value:easternStartTime::timestamp_ntz as eastern_Start_dtm,
f.value:gameDate::date as game_date,
f.value:gameNumber::int as game_num,
f.value:gameScheduleStateId::int as game_scheduled_state_id,
f.value:gameStateId::int as game_state_id,
f.value:gameType as game_type,
f.value:homeScore as home_score,
f.value:homeTeamId::int as home_team_id,
f.value:id::int as game_id,
f.value:period as period,
f.value:season as season_id,
f.value:visitingScore as visiting_score,
f.value:visitingTeamId::int as visiting_team_id,
RETRIEVE_DTM AS CREATE_DTM,
RETRIEVE_DTM as UPDATE_DTM
FROM staging.games g,
  lateral flatten(input => g.games_json) f 
  where not exists (
  select 1 from edw.games where game_id= f.value:id and game_state_id = f.value:gameStateId
  )
  )
  base
  on base.game_id = games.game_id
  when matched then update SET
   season_id = base.season_id,
    eastern_start_dtm = base.eastern_start_dtm,
    game_date = base.game_date,
    game_num = base.game_num,
    game_state_id = base.game_state_id,
    game_scheduled_state_id= base.game_scheduled_state_id,
    game_type= base.game_type,
    period= base.period,
    home_score= base.home_score,
    home_team_id= base.home_team_id,
    visiting_score= base.visiting_score,
    visiting_team_id= base.visiting_team_id,
    update_dtm= base.UPDATE_DTM
  
  when NOT MATCHED  then insert (game_id,
    season_id,
    eastern_start_dtm,
    game_date,
    game_num,
    game_state_id,
    game_scheduled_state_id,
    game_type,
    period,
    home_score,
    home_team_id,
    visiting_score,
    visiting_team_id,
    create_dtm,
    update_dtm )
  values
  (base.game_id,
    base.season_id,
    base.eastern_start_dtm,
    base.game_date,
    base.game_num,
    base.game_state_id,
    base.game_scheduled_state_id,
    base.game_type,
    base.period,
    base.home_score,
    base.home_team_id,
    base.visiting_score,
    base.visiting_team_id,
    base.CREATE_DTM,
    base.create_dtm )
  ;


    select * from edw.games;


 
CREATE OR REPLACE FUNCTION get_nhl_teams() 
  RETURNS variant
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.8
  packages=('requests==2.31.0')
  HANDLER = 'get_nhl_teams'
  EXTERNAL_ACCESS_INTEGRATIONS = (nhl_apis_access_integration)
  AS
  $$
import _snowflake
import requests
import json
def get_nhl_teams():
    session = requests.Session()
    url = "https://api.nhle.com/stats/rest/en/team"
    response = session.get(url)
    return(response.json()['data'])
  $$;
--LETS SEE THE DATA
  SELECT  GET_NHL_TEAMS();


  CREATE OR REPLACE TABLE STAGING.TEAMS(
  TEAMS_JSON VARIANT,
  RETRIEVE_DTM TIMESTAMP
  );
  INSERT INTO STAGING.TEAMS
  SELECT  GET_NHL_TEAMS(),CURRENT_TIMESTAMP;

  --SELECT * FROM STAGING.TEAMS;



    create OR REPLACE table edw.teams (
    team_id integer,
    team_name string,
    franchise_id integer,
    league_id integer,
    team_raw_tricode string,
    team_tricode string,
    create_dtm timestamp,
    update_dtm timestamp 
    );



    --just reload teams yearly before season starts
    delete from edw.teams;
    insert into edw.teams 
    SELECT 
    f.value:id::int as team_id,
f.value:fullName::string as team_name,
f.value:franchiseId::int as franchise_id,
f.value:leagueId::int as league_id,
f.value:rawTricode::string as team_raw_tricode,
f.value:triCode::string as team_tricode,
RETRIEVE_DTM AS CREATE_DTM,
RETRIEVE_DTM as UPDATE_DTM
FROM staging.teams t,
  lateral flatten(input => t.teams_json) f;

  SELECT * FROM EDW.TEAMS;




  CREATE OR REPLACE FUNCTION get_nhl_seasons() 
  RETURNS variant
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.8
  packages=('requests==2.31.0')
  HANDLER = 'get_nhl_seasons'
  EXTERNAL_ACCESS_INTEGRATIONS = (nhl_apis_access_integration)
  AS
  $$
import _snowflake
import requests
import json
def get_nhl_seasons():
    session = requests.Session()
    url = "https://api.nhle.com/stats/rest/en/season"
    response = session.get(url)
    return(response.json()['data'])
  $$;
  --LET'S SEE THE DATA
  SELECT get_nhl_seasons() ;

  CREATE OR REPLACE TABLE STAGING.SEASON(
  SEASONS_JSON VARIANT,
  RETRIEVE_DTM TIMESTAMP
  );
  INSERT INTO STAGING.SEASON
SELECT get_nhl_seasons(),CURRENT_TIMESTAMP ;

SELECT * FROM STAGING.SEASON;


    create OR REPLACE table edw.seasons (
    season_id integer,
    formatted_season_id string,
    all_star_game_in_use integer,
    conferences_in_use integer,
    divisions_in_use integer,
    season_end_dtm timestamp,
    entry_draft_in_use integer,
    minimum_playoff_minutes_for_goalie_stats_leaders integer,
    minimum_regular_games_for_goalie_stats_leaders integer,
    nhl_stanley_cup_owner integer,
    num_of_games integer,
    olympics_Participation integer,
    point_For_OT_Loss_In_Us integer, 
    presason_start_dtm timestamp,
    regular_season_end_dtm timestamp,
    row_in_use integer,
    season_ordinal_num integer,
    season_start_dtm timestamp,
    supplemental_Draft_In_Use integer,
    ties_in_use integer,
    total_playoff_games integer,
    total_regular_season_games integer,
    wildcard_In_Use integer,
    create_dtm timestamp,
    update_dtm timestamp 
    );


merge into edw.seasons seasons using (
  SELECT  
  f.value:id::int as season_id,
  f.value:formattedSeasonId::varchar as formatted_season_id,
f.value:allStarGameInUse::int as all_star_game_in_use,
f.value:conferencesInUse::int as conferences_in_use,
f.value:divisionsInUse::int as divisions_in_use,
f.value:endDate::timestamp_ntz as season_end_dtm,
f.value:entryDraftInUse::int as entry_draft_in_use,
f.value:minimumPlayoffMinutesForGoalieStatsLeaders::int as minimum_playoff_minutes_for_goalie_stats_leaders,
f.value:minimumRegularGamesForGoalieStatsLeaders::int as minimum_regular_games_for_goalie_stats_leaders,
f.value:nhlStanleyCupOwner::int as nhl_stanley_cup_owner,
f.value:numberOfGames::int as num_of_games,
f.value:olympicsParticipation::int as olympics_Participation,
f.value:pointForOTLossInUse::int as point_For_OT_Loss_In_Us, 
f.value:preseasonStartdate::timestamp_ntz as presason_start_dtm,
f.value:regularSeasonEndDate::timestamp_ntz as regular_season_end_dtm,
f.value:rowInUse::int as row_in_use,
f.value:seasonOrdinal::int as season_ordinal_num,
f.value:startDate::timestamp_ntz as season_start_dtm,
f.value:supplementalDraftInUse::int as supplemental_Draft_In_Use,
f.value:tiesInUse::int as ties_in_use,
f.value:totalPlayoffGames::int as total_playoff_games,
f.value:totalRegularSeasonGames::int as total_regular_season_games,
f.value:wildcardInUse::int as wildcard_In_Use,
RETRIEVE_DTM AS CREATE_DTM,
RETRIEVE_DTM as UPDATE_DTM
FROM staging.season s,
  lateral flatten(input => s.seasons_json) f 
  --not already in seasons, or the season hasn't ended yet
  where not exists (select 1 from edw.seasons where f.value:id::int=season_id and update_dtm>=season_end_dtm and s.RETRIEVE_DTM >=season_end_dtm)
  ) base
  on base.season_id = seasons.season_id
  when matched then update set
FORMATTED_SEASON_ID = BASE.FORMATTED_SEASON_ID,
ALL_STAR_GAME_IN_USE = BASE.ALL_STAR_GAME_IN_USE,
CONFERENCES_IN_USE = BASE.CONFERENCES_IN_USE,
DIVISIONS_IN_USE = BASE.DIVISIONS_IN_USE,
SEASON_END_DTM = BASE.SEASON_END_DTM,
ENTRY_DRAFT_IN_USE = BASE.ENTRY_DRAFT_IN_USE,
MINIMUM_PLAYOFF_MINUTES_FOR_GOALIE_STATS_LEADERS = BASE.MINIMUM_PLAYOFF_MINUTES_FOR_GOALIE_STATS_LEADERS,
MINIMUM_REGULAR_GAMES_FOR_GOALIE_STATS_LEADERS = BASE.MINIMUM_REGULAR_GAMES_FOR_GOALIE_STATS_LEADERS,
NHL_STANLEY_CUP_OWNER = BASE.NHL_STANLEY_CUP_OWNER,
NUM_OF_GAMES = BASE.NUM_OF_GAMES,
OLYMPICS_PARTICIPATION = BASE.OLYMPICS_PARTICIPATION,
POINT_FOR_OT_LOSS_IN_US = BASE.POINT_FOR_OT_LOSS_IN_US,
PRESASON_START_DTM = BASE.PRESASON_START_DTM,
REGULAR_SEASON_END_DTM = BASE.REGULAR_SEASON_END_DTM,
ROW_IN_USE = BASE.ROW_IN_USE,
SEASON_ORDINAL_NUM = BASE.SEASON_ORDINAL_NUM,
SEASON_START_DTM = BASE.SEASON_START_DTM,
SUPPLEMENTAL_DRAFT_IN_USE = BASE.SUPPLEMENTAL_DRAFT_IN_USE,
TIES_IN_USE = BASE.TIES_IN_USE,
TOTAL_PLAYOFF_GAMES = BASE.TOTAL_PLAYOFF_GAMES,
TOTAL_REGULAR_SEASON_GAMES = BASE.TOTAL_REGULAR_SEASON_GAMES,
WILDCARD_IN_USE = BASE.WILDCARD_IN_USE,
update_dtm = BASE.UPDATE_DTM
  when not matched then insert (SEASON_ID, FORMATTED_SEASON_ID, ALL_STAR_GAME_IN_USE, CONFERENCES_IN_USE, DIVISIONS_IN_USE, SEASON_END_DTM, ENTRY_DRAFT_IN_USE, MINIMUM_PLAYOFF_MINUTES_FOR_GOALIE_STATS_LEADERS, MINIMUM_REGULAR_GAMES_FOR_GOALIE_STATS_LEADERS, NHL_STANLEY_CUP_OWNER, NUM_OF_GAMES, OLYMPICS_PARTICIPATION, POINT_FOR_OT_LOSS_IN_US, PRESASON_START_DTM, REGULAR_SEASON_END_DTM, ROW_IN_USE, SEASON_ORDINAL_NUM, SEASON_START_DTM, SUPPLEMENTAL_DRAFT_IN_USE, TIES_IN_USE, TOTAL_PLAYOFF_GAMES, TOTAL_REGULAR_SEASON_GAMES, WILDCARD_IN_USE, CREATE_DTM, UPDATE_DTM)
  values (base.SEASON_ID,BASE.FORMATTED_SEASON_ID,BASE.ALL_STAR_GAME_IN_USE,BASE.CONFERENCES_IN_USE,BASE.DIVISIONS_IN_USE,BASE.SEASON_END_DTM,BASE.ENTRY_DRAFT_IN_USE,BASE.MINIMUM_PLAYOFF_MINUTES_FOR_GOALIE_STATS_LEADERS,BASE.MINIMUM_REGULAR_GAMES_FOR_GOALIE_STATS_LEADERS,BASE.NHL_STANLEY_CUP_OWNER,BASE.NUM_OF_GAMES,BASE.OLYMPICS_PARTICIPATION,BASE.POINT_FOR_OT_LOSS_IN_US,BASE.PRESASON_START_DTM,BASE.REGULAR_SEASON_END_DTM,BASE.ROW_IN_USE,BASE.SEASON_ORDINAL_NUM,BASE.SEASON_START_DTM,BASE.SUPPLEMENTAL_DRAFT_IN_USE,BASE.TIES_IN_USE,BASE.TOTAL_PLAYOFF_GAMES,BASE.TOTAL_REGULAR_SEASON_GAMES,BASE.WILDCARD_IN_USE,BASE.CREATE_DTM,BASE.UPDATE_DTM);

SELECT * FROM EDW.SEASONS;


--67,491
select * from edw.games where game_type in (2,3) and game_state_id = 7;




  
CREATE OR REPLACE FUNCTION get_nhl_play_by_play(game_ids ARRAY) 
  RETURNS variant
  LANGUAGE PYTHON
  RUNTIME_VERSION = 3.8
  packages=('requests==2.31.0')
  HANDLER = 'get_nhl_play_by_play'
  EXTERNAL_ACCESS_INTEGRATIONS = (nhl_apis_access_integration)
  AS
  $$
import _snowflake
import requests
import time
import json

def get_nhl_play_by_play(game_ids):
    session = requests.Session()
    resp = []
    for game_id in game_ids:
        url = f"https://api-web.nhle.com/v1/gamecenter/{game_id}/play-by-play"
        response = session.get(url).json()
        resp.append(response)
        time.sleep(.5)
    return(resp)
  $$;

create OR REPLACE table staging.play_by_play_t0 (
  play_by_play_json variant,
  RETRIEVE_DTM TIMESTAMP
  );
  
    create OR REPLACE table staging.play_by_play (
  play_by_play_json variant,
  RETRIEVE_DTM TIMESTAMP
  );

--put this in a SP FOR AUTOMATION
DECLARE
counter INTEGER;
BEGIN
counter := (select count(1) FROM edw.games where game_id not in (select play_by_play_json:id from staging.play_by_play) and game_type in (1,2,3) and game_state_id = 7);
  WHILE (counter > 0) DO
     insert into staging.play_by_play_t0 (
select *,current_timestamp from (
  select get_nhl_play_by_play(SELECT array_agg(game_id) as game_id from (select game_id FROM edw.games where game_id not in (select play_by_play_json:id from staging.play_by_play) and game_type in (1,2,3) and game_state_id = 7 limit 100)) as res
  ) base);

  insert into staging.play_by_play
  select r.value,t.retrieve_dtm from  staging.play_by_play_t0 t,
  lateral flatten (input => t.play_by_play_json) r where r.value:id not in (select play_by_play_json:id from staging.play_by_play) ;
  counter := (select count(1) FROM edw.games where game_id not in (select play_by_play_json:id from staging.play_by_play union all select game_id from edw.play_by_play) and game_type in (1,2,3) and game_state_id = 7);
  truncate staging.play_by_play_t0 ;
  END WHILE;
END;
  
--67491
--67391
select count(1) FROM edw.games where game_id not in (select play_by_play_json:id from staging.play_by_play) and game_type in (1,2,3) and game_state_id = 7;

--let's look
select TOP 100 * from staging.play_by_play;

--all base keys
select j.value,min(j.index),max(j.index) from(
select object_keys(play_by_play_json) as json
from staging.play_by_play) z,
lateral flatten (input=>json) j
group by 1 order by 2 asc, 3 desc;

select j.value,min(j.index),max(j.index) from(
select object_keys(play_by_play_json:plays[0]) as json
from staging.play_by_play) z,
lateral flatten (input=>json) j
group by 1 order by 2 asc, 3 desc;

--all detail keys
select j.value,min(j.index),max(j.index) from(
select object_keys(play_by_play_json:plays[0]:details) as json
from staging.play_by_play) z,
lateral flatten (input=>json) j
group by 1 order by 2 asc, 3 desc
;







create or replace table edw.play_by_play as (
select 
play_by_play_json:id::int as game_id
,play_by_play_json:periodDescriptor.number::int as period_num
,play_by_play_json:periodDescriptor:otPeriods::int as ot_periods
,play_by_play_json:periodDescriptor:periodType::text as period_type
,p.value:sortOrder::int as play_sort_order
,p.value:timeInPeriod::text as play_time_in_period
,p.value:periodDescriptor.number::int as play_period_num
,p.value:periodDescriptor:otPeriods::int as play_ot_periods
,p.value:periodDescriptor:periodType::text as play_period_type
,p.value:typeCode::int as play_type_code
,p.value:homeTeamDefendingSide::text as play_home_team_defending_side
,p.value:typeDescKey::text as play_type_description_key
,p.value:situationCode::int as play_situation_code
,p.value:eventId::int as play_event_id
,p.value:timeRemaining::text as play_time_remaining
,p.value:details.awaySOG::int as play_detail_away_shots_on_goal
,p.value:details.homeSOG::int as play_detail_home_shots_on_goal
,p.value:details.committedByPlayerId::int as play_detail_committed_by_player_id
,p.value:details.assist1PlayerId::int as play_detail_assist1_player_id
,p.value:details.assist2PlayerId::int as play_detail_assist2_player_id
,p.value:details.descKey::text as play_detail_description_key
,p.value:details.reason::text as play_detail_reason
,p.value:details.awayScore::int as play_detail_away_score
,p.value:details.homeScore::int as play_detail_home_score
,p.value:details.eventOwnerTeamId::int as play_detail_event_owner_team_id
,p.value:details.assist1PlayerTotal::int as play_detail_assist1_player_total
,p.value:details.assist2PlayerTotal::int as play_detail_assist2_player_total
,p.value:details.hittingPlayerId::int as play_detail_hitting_player_id
,p.value:details.hitteePlayerId::int as play_detail_hittee_player_id
,p.value:details.secondaryReason::text as play_detail_secondary_reason
,p.value:details.playerId::int as play_detail_player_id
,p.value:details.duration::int as play_detail_duration
,p.value:details.goalieInNetId::int as play_detail_goalie_in_net_id
,p.value:details.drawnByPlayerId::int as play_detail_drawn_by_player_id
,p.value:details.winningPlayerId::int as play_detail_winning_player_id
,p.value:details.losingPlayerId::int as play_detail_losing_player_id
,p.value:details.xCoord::number as play_detail_x_coordinate
,p.value:details.yCoord::number as play_detail_y_coordinate
,p.value:details.servedByPlayerId::int as play_detail_served_by_player_id
,p.value:details.typeCode::text as play_detail_type_code
,p.value:details.shootingPlayerId::int as play_detail_shooting_player_id
,p.value:details.scoringPlayerId::int as play_detail_scoring_player_id
,p.value:details.scoringPlayerTotal::int as play_detail_scoring_player_total
,p.value:details.shotType::text as play_detail_shot_type
,p.value:details.zoneCode::text as play_detail_zone_code
,retrieve_dtm as create_dtm
,current_timestamp as update_dtm
from(
select * from 
staging.play_by_play
),
lateral flatten (input=>play_by_play_json:plays) p
)
;

create or replace table edw.game_roster as (
select 
play_by_play_json:id::int as game_id
,r.value:playerId::int as player_id
,r.value:firstName:default::text as player_first_name
,r.value:headshot::text as player_headshot
,r.value:lastName:default::text as player_last_name
,r.value:positionCode::text as player_position_code
,r.value:sweaterNumber::int as player_sweater_number
,r.value:teamId::int as team_id
,retrieve_dtm as create_dtm
,current_timestamp as update_dtm
from(
select * from 
staging.play_by_play
),
lateral flatten (input=>play_by_play_json:rosterSpots) r
)
;

--let's play 
select * from  edw.play_by_play;



--lawlers law
select scored_point,sum(scored_first_and_won) won_cnt,sum(case when scored_first_and_won = 1 then 0 else 1 end) no_won_cnt,won_cnt::decimal/(won_cnt+no_won_cnt)*100 as win_pct from (
select game_id,home_win,visiting_win,play_sort_order,
play_detail_home_score,
play_detail_away_score,lag(play_detail_away_score,1,0) over (partition by game_id order by play_sort_order asc),
case when lag(play_detail_away_score,1,0) over (partition by game_id order by play_sort_order asc) < play_detail_away_score then 1 else 0 end as visiting_scored,
case when lag(play_detail_home_score,1,0) over (partition by game_id order by play_sort_order asc) < play_detail_home_score then 1 else 0 end as home_scored,
case when visiting_scored = 1 and visiting_win then 1 when home_scored=1 and home_win then 1 else 0 end as scored_and_won,
iff(visiting_scored=1,play_detail_away_score,play_detail_home_score) as scored_point,
iff((visiting_scored=1 and play_detail_home_score<play_detail_away_score) or (home_scored=1 and play_detail_home_score>play_detail_away_score),1,0) as scored_first,
iff(scored_and_won =1 and scored_first =1,1,0) as scored_first_and_won
from (
select case when g.home_score>g.visiting_score then 1 else 0 end as home_win,case when g.visiting_score>g.home_score then 1 else 0 end as visiting_win,
pbp.*  from edw.play_by_play pbp
inner join edw.games g on g.game_id = pbp.game_id
where
(pbp.play_detail_away_score is not null or pbp.play_detail_home_score is not null)
)
) 
where scored_first = 1
group by 1
order by 1 asc
;

