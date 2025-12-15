CREATE OR ALTER PROCEDURE dbo.sp_merge_fact_matches
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.fact_matches AS tgt
    USING dbo.stg_fact_matches AS src
        ON tgt.ID_MATCH = src.ID_MATCH

    WHEN MATCHED THEN 
        UPDATE SET
            tgt.MATCH_DATE = src.MATCH_DATE,
            tgt.MATCH_TIME = src.MATCH_TIME,
            tgt.ID_DATE = src.ID_DATE,
            tgt.ID_TIME = src.ID_TIME,
            tgt.MATCH_DATETIME = src.MATCH_DATETIME,
            tgt.ID_DATETIME = src.ID_DATETIME, 
            tgt.ID_HOME_TEAM = src.ID_HOME_TEAM, 
            tgt.ID_AWAY_TEAM = src.ID_AWAY_TEAM,
            tgt.ID_LEAGUE = src.ID_LEAGUE,
            tgt.FULL_TIME_HOME_GOAL = src.FULL_TIME_HOME_GOAL,
            tgt.FULL_TIME_AWAY_GOAL = src.FULL_TIME_AWAY_GOAL,
            tgt.FULL_TIME_RESULT = src.FULL_TIME_RESULT,
            tgt.HALFTIME_HOME_GOAL = src.HALFTIME_HOME_GOAL,
            tgt.HALFTIME_AWAY_GOAL = src.HALFTIME_AWAY_GOAL,
            tgt.HALFTIME_RESULT = src.HALFTIME_RESULT,
            tgt.HOME_SHOTS = src.HOME_SHOTS,
            tgt.AWAY_SHOTS = src.AWAY_SHOTS,
            tgt.HOME_SHOTS_ON_TARGET = src.HOME_SHOTS_ON_TARGET,
            tgt.AWAY_SHOTS_ON_TARGET = src.AWAY_SHOTS_ON_TARGET,
            tgt.HOME_FOULS = src.HOME_FOULS,
            tgt.AWAY_FOULS = src.AWAY_FOULS,
            tgt.HOME_CORNERS = src.HOME_CORNERS,
            tgt.AWAY_CORNERS = src.AWAY_CORNERS,
            tgt.HOME_YELLOW = src.HOME_YELLOW,
            tgt.AWAY_YELLOW = src.AWAY_YELLOW,
            tgt.HOME_RED = src.HOME_RED,
            tgt.AWAY_RED = src.AWAY_RED

    WHEN NOT MATCHED THEN
        INSERT (
            ID_MATCH, MATCH_DATE, MATCH_TIME, ID_DATE, ID_TIME, MATCH_DATETIME,
            ID_DATETIME, ID_HOME_TEAM, ID_AWAY_TEAM,ID_LEAGUE,
            FULL_TIME_HOME_GOAL, FULL_TIME_AWAY_GOAL, FULL_TIME_RESULT,
            HALFTIME_HOME_GOAL, HALFTIME_AWAY_GOAL, HALFTIME_RESULT,
            HOME_SHOTS, AWAY_SHOTS, HOME_SHOTS_ON_TARGET, AWAY_SHOTS_ON_TARGET,
            HOME_FOULS, AWAY_FOULS, HOME_CORNERS, AWAY_CORNERS,
            HOME_YELLOW, AWAY_YELLOW, HOME_RED, AWAY_RED
        )
        VALUES (
            src.ID_MATCH, src.MATCH_DATE, src.MATCH_TIME, src.ID_DATE, src.ID_TIME, src.MATCH_DATETIME,
            src.ID_DATETIME, src.ID_HOME_TEAM, src.ID_AWAY_TEAM,src.ID_LEAGUE,
            src.FULL_TIME_HOME_GOAL, src.FULL_TIME_AWAY_GOAL, src.FULL_TIME_RESULT,
            src.HALFTIME_HOME_GOAL, src.HALFTIME_AWAY_GOAL, src.HALFTIME_RESULT,
            src.HOME_SHOTS, src.AWAY_SHOTS, src.HOME_SHOTS_ON_TARGET, src.AWAY_SHOTS_ON_TARGET,
            src.HOME_FOULS, src.AWAY_FOULS, src.HOME_CORNERS, src.AWAY_CORNERS,
            src.HOME_YELLOW, src.AWAY_YELLOW, src.HOME_RED, src.AWAY_RED
        );

END;
GO
