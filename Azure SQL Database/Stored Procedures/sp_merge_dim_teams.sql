CREATE OR ALTER PROCEDURE dbo.sp_merge_dim_teams
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_teams AS tgt
    USING dbo.stg_dim_teams AS src
        ON tgt.ID_TEAM = src.ID_TEAM

    WHEN MATCHED THEN
        UPDATE SET
            tgt.TEAM_NAME = src.TEAM_NAME

    WHEN NOT MATCHED THEN
        INSERT (ID_TEAM, TEAM_NAME)
        VALUES (src.ID_TEAM, src.TEAM_NAME);
END;
GO
