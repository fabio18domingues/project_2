CREATE OR ALTER PROCEDURE dbo.sp_merge_dim_leagues
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_leagues AS tgt
    USING dbo.stg_dim_leagues AS src
        ON tgt.ID_LEAGUE = src.ID_LEAGUE

    WHEN MATCHED THEN 
        UPDATE SET
            tgt.LEAGUE_NAME = src.LEAGUE_NAME,
            tgt.COUNTRY = src.COUNTRY,
            tgt.DIVISION = src.DIVISION

    WHEN NOT MATCHED THEN
        INSERT (
            ID_LEAGUE,
            LEAGUE_NAME,
            COUNTRY,
            DIVISION
        )
        VALUES (
            src.ID_LEAGUE,
            src.LEAGUE_NAME,
            src.COUNTRY,
            src.DIVISION
        );
END;
GO
