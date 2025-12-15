CREATE OR ALTER PROCEDURE dbo.sp_merge_dim_date
AS
BEGIN
    SET NOCOUNT ON;

    MERGE dbo.dim_date AS tgt
    USING dbo.stg_dim_date AS src
        ON tgt.ID_DATE = src.ID_DATE

    WHEN MATCHED THEN
        UPDATE SET
            tgt.DATE_VALUE = src.DATE_VALUE,
            tgt.YEAR = src.YEAR,
            tgt.MONTH = src.MONTH,
            tgt.DAY = src.DAY,
            tgt.QUARTER = src.QUARTER,
            tgt.WEEKOFYEAR = src.WEEKOFYEAR,
            tgt.WEEKDAYNUMBER = src.WEEKDAYNUMBER,
            tgt.WEEKDAYNAME = src.WEEKDAYNAME,
            tgt.MONTHNAME = src.MONTHNAME,
            tgt.DAYTYPE = src.DAYTYPE,
            tgt.SEASON = src.SEASON

    WHEN NOT MATCHED THEN
        INSERT (
            ID_DATE, DATE_VALUE, YEAR, MONTH, DAY, QUARTER, WEEKOFYEAR, WEEKDAYNUMBER,
            WEEKDAYNAME, MONTHNAME, DAYTYPE, SEASON
        )
        VALUES (
            src.ID_DATE, src.DATE_VALUE, src.YEAR, src.MONTH, src.DAY, src.QUARTER,
            src.WEEKOFYEAR, src.WEEKDAYNUMBER, src.WEEKDAYNAME, src.MONTHNAME, src.DAYTYPE, src.SEASON
        );
END;
GO
