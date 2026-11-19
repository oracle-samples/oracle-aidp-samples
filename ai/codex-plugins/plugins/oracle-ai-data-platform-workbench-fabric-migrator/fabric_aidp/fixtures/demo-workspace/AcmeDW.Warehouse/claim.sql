CREATE TABLE [dbo].[claim] (
    [claim id] BIGINT NOT NULL,
    policy_no NVARCHAR(50) NOT NULL,
    opened DATETIME2(7),
    is_open BIT
)
