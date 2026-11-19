CREATE VIEW dbo.v_open_claims AS
SELECT TOP 100
       [claim id],
       ISNULL(policy_no, 'unknown') AS policy_no,
       DATEDIFF(day, opened, GETDATE()) AS age_days,
       IIF(is_open = 1, 'open', 'closed') AS status
FROM dbo.claim
ORDER BY opened
