CREATE VIEW dbo.v_agent_names AS
SELECT id, 'Agent ' + name AS label FROM dbo.agent
