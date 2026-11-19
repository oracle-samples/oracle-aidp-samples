-- Auto-refresh the per-user tokens on a timer, so the chat never breaks on expiry.
--
-- Two parts: (1) a routine that refreshes tokens close to expiry, (2) a DBMS_SCHEDULER job
-- that runs it on a repeating interval. The routine delegates the actual token acquisition to
-- your phase-2 minting/refresh routine - this phase only schedules and drives it.
--
-- Placeholders:
--   <SCHEMA>            the owning schema
--   <TOKEN_STORE>       your per-user token store (from phase 2)
--   <MINT_OR_REFRESH>   your phase-2 routine that (re)acquires a token for one user
--   <SKEW_MINUTES>      refresh this many minutes before expiry (e.g. 5)
--   <EVERY_MINUTES>     how often the job runs (e.g. 10)

-- 1) Refresh anything expiring within the skew window.
create or replace procedure refresh_expiring_tokens is
begin
  for r in (
    select username
    from   <TOKEN_STORE>_table
    where  expires_at <= sys_extract_utc(systimestamp) + numtodsinterval(<SKEW_MINUTES> * 60, 'second')
  ) loop
    begin
      <MINT_OR_REFRESH>(r.username);   -- phase-2 routine: (re)acquire this user's token
    exception
      when others then
        -- one user's refresh failure must not stop the batch; log and continue.
        null;   -- replace with your logging (do not log token values)
    end;
  end loop;
end;
/

-- 2) Run it on a timer.
begin
  dbms_scheduler.create_job(
    job_name        => 'REFRESH_USER_TOKENS_JOB',
    job_type        => 'STORED_PROCEDURE',
    job_action      => '<SCHEMA>.REFRESH_EXPIRING_TOKENS',
    start_date      => systimestamp,
    repeat_interval => 'FREQ=MINUTELY; INTERVAL=<EVERY_MINUTES>',
    enabled         => true,
    comments        => 'Keeps per-user downstream tokens fresh for the APEX chat.'
  );
end;
/
