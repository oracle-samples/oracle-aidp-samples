begin
    dbms_scheduler.create_job(
        job_name        => '"ACP_QUEUE_SWEEPER"',
        program_name    => '"ACP_QUEUE_WORKER_PGM"',
        job_style       => 'REGULAR',
        start_date      => timestamp '2026-03-10 12:11:40.753892',
        repeat_interval => 'FREQ=MINUTELY;INTERVAL=1',
        end_date        => null,
        job_class       => 'DEFAULT_JOB_CLASS',
        comments        => 'Drains any ACP queue backlog not handled by callbacks.',
        auto_drop       => false
    );

    dbms_scheduler.set_attribute(
        name      => '"ACP_QUEUE_SWEEPER"',
        attribute => 'logging_level',
        value     => dbms_scheduler.logging_off
    );

    dbms_scheduler.set_attribute(
        name      => '"ACP_QUEUE_SWEEPER"',
        attribute => 'job_priority',
        value     => 3
    );

    dbms_scheduler.set_job_argument_value(
        job_name          => '"ACP_QUEUE_SWEEPER"',
        argument_position => 1,
        argument_value    => '50'
    );

    dbms_scheduler.enable('"ACP_QUEUE_SWEEPER"');
end;
/

