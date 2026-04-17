begin
  sys.dbms_aqadm.create_queue(
    queue_name          => '"ACP_QUEUE"',
    queue_table         => '"ACP_AQ_TAB"',
    queue_type          => 0,
    max_retries         => 3,
    retry_delay         => 60,
    dependency_tracking => false
  );

  dbms_aqadm.start_queue(
    queue_name => '"ACP_QUEUE"'
  );
end;
/
