begin
  sys.dbms_aqadm.create_queue_table(
    queue_table        => '"ACP_AQ_TAB"',
    queue_payload_type => 'JSON',
    multiple_consumers => true
  );
end;
/
