begin
  dbms_aqadm.add_subscriber(
    queue_name => '"ACP_QUEUE"',
    subscriber => sys.aq$_agent(
      'ACP_QUEUE_SUB',
      null,
      null
    )
  );

  dbms_aq.register(
    reg_list => sys.aq$_reg_info_list(
      sys.aq$_reg_info(
        name      => 'ACP_QUEUE:ACP_QUEUE_SUB',
        namespace => dbms_aq.namespace_aq,
        callback  => 'plsql://ACP_CHAT.ON_ACP_QUEUE_NOTIFICATION?PR=0',
        context   => null
      )
    ),
    reg_count => 1
  );
end;
/
