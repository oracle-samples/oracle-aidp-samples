begin
  dbms_scheduler.create_program(
    program_name        => '"ACP_QUEUE_WORKER_PGM"',
    program_type        => 'STORED_PROCEDURE',
    program_action      => 'ACP_CHAT.PROCESS_ACP_QUEUE',
    number_of_arguments => 1,
    comments            => 'Program used by ACP queue workers.'
  );

  dbms_scheduler.define_program_argument(
    program_name      => '"ACP_QUEUE_WORKER_PGM"',
    argument_position => 1,
    argument_name     => 'P_MAX_MESSAGES',
    argument_type     => 'NUMBER'
  );

  dbms_scheduler.enable('"ACP_QUEUE_WORKER_PGM"');
end;
/
