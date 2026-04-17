create table acp_messages (
    message_id            number default on null to_number(sys_guid(), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') not null enable,
    conversation_id       number not null enable,
    request_payload       json not null enable,
    endpoint_url          varchar2(32767 byte) not null enable,
    response_status       varchar2(20 byte) default on null 'PENDING' not null enable,
    created_on            timestamp(6) with local time zone default on null current_timestamp not null enable,
    response_requested_on timestamp(6) with local time zone,
    response_started_on   timestamp(6) with local time zone,
    response_completed_on timestamp(6) with local time zone,
    response_attempts     number default on null 0 not null enable,
    response_error        varchar2(4000 byte),
    prompt_id             varchar2(128 byte),
    message_context       json,
    response_payload      json
);

alter table acp_messages add primary key ( message_id )
    using index enable;

