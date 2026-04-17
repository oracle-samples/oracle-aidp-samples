create table acp_conversations (
    conversation_id      number default on null to_number(sys_guid(), 'XXXXXXXXXXXXXXXXXXXXXXXXXXXXXXXX') not null enable,
    session_key          varchar2(128 byte) not null enable,
    user_name            varchar2(128 byte) not null enable,
    summary              varchar2(50 byte),
    summary_status       varchar2(20 byte) default on null 'NOT_REQUESTED' not null enable,
    summary_mode         varchar2(20 byte),
    summary_requested_on timestamp(6) with local time zone,
    summary_started_on   timestamp(6) with local time zone,
    summary_completed_on timestamp(6) with local time zone,
    summary_attempts     number default on null 0 not null enable,
    summary_error        varchar2(4000 byte),
    created_on           timestamp(6) with local time zone default on null localtimestamp not null enable,
    last_activity_on     timestamp(6) with local time zone default on null localtimestamp not null enable,
    deleted_on           timestamp(6) with local time zone,
    deleted_by           varchar2(128 byte)
);

alter table acp_conversations add constraint acp_conv_session_key_unq unique ( session_key )
    using index enable;

alter table acp_conversations add primary key ( conversation_id )
    using index enable;

