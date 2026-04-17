alter table acp_messages
    add constraint acp_msg_conversation_id_fk
        foreign key ( conversation_id )
            references acp_conversations ( conversation_id )
        enable;

