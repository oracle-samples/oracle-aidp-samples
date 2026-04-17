create index acp_conv_active_ix on
    acp_conversations (
        user_name,
        deleted_on,
        last_activity_on
    );

