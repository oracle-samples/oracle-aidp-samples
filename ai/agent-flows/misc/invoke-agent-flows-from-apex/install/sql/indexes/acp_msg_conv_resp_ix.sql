create index acp_msg_conv_resp_ix on
    acp_messages (
        conversation_id,
        response_status,
        response_requested_on,
        message_id
    );

