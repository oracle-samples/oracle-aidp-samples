create index acp_msg_resp_stat_ix on
    acp_messages (
        response_status,
        response_requested_on
    );

