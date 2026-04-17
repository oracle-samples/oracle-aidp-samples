create index acp_conv_summary_stat_ix on
    acp_conversations (
        summary_status,
        summary_requested_on
    );

