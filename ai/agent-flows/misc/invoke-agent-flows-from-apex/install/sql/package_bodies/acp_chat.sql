create or replace package body acp_chat as
--------------------------------------------------------------------------------
    c_queue_name            constant varchar2(128) := 'ACP_QUEUE';
    c_response_pending      constant varchar2(20) := 'PENDING';
    c_response_in_progress  constant varchar2(20) := 'IN_PROGRESS';
    c_response_completed    constant varchar2(20) := 'COMPLETED';
    c_response_failed       constant varchar2(20) := 'FAILED';
    c_summary_not_requested constant varchar2(20) := 'NOT_REQUESTED';
    c_summary_pending       constant varchar2(20) := 'PENDING';
    c_summary_in_progress   constant varchar2(20) := 'IN_PROGRESS';
    c_summary_completed     constant varchar2(20) := 'COMPLETED';
    c_summary_failed        constant varchar2(20) := 'FAILED';
    c_job_type_prompt       constant varchar2(30) := 'getPromptResponse';
    c_job_type_summary      constant varchar2(30) := 'getConversationSummary';
    c_summary_mode_initial  constant varchar2(20) := 'INITIAL';
    c_summary_mode_full     constant varchar2(20) := 'FULL';
--------------------------------------------------------------------------------
    procedure resolve_owned_conversation (
        p_session_key     in acp_conversations.session_key%type,
        p_user_name       in acp_conversations.user_name%type,
        p_conversation_id out acp_conversations.conversation_id%type,
        p_session_key_out out acp_conversations.session_key%type
    );
--------------------------------------------------------------------------------
    function create_conversation (
        p_user_name in acp_conversations.user_name%type
    ) return acp_conversations.session_key%type;
--------------------------------------------------------------------------------
    function get_owned_session_key (
        p_conversation_id in acp_conversations.conversation_id%type,
        p_user_name       in acp_conversations.user_name%type
    ) return acp_conversations.session_key%type
        result_cache;
--------------------------------------------------------------------------------
    procedure enqueue_queue_message (
        p_job_type        in varchar2,
        p_message_id      in acp_messages.message_id%type default null,
        p_conversation_id in acp_conversations.conversation_id%type default null,
        p_session_key     in acp_conversations.session_key%type default null,
        p_user_name       in acp_conversations.user_name%type default null,
        p_summary_mode    in varchar2 default null
    );
--------------------------------------------------------------------------------
    procedure enqueue_next_pending_prompt (
        p_conversation_id   in acp_conversations.conversation_id%type,
        p_user_name         in acp_conversations.user_name%type,
        p_target_message_id in acp_messages.message_id%type default null
    );
--------------------------------------------------------------------------------
    procedure send_message (
        p_region          in apex_plugin.t_region,
        p_session_key     acp_conversations.session_key%type,
        p_user_name       acp_conversations.user_name%type,
        p_user_prompt     clob,
        p_message_id      out acp_messages.message_id%type,
        p_response_status out varchar2
    );
--------------------------------------------------------------------------------
    function evaluate_message_context (
        p_region      in apex_plugin.t_region,
        p_session_key in acp_conversations.session_key%type,
        p_user_prompt in clob
    ) return varchar2;
--------------------------------------------------------------------------------
    procedure rename_conversation (
        p_session_key acp_conversations.session_key%type,
        p_user_name   acp_conversations.user_name%type,
        p_summary     acp_conversations.summary%type
    );
--------------------------------------------------------------------------------
    procedure summarize_conversation (
        p_session_key      acp_conversations.session_key%type,
        p_user_name        acp_conversations.user_name%type,
        p_use_full_history boolean default false
    );
--------------------------------------------------------------------------------
    procedure process_queue_payload (
        p_payload_clob in clob
    );
--------------------------------------------------------------------------------
    procedure process_prompt_response_task (
        p_payload_json in json_object_t
    );
--------------------------------------------------------------------------------
    procedure process_summary_task (
        p_payload_json in json_object_t
    );
--------------------------------------------------------------------------------
    procedure resolve_owned_conversation (
        p_session_key     in acp_conversations.session_key%type,
        p_user_name       in acp_conversations.user_name%type,
        p_conversation_id out acp_conversations.conversation_id%type,
        p_session_key_out out acp_conversations.session_key%type
    ) is
    begin
        p_conversation_id := null;
        p_session_key_out := null;
        if p_session_key is null then
            return;
        end if;
        if p_user_name is null then
            raise_application_error(-20022, 'User name is required to resolve conversation ownership.');
        end if;
        begin
            select
                c.conversation_id,
                c.session_key
            into
                p_conversation_id,
                p_session_key_out
            from
                acp_conversations c
            where
                    c.session_key = p_session_key
                and c.user_name = p_user_name
                and c.deleted_on is null;

        exception
            when no_data_found then
                raise_application_error(-20021, 'Conversation not found.');
        end;

    end resolve_owned_conversation;
--------------------------------------------------------------------------------
    procedure render_region (
        p_plugin in apex_plugin.t_plugin,
        p_region in apex_plugin.t_region,
        p_param  in apex_plugin.t_region_render_param,
        p_result in out nocopy apex_plugin.t_region_render_result
    ) is

        l_session_key_item       varchar2(32767);
        l_items_to_submit_raw    varchar2(32767);
        l_items_to_submit_jquery varchar2(32767);
        l_session_key            acp_conversations.session_key%type;
        l_history_position       varchar2(20);
        l_show_welcome_message   varchar2(1);
        l_welcome_message_text   varchar2(32767);
        l_rendered_rows          pls_integer := 0;
    begin
        l_session_key_item := trim(p_region.attributes.get_varchar2(p_static_id => 'session_key_item'));
        l_items_to_submit_raw := trim(p_region.attributes.get_varchar2(p_static_id => 'items_to_submit'));
        if l_items_to_submit_raw is not null then
            l_items_to_submit_jquery := apex_plugin_util.page_item_names_to_jquery(l_items_to_submit_raw);
        end if;

        if l_session_key_item is not null then
            l_session_key := trim(apex_util.get_session_state(p_item => l_session_key_item));
        end if;

        l_history_position := upper(nvl(
            trim(p_region.attributes.get_varchar2(p_static_id => 'history_position')),
            'MAIN_NAV'
        ));

        if l_history_position not in ( 'MAIN_NAV', 'PAGE_SIDE' ) then
            l_history_position := 'MAIN_NAV';
        end if;
        l_show_welcome_message := upper(nvl(
            trim(p_region.attributes.get_varchar2(p_static_id => 'show_welcome_message')),
            'Y'
        ));

        if l_show_welcome_message not in ( 'Y', 'N' ) then
            l_show_welcome_message := 'Y';
        end if;
        l_welcome_message_text := trim(p_region.attributes.get_varchar2(p_static_id => 'welcome_message_text'));
        if l_welcome_message_text is null then
            l_welcome_message_text := 'Hello! How can I help today?';
        end if;
        l_welcome_message_text := apex_plugin_util.replace_substitutions(p_value => l_welcome_message_text);
        sys.htp.p('<div class="acp-chat" id="'
                  || apex_escape.html_attribute(p_region.static_id) || '_chat">');

        sys.htp.p('  <div class="acp-chat__messages" aria-live="polite" aria-label="Conversation messages">');
        if l_session_key is not null then
            for rec in (
                select
                    json_value(m.request_payload, '$.input[0].content[0].text' returning varchar2(32767)) as prompt_text,
                    case
                        when m.response_payload is not null then
                            json_value(m.response_payload, '$.output[0].content[0].text' returning varchar2(32767))
                        when nvl(m.response_status, c_response_pending) in ( c_response_pending, c_response_in_progress ) then
                            'Thinking...'
                        when nvl(m.response_status, c_response_pending) = c_response_failed then
                            'Failed to get a response.'
                        else
                            null
                    end                                                                                   as response_text
                from
                         acp_conversations c
                    join acp_messages m on m.conversation_id = c.conversation_id
                where
                        c.session_key = l_session_key
                    and c.user_name = sys_context('APEX$SESSION', 'APP_USER')
                    and c.deleted_on is null
                order by
                    m.response_requested_on nulls last,
                    m.message_id
            ) loop
                l_rendered_rows := l_rendered_rows + 1;
                if rec.prompt_text is not null then
                    sys.htp.p('    <div class="acp-chat__message acp-chat__message--user">');
                    sys.htp.p('      <div class="acp-chat__bubble">'
                              || apex_escape.html(rec.prompt_text) || '</div>');

                    sys.htp.p('    </div>');
                end if;

                if rec.response_text is not null then
                    sys.htp.p('    <div class="acp-chat__message acp-chat__message--assistant">');
                    sys.htp.p('      <div class="acp-chat__bubble">'
                              || apex_escape.html(rec.response_text) || '</div>');

                    sys.htp.p('    </div>');
                end if;

            end loop;

            if l_rendered_rows = 0 then
                sys.htp.p('    <div class="acp-chat__messagesEmpty">No conversation history yet.</div>');
            end if;
        else
            sys.htp.p('    <div class="acp-chat__messagesEmpty">No conversation history yet.</div>');
        end if;

        sys.htp.p('  </div>');
        sys.htp.p('</div>');
        apex_javascript.add_onload_code(p_code => 'window.acpAgentChat && window.acpAgentChat.init('
                                                  || apex_javascript.add_value(p_region.static_id, true)
                                                  || apex_javascript.add_value(apex_plugin.get_ajax_identifier, true)
                                                  || apex_javascript.add_value(l_session_key_item, true)
                                                  || apex_javascript.add_value(l_history_position, true)
                                                  || apex_javascript.add_value(l_items_to_submit_jquery, true)
                                                  || apex_javascript.add_value(l_show_welcome_message, true)
                                                  || apex_javascript.add_value(l_welcome_message_text, false)
                                                  || ');');

    end render_region;
--------------------------------------------------------------------------------
    procedure handle_ajax (
        p_plugin in apex_plugin.t_plugin,
        p_region in apex_plugin.t_region,
        p_param  in apex_plugin.t_region_ajax_param,
        p_result in out nocopy apex_plugin.t_region_ajax_result
    ) is

        l_x01_action_raw         varchar2(32767) := trim(apex_application.g_x01);
        l_x02_target_raw         varchar2(32767) := trim(apex_application.g_x02);
        l_x03_value_raw          varchar2(32767) := trim(apex_application.g_x03);
        l_action                 varchar2(100) := lower(nvl(l_x01_action_raw, 'send'));
        l_session_key            acp_conversations.session_key%type := l_x02_target_raw;
        l_message_id_text        varchar2(32767) := l_x02_target_raw;
        l_new_summary            varchar2(32767) := l_x03_value_raw;
        l_app_user               acp_conversations.user_name%type := sys_context('APEX$SESSION', 'APP_USER');
        l_message                clob := apex_application.g_clob_01;
        l_response_text          varchar2(32767);
        l_response_status        varchar2(20);
        l_summary_status         varchar2(20);
        l_message_id             acp_messages.message_id%type;
        l_conversation_id        acp_conversations.conversation_id%type;
        l_poll_message_id        number;
        l_error_text             varchar2(4000);
        l_summary                acp_conversations.summary%type;
        l_deleted_count          pls_integer := 0;
        l_renamed                boolean := false;
        l_generated              boolean := false;
        l_session_key_item       varchar2(32767) := trim(p_region.attributes.get_varchar2(p_static_id => 'session_key_item'));
        l_session_key_item_value varchar2(32767);

        procedure assert_session_key_required (
            p_action_name in varchar2
        ) is
        begin
            if l_session_key is null then
                raise_application_error(-20023, 'Session key is required for action: ' || p_action_name);
            end if;
        end assert_session_key_required;

    begin
        case l_action
            when 'send' then
                if l_message is null
                   or dbms_lob.getlength(l_message) = 0 then
                    raise_application_error(-20001, 'Message is required.');
                end if;

                if l_session_key is not null then
                    resolve_owned_conversation(
                        p_session_key     => l_session_key,
                        p_user_name       => l_app_user,
                        p_conversation_id => l_conversation_id,
                        p_session_key_out => l_session_key
                    );

                end if;

                if
                    l_x02_target_raw is not null
                    and l_conversation_id is null
                then
                    raise_application_error(-20021, 'Conversation not found.');
                end if;

                if l_session_key is null then
                    l_session_key := create_conversation(p_user_name => l_app_user);
                    if l_session_key_item is not null then
                        apex_util.set_session_state(
                            p_name  => l_session_key_item,
                            p_value => l_session_key
                        );
                    end if;

                end if;

                send_message(
                    p_region          => p_region,
                    p_session_key     => l_session_key,
                    p_user_name       => l_app_user,
                    p_user_prompt     => l_message,
                    p_message_id      => l_message_id,
                    p_response_status => l_response_status
                );

                begin
                    select
                        summary,
                        summary_status
                    into
                        l_summary,
                        l_summary_status
                    from
                        acp_conversations
                    where
                            session_key = l_session_key
                        and user_name = l_app_user
                        and deleted_on is null;

                exception
                    when no_data_found then
                        l_summary := null;
                        l_summary_status := c_summary_not_requested;
                end;

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('sessionKey', l_session_key);
                apex_json.write('messageId',
                                to_char(l_message_id));
                apex_json.write('responseStatus', l_response_status);
                apex_json.write('summary', l_summary);
                apex_json.write('summaryStatus', l_summary_status);
                apex_json.close_object;
            when 'delete_conversation' then
                assert_session_key_required('delete_conversation');
                resolve_owned_conversation(
                    p_session_key     => l_session_key,
                    p_user_name       => l_app_user,
                    p_conversation_id => l_conversation_id,
                    p_session_key_out => l_session_key
                );

                if l_conversation_id is null then
                    raise_application_error(-20021, 'Conversation not found.');
                end if;
                update acp_conversations c
                set
                    c.deleted_on = current_timestamp,
                    c.deleted_by = l_app_user
                where
                        c.conversation_id = l_conversation_id
                    and c.user_name = l_app_user
                    and c.deleted_on is null;

                l_deleted_count := sql%rowcount;
                if
                    l_deleted_count > 0
                    and l_session_key_item is not null
                then
                    l_session_key_item_value := trim(apex_util.get_session_state(p_item => l_session_key_item));
                    if l_session_key_item_value = l_session_key then
                        apex_util.set_session_state(
                            p_name  => l_session_key_item,
                            p_value => null
                        );
                    end if;

                end if;

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('sessionKey', l_session_key);
                apex_json.write('deleted', l_deleted_count > 0);
                apex_json.close_object;
            when 'rename_conversation' then
                assert_session_key_required('rename_conversation');
                resolve_owned_conversation(
                    p_session_key     => l_session_key,
                    p_user_name       => l_app_user,
                    p_conversation_id => l_conversation_id,
                    p_session_key_out => l_session_key
                );

                if l_conversation_id is null then
                    raise_application_error(-20021, 'Conversation not found.');
                end if;
                rename_conversation(
                    p_session_key => l_session_key,
                    p_user_name   => l_app_user,
                    p_summary     => substr(l_new_summary, 1, 50)
                );

                begin
                    select
                        c.summary
                    into l_summary
                    from
                        acp_conversations c
                    where
                            c.session_key = l_session_key
                        and c.user_name = l_app_user
                        and c.deleted_on is null;

                exception
                    when no_data_found then
                        l_summary := null;
                end;

                l_renamed :=
                    l_new_summary is not null
                    and trim(l_new_summary) is not null
                    and l_summary = substr(
                        trim(l_new_summary),
                        1,
                        50
                    );

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('sessionKey', l_session_key);
                apex_json.write('renamed', l_renamed);
                apex_json.write('summary', l_summary);
                apex_json.close_object;
            when 'generate_summary' then
                assert_session_key_required('generate_summary');
                resolve_owned_conversation(
                    p_session_key     => l_session_key,
                    p_user_name       => l_app_user,
                    p_conversation_id => l_conversation_id,
                    p_session_key_out => l_session_key
                );

                if l_conversation_id is null then
                    raise_application_error(-20021, 'Conversation not found.');
                end if;
                if l_conversation_id is not null then
                    update acp_conversations c
                    set
                        c.summary_status = c_summary_pending,
                        c.summary_mode = c_summary_mode_full,
                        c.summary_requested_on = current_timestamp,
                        c.summary_started_on = null,
                        c.summary_completed_on = null,
                        c.summary_error = null
                    where
                            c.conversation_id = l_conversation_id
                        and c.deleted_on is null;

                    enqueue_queue_message(
                        p_job_type        => c_job_type_summary,
                        p_conversation_id => l_conversation_id,
                        p_session_key     => l_session_key,
                        p_user_name       => l_app_user,
                        p_summary_mode    => c_summary_mode_full
                    );

                end if;

                begin
                    select
                        c.summary,
                        c.summary_status
                    into
                        l_summary,
                        l_summary_status
                    from
                        acp_conversations c
                    where
                            c.session_key = l_session_key
                        and c.user_name = l_app_user
                        and c.deleted_on is null;

                exception
                    when no_data_found then
                        l_summary := null;
                        l_summary_status := c_summary_not_requested;
                end;

                l_generated := l_summary is not null;
                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('sessionKey', l_session_key);
                apex_json.write('generated', l_generated);
                apex_json.write('summary', l_summary);
                apex_json.write('summaryStatus', l_summary_status);
                apex_json.close_object;
            when 'poll_message_status' then
                begin
                    l_poll_message_id := to_number ( l_message_id_text );
                exception
                    when others then
                        l_poll_message_id := null;
                end;

                begin
                    select
                        json_value(m.response_payload, '$.output[0].content[0].text' returning varchar2(32767)) as response_text,
                        m.response_status,
                        m.response_error
                    into
                        l_response_text,
                        l_response_status,
                        l_error_text
                    from
                             acp_messages m
                        join acp_conversations c on c.conversation_id = m.conversation_id
                    where
                            m.message_id = l_poll_message_id
                        and c.user_name = l_app_user
                        and c.deleted_on is null;

                exception
                    when no_data_found then
                        l_response_text := null;
                        l_response_status := c_response_pending;
                        l_error_text := null;
                end;

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('messageId', l_message_id_text);
                apex_json.write('responseStatus', l_response_status);
                apex_json.write('response', l_response_text);
                apex_json.write('error', l_error_text);
                apex_json.close_object;
            when 'poll_summary_status' then
                assert_session_key_required('poll_summary_status');
                resolve_owned_conversation(
                    p_session_key     => l_session_key,
                    p_user_name       => l_app_user,
                    p_conversation_id => l_conversation_id,
                    p_session_key_out => l_session_key
                );

                if l_conversation_id is null then
                    raise_application_error(-20021, 'Conversation not found.');
                end if;
                begin
                    select
                        c.summary,
                        c.summary_status,
                        c.summary_error
                    into
                        l_summary,
                        l_summary_status,
                        l_error_text
                    from
                        acp_conversations c
                    where
                            c.conversation_id = l_conversation_id
                        and c.user_name = l_app_user
                        and c.deleted_on is null;

                exception
                    when no_data_found then
                        l_summary := null;
                        l_summary_status := c_summary_not_requested;
                        l_error_text := null;
                end;

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('sessionKey', l_session_key);
                apex_json.write('summaryStatus', l_summary_status);
                apex_json.write('summary', l_summary);
                apex_json.write('error', l_error_text);
                apex_json.close_object;
            when 'history_list' then
                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.open_array('conversations');
                for rec in (
                    select
                        c.session_key,
                        coalesce(
                            trim(c.summary),
                            'Conversation '
                            || to_char(c.created_on, 'YYYY-MM-DD HH24:MI')
                        ) as title,
                        c.created_on,
                        greatest(
                            nvl(
                                max(coalesce(m.response_completed_on, m.response_requested_on)),
                                c.created_on
                            ),
                            nvl(c.last_activity_on, c.created_on)
                        ) as updated_on
                    from
                        acp_conversations c
                        left join acp_messages      m on m.conversation_id = c.conversation_id
                    where
                            c.user_name = l_app_user
                        and c.deleted_on is null
                    group by
                        c.session_key,
                        c.summary,
                        c.created_on,
                        c.last_activity_on
                    order by
                        greatest(
                            nvl(
                                max(coalesce(m.response_completed_on, m.response_requested_on)),
                                c.created_on
                            ),
                            nvl(c.last_activity_on, c.created_on)
                        ) desc,
                        c.created_on desc
                ) loop
                    apex_json.open_object;
                    apex_json.write('sessionKey', rec.session_key);
                    apex_json.write('title', rec.title);
                    apex_json.write('createdOn',
                                    to_char(rec.created_on at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'));

                    apex_json.write('updatedOn',
                                    to_char(rec.updated_on at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'));

                    apex_json.close_object;
                end loop;

                apex_json.close_array;
                apex_json.close_object;
            when 'load_conversation' then
                assert_session_key_required('load_conversation');
                resolve_owned_conversation(
                    p_session_key     => l_session_key,
                    p_user_name       => l_app_user,
                    p_conversation_id => l_conversation_id,
                    p_session_key_out => l_session_key
                );

                if l_conversation_id is null then
                    raise_application_error(-20021, 'Conversation not found.');
                end if;
                if l_session_key_item is not null then
                    apex_util.set_session_state(
                        p_name  => l_session_key_item,
                        p_value => l_session_key
                    );
                end if;

                l_summary := null;
                if l_conversation_id is not null then
                    begin
                        select
                            c.summary,
                            c.summary_status
                        into
                            l_summary,
                            l_summary_status
                        from
                            acp_conversations c
                        where
                                c.conversation_id = l_conversation_id
                            and c.user_name = l_app_user
                            and c.deleted_on is null;

                    exception
                        when no_data_found then
                            l_summary := null;
                            l_summary_status := c_summary_not_requested;
                    end;
                end if;

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.write('sessionKey', l_session_key);
                apex_json.write('summary', l_summary);
                apex_json.write('summaryStatus', l_summary_status);
                apex_json.open_array('messages');
                if l_conversation_id is not null then
                    for rec in (
                        select
                            msg_role,
                            msg_text,
                            msg_time,
                            message_id,
                            msg_status
                        from
                            (
                                select
                                    m.message_id,
                                    m.response_requested_on                                                               as msg_time
                                    ,
                                    1                                                                                     as role_order
                                    ,
                                    'user'                                                                                as msg_role
                                    ,
                                    cast(null as varchar2(20))                                                            as msg_status
                                    ,
                                    json_value(m.request_payload, '$.input[0].content[0].text' returning varchar2(32767)) as msg_text
                                from
                                         acp_conversations c
                                    join acp_messages m on m.conversation_id = c.conversation_id
                                where
                                        c.conversation_id = l_conversation_id
                                    and c.user_name = l_app_user
                                    and c.deleted_on is null
                                union all
                                select
                                    m.message_id,
                                    coalesce(m.response_completed_on, m.response_requested_on) as msg_time,
                                    2                                                          as role_order,
                                    'assistant'                                                as msg_role,
                                    nvl(m.response_status, c_response_pending)                 as msg_status,
                                    case
                                        when m.response_payload is not null then
                                            json_value(m.response_payload, '$.output[0].content[0].text' returning varchar2(32767))
                                        when nvl(m.response_status, c_response_pending) in ( c_response_pending, c_response_in_progress
                                        ) then
                                            'Thinking...'
                                        when nvl(m.response_status, c_response_pending) = c_response_failed then
                                            'Failed to get a response.'
                                        else
                                            null
                                    end                                                        as msg_text
                                from
                                         acp_conversations c
                                    join acp_messages m on m.conversation_id = c.conversation_id
                                where
                                        c.conversation_id = l_conversation_id
                                    and c.user_name = l_app_user
                                    and c.deleted_on is null
                            )
                        where
                            msg_text is not null
                        order by
                            msg_time nulls last,
                            message_id,
                            role_order
                    ) loop
                        apex_json.open_object;
                        apex_json.write('role', rec.msg_role);
                        apex_json.write('text', rec.msg_text);
                        apex_json.write('messageId',
                                        to_char(rec.message_id));
                        apex_json.write('status', rec.msg_status);
                        apex_json.write('timestamp',
                                        to_char(rec.msg_time at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'));

                        apex_json.close_object;
                    end loop;
                end if;

                apex_json.close_array;
                apex_json.close_object;
            when 'clear_session_key' then
                if l_session_key_item is not null then
                    apex_util.set_session_state(
                        p_name  => l_session_key_item,
                        p_value => null
                    );
                end if;

                apex_json.open_object;
                apex_json.write('status', 'ok');
                apex_json.write('action', l_action);
                apex_json.write('handled', true);
                apex_json.close_object;
            else
                raise_application_error(-20000, 'Unsupported action: ' || l_action);
        end case;
    end handle_ajax;
--------------------------------------------------------------------------------
    function create_conversation (
        p_user_name in acp_conversations.user_name%type
    ) return acp_conversations.session_key%type is
        l_session_key acp_conversations.session_key%type;
    begin
        select
            raw_to_uuid(uuid())
        into l_session_key;

        insert into acp_conversations (
            session_key,
            user_name,
            created_on,
            last_activity_on
        ) values ( l_session_key,
                   p_user_name,
                   current_timestamp,
                   current_timestamp );

        return l_session_key;
    end create_conversation;
--------------------------------------------------------------------------------
    function get_owned_session_key (
        p_conversation_id in acp_conversations.conversation_id%type,
        p_user_name       in acp_conversations.user_name%type
    ) return acp_conversations.session_key%type
        result_cache
    is
        l_session_key acp_conversations.session_key%type;
    begin
        if p_conversation_id is null
           or p_user_name is null then
            raise_application_error(-20058, 'get_owned_session_key requires conversation_id and user_name.');
        end if;

        begin
            select
                c.session_key
            into l_session_key
            from
                acp_conversations c
            where
                    c.conversation_id = p_conversation_id
                and c.user_name = p_user_name
                and c.deleted_on is null;

        exception
            when no_data_found then
                return null;
        end;

        return l_session_key;
    end get_owned_session_key;
--------------------------------------------------------------------------------
    procedure enqueue_queue_message (
        p_job_type        in varchar2,
        p_message_id      in acp_messages.message_id%type default null,
        p_conversation_id in acp_conversations.conversation_id%type default null,
        p_session_key     in acp_conversations.session_key%type default null,
        p_user_name       in acp_conversations.user_name%type default null,
        p_summary_mode    in varchar2 default null
    ) is

        l_payload_json       json_object_t := json_object_t();
        l_enqueue_options    dbms_aq.enqueue_options_t;
        l_message_properties dbms_aq.message_properties_t;
        l_payload            json;
        l_msgid              raw(16);
    begin
        l_payload_json.put('jobType', p_job_type);
        if p_message_id is not null then
            l_payload_json.put('messageId', p_message_id);
        end if;
        if p_conversation_id is not null then
            l_payload_json.put('conversationId', p_conversation_id);
        end if;
        if p_session_key is not null then
            l_payload_json.put('sessionKey', p_session_key);
        end if;
        if p_user_name is not null then
            l_payload_json.put('userName', p_user_name);
        end if;
        if p_summary_mode is not null then
            l_payload_json.put('summaryMode',
                               upper(trim(p_summary_mode)));
        end if;

        l_payload_json.put('requestId',
                           rawtohex(sys_guid()));
        l_payload_json.put('requestedOn',
                           to_char(systimestamp at time zone 'UTC', 'YYYY-MM-DD"T"HH24:MI:SS.FF3"Z"'));
        l_payload :=
            json(
                l_payload_json.to_clob()
            );
        dbms_aq.enqueue(
            queue_name         => c_queue_name,
            enqueue_options    => l_enqueue_options,
            message_properties => l_message_properties,
            payload            => l_payload,
            msgid              => l_msgid
        );

    end enqueue_queue_message;
--------------------------------------------------------------------------------
    procedure enqueue_next_pending_prompt (
        p_conversation_id   in acp_conversations.conversation_id%type,
        p_user_name         in acp_conversations.user_name%type,
        p_target_message_id in acp_messages.message_id%type default null
    ) is
        l_session_key     acp_conversations.session_key%type;
        l_next_message_id acp_messages.message_id%type;
    begin
        if p_conversation_id is null
           or p_user_name is null then
            raise_application_error(-20057, 'enqueue_next_pending_prompt requires conversation_id and user_name.');
        end if;

        l_session_key := get_owned_session_key(
            p_conversation_id => p_conversation_id,
            p_user_name       => p_user_name
        );
        if l_session_key is null then
            return;
        end if;
        begin
            if p_target_message_id is not null then
                select
                    m.message_id
                into l_next_message_id
                from
                    acp_messages m
                where
                        m.message_id = p_target_message_id
                    and m.conversation_id = p_conversation_id
                    and m.response_status = c_response_pending
                    and not exists (
                        select
                            1
                        from
                            acp_messages m2
                        where
                                m2.conversation_id = m.conversation_id
                            and m2.response_status = c_response_in_progress
                    )
                    and not exists (
                        select
                            1
                        from
                            acp_messages m2
                        where
                                m2.conversation_id = m.conversation_id
                            and m2.response_status = c_response_pending
                            and m2.response_requested_on < m.response_requested_on
                    );

            else
                select
                    m.message_id
                into l_next_message_id
                from
                    acp_messages m
                where
                        m.conversation_id = p_conversation_id
                    and m.response_status = c_response_pending
                    and not exists (
                        select
                            1
                        from
                            acp_messages m2
                        where
                                m2.conversation_id = m.conversation_id
                            and m2.response_status = c_response_in_progress
                    )
                    and not exists (
                        select
                            1
                        from
                            acp_messages m2
                        where
                                m2.conversation_id = m.conversation_id
                            and m2.response_status = c_response_pending
                            and m2.response_requested_on < m.response_requested_on
                    )
                fetch first 1 row only;

            end if;
        exception
            when no_data_found then
                return;
        end;

        enqueue_queue_message(
            p_job_type        => c_job_type_prompt,
            p_message_id      => l_next_message_id,
            p_conversation_id => p_conversation_id,
            p_session_key     => l_session_key,
            p_user_name       => p_user_name
        );

    end enqueue_next_pending_prompt;
--------------------------------------------------------------------------------
    function evaluate_message_context (
        p_region      in apex_plugin.t_region,
        p_session_key in acp_conversations.session_key%type,
        p_user_prompt in clob
    ) return varchar2 is

        l_context_function_body varchar2(32767);
        l_plsql_block           clob;
        l_context_payload       varchar2(32767);
        l_sql_parameters        apex_exec.t_parameters;
        l_context_object_json   json_object_t;
        l_user_prompt_varchar2  varchar2(32767);
    begin
        l_context_function_body := trim(p_region.attributes.get_varchar2(p_static_id => 'message_context_function'));
        if l_context_function_body is null then
            return null;
        end if;
        l_user_prompt_varchar2 :=
            case
                when p_user_prompt is null then
                    null
                else
                    dbms_lob.substr(p_user_prompt, 32767, 1)
            end;

        l_plsql_block := 'declare '
                         || '  l_session_key varchar2(32767) := :SESSION_KEY; '
                         || '  l_user_prompt varchar2(32767) := :USER_PROMPT; '
                         || '  function f return varchar2 is begin '
                         || l_context_function_body
                         || ' end; '
                         || 'begin '
                         || '  :MESSAGE_CONTEXT_RESULT := f; '
                         || 'end;';

        apex_exec.add_parameter(
            p_parameters => l_sql_parameters,
            p_name       => 'SESSION_KEY',
            p_value      => p_session_key
        );

        apex_exec.add_parameter(
            p_parameters => l_sql_parameters,
            p_name       => 'USER_PROMPT',
            p_value      => l_user_prompt_varchar2
        );

        apex_exec.add_parameter(
            p_parameters => l_sql_parameters,
            p_name       => 'MESSAGE_CONTEXT_RESULT',
            p_value      => ''
        );

        apex_exec.execute_plsql(
            p_plsql_code      => l_plsql_block,
            p_auto_bind_items => false,
            p_sql_parameters  => l_sql_parameters
        );

        l_context_payload := apex_exec.get_parameter_varchar2(
            p_parameters => l_sql_parameters,
            p_name       => 'MESSAGE_CONTEXT_RESULT'
        );
        if l_context_payload is null then
            raise_application_error(-20013, 'message_context_function must return a JSON object payload.');
        end if;
        begin
            l_context_object_json := json_object_t.parse(l_context_payload);
        exception
            when others then
                raise_application_error(-20014, 'message_context_function returned invalid JSON object text.');
        end;

        return l_context_payload;
    end evaluate_message_context;
--------------------------------------------------------------------------------
    procedure send_message (
        p_region          in apex_plugin.t_region,
        p_session_key     acp_conversations.session_key%type,
        p_user_name       acp_conversations.user_name%type,
        p_user_prompt     clob,
        p_message_id      out acp_messages.message_id%type,
        p_response_status out varchar2
    ) is

        l_request_payload   clob;
        l_body_json         json_object_t := json_object_t();
        l_input_arr_json    json_array_t := json_array_t();
        l_input_obj_json    json_object_t := json_object_t();
        l_content_arr_json  json_array_t := json_array_t();
        l_content_obj_json  json_object_t := json_object_t();
    --
        l_conversation_id   acp_conversations.conversation_id%type;
        l_message_id        acp_messages.message_id%type;
        l_endpoint_url      varchar2(32767);
        l_endpoint_source   varchar2(30);
        l_endpoint_function varchar2(32767);
        l_message_context   varchar2(32767);
    begin
        p_message_id := null;
        p_response_status := c_response_pending;
        l_endpoint_source := upper(nvl(
            trim(p_region.attributes.get_varchar2(p_static_id => 'endpoint_url_source')),
            'STATIC'
        ));

        case l_endpoint_source
            when 'STATIC' then
                l_endpoint_url := trim(p_region.attributes.get_varchar2(
                    p_static_id        => 'endpoint_url',
                    p_do_substitutions => true
                ));
            when 'DYNAMIC' then
                l_endpoint_function := p_region.attributes.get_varchar2(p_static_id => 'endpoint_url_function');
                l_endpoint_url := trim(apex_plugin_util.get_plsql_function_result(p_plsql_function => l_endpoint_function));
            else
                raise_application_error(-20010, 'Invalid Endpoint URL Source: ' || l_endpoint_source);
        end case;

        if l_endpoint_url is null then
            raise_application_error(-20011, 'Endpoint URL is not configured.');
        end if;
        select
            conversation_id
        into l_conversation_id
        from
            acp_conversations
        where
                session_key = p_session_key
            and user_name = p_user_name
            and deleted_on is null;
    --
        l_content_obj_json.put('type', 'INPUT_TEXT');
        l_content_obj_json.put('text', p_user_prompt);
    --
        l_content_arr_json.append(l_content_obj_json);
    --
        l_input_obj_json.put('role', 'User');
        l_input_obj_json.put('content', l_content_arr_json);
        l_input_arr_json.append(l_input_obj_json);
    --
        l_body_json.put('isStreamEnabled', false);
        l_body_json.put('sessionKey', p_session_key);
        l_body_json.put('trace', false);
        l_body_json.put('input', l_input_arr_json);
        l_request_payload := l_body_json.to_clob();
        l_message_context := evaluate_message_context(
            p_region      => p_region,
            p_session_key => p_session_key,
            p_user_prompt => p_user_prompt
        );
    --
        insert into acp_messages (
            conversation_id,
            request_payload,
            message_context,
            endpoint_url,
            response_status,
            created_on,
            response_requested_on
        ) values ( l_conversation_id,
                   json(l_request_payload),
                   case
                       when l_message_context is null then
                           null
                       else
                           json(l_message_context)
                   end,
                   l_endpoint_url,
                   c_response_pending,
                   current_timestamp,
                   current_timestamp ) returning message_id into l_message_id;

        enqueue_next_pending_prompt(
            p_conversation_id   => l_conversation_id,
            p_user_name         => p_user_name,
            p_target_message_id => l_message_id
        );
        p_message_id := l_message_id;
        p_response_status := c_response_pending;
    end send_message;
--------------------------------------------------------------------------------
    procedure rename_conversation (
        p_session_key acp_conversations.session_key%type,
        p_user_name   acp_conversations.user_name%type,
        p_summary     acp_conversations.summary%type
    ) is
        l_summary acp_conversations.summary%type := substr(
            trim(p_summary),
            1,
            50
        );
    begin
        if l_summary is null then
            return;
        end if;
        update acp_conversations
        set
            summary = l_summary
        where
                session_key = p_session_key
            and user_name = p_user_name
            and deleted_on is null;

    end rename_conversation;
--------------------------------------------------------------------------------
    procedure summarize_conversation (
        p_session_key      acp_conversations.session_key%type,
        p_user_name        acp_conversations.user_name%type,
        p_use_full_history boolean default false
    ) is
        l_summary     acp_conversations.summary%type;
        l_user_prompt clob;
    begin
        if p_use_full_history then
            begin
                select
                    xmlserialize(content xmlagg(xmlelement(
                        e,
       rec.msg_role
       || ': '
       || rec.msg_text
       || chr(10)
                    )
                        order by
                            rec.msg_time nulls last,
                            rec.message_id,
                            rec.role_order
                    ) as clob)
                into l_user_prompt
                from
                    (
                        select
                            m.message_id,
                            m.response_requested_on                                                               as msg_time,
                            1                                                                                     as role_order,
                            'User'                                                                                as msg_role,
                            json_value(m.request_payload, '$.input[0].content[0].text' returning varchar2(32767)) as msg_text
                        from
                                 acp_conversations c
                            join acp_messages m on c.conversation_id = m.conversation_id
                        where
                                c.session_key = p_session_key
                            and c.user_name = p_user_name
                            and c.deleted_on is null
                        union all
                        select
                            m.message_id,
                            coalesce(m.response_completed_on, m.response_requested_on)                              as msg_time,
                            2                                                                                       as role_order,
                            'Assistant'                                                                             as msg_role,
                            json_value(m.response_payload, '$.output[0].content[0].text' returning varchar2(32767)) as msg_text
                        from
                                 acp_conversations c
                            join acp_messages m on c.conversation_id = m.conversation_id
                        where
                                c.session_key = p_session_key
                            and c.user_name = p_user_name
                            and c.deleted_on is null
                    ) rec
                where
                    rec.msg_text is not null;

            exception
                when no_data_found then
                    l_user_prompt := null;
            end;
        else
            begin
                select
                    json_value(m.request_payload, '$.input[0].content[0].text' returning clob)
                into l_user_prompt
                from
                    (
                        select
                            m.request_payload
                        from
                                 acp_conversations c
                            join acp_messages m on c.conversation_id = m.conversation_id
                        where
                                c.session_key = p_session_key
                            and c.user_name = p_user_name
                            and c.deleted_on is null
                        order by
                            m.response_requested_on desc nulls last,
                            m.message_id desc
                    ) m
                where
                    rownum = 1;

            exception
                when no_data_found then
                    l_user_prompt := null;
            end;
        end if;

        if l_user_prompt is null then
            raise_application_error(-20054, 'No prompt history available for summary.');
        end if;
        l_summary := dbms_cloud_ai.generate(
            prompt       => 'Create a summary of the following user prompt. '
                      || chr(10)
                      || 'Keep the summary to less than 40 characters. '
                      || chr(10)
                      || 'Don''t use third person phrasing, keep the summary simple. '
                      || chr(10)
                      || 'Add no punctuation, just return a phrase. '
                      || chr(10)
                      || 'Return the summary only, no other content or markup. '
                      || chr(10)
                      || 'Capitalize the first letter of the summary and make the rest lowercase,'
                      || chr(10)
                      || 'unless a word is an acronymm, proper noun, etc..'
                      || chr(10)
                      || chr(10)
                      || l_user_prompt,
            profile_name => 'ACP_CHAT_PROFILE',
            action       => 'chat'
        );

        l_summary := substr(
            trim(l_summary),
            1,
            50
        );
        if l_summary is null then
            raise_application_error(-20055, 'Summary generation returned no content.');
        end if;
    --
        update acp_conversations
        set
            summary = l_summary,
            summary_status = c_summary_completed,
            summary_completed_on = current_timestamp,
            summary_error = null,
            last_activity_on = current_timestamp
        where
                session_key = p_session_key
            and user_name = p_user_name
            and deleted_on is null;

    end summarize_conversation;
--------------------------------------------------------------------------------
    procedure process_prompt_response_task (
        p_payload_json in json_object_t
    ) is

        l_message_id         acp_messages.message_id%type;
        l_conversation_id    acp_conversations.conversation_id%type;
        l_user_name          acp_conversations.user_name%type;
        l_endpoint_url       acp_messages.endpoint_url%type;
        l_request_payload    clob;
        l_response           dbms_cloud_types.resp;
        l_response_payload   clob;
        l_response_body_json json_object_t;
        l_prompt_id          acp_messages.prompt_id%type;
        l_session_key        acp_conversations.session_key%type;
        l_summary            acp_conversations.summary%type;
        l_error_text         varchar2(4000);
    begin
        l_message_id := p_payload_json.get_number('messageId');
        l_user_name := p_payload_json.get_string('userName');
        if l_message_id is null
           or l_user_name is null then
            raise_application_error(-20050, 'Invalid prompt response queue payload.');
        end if;

        update acp_messages m
        set
            m.response_status = c_response_in_progress,
            m.response_started_on = current_timestamp,
            m.response_attempts = nvl(m.response_attempts, 0) + 1,
            m.response_error = null
        where
                m.message_id = l_message_id
            and exists (
                select
                    1
                from
                    acp_conversations c
                where
                        c.conversation_id = m.conversation_id
                    and c.user_name = l_user_name
                    and c.deleted_on is null
            )
            and m.response_status in ( c_response_pending, c_response_failed )
            and not exists (
                select
                    1
                from
                    acp_messages m2
                where
                        m2.conversation_id = m.conversation_id
                    and m2.message_id <> m.message_id
                    and m2.response_status = c_response_in_progress
            )
        returning m.conversation_id,
                  m.request_payload,
                  m.endpoint_url into l_conversation_id, l_request_payload, l_endpoint_url;

        if sql%rowcount = 0 then
            return;
        end if;
        l_session_key := get_owned_session_key(
            p_conversation_id => l_conversation_id,
            p_user_name       => l_user_name
        );
        if l_session_key is null then
            raise_application_error(-20059, 'Conversation ownership not found for prompt response processing.');
        end if;
        l_endpoint_url := trim(l_endpoint_url);
        if l_endpoint_url is null then
            raise_application_error(-20056, 'Message endpoint URL is missing for prompt response processing.');
        end if;
        l_response := dbms_cloud.send_request(
            credential_name => 'ACP_CHAT_CRED',
            uri             => l_endpoint_url,
            method          => dbms_cloud.method_post,
            body            => utl_raw.cast_to_raw(l_request_payload)
        );

        l_response_payload := dbms_cloud.get_response_text(l_response);
        l_response_body_json := json_object_t.parse(l_response_payload);
        l_prompt_id := l_response_body_json.get_string('id');
        update acp_messages m
        set
            m.prompt_id = l_prompt_id,
            m.response_payload =
                json(l_response_payload),
            m.response_status = c_response_completed,
            m.response_completed_on = current_timestamp,
            m.response_error = null
        where
            m.message_id = l_message_id;

        update acp_conversations c
        set
            c.last_activity_on = current_timestamp
        where
                c.conversation_id = l_conversation_id
            and c.user_name = l_user_name
            and c.deleted_on is null;

        begin
            select
                c.summary
            into l_summary
            from
                acp_conversations c
            where
                    c.conversation_id = l_conversation_id
                and c.user_name = l_user_name
                and c.deleted_on is null;

        exception
            when no_data_found then
                l_summary := null;
        end;

        if trim(l_summary) is null then
            update acp_conversations c
            set
                c.summary_status = c_summary_pending,
                c.summary_mode = c_summary_mode_initial,
                c.summary_requested_on = current_timestamp,
                c.summary_started_on = null,
                c.summary_completed_on = null,
                c.summary_error = null
            where
                    c.conversation_id = l_conversation_id
                and c.user_name = l_user_name
                and c.summary_status not in ( c_summary_pending, c_summary_in_progress );

            if sql%rowcount > 0 then
                enqueue_queue_message(
                    p_job_type        => c_job_type_summary,
                    p_conversation_id => l_conversation_id,
                    p_session_key     => l_session_key,
                    p_user_name       => l_user_name,
                    p_summary_mode    => c_summary_mode_initial
                );

            end if;

        end if;

        enqueue_next_pending_prompt(
            p_conversation_id => l_conversation_id,
            p_user_name       => l_user_name
        );
    exception
        when others then
            l_error_text := substr(sqlerrm, 1, 4000);
            update acp_messages m
            set
                m.response_status = c_response_failed,
                m.response_completed_on = current_timestamp,
                m.response_error = l_error_text
            where
                    m.message_id = l_message_id
                and ( l_user_name is null
                      or exists (
                    select
                        1
                    from
                        acp_conversations c
                    where
                            c.conversation_id = m.conversation_id
                        and c.user_name = l_user_name
                        and c.deleted_on is null
                ) );

            enqueue_next_pending_prompt(
                p_conversation_id => l_conversation_id,
                p_user_name       => l_user_name
            );
    end process_prompt_response_task;
--------------------------------------------------------------------------------
    procedure process_summary_task (
        p_payload_json in json_object_t
    ) is

        l_conversation_id acp_conversations.conversation_id%type;
        l_session_key     acp_conversations.session_key%type;
        l_user_name       acp_conversations.user_name%type;
        l_summary_mode    varchar2(20);
        l_error_text      varchar2(4000);
    begin
        l_conversation_id := p_payload_json.get_number('conversationId');
        l_user_name := p_payload_json.get_string('userName');
        l_summary_mode := upper(nvl(
            trim(p_payload_json.get_string('summaryMode')),
            c_summary_mode_initial
        ));

        if l_conversation_id is null
           or l_user_name is null then
            raise_application_error(-20052, 'Invalid summary queue payload.');
        end if;

        update acp_conversations c
        set
            c.summary_status = c_summary_in_progress,
            c.summary_mode = l_summary_mode,
            c.summary_started_on = current_timestamp,
            c.summary_attempts = nvl(c.summary_attempts, 0) + 1,
            c.summary_error = null
        where
                c.conversation_id = l_conversation_id
            and c.user_name = l_user_name
            and c.deleted_on is null
            and c.summary_status in ( c_summary_pending, c_summary_failed );

        if sql%rowcount = 0 then
            return;
        end if;
        l_session_key := get_owned_session_key(
            p_conversation_id => l_conversation_id,
            p_user_name       => l_user_name
        );
        if l_session_key is null then
            raise_application_error(-20060, 'Conversation ownership not found for summary processing.');
        end if;
        summarize_conversation(
            p_session_key      => l_session_key,
            p_user_name        => l_user_name,
            p_use_full_history => l_summary_mode = c_summary_mode_full
        );

    exception
        when others then
            l_error_text := substr(sqlerrm, 1, 4000);
            update acp_conversations c
            set
                c.summary_status = c_summary_failed,
                c.summary_completed_on = current_timestamp,
                c.summary_error = l_error_text
            where
                    c.conversation_id = l_conversation_id
                and ( l_user_name is null
                      or c.user_name = l_user_name )
                and c.deleted_on is null;

    end process_summary_task;
--------------------------------------------------------------------------------
    procedure process_queue_payload (
        p_payload_clob in clob
    ) is
        l_payload_json json_object_t;
        l_job_type     varchar2(30);
    begin
        l_payload_json := json_object_t.parse(p_payload_clob);
        l_job_type := l_payload_json.get_string('jobType');
        case l_job_type
            when c_job_type_prompt then
                process_prompt_response_task(l_payload_json);
            when c_job_type_summary then
                process_summary_task(l_payload_json);
            else
                raise_application_error(-20053, 'Unsupported queue job type: ' || l_job_type);
        end case;

    end process_queue_payload;
--------------------------------------------------------------------------------
    procedure process_acp_queue (
        p_max_messages in pls_integer default 5
    ) is

        l_dequeue_options    dbms_aq.dequeue_options_t;
        l_message_properties dbms_aq.message_properties_t;
        l_payload            json;
        l_payload_clob       clob;
        l_msgid              raw(16);
        l_processed          pls_integer := 0;
    begin
        l_dequeue_options.wait := dbms_aq.no_wait;
        l_dequeue_options.navigation := dbms_aq.first_message;
        l_dequeue_options.consumer_name := 'ACP_QUEUE_SUB';
        loop
            exit when l_processed >= nvl(p_max_messages, 5);
            begin
                dbms_aq.dequeue(
                    queue_name         => c_queue_name,
                    dequeue_options    => l_dequeue_options,
                    message_properties => l_message_properties,
                    payload            => l_payload,
                    msgid              => l_msgid
                );

            exception
                when others then
                    if sqlcode = -25228 then
                        exit;
                    end if;
                    raise;
            end;

            select
                json_serialize(l_payload returning clob)
            into l_payload_clob
            from
                dual;

            process_queue_payload(l_payload_clob);
            commit;
            l_processed := l_processed + 1;
            l_dequeue_options.navigation := dbms_aq.next_message;
        end loop;

    exception
        when others then
            rollback;
            raise;
    end process_acp_queue;
--------------------------------------------------------------------------------
    procedure on_acp_queue_notification (
        context  raw,
        reginfo  sys.aq$_reg_info,
        descr    sys.aq$_descriptor,
        payload  raw,
        payloadl number
    ) is
        l_job_name varchar2(30);
    begin
        l_job_name := 'ACP_QW_'
                      || to_char(systimestamp, 'YYMMDDHH24MISSFF3')
                      || '_'
                      || substr(
            rawtohex(sys_guid()),
            1,
            5
        );

        dbms_scheduler.create_job(
            job_name     => l_job_name,
            program_name => 'ACP_QUEUE_WORKER_PGM',
            enabled      => false,
            auto_drop    => true
        );

        dbms_scheduler.set_job_argument_value(
            job_name          => l_job_name,
            argument_position => 1,
            argument_value    => '1'
        );

        dbms_scheduler.enable(l_job_name);
    end on_acp_queue_notification;

end acp_chat;
/

