create or replace package acp_chat as
  --------------------------------------------------------------------------------
    procedure render_region (
        p_plugin in apex_plugin.t_plugin,
        p_region in apex_plugin.t_region,
        p_param  in apex_plugin.t_region_render_param,
        p_result in out nocopy apex_plugin.t_region_render_result
    );
  --------------------------------------------------------------------------------
    procedure handle_ajax (
        p_plugin in apex_plugin.t_plugin,
        p_region in apex_plugin.t_region,
        p_param  in apex_plugin.t_region_ajax_param,
        p_result in out nocopy apex_plugin.t_region_ajax_result
    );
  --------------------------------------------------------------------------------
    procedure process_acp_queue (
        p_max_messages in pls_integer default 5
    );
  --------------------------------------------------------------------------------
    procedure on_acp_queue_notification (
        context  raw,
        reginfo  sys.aq$_reg_info,
        descr    sys.aq$_descriptor,
        payload  raw,
        payloadl number
    );
  --------------------------------------------------------------------------------
end acp_chat;
/

