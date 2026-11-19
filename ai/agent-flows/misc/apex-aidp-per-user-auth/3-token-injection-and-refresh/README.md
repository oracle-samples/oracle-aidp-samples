# 3-token-injection-and-refresh (injection + refresh phase)

Two small pieces that make the per-user token actually usable end to end:

1. **Injection** - put the user's token into each agent `/chat` call so the agent's OAC MCP
   tool receives it (as a session variable). Without this, the token is minted but never reaches
   the agent.

   > **The token is not the call's auth.** The call to the AIDP agent is authenticated by **OCI
   > request signing as a system user** (a DBMS_CLOUD OCI credential) - the agent expects OCI
   > signing, not an OAuth Bearer token. The per-user OAC token rides **inside the request body**
   > as a session variable and is used by the agent for its downstream OAC access.
2. **Auto-refresh** - keep the token fresh on a timer, so the chat doesn't break when a token
   expires (~15-20 min). This removes the manual "refresh by hand all day" problem.

This phase is **independent of the minting approach** in `../2-per-user-oac-token/` - it works
the same whether the token was minted in-DB or via a downstream exchange. It reads the token
from a store and injects/refreshes it.

## Files

| File | What it does |
|------|--------------|
| [inject-token-into-chat-call.sql](inject-token-into-chat-call.sql) | The snippet added to the chat plugin's request builder that injects the token as `metadata.sessionvariables.cred.mcp.<tool>.bearer`. |
| [refresh-and-scheduler.sql](refresh-and-scheduler.sql) | A `DBMS_SCHEDULER` job that calls the refresh routine on a timer to keep the cached token valid. |

## Key detail (the one that bites people)

The session-variable key **must match the agent's MCP tool display name**:

```
metadata.sessionvariables.cred.mcp.<TOOL_DISPLAY_NAME>.bearer
```

If the key doesn't match the tool name, the agent silently never receives the token and every
answer falls back (or fails) - with no obvious error.
