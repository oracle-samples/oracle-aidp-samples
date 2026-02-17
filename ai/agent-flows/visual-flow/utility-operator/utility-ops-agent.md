Strom Prep & Work Prioritization Assistant for Utilities Grid Operations

## Synopsis

The Storm Prep & Work Prioritization Assistant is an internal
conversational agent designed for electric utility grid operations teams
to proactively manage risk and readiness ahead of severe weather and
major operational events. Rather than handling customer inquiries, the
agent supports operational decision-making by combining institutional
knowledge---such as outage restoration priorities, field safety
procedures, DER backfeed controls, and vegetation risk guidelines---with
real-time operational data on assets, inspections, and work orders.
Through natural language conversation, operations managers can quickly
understand policy guardrails, clarify safety requirements, and retrieve
targeted insights about which feeders, transformers, or circuits present
the highest reliability or public safety risk.

By integrating a RAG tool for authoritative operational playbooks and a
SQL tool for querying live asset health data, the agent transforms
fragmented operational information into clear, defensible action plans.
It can surface high-risk vegetation conditions, unresolved equipment
defects, and open high-priority work orders, then rank them based on
criticality, exposure, and restoration impact. The result is a
storm-readiness agent that not only answers "what's happening on the
grid," but also explains "why this should be addressed first," helping
utilities deploy limited crews more effectively, improve safety
outcomes, and reduce outage duration and regulatory risk.

## Service Features Tested

* Knowledge base
* RAG tool
* SQL tool
* Agent flow no-code canvas
* Playground UI.

## Lab Data

There are documents in the `data/Knowledge_Base` folder containing
regulatory reporting requirements, outage triage playbook, vegetation
risk scoring guidelines, field safety policies, etc.

There are also three database tables in `data/Database_Tables`. You will have 
to ingest the tables into an Oracle vector 26ai database. 

## Step-by-step Instructions

## Part 1: Data Setup

### Create a Standard Catalog to store AI Assets

I recommend that you create a standard catalog to store all your AI
related artifacts. For this use case, you will create volumes and upload
files to them. In addition, you will create a knowledge base in the
catalog.

1. Click on "Master Catalog"

2. Click on the button "Create catalog" in the upper right corner.
Alternatively, you can click on the "+" button to add a catalog.

3. Give your catalog a name and a description. Suggestions:

    Name: utility

    Description: Contains assets related to the utility assistant.

4. Select a "standard catalog" under catalog type. Leave the compartment field blank.

_Creating a catalog takes a few seconds to complete._

### Create a Volume

After the catalog is available, the next step is to create a volume that
will contain various internal playbooks, regulatory and reporting
requirements documents.

1. Go to your newly created standard catalog.

2. Click on Volumes

3. Click the "+" button to add a new volume.

4. Select the "Managed" volume option. Give your volume a name and a
description. Suggestions:

    Name: ops_utilities

    Description: This volume stores various documents including regulatory
reporting requirements, outage triage playbook, vegetation risk scoring
guidelines, field safety policies, etc

5. Upload the documents to the volume

6. Next step is to upload the content of the `Data/Knowledge_Base` folder
to the Volume you just created.

7. Click on the volume you just created.

8. Click on the "default" schema, followed by "volumes"

9. Click on the "+" button.

10. Select "Upload file"

11. Drag and drop the files from the `Data/Knowledge_Base`folder.

12. Click "Upload".

### Create knowledge base

1. In the same or different schema, go to Knowledge Bases, and create a new
knowledge base. Leave the advanced settings untouched for now.

2. Create a new Data Source for the knowledge base. Select the volume where
you stored the policy txt files. Leave the advanced settings untouched.

3. Check the status of the ingestion job in the History tab. Should take
less than 1 min to complete.


### Ingest the SQL data into an Oracle AI Database 

1) Ingest the files stored in the `data/Database_Tables` folder into an Oracle AI Database. The table names should match the name of each csv file. We recommend creating a new schema (e.g. `utility`) for the purpose of this example. The tables will be used later with a SQL tool. 


## Part II: Agent Flow Setup

### Create the Agent

1. Go to Agent flows and create a new agent flow.

2. Drag and drop the agent node on the canvas

3. Give your agent a name and a description

4. Select us-ashburn-1 and xai.grok-4-fast-reasoning

5. Put those agent instructions:

    You are an internal operations assistant for an electric utility's
    distribution organization. Your job is to help grid operations and asset
    health teams prepare for storms and prioritize field work (vegetation,
    equipment repair, switching readiness) by combining:

    -   Policy and safety guidance from the RAG knowledge base, and

    -   Operational facts from the SQL database tables (assets, inspections,
    work_orders).

    You are not a customer support agent. Assume the user is an internal
utility employee.

    Core behaviors:

    1)  Be safety-first

    -   When questions involve energization, switching, hazards, LOTO, or
    DER/backfeed risk, always surface relevant safety guardrails from
    the KB.

    -   If the user asks for steps that could be unsafe (e.g., "just
    energize it"), remind them that final authority follows approved
    switching orders and safety procedures, and cite the KB guidance.

    -   Use RAG for "how/why" and definitions

    2)  Use the RAG tool to answer questions about:

    -   Restoration prioritization logic

    -   LOTO / re-energization checks

    -   DER backfeed considerations

    -   Vegetation risk scoring and action thresholds

    -   Reliability reporting concepts (SAIDI/SAIFI, cause codes, reporting
    fields)

    -   Quote/paraphrase briefly and keep it actionable.

    3)  Use SQL tools for "what/which/how many"

    -   Use the SQL tool when the user asks for lists, counts, rankings,
    status, or specifics about assets/inspections/work orders.

    -   Prefer simple, explainable queries. Tell the user what you're
    querying (tables + intent) before returning results.

    -   Join tables only when needed, using asset_id as the key.

    4)  Synthesize into an operational recommendation

    -   When the user asks "what should we do" or "what's the priority,"
    produce:

    -   A ranked shortlist (top items first),

    -   The rationale (clearance, exposure, criticality, defect history,
    open P1/P2 work),

    -   The recommended action (schedule trim/repair/inspection,
    escalation),

    -   Any safety/regulatory reminders relevant to the recommendation.

    5)  Ask only the minimum clarifying questions

    -   If needed, ask at most 1--2 targeted questions to proceed (e.g.,
    region, time window, event type, constraints).

    -   If the user doesn't specify, make reasonable defaults:

    -   Region: all regions

    -   Time window for "recent": last 90 days

    -   Priority focus: High criticality + low vegetation clearance + open
    P1/P2 work

    6)  Operating constraints and guardrails

    -   You provide decision support, not field authority. Do not claim you
    are dispatching crews or issuing switching orders.

    -   Do not invent asset records, inspection results, or work orders. If
    data is missing, say so and propose the next best query or
    assumption.

    -   Do not provide instructions that bypass safety steps. When safety
    procedures apply, explicitly reference the KB (LOTO, DER checks,
    re-energization checklist).

    -   Keep outputs concise and operational (bullet lists, tables if
    appropriate). Avoid long essays.

6)  Leave the model parameters and safety guardrails untouched.

### Create and Attach an AI Compute

Next step is to create an AI Compute and attach it to your agent flow.
You'll be able to start testing your agetn flow after the compute is
attached.

1) In your workspace, click on Compute.

2) In the Compute window, click on the AI Compute tab.

3) Click on the "+" button or the Create button in the top right cornder

4) Give your compute a name and description. I called mine utility_agent. 1
OCPU and 16 GB of memory is enough.

5) Now go back to your agent flow and click on the compute button in the
top right corner, select Attach to AI compute and pick the AI compute
you created in the previous step.

6) You are now ready to create and test each individual tool and the agent
flow.

### Create the RAG tool

Let's first create the RAG tool connected to the knowledge base we
previously created.

1) Drag and drop the RAG node on the canvas.

2) Give your RAG tool a descriptive name and a thorough description to
provide proper context to your agent. For example:

    Name: ops_policies_and_playbook

    Description: This tool retrieves policy documents through natural
language queries using retrieval augmented generation (RAG) techniques.
The policy documents include triage playbooks, field safety playbook
(LOTO) and DER considerations, vegetation management and risk scoring
guidelines, and regulatory reporting reference guides.

3) Select the knowlege base you created. Limit the number of documents to
retrieve to 5. Leave the query box untouched.

### Test the RAG tool

You can test the RAG tool independently.

1)  Test the tool using the "Test" tab of the tool. Example questions
    can include:

    _How is vegetation risk scored, and what actions correspond to each score
range?_

    (answer is in the vegetation management: risk scoring guidelines)

    _What's the priority rule for triaging restoration work?_

    (answer is in the outage triage playbook)

    _What's an ETR?_

    (answer is in the outage triage playbook)

    For each of the query above, a Test result is generated. A summary is
provided along with a list of relevant text chunks that were retrieved
from the knowledge base. For each chunk you can see a similarity score
along with a reference to the document where that chunk is found.

### Create the SQL tool

We are now ready to add a SQL tool.

1) Drag and drop the SQl node on the canvas

2) Give your SQL tool node a descriptive name and a thorough description.
For example:

    Name: pull_operational_facts

    Description: This query pulls operational facts about each asset,
including their type, their last inspection date, the vegetation
clearance distance, and details on any pending work orders on that
asset.

3) Select the catalog + schema were the database tables are located. In my
case, it is under vectordb23ai.aidp_scenario_4.

4) Copy and paste the following SQL query in the Query text box:

```sql
SELECT
a.asset_id,
a.asset_type,
a.region,
a.criticality,
i.inspection_date,
i.veg_clearance_ft,
i.weather_exposure,
w.work_order_id,
w.status,
w.priority,
w.work_type
FROM assets a
JOIN inspections i ON a.asset_id = i.asset_id
LEFT JOIN work_orders w ON a.asset_id = w.asset_id AND w.status IN
('Open','Scheduled','In Progress')
WHERE a.region = {{region}}
AND a.criticality IN ({{criticality}})
AND i.veg_clearance_ft < {{clearance_distance_in_feet}}
AND a.asset_type IN ({{asset_type}})
ORDER BY a.criticality DESC, i.veg_clearance_ft ASC;
```

5)  The value of each variable labeled as {{variableName}} is provided
    at runtime by the agent. The next step is to edit the metadata of
    each variable. Each variable should be automatically displayed in
    the AI Tool Definition window after you paste the query in the query
    box. I included below a recommendation for each variable, including
    the type, default value, and description.
```
    {{region}}
        Type: String
        Default Value: (empty)
        Description: This is the region where the assets are located. Possible values are North, South, Central. Ask the user if unsure of the region.

    {{criticality}}
        Type: String
        Default Value: (empty)
        Description: The criticality level of the assets. Possible values are High, Medium, Low. Ask the user if unsure what should be the criticality level.

    {{clearance_distance_in_feet}}
        Type: Float
        Default Value: 4.0
        Description: This is the vegetation clearance distance in feet. Provide a distance value in feet. The default is 4.0 feet. Ask user if unsure of the distance.

    {{asset_type}}
        Type: String
        Default Value: Transformer
        Description: This is the type of asset of interest. There are multiple possible values for this field, including: Transformer CapacitorBank, Feeder, Recloser, and Switch. If unsure, ask the user what asset type they want to look at.
```

### Test the SQL tool

You are now ready to test the SQL tool.

1) Click on the Test tab of the tool. In the "Test parameters" section on
the left, you should see a form with the four parameters: region,
criticality, clearance_distance_in_fee, and asset_type.

2) If default values were assigned, they should be displayed in the text
box already.

3_ Change the values, click Submit, and inspect the results in the Test
results window. I tried the following combination:

```
    Region: North
    Criticality: High
    Clearance distance: 4.0 (I used the default)
    Asset_type: Transformer
```

Two rows for asset ID A0001 should be returned with work orders WO00009
and WO000006.

## Testing E2E Agent Flow

You are now ready to test your agent flow in our testing playground.

1) Click on the `Playground` tab that is next to the `Develop` tab.

2) Create a new session by clicking on the "+" button at the top of the agent flow graph diagram. 

3) Start chatting with the agent flow.

    Here's a few examples of user messages simulating a conversation between
    the end user and the agent. All messages are from the end user.

    _Summarize what I should prioritize for my highly critical assets in the south_

    _Could you look at Transformer only_

    _ok can you expand this to 10 feet instead of 4?_

    _How about what's in the north and central?_

    _yeah let's look at the all the other asset types but this time just look
at the South._

    _ok how about Medium criticality assets in the same region?_

    _Can you give the top 5 most important work orders I should prioritize in
the south for my high criticality assets? I want to follow up on them
with the dispatchers_

    _ok yeah let's include medium criticality assets_

    _thanks! Can you return the above as a table with columns beting work
order ID, Asset ID, Status, Priority, Criticality, and Recommended
Follow-Up ?_

    Here's the table I got from the agent at the end: I can immediately
    decide what the next steps are. The agent flow also provided recommended
    follow up instructions.


| Work Order ID | Asset ID | Status | Priority | Criticality | Recommended Follow-Up 
|---|---|---|---|---|---|
|WO00004 | A0017 |Scheduled | P1 | Medium | Escalate to dispatch for immediate scheduling (within 24 hours); bundle with veg trim if needed. Track progress daily. |
| WO00020 | A0021 | Scheduled | P2 | High | Coordinate with dispatch for crew assignment this week; confirm if storm prep accelerates to P1. Review for integration with other switching orders. |
| WO00025 | A0011 | In Progress | P3 | Medium | Push dispatch to complete within 7 days; monitor for upgrades if veg worsens. If stalled >14 days, reassign resources. | 

## Recap 

You uploaded files to a managed volume, created a knowledge based that you later connected to an agent flow via a RAG tool. You also developed A SQL too and connected it to the agent flow. 