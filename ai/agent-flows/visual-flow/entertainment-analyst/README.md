# Release & Performance Analyst Agent for the Entertainment Industry

## Lab Synopsis
You are an internal analytics and decision-support agent for an entertainment studio or streaming platform.
Your primary objective is to help content strategy, release operations, marketing, and finance teams understand title performance, release effectiveness, and marketing ROI using a combination of:

* Retrieval-Augmented Generation (RAG) over internal release playbooks and policies
* Strictly defined SQL tools that execute parameterized, read-only queries

Your agent helps content strategy, release operations, marketing, and finance teams answer day-to-day questions like:
* “How did Neon Knights perform in US vs UK last weekend?”
* “Which regions show weak completion for Maple Street Mysteries S1 in the last 2 weeks?”
* “What’s the ROI trend for the launch campaign, and which channel is driving it?”

## Service Features Used 
* Knowledge base
* No code canvas 
* Agent Flow Playground 
* RAG tool 
* SQL tool 

# Step-by-step Instructions 

## Part I: Data Setup 

### Create a Standard Catalog to store AI assets 

I recommend that you create a standard catalog to store all your AI related artifacts. For this use case, you will create volumes and upload files to them. In addition, you will create a knowledge base in the catalog. 

1.	Click on “Master Catalog” 
2.	Click on the button “Create catalog” in the upper right corner. Alternatively, you can click on the “+” button to add a catalog. 
3.	Give your catalog a name and a description. Suggestions: 
```
    Name: entertainment_analyst
    Description: a catalog that stores the assets needed by the entertainment industry analyst agent. 
```
4.	Select a “standard catalog” under catalog type 
5.	Leave the compartment field blank.

_Creating a catalog takes a few seconds to complete._

### Create A Volume 

First thing we need to do is to create a volume that will store various release and marketing strategy documents and playbooks. 

1.	Go to your newly created standard catalog. Click on the default schema. 
2.	Click on Volumes
3.	Click the “+” button to add a new volume. 
4.	Select the “Managed” volume option. Give your volume a name and a description. Suggestions:
```
    Name: entertainment_analyst
    Description: this volume stores release playbooks, market prioritization, etc. 
```
### Upload the documents to the volume 

Next step is to upload the content of the `Data/Knowledge_Base` folder to the Volume you just created. 

1.	Click on the volume you just created. 
2.	Click on the “default” schema, followed by “volumes”
3.	Click on the “+” button. 
4.	Select “Upload file” 
5.	Drag and drop the files from the `Data/Knowledge_Base` folder. 
6.	Click “Upload”. 

### Create a Knowledge Base 

Now the last step we need before creating our agent is to create a knowledge base. The knowledge base will create vector representation of the document we just uploaded to the volume. We will connect this knowledge base to our agent such that the agent can retrieve semantically relevant passages from the documents in relation to the question/query provided by the end user. This contextually relevant data is used to augment the context of the LLM. Knowledge bases in conjunction with RAG tools connected to agents are powerful constructs to retrieve data from large, unstructured, and potentially sensitive or private corpora. 
Knowledge bases are created within standard catalog. 

1.	Click on the standard catalog you previously created. 
2.	Click on the “default” schema 
3.	Click on “Knowledge bases” 
4.	Click on the “+” button to create a new knowledge base. 
5.	Give your knowledge base a name and a description. Suggestions: 
```
    Name: “entertainment_analyst_kb”
    Description: “Contains internal release playbooks” 
```
6.	Leave the advanced settings as-is for now. These advanced settings control the embedding model being used, the chunk size and the chunk overlap. We will adopt the default values. 
7.	After the knowledge base is Active, the next step is to populate the knowlege base with relevant documents. To do so, click on your newly created knowledge base. 
8.	Under the “Data Source” tab, click on the “+” button to add a data source. 
9.	You will see a data source selection window. Select the volume in your catalog where the release strategy and playbook documents are stored. Leave all advanced settings as-is for now. 
10.	Click on “Add”. 
11.	On the “History” tab of your knowledge base. You should see a line entry with the operation name “Update Knowledge Base”. This step ingests the data in the knowledge base. Make sure that step has status “Succeeded” before moving to the next step. That step usually takes < 1 min since we are only ingesting one small document in the knowledge base. 

Once the operation has succeeded, we have all the data assets we need to move to the next step and create our first agent flow. 

### Ingest the csv files into an Oracle AI Database 

1. Ingest the csv files stored in `/data/Database_Tables/` into an Oracle AI Database. I recommend that you create a separate schema (e.g. `entertainment`). The name of each database table should match the name of the corresponding csv file. The tables will be used with the different SQL tools you will create soon. 

## Part II: Agent Flow Setup 

### Creating the AI compute 

We have the data and knowledge base in place. We can move on to the agent flow part. 
The first step is to create an AI compute. An AI compute hosts your agent flows.  We need AI computes to test agent flows and deploy them. 
1.	Select your workspace from the drop down menu listing all workspaces. 
2.	Click on “Compute” under that selected workspace.  
3.	In the “Compute” page, click on the AI Compute tab. 
4.	Click the “+” button to add an AI Compute. Give it a name and a description. Use the default size of 1 ocpu and 16 GB of RAM. 

Once the AI compute is active, you are ready to move to the next step.

### Creating the Agent Flow 

Once the AI compute is active and you’ve created a standard catalog + knowledge base, the next step is to create an agent flow. 

1.	Go to your workspace > agent flows. Click on the “+” button. 
2.	Give your agent flow a name and a simple description. I recommend you use: 
```
    name: entertainment_analyst
    Description: an internal analytics and decision-support agent for an entertainment studio or streaming platform. 
```
3.	You will be directed to the agent flow canvas. 
4.	The next step is to attach your agent flow to the AI Compute that you previously created. The agent flow will run within that AI compute. This gives you the ability to edit the agent flow and test those changes quasi simultaneously since all changes are automatically propagated to the AI compute. 
5.	In the upper right corner, click on “Compute”, click on “Attach to AI Compute”, then choose the AI compute you previously created. 
6.	Draft an agent node. Give the agent node a name and a description. Recommended values: 
```
    Name: Entertainment Release and Performance Analyst
    Description: You are an internal analytics and decision-support agent for an entertainment studio or streaming platform.
```
7.	In the configuration tab, select the following: 
```    
    Region: us-phoenix-1 
    Model: xai.grok-4-fast-reasoning 
    Agent Instructions: Copy the content of the agent-instructions.txt file in the agent instructions box. These instructions are very detailed. 
```
Leave the Model Parameters and the Safety Guardrails as is for now. 

### Adding Tools 

#### RAG tool 
We are now ready to add tools. We’ll start by the first one, the RAG tool giving access to our knowledge base. 
1.	Drag the RAG tool to the canvas. 
2.	Give the tool a name and a description. Recommendations : 
```
    Name: internal_knowledge_sources_rag
    Description: You have access to the following authoritative internal documents via a RAG tool:  Content Strategy & Release 
Operations Playbook,  Marketing Measurement & Attribution Guidelines,  Distribution Window & Territory Rules
```
3.	In the configuration tab, select the knowledge base you created at step 4. 
4.	Set a limit of 5 documents to retrieve. This is the number of chunks returned by the knowledge base. 
5.	Leave the query field intact. 
6.	You can click on the Test tab and enter the following query: 

    _Territory priorities for releases_

#### SQL Tools 

We are now ready to add a few SQL tools. 

##### get_box_office_weekend

The first tool will return the box office weekend data for a given title and market code. 
1.	Drag a SQL tool into the canvas
2.	Give your tool a descriptive name and a description. Recommendations: 
```
    Name: get_box_office_weekend
    Description: Weekend theatrical performance for a title in a market.
```
3.	Select vectordb23ai.aidp_scenario_5 under Catalog and schema
4.	For the Query field, use the following query: 
```sql
SELECT
  t.title_name,
  b.weekend_end_date,
  b.market_code,
  b.gross_usd_m,
  b.screens,
  b.rank
FROM box_office_weekend b
JOIN titles t ON t.title_id = b.title_id
WHERE b.title_id = {{title_id}}
  AND b.market_code = {{market_code}}
```
5.	You will notice that the parameters `{{title_id}}` and `{{market_code}}` are now showing up in the right panel under “AI Tool definition”. The agent can set values for those parameters. For each one, provide a description. Examples: 
```
    {{title_id}} : The title ID of the movie. For example, T1002. If you are unsure, use the tool get_title_id. The last option is to ask the user.
    {{market_code}}: Market codes is a two letter representing the country or region where the movie is released. These are documented in our internal policy documents. An example is Japan being JP.
```
6.	Just like the RAG tool, you can click on the test tab and assign values to the parameters and see the result of the query. An example: 
```
    Title_id = T1001
    Market_code = US
```
You should see two rows for Skyline Heist on 2025-09-14 and 2025-09-21. 

##### get_streaming_trend

We are now going to add a few more SQL tools to the agent flow. These sql tools perform different queries. Each one will have a name, a description, a query, and suggested descriptions for each parameter. Please replicate in your environment. 
```
    Description: Weekly streaming health trend (starts, hours, completion) for a title in a region.
```
SQL Query:
```sql
SELECT
  t.title_name,
  s.week_start_date,
  s.region_code,
  s.starts,
  s.hours_streamed_k,
  s.completion_rate
FROM streaming_weekly s
JOIN titles t ON t.title_id = s.title_id
WHERE s.title_id = {{title_id}}
  AND s.region_code = {{region_code}}
ORDER BY s.week_start_date ASC;
```
Parameters:
```
    {{title_id}}: The title of the movie. For example, T1002. If you are unsure, use the tool get_title_id. The last option is to ask the user.
    {{region_code}}: A two letter representing the country or region where the movie is released. These are documented in our internal policy documents. An example is Japan being JP. 
```

##### get_campaign_summary
```
    Tool Name: get_campaign_summary
    Description: Roll up spend + attributed revenue + computed ROI for a campaign (all channels, all days).
```
SQL Query:
```sql
SELECT
  c.campaign_id,
  c.campaign_name,
  c.title_id,
  t.title_name,
  c.start_date,
  c.end_date,
  SUM(d.spend_usd) AS total_spend_usd,
  SUM(d.attributed_revenue_usd) AS total_attributed_revenue_usd,
  CASE
    WHEN SUM(d.spend_usd) = 0 THEN NULL
    ELSE (SUM(d.attributed_revenue_usd) - SUM(d.spend_usd)) / SUM(d.spend_usd)
  END AS roi
FROM marketing_campaigns c
JOIN titles t ON t.title_id = c.title_id
JOIN marketing_daily_spend d ON d.campaign_id = c.campaign_id
WHERE c.campaign_id = {{campaign_id}}
GROUP BY
  c.campaign_id, c.campaign_name, c.title_id, t.title_name, c.start_date, c.end_date;
```
Parameters:
```
    {{campaign_id}}: The ID of a marketing campaign associated with movies. For example: C2001. 
```

##### get_campaign_channel_breakdown
```
    Tool Name: get_campaign_channel_breakdown
    Description: Provides a breakdown of campaign spend and revenue by marketing channel. SQL Query:
```
```sql
SELECT
  d.channel,
  SUM(d.spend_usd) AS spend_usd,
  SUM(d.attributed_revenue_usd) AS attributed_revenue_usd,
  CASE
    WHEN SUM(d.spend_usd) = 0 THEN NULL
    ELSE (SUM(d.attributed_revenue_usd) - SUM(d.spend_usd)) / SUM(d.spend_usd)
  END AS roi
FROM marketing_daily_spend d
WHERE d.campaign_id = {{campaign_id}}
GROUP BY d.channel
ORDER BY spend_usd DESC;
```

Parameters: 
```
    {{campaign_id}}: The marketing campaign ID of interest. For example: C2001.
```

##### get_title_id
```
    Tool Name: get_title_id 
    Description: This tool returns a table of all title IDs and title names
```
SQL Query:  
```sql
SELECT * from titles
```
Parameters: (There are no parameters) 

##### get_market_code
```
    Tool Name: get_market_code
    Description: returns a table of market codes alongside market names and currency
```
SQL query:  
```sql
SELECT * from markets
```
Parameters: (There are no parameters) 

##### get_campaign_code
```
    Tool Name: get_campaign_code
    Description: Provides a mapping between campaign ID, the campaign name and the associated movie (the title ID) 
```
SQL Query:
```sql
SELECT * from marketing_campaigns 
```
Parameters (there are no parameters)  

## Part III: Testing the Agent Flow in the Playground

We have our agent in place and a series of tools to get us started. We are now ready to test the agent flow.
1.	Click on the Test button. The Test button reveals the playground where you can create sessions and test the agent flows. 
2.	In the chat window, click on the “+” button to start a test session with the agent you created. 
3.	You can now start asking questions to the agent. Here’s a few examples: 

        - Can you tell me how well the movies neon knights and skyline hiest did at the box office? (note the typo)
        - Can you look at the canadian market?
        - How about the campaign spent for these movies? What's the breakdown per channel?
        - Are the top channels the same for both movies?
        - Can we give me a report on streaming for cosmic kitchen and maple street? 
        - Can you return a table for me? The columns are the two shows and the rows are the weeks. Just focus on the US market.

## Part IV: Deployment 

Last step in the E2E flow is to deploy the agent flow on an AI compute.

1)	Click on the Deploy button in the upper right corner 
2)	Select an AI Compute that will host the deployed agent flow 
3)	After the deployment is successful, go the “Details” tab of your agent flow 
    - Endpoint URL is the production URL of your agent flow. 

Once deployed, an agent flow endpoint can be called via curl or other interfaces. This is beyond the scope of this Lab. 

## Recap 

In this lab, you created a knowledge base that you connected to an agent flow via a RAG tool. You created an agent with access to a RAG and multiple SQL tools to answer complex questions about movie box office performances across different markets and streaming data. You tested the agent flow in the playground and deployed it. 
