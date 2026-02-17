# ACME Inc. Pet Insurance 

## Synopsis  

We are building a simple customer support agent for ACME Inc, a company that issues pet insurance policies for pet owners. We'll use the RAG tool to answer questions about pet insurance policies.  


## Service Features Tested  

* Knowledge base and pdf file ingestion  
* Agent flow no-code canvas   
* RAG tool  
* Agent flow playground 


## Steps  

1) Go to OCI console > Storage > Buckets.  

2) Select a compartment and click on “Create bucket”.  

3) Give the bucket a name (i named mine acme-insurance-policies). Leave all other config params untouched.  

4) Click on that bucket link and click on “Upload Objects”.  

5) Drop the insurance pdf files from the data/ folder. Click Next and click Upload Objects”. Click on Close button after all files are uploaded. Double-check that the files are in the bucket by clicking on the “Objects” tab of the bucket.  

6) Go back to your AIDP Workbench instance.  

7) Create a new external volume in a new schema or in the same schema you used in Agent 2. Select the compartment of your newly created bucket as well as the bucket.  

8) Check that the policy files are in the volume.  

9) Now create a new knowledge base which includes the ACME insurance policies. In the same schema, under “knowledge bases”, click on “+” to create a new knowledge base. Give your knowledge base a name and a description. Leave the advanced settings parameters untouched for now.  

10) Once the kb is created successfully, click on the kb. In the “Data source” tab, click on the “+” and add the external volume you created earlier.  

File ingestion will start right away. Follow the progress in the History tab of the kb. It should take less than 1 min to complete succesfully.  

11) Go to Agent flows and create a new agent flow with the visual flow authoring mode. 

12) Drag the agent node to the canvas. Select us-ashburn-1 region and the xai.grok-4-fast-reasoning model.  

13) Add the following agent instructions:  

    _You are an AI support agent responsible for accurately answering questions about pet insurance policies issued by ACME Inc. You have access to a Retrieval-Augmented Generation (RAG) tool connected to a knowledge base containing detailed ACME Inc. pet insurance policy information._ 

    _Instructions:_ 

    _Use the RAG tool to retrieve the most relevant, up-to-date information from the ACME Inc. insurance policy knowledge base for every user query._ 

    _Provide clear, concise, and accurate responses based strictly on the information found in the knowledge base._ 

    _If the policy details do not cover the user’s question, inform the user politely and suggest contacting ACME Inc. customer support for further assistance._ 

    _Do not provide advice or make assumptions beyond the knowledge base content._ 

    _Adhere to compliance, privacy, and security standards at all times._ 


    _Example opening response:_

    _“Thank you for your question about ACME Inc. pet insurance. Based on our policy information, here’s what I found:”_ 

 

14) Leave the model parameters and safety guardrails untouched.  

15) Drag and drop the RAG tool in the canvas  

16) Configure your RAG tool with a descriptive name and a complete description to give the agent proper context about what the tool does. Here’s an example:  

    Tool name: acme_insurance_policies_retrieval 

    Description: Retrieval augmented generation tool (RAG) connected to a knowledge base of ACME Inc insurance policies. The RAG tool performs natural language queries over the insurance policies. 

17) In the RAG tool config window, select the knowledge base you created for the insurance policies. Set the limit of docs retrieved to 5. Leave the query box empty.  

18) Attach your knowledge base to an AI compute. If you don't already have an AI compute, create a new one. 

19) Test your tool with a sample query in the “Playground” tab. An example could be:  

    _For Bella, what’s the annual coverage limit?_  

## Recap 

You created an external volume, uploaded pdf documents to that volume, and created a knowledge base. You then connected an agent to that knowledge base by creating a simple RAG tool. 