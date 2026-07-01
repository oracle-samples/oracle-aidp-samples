// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The required details for testing an mcp tool
*/
export interface FetchMcpObjectsDetails {
    /**
    * Agent flow id for which the tool is being tested
    */
    'agentFlowId'?: string;
    /**
    * Type of object. Tool, prompt or resource hosted on an MCP.
    */
    'type': model.McpObjectType;
    'mcpTool': model.McpTool;
    /**
    * The sort order to use, either ascending ({@code ASC}) or descending ({@code DESC}). The {@code displayName}
* sort order is case sensitive.
* 
    */
    'sortOrder'?: FetchMcpObjectsDetails.SortOrder;
    /**
    * The field to sort by.
* 
    */
    'sortBy'?: FetchMcpObjectsDetails.SortBy;
    /**
    * For list pagination. The maximum number of results per page, or items to return in a
* paginated \"List\" call. For important details about how pagination works, see
* [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'limit'?: number;
    /**
    * For list pagination. The value of the opc-next-page response header from the previous
* \"List\" call. For important details about how pagination works, see
* [List Pagination]({{DOC_SERVER_URL}}/iaas/Content/API/Concepts/usingapi.htm#nine).
* 
    */
    'page'?: string;
    /**
    * Map of parameter names to their string values.
    */
    'paramValues'?: { [key: string]: string; };

}

export namespace FetchMcpObjectsDetails {




    export enum SortOrder {
    
    Asc = "ASC",
    Desc = "DESC"

}


    export enum SortBy {
    
    TimeCreated = "TIME_CREATED"

}





    export function getJsonObj(obj: FetchMcpObjectsDetails): object {
        const jsonObj = {...obj, ...{
            


                'mcpTool': obj.mcpTool ?
                
                
                model.McpTool.getJsonObj(obj.mcpTool) : undefined,





        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: FetchMcpObjectsDetails): object {
        const jsonObj = {...obj, ...{
            


                    'mcpTool': obj.mcpTool ?
                
                
                model.McpTool.getDeserializedJsonObj(obj.mcpTool) : undefined,





         }};

        
        
        return jsonObj;
    }
}
