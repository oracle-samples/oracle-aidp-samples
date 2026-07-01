// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Mcp object.
*/
export interface McpObject {
    /**
    * name of the mcp object
    */
    'name'?: string;
    /**
    * description of the mcp object
    */
    'description'?: string;

   "objectType": string;
}

export namespace McpObject {



    export function getJsonObj(obj: McpObject): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        if (obj && "objectType" in obj && obj.objectType) {
            switch (obj.objectType) {
                case "PROMPT":
                    return model.McpPromptObject.getJsonObj(<model.McpPromptObject>(<object>jsonObj), true);
                case "TOOL":
                    return model.McpToolObject.getJsonObj(<model.McpToolObject>(<object>jsonObj), true);
                case "RESOURCE":
                    return model.McpResourceObject.getJsonObj(<model.McpResourceObject>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.objectType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: McpObject): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        if (obj && "objectType" in obj && obj.objectType) {
            switch (obj.objectType) {
                case "PROMPT":
                    return model.McpPromptObject.getDeserializedJsonObj(<model.McpPromptObject>(<object>jsonObj), true);
                case "TOOL":
                    return model.McpToolObject.getDeserializedJsonObj(<model.McpToolObject>(<object>jsonObj), true);
                case "RESOURCE":
                    return model.McpResourceObject.getDeserializedJsonObj(<model.McpResourceObject>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.objectType}`)
        }
        }
        return jsonObj;
    }
}
