// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create a tool.
*/
export interface CreateToolDetails {
    /**
    * Tool name.
    */
    'displayName': string;
    /**
    * Tool description.
    */
    'description'?: string;
    /**
    * A list of key-value pairs to use for configuring the tool
    */
    'properties'?: { [key: string]: any; };

   "toolType": string;
}

export namespace CreateToolDetails {




    export function getJsonObj(obj: CreateToolDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "SQL":
                    return model.CreateSqlToolDetails.getJsonObj(<model.CreateSqlToolDetails>(<object>jsonObj), true);
                case "RAG":
                    return model.CreateRagToolDetails.getJsonObj(<model.CreateRagToolDetails>(<object>jsonObj), true);
                case "PROMPT":
                    return model.CreatePromptToolDetails.getJsonObj(<model.CreatePromptToolDetails>(<object>jsonObj), true);
                case "CUSTOM":
                    return model.CreateCustomToolDetails.getJsonObj(<model.CreateCustomToolDetails>(<object>jsonObj), true);
                case "NL2SQL":
                    return model.CreateNlToSqlToolDetails.getJsonObj(<model.CreateNlToSqlToolDetails>(<object>jsonObj), true);
                case "HTTP":
                    return model.CreateHttpToolDetails.getJsonObj(<model.CreateHttpToolDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateToolDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "SQL":
                    return model.CreateSqlToolDetails.getDeserializedJsonObj(<model.CreateSqlToolDetails>(<object>jsonObj), true);
                case "RAG":
                    return model.CreateRagToolDetails.getDeserializedJsonObj(<model.CreateRagToolDetails>(<object>jsonObj), true);
                case "PROMPT":
                    return model.CreatePromptToolDetails.getDeserializedJsonObj(<model.CreatePromptToolDetails>(<object>jsonObj), true);
                case "CUSTOM":
                    return model.CreateCustomToolDetails.getDeserializedJsonObj(<model.CreateCustomToolDetails>(<object>jsonObj), true);
                case "NL2SQL":
                    return model.CreateNlToSqlToolDetails.getDeserializedJsonObj(<model.CreateNlToSqlToolDetails>(<object>jsonObj), true);
                case "HTTP":
                    return model.CreateHttpToolDetails.getDeserializedJsonObj(<model.CreateHttpToolDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)
        }
        }
        return jsonObj;
    }
}
