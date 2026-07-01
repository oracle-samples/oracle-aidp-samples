// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a tool.
*/
export interface UpdateToolDetails {
    /**
    * Tool name.
    */
    'displayName'?: string;
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

export namespace UpdateToolDetails {




    export function getJsonObj(obj: UpdateToolDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "CUSTOM":
                    return model.UpdateCustomToolDetails.getJsonObj(<model.UpdateCustomToolDetails>(<object>jsonObj), true);
                case "HTTP":
                    return model.UpdateHttpToolDetails.getJsonObj(<model.UpdateHttpToolDetails>(<object>jsonObj), true);
                case "PROMPT":
                    return model.UpdatePromptToolDetails.getJsonObj(<model.UpdatePromptToolDetails>(<object>jsonObj), true);
                case "RAG":
                    return model.UpdateRagToolDetails.getJsonObj(<model.UpdateRagToolDetails>(<object>jsonObj), true);
                case "SQL":
                    return model.UpdateSqlToolDetails.getJsonObj(<model.UpdateSqlToolDetails>(<object>jsonObj), true);
                case "NL2SQL":
                    return model.UpdateNlToSqlToolDetails.getJsonObj(<model.UpdateNlToSqlToolDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateToolDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        if (obj && "toolType" in obj && obj.toolType) {
            switch (obj.toolType) {
                case "CUSTOM":
                    return model.UpdateCustomToolDetails.getDeserializedJsonObj(<model.UpdateCustomToolDetails>(<object>jsonObj), true);
                case "HTTP":
                    return model.UpdateHttpToolDetails.getDeserializedJsonObj(<model.UpdateHttpToolDetails>(<object>jsonObj), true);
                case "PROMPT":
                    return model.UpdatePromptToolDetails.getDeserializedJsonObj(<model.UpdatePromptToolDetails>(<object>jsonObj), true);
                case "RAG":
                    return model.UpdateRagToolDetails.getDeserializedJsonObj(<model.UpdateRagToolDetails>(<object>jsonObj), true);
                case "SQL":
                    return model.UpdateSqlToolDetails.getDeserializedJsonObj(<model.UpdateSqlToolDetails>(<object>jsonObj), true);
                case "NL2SQL":
                    return model.UpdateNlToSqlToolDetails.getDeserializedJsonObj(<model.UpdateNlToSqlToolDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.toolType}`)
        }
        }
        return jsonObj;
    }
}
