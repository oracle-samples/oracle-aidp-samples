// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response for previewing an agent card.
*/
export interface AgentCardPreviewResponse {
    /**
    * JSON string representation of AgentCard
    */
    'agentCardJson': string;

}

export namespace AgentCardPreviewResponse {


    export function getJsonObj(obj: AgentCardPreviewResponse): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentCardPreviewResponse): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
