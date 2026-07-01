// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing Agent flows in a schema.
*/
export interface AgentFlowSessionCollection {
    /**
    * List of Agent flow sessions.
    */
    'items': Array<model.AgentFlowSessionSummary>;

}

export namespace AgentFlowSessionCollection {


    export function getJsonObj(obj: AgentFlowSessionCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.AgentFlowSessionSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowSessionCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.AgentFlowSessionSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
