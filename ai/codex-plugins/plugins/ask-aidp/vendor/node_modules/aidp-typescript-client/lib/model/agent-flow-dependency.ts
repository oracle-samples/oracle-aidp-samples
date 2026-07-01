// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The response object for getting the agent flow dependencies
*/
export interface AgentFlowDependency {
    /**
    * AgentFlow Key
    */
    'agentFlowKey': string;
    /**
    * AgentFlow
    */
    'type'?: string;
    /**
    * List of AgentFlow dependencies.
    */
    'dependencies'?: Array<model.AgentFlowDependencyItem>;

}

export namespace AgentFlowDependency {




    export function getJsonObj(obj: AgentFlowDependency): object {
        const jsonObj = {...obj, ...{
            


                'dependencies': obj.dependencies ?
                
                obj.dependencies.map((item)=>{return model.AgentFlowDependencyItem.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowDependency): object {
        const jsonObj = {...obj, ...{
            


                    'dependencies': obj.dependencies ?
                
                obj.dependencies.map((item)=>{return model.AgentFlowDependencyItem.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
