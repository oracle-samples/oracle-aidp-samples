// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing agent flow deployments of an agent flow.
*/
export interface AgentFlowDeploymentCollection {
    /**
    * List of Agent Flow Deployments.
    */
    'items': Array<model.AgentFlowDeploymentSummary>;

}

export namespace AgentFlowDeploymentCollection {


    export function getJsonObj(obj: AgentFlowDeploymentCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.AgentFlowDeploymentSummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowDeploymentCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.AgentFlowDeploymentSummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
