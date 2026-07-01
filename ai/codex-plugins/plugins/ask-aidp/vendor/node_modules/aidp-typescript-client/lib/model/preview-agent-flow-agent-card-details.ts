// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details for previewing an agent card.
*/
export interface PreviewAgentFlowAgentCardDetails {
    'agentCardConfigDetails': model.AgentCardConfigDetail;

}

export namespace PreviewAgentFlowAgentCardDetails {


    export function getJsonObj(obj: PreviewAgentFlowAgentCardDetails): object {
        const jsonObj = {...obj, ...{
            
                'agentCardConfigDetails': obj.agentCardConfigDetails ?
                
                
                model.AgentCardConfigDetail.getJsonObj(obj.agentCardConfigDetails) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PreviewAgentFlowAgentCardDetails): object {
        const jsonObj = {...obj, ...{
            
                    'agentCardConfigDetails': obj.agentCardConfigDetails ?
                
                
                model.AgentCardConfigDetail.getDeserializedJsonObj(obj.agentCardConfigDetails) : undefined,
         }};

        
        
        return jsonObj;
    }
}
