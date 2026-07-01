// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Agent flow session context.
*/
export interface AgentFlowSessionContext {
    'retentionConfig'?: model.SessionRetentionConfiguration;

}

export namespace AgentFlowSessionContext {


    export function getJsonObj(obj: AgentFlowSessionContext): object {
        const jsonObj = {...obj, ...{
            
                'retentionConfig': obj.retentionConfig ?
                
                
                model.SessionRetentionConfiguration.getJsonObj(obj.retentionConfig) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowSessionContext): object {
        const jsonObj = {...obj, ...{
            
                    'retentionConfig': obj.retentionConfig ?
                
                
                model.SessionRetentionConfiguration.getDeserializedJsonObj(obj.retentionConfig) : undefined,
         }};

        
        
        return jsonObj;
    }
}
