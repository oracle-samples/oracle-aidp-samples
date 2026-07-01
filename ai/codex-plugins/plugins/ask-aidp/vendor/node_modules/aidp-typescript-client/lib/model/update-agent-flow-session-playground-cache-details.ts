// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Playground cache for an Agent Flow session.
*/
export interface UpdateAgentFlowSessionPlaygroundCacheDetails {
    /**
    * Map of variable name to value for this session's playground cache.
    */
    'variables': { [key: string]: model.SessionVariable; };

}

export namespace UpdateAgentFlowSessionPlaygroundCacheDetails {


    export function getJsonObj(obj: UpdateAgentFlowSessionPlaygroundCacheDetails): object {
        const jsonObj = {...obj, ...{
            
                'variables': obj.variables ?
                
                
                common.mapContainer(obj.variables, model.SessionVariable.getJsonObj)
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateAgentFlowSessionPlaygroundCacheDetails): object {
        const jsonObj = {...obj, ...{
            
                    'variables': obj.variables ?
                
                
                common.mapContainer(obj.variables, model.SessionVariable.getDeserializedJsonObj)
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
