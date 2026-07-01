// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to stop a session of an Agent Flow.
*/
export interface StopAgentFlowSessionDetails {

}

export namespace StopAgentFlowSessionDetails {

    export function getJsonObj(obj: StopAgentFlowSessionDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: StopAgentFlowSessionDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
