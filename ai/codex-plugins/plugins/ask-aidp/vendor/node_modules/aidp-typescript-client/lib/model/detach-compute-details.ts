// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details required to detach an agent flow to a compute
*/
export interface DetachComputeDetails {
    /**
    * The key of the Agent Flow Compute associated with this Agent Flow
    */
    'agentFlowComputeKey': string;

}

export namespace DetachComputeDetails {


    export function getJsonObj(obj: DetachComputeDetails): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DetachComputeDetails): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
