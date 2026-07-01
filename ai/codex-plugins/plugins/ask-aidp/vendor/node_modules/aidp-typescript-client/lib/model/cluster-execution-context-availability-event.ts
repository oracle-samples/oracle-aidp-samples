// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about the cluster execution context availability event.
*/
export interface ClusterExecutionContextAvailabilityEvent extends model.ClusterEvent {
    /**
    * Number of available execution contexts for this cluster. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'numberOfExecutionContexts': number;

   "type": string;
}

export namespace ClusterExecutionContextAvailabilityEvent {


    export function getJsonObj(obj: ClusterExecutionContextAvailabilityEvent, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterEvent.getJsonObj(obj) as ClusterExecutionContextAvailabilityEvent, ...{
            

        }};

        
        
        return jsonObj;
    }
    export const type = 'CLUSTER_EXECUTION_CONTEXT_AVAILABILITY_EVENT';
    export function getDeserializedJsonObj(obj: ClusterExecutionContextAvailabilityEvent, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterEvent.getDeserializedJsonObj(obj) as ClusterExecutionContextAvailabilityEvent, ...{
            

         }};

        
        
        return jsonObj;
    }
}
