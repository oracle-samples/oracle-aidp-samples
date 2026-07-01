// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Properties of a event provided by the cluster.
*/
export interface ClusterEvent {

   "type": string;
}

export namespace ClusterEvent {

    export function getJsonObj(obj: ClusterEvent): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "CLUSTER_STATE_EVENT":
                    return model.ClusterStateEvent.getJsonObj(<model.ClusterStateEvent>(<object>jsonObj), true);
                case "CLUSTER_PATCH_EVENT":
                    return model.ClusterPatchEvent.getJsonObj(<model.ClusterPatchEvent>(<object>jsonObj), true);
                case "CLUSTER_EXECUTION_CONTEXT_AVAILABILITY_EVENT":
                    return model.ClusterExecutionContextAvailabilityEvent.getJsonObj(<model.ClusterExecutionContextAvailabilityEvent>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterEvent): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "CLUSTER_STATE_EVENT":
                    return model.ClusterStateEvent.getDeserializedJsonObj(<model.ClusterStateEvent>(<object>jsonObj), true);
                case "CLUSTER_PATCH_EVENT":
                    return model.ClusterPatchEvent.getDeserializedJsonObj(<model.ClusterPatchEvent>(<object>jsonObj), true);
                case "CLUSTER_EXECUTION_CONTEXT_AVAILABILITY_EVENT":
                    return model.ClusterExecutionContextAvailabilityEvent.getDeserializedJsonObj(<model.ClusterExecutionContextAvailabilityEvent>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
