// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to update a cluster.
*/
export interface UpdateClusterDetails {
    /**
    * Cluster name.
    */
    'displayName'?: string;
    /**
    * Cluster description.
    */
    'description'?: string;
    'driverConfig'?: model.DriverConfig;
    /**
    * Cluster node type encodes the node shape and associated resources.
    */
    'nodeType'?: string;

   "type": string;
}

export namespace UpdateClusterDetails {





    export function getJsonObj(obj: UpdateClusterDetails): object {
        const jsonObj = {...obj, ...{
            


                'driverConfig': obj.driverConfig ?
                
                
                model.DriverConfig.getJsonObj(obj.driverConfig) : undefined,

        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "AGENT_FLOW_COMPUTE":
                    return model.UpdateAgentFlowComputeDetails.getJsonObj(<model.UpdateAgentFlowComputeDetails>(<object>jsonObj), true);
                case "USER":
                    return model.UpdateSparkClusterDetails.getJsonObj(<model.UpdateSparkClusterDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateClusterDetails): object {
        const jsonObj = {...obj, ...{
            


                    'driverConfig': obj.driverConfig ?
                
                
                model.DriverConfig.getDeserializedJsonObj(obj.driverConfig) : undefined,

         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "AGENT_FLOW_COMPUTE":
                    return model.UpdateAgentFlowComputeDetails.getDeserializedJsonObj(<model.UpdateAgentFlowComputeDetails>(<object>jsonObj), true);
                case "USER":
                    return model.UpdateSparkClusterDetails.getDeserializedJsonObj(<model.UpdateSparkClusterDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
