// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The information about a new compute cluster.
*/
export interface CreateClusterDetails {
    /**
    * Cluster name.
    */
    'displayName': string;
    /**
    * Cluster description.
    */
    'description'?: string;
    'driverConfig': model.DriverConfig;
    /**
    * Cluster node type encodes the node shape and associated resources.
    */
    'nodeType'?: string;

   "type": string;
}

export namespace CreateClusterDetails {





    export function getJsonObj(obj: CreateClusterDetails): object {
        const jsonObj = {...obj, ...{
            


                'driverConfig': obj.driverConfig ?
                
                
                model.DriverConfig.getJsonObj(obj.driverConfig) : undefined,

        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "AGENT_FLOW_COMPUTE":
                    return model.CreateAgentFlowComputeDetails.getJsonObj(<model.CreateAgentFlowComputeDetails>(<object>jsonObj), true);
                case "USER":
                    return model.CreateSparkClusterDetails.getJsonObj(<model.CreateSparkClusterDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateClusterDetails): object {
        const jsonObj = {...obj, ...{
            


                    'driverConfig': obj.driverConfig ?
                
                
                model.DriverConfig.getDeserializedJsonObj(obj.driverConfig) : undefined,

         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "AGENT_FLOW_COMPUTE":
                    return model.CreateAgentFlowComputeDetails.getDeserializedJsonObj(<model.CreateAgentFlowComputeDetails>(<object>jsonObj), true);
                case "USER":
                    return model.CreateSparkClusterDetails.getDeserializedJsonObj(<model.CreateSparkClusterDetails>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
