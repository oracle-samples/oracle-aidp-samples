// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a workspace cluster.
*/
export interface ClusterSummary {
    /**
    * Cluster key.
    */
    'key': string;
    /**
    * Cluster name.
    */
    'displayName': string;
    /**
    * Cluster description.
    */
    'description'?: string;
    /**
    * ClusterType
    */
    'type': model.ClusterType;
    /**
    * Date and time the cluster was created.
    */
    'timeCreated': Date;
    /**
    * Date and time the cluster was updated.
    */
    'timeUpdated'?: Date;
    /**
    * The current state of the cluster.
    */
    'state': string;
    /**
    * A message that describes the current state of the workspace cluster in more detail. For example, can be used to provide actionable information for a resource in the Failed state.
    */
    'stateDetails'?: string;
    /**
    * OCID of the user who created this record.
    */
    'createdBy'?: string;
    /**
    * Name of the user who created this record.
    */
    'createdByName'?: string;
    /**
    * OCID of the user who updated this record.
    */
    'updatedBy'?: string;
    /**
    * Name of the user who updated this record.
    */
    'updatedByName'?: string;
    /**
    * OCID of the user who stopped the cluster. Value will be 'SYSTEM' if it was auto stopped.
    */
    'stoppedBy'?: string;
    /**
    * Name of the user who stopped the cluster. Value will be 'SYSTEM' if it was auto stopped.
    */
    'stoppedByName'?: string;
    'clusterRuntimeConfig'?: model.SparkRuntimeConfig;
    'activeClusterResources'?: model.ActiveClusterResources;
    'driverConfig'?: model.DriverConfig;
    'workerConfig'?: model.WorkerConfig;
    /**
    * List of notebooks attached to a specific cluster.
    */
    'attachedNotebooks'?: Array<string>;
    /**
    * List of sessions attached to a specific cluster.
    */
    'attachedSessions'?: Array<model.AttachedSession>;
    /**
    * Count of agent flow attached to a specific cluster. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'attachedAgentFlowCount'?: number;

}

export namespace ClusterSummary {






















    export function getJsonObj(obj: ClusterSummary): object {
        const jsonObj = {...obj, ...{
            














                'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getJsonObj(obj.clusterRuntimeConfig) : undefined,
                'activeClusterResources': obj.activeClusterResources ?
                
                
                model.ActiveClusterResources.getJsonObj(obj.activeClusterResources) : undefined,
                'driverConfig': obj.driverConfig ?
                
                
                model.DriverConfig.getJsonObj(obj.driverConfig) : undefined,
                'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getJsonObj(obj.workerConfig) : undefined,

                'attachedSessions': obj.attachedSessions ?
                
                obj.attachedSessions.map((item)=>{return model.AttachedSession.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterSummary): object {
        const jsonObj = {...obj, ...{
            














                    'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getDeserializedJsonObj(obj.clusterRuntimeConfig) : undefined,
                    'activeClusterResources': obj.activeClusterResources ?
                
                
                model.ActiveClusterResources.getDeserializedJsonObj(obj.activeClusterResources) : undefined,
                    'driverConfig': obj.driverConfig ?
                
                
                model.DriverConfig.getDeserializedJsonObj(obj.driverConfig) : undefined,
                    'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getDeserializedJsonObj(obj.workerConfig) : undefined,

                    'attachedSessions': obj.attachedSessions ?
                
                obj.attachedSessions.map((item)=>{return model.AttachedSession.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
