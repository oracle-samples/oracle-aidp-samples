// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* User-created cluster.
*/
export interface SparkCluster extends model.Cluster {
    'workerConfig'?: model.WorkerConfig;
    'clusterRuntimeConfig'?: model.SparkRuntimeConfig;
    'loggingConfig'?: model.OciLogging;
    /**
    * Optional timeout value in minutes used to automatically stop idle compute clusters. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'autoTerminationMinutes'?: number;
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
    /**
    * Spark JDBC URL.
    */
    'jdbcEndpointUrl'?: string;
    /**
    * The OCID of the log where cluster logs are published and retrieved. This logId is always created within the logGroupId returned in the response payload.
* 
    */
    'logId'?: string;
    /**
    * The unique OCID that identifies a specific log group within OCI Logging.
* This log group is exclusively associated with the AI Data Platform Workbench instance and is created in the same compartment within the customer\u2019s tenancy as the AI Data Platform Workbench instance.
* 
    */
    'logGroupId'?: string;
    'subscription'?: model.SubscriptionDetails;

   "sourceApi": string;
}

export namespace SparkCluster {












    export function getJsonObj(obj: SparkCluster, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Cluster.getJsonObj(obj) as SparkCluster, ...{
            
                'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getJsonObj(obj.workerConfig) : undefined,
                'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getJsonObj(obj.clusterRuntimeConfig) : undefined,
                'loggingConfig': obj.loggingConfig ?
                
                
                model.LoggingConfig.getJsonObj(obj.loggingConfig) : undefined,


                'attachedSessions': obj.attachedSessions ?
                
                obj.attachedSessions.map((item)=>{return model.AttachedSession.getJsonObj(item)})
                
                 : undefined,




                'subscription': obj.subscription ?
                
                
                model.SubscriptionDetails.getJsonObj(obj.subscription) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const sourceApi = 'CLUSTER_API';
    export function getDeserializedJsonObj(obj: SparkCluster, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Cluster.getDeserializedJsonObj(obj) as SparkCluster, ...{
            
                    'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getDeserializedJsonObj(obj.workerConfig) : undefined,
                    'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getDeserializedJsonObj(obj.clusterRuntimeConfig) : undefined,
                    'loggingConfig': obj.loggingConfig ?
                
                
                model.LoggingConfig.getDeserializedJsonObj(obj.loggingConfig) : undefined,


                    'attachedSessions': obj.attachedSessions ?
                
                obj.attachedSessions.map((item)=>{return model.AttachedSession.getDeserializedJsonObj(item)})
                
                 : undefined,




                    'subscription': obj.subscription ?
                
                
                model.SubscriptionDetails.getDeserializedJsonObj(obj.subscription) : undefined,
         }};

        
        
        return jsonObj;
    }
}
