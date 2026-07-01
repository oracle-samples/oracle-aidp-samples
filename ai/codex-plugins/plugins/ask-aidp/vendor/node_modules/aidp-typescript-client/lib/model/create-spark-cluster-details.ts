// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Spark cluster details for creation.
*/
export interface CreateSparkClusterDetails extends model.CreateClusterDetails {
    'workerConfig'?: model.WorkerConfig;
    'clusterRuntimeConfig'?: model.SparkRuntimeConfig;
    'loggingConfig'?: model.OciLogging;
    /**
    * Optional timeout value in minutes used to automatically stop idle compute clusters. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'autoTerminationMinutes'?: number;
    'attachToNotebookConfig'?: model.AttachToNotebookConfig;
    'subscription'?: model.SubscriptionDetails;

   "type": string;
}

export namespace CreateSparkClusterDetails {







    export function getJsonObj(obj: CreateSparkClusterDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateClusterDetails.getJsonObj(obj) as CreateSparkClusterDetails, ...{
            
                'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getJsonObj(obj.workerConfig) : undefined,
                'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getJsonObj(obj.clusterRuntimeConfig) : undefined,
                'loggingConfig': obj.loggingConfig ?
                
                
                model.LoggingConfig.getJsonObj(obj.loggingConfig) : undefined,

                'attachToNotebookConfig': obj.attachToNotebookConfig ?
                
                
                model.AttachToNotebookConfig.getJsonObj(obj.attachToNotebookConfig) : undefined,
                'subscription': obj.subscription ?
                
                
                model.SubscriptionDetails.getJsonObj(obj.subscription) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'USER';
    export function getDeserializedJsonObj(obj: CreateSparkClusterDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.CreateClusterDetails.getDeserializedJsonObj(obj) as CreateSparkClusterDetails, ...{
            
                    'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getDeserializedJsonObj(obj.workerConfig) : undefined,
                    'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getDeserializedJsonObj(obj.clusterRuntimeConfig) : undefined,
                    'loggingConfig': obj.loggingConfig ?
                
                
                model.LoggingConfig.getDeserializedJsonObj(obj.loggingConfig) : undefined,

                    'attachToNotebookConfig': obj.attachToNotebookConfig ?
                
                
                model.AttachToNotebookConfig.getDeserializedJsonObj(obj.attachToNotebookConfig) : undefined,
                    'subscription': obj.subscription ?
                
                
                model.SubscriptionDetails.getDeserializedJsonObj(obj.subscription) : undefined,
         }};

        
        
        return jsonObj;
    }
}
