// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Spark cluster details for creation
*/
export interface UpdateSparkClusterDetails extends model.UpdateClusterDetails {
    'workerConfig'?: model.WorkerConfig;
    'clusterRuntimeConfig'?: model.SparkRuntimeConfig;
    'loggingConfig'?: model.OciLogging;
    /**
    * Optional timeout value in minutes used to automatically stop idle compute clusters. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'autoTerminationMinutes'?: number;
    'subscription'?: model.SubscriptionDetails;

   "type": string;
}

export namespace UpdateSparkClusterDetails {






    export function getJsonObj(obj: UpdateSparkClusterDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateClusterDetails.getJsonObj(obj) as UpdateSparkClusterDetails, ...{
            
                'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getJsonObj(obj.workerConfig) : undefined,
                'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getJsonObj(obj.clusterRuntimeConfig) : undefined,
                'loggingConfig': obj.loggingConfig ?
                
                
                model.LoggingConfig.getJsonObj(obj.loggingConfig) : undefined,

                'subscription': obj.subscription ?
                
                
                model.SubscriptionDetails.getJsonObj(obj.subscription) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'USER';
    export function getDeserializedJsonObj(obj: UpdateSparkClusterDetails, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.UpdateClusterDetails.getDeserializedJsonObj(obj) as UpdateSparkClusterDetails, ...{
            
                    'workerConfig': obj.workerConfig ?
                
                
                model.WorkerConfig.getDeserializedJsonObj(obj.workerConfig) : undefined,
                    'clusterRuntimeConfig': obj.clusterRuntimeConfig ?
                
                
                model.ClusterRuntimeConfig.getDeserializedJsonObj(obj.clusterRuntimeConfig) : undefined,
                    'loggingConfig': obj.loggingConfig ?
                
                
                model.LoggingConfig.getDeserializedJsonObj(obj.loggingConfig) : undefined,

                    'subscription': obj.subscription ?
                
                
                model.SubscriptionDetails.getDeserializedJsonObj(obj.subscription) : undefined,
         }};

        
        
        return jsonObj;
    }
}
