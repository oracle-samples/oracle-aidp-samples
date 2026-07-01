// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Spark runtime information.
*/
export interface SparkRuntimeConfig extends model.ClusterRuntimeConfig {
    /**
    * Spark version used to run the application.
    */
    'sparkVersion'?: string;
    /**
    * Spark configuration passed to the running process.
    */
    'sparkAdvancedConfigurations'?: { [key: string]: string; };
    /**
    * Environment variables passed to the running process. See <a href=\"https://spark.apache.org/docs/latest/configuration.html#available-properties\" target=\"_blank\" rel=\"noopener noreferrer\">Available Properties</a>.
* Example - { \"spark.app.name\" : \"My App Name\", \"spark.shuffle.io.maxRetries\" : \"4\" }
* Note: Not all Spark properties are permitted to be set. Attempting to set a property that is
* not allowed to be overwritten will cause a 400 status to be returned.
* 
    */
    'sparkEnvVariables'?: { [key: string]: string; };

   "type": string;
}

export namespace SparkRuntimeConfig {




    export function getJsonObj(obj: SparkRuntimeConfig, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterRuntimeConfig.getJsonObj(obj) as SparkRuntimeConfig, ...{
            



        }};

        
        
        return jsonObj;
    }
    export const type = 'SPARK';
    export function getDeserializedJsonObj(obj: SparkRuntimeConfig, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.ClusterRuntimeConfig.getDeserializedJsonObj(obj) as SparkRuntimeConfig, ...{
            



         }};

        
        
        return jsonObj;
    }
}
