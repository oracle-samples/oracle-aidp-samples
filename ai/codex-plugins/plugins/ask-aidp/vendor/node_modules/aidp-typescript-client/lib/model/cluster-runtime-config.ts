// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Cluster runtime configurations.
*/
export interface ClusterRuntimeConfig {
    /**
    * Init script paths that are run in the order of definition.
    */
    'initScripts'?: Array<model.InitScriptPath>;

   "type": string;
}

export namespace ClusterRuntimeConfig {


    export function getJsonObj(obj: ClusterRuntimeConfig): object {
        const jsonObj = {...obj, ...{
            
                'initScripts': obj.initScripts ?
                
                obj.initScripts.map((item)=>{return model.InitScriptPath.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "SPARK":
                    return model.SparkRuntimeConfig.getJsonObj(<model.SparkRuntimeConfig>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ClusterRuntimeConfig): object {
        const jsonObj = {...obj, ...{
            
                    'initScripts': obj.initScripts ?
                
                obj.initScripts.map((item)=>{return model.InitScriptPath.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "SPARK":
                    return model.SparkRuntimeConfig.getDeserializedJsonObj(<model.SparkRuntimeConfig>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
