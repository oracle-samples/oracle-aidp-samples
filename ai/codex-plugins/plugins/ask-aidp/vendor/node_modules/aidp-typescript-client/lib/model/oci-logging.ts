// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* OCI logging configuration.
*/
export interface OciLogging extends model.LoggingConfig {
    /**
    * Init script paths that are run in the order of definition.
    */
    'logGroups'?: Array<model.LogGroup>;

   "type": string;
}

export namespace OciLogging {


    export function getJsonObj(obj: OciLogging, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.LoggingConfig.getJsonObj(obj) as OciLogging, ...{
            
                'logGroups': obj.logGroups ?
                
                obj.logGroups.map((item)=>{return model.LogGroup.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'OCI_LOGGING';
    export function getDeserializedJsonObj(obj: OciLogging, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.LoggingConfig.getDeserializedJsonObj(obj) as OciLogging, ...{
            
                    'logGroups': obj.logGroups ?
                
                obj.logGroups.map((item)=>{return model.LogGroup.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
