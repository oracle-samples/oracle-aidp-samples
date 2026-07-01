// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Memory limit configuration for an agent node.
*/
export interface MemoryLimitConfiguration {
    /**
    * Type used for memory limiting. TRUNCATION applies truncation middleware.
    */
    'type'?: model.MemoryLimitStrategy;
    'config'?: model.MemoryLimitConfigurationDetails;

}

export namespace MemoryLimitConfiguration {



    export function getJsonObj(obj: MemoryLimitConfiguration): object {
        const jsonObj = {...obj, ...{
            

                'config': obj.config ?
                
                
                model.MemoryLimitConfigurationDetails.getJsonObj(obj.config) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MemoryLimitConfiguration): object {
        const jsonObj = {...obj, ...{
            

                    'config': obj.config ?
                
                
                model.MemoryLimitConfigurationDetails.getDeserializedJsonObj(obj.config) : undefined,
         }};

        
        
        return jsonObj;
    }
}
