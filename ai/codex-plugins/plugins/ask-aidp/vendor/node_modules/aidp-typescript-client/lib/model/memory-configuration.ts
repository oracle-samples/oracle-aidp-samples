// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Memory configuration for an agent node.
*/
export interface MemoryConfiguration {
    /**
    * Whether agent memory is enabled. When disabled, no conversation history is injected into the LLM context window.
    */
    'isEnabled'?: boolean;
    'limit'?: model.MemoryLimitConfiguration;
    /**
    * Some extra named memory properties.
    */
    'memoryProperties'?: { [key: string]: any; };

}

export namespace MemoryConfiguration {




    export function getJsonObj(obj: MemoryConfiguration): object {
        const jsonObj = {...obj, ...{
            

                'limit': obj.limit ?
                
                
                model.MemoryLimitConfiguration.getJsonObj(obj.limit) : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: MemoryConfiguration): object {
        const jsonObj = {...obj, ...{
            

                    'limit': obj.limit ?
                
                
                model.MemoryLimitConfiguration.getDeserializedJsonObj(obj.limit) : undefined,

         }};

        
        
        return jsonObj;
    }
}
