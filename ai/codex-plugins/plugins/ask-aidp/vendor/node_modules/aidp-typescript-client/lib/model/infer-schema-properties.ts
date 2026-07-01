// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Properties to be supplied for infer schema.
*/
export interface InferSchemaProperties {
    /**
    * Name of the property.
    */
    'propertyName'?: string;
    /**
    * Property value.
    */
    'propertyValue'?: string;

}

export namespace InferSchemaProperties {



    export function getJsonObj(obj: InferSchemaProperties): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: InferSchemaProperties): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
