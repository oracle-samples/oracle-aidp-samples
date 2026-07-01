// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Infer schema details.
*/
export interface PerformInferSchemaDetails {
    /**
    * Data format of the schema location.
    */
    'dataFormat': model.DataFormat;
    /**
    * Location of the table to infer schema.
    */
    'location': string;
    /**
    * Number of root partition folders to scan. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'numberOfPartitions'?: number;
    /**
    * Properties which are needed for inferring schema.
    */
    'inferSchemaProperties'?: Array<model.InferSchemaProperties>;

}

export namespace PerformInferSchemaDetails {





    export function getJsonObj(obj: PerformInferSchemaDetails): object {
        const jsonObj = {...obj, ...{
            



                'inferSchemaProperties': obj.inferSchemaProperties ?
                
                obj.inferSchemaProperties.map((item)=>{return model.InferSchemaProperties.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: PerformInferSchemaDetails): object {
        const jsonObj = {...obj, ...{
            



                    'inferSchemaProperties': obj.inferSchemaProperties ?
                
                obj.inferSchemaProperties.map((item)=>{return model.InferSchemaProperties.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
