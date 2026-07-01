// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Inferred schema from location.
*/
export interface InferSchema {
    /**
    * Column information obtained by inferring schema.
    */
    'inferSchemaColumn': Array<model.InferSchemaColumn>;

}

export namespace InferSchema {


    export function getJsonObj(obj: InferSchema): object {
        const jsonObj = {...obj, ...{
            
                'inferSchemaColumn': obj.inferSchemaColumn ?
                
                obj.inferSchemaColumn.map((item)=>{return model.InferSchemaColumn.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: InferSchema): object {
        const jsonObj = {...obj, ...{
            
                    'inferSchemaColumn': obj.inferSchemaColumn ?
                
                obj.inferSchemaColumn.map((item)=>{return model.InferSchemaColumn.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
