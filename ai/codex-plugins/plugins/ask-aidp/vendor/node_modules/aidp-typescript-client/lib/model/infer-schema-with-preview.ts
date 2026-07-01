// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Inferred schema and data.
*/
export interface InferSchemaWithPreview {
    /**
    * Column information obtained by inferring schema.
    */
    'schema': Array<model.InferSchemaColumn>;
    /**
    * Sample preview data.
    */
    'data': Array<{ [key: string]: string; }>;

}

export namespace InferSchemaWithPreview {



    export function getJsonObj(obj: InferSchemaWithPreview): object {
        const jsonObj = {...obj, ...{
            
                'schema': obj.schema ?
                
                obj.schema.map((item)=>{return model.InferSchemaColumn.getJsonObj(item)})
                
                 : undefined,

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: InferSchemaWithPreview): object {
        const jsonObj = {...obj, ...{
            
                    'schema': obj.schema ?
                
                obj.schema.map((item)=>{return model.InferSchemaColumn.getDeserializedJsonObj(item)})
                
                 : undefined,

         }};

        
        
        return jsonObj;
    }
}
