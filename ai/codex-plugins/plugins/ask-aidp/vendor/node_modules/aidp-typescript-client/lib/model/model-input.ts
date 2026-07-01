// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model input.
*/
export interface ModelInput {
    /**
    * ID of the model input.
    */
    'modelId': string;

}

export namespace ModelInput {


    export function getJsonObj(obj: ModelInput): object {
        const jsonObj = {...obj, ...{
            
                'model_id': obj.modelId,

        }};

        delete (jsonObj as Partial<ModelInput>).modelId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelInput): object {
        const jsonObj = {...obj, ...{
            
                'modelId': (obj as any)["model_id"],

         }};

        delete (jsonObj as any)["model_id"];
        
        return jsonObj;
    }
}
