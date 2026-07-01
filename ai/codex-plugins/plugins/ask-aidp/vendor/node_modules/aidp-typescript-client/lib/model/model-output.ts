// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Model output.
*/
export interface ModelOutput {
    /**
    * ID of the model input.
    */
    'modelId': string;
    /**
    * Step at which the model was produced. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'step'?: number;

}

export namespace ModelOutput {



    export function getJsonObj(obj: ModelOutput): object {
        const jsonObj = {...obj, ...{
            
                'model_id': obj.modelId,


        }};

        delete (jsonObj as Partial<ModelOutput>).modelId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelOutput): object {
        const jsonObj = {...obj, ...{
            
                'modelId': (obj as any)["model_id"],


         }};

        delete (jsonObj as any)["model_id"];
        
        return jsonObj;
    }
}
