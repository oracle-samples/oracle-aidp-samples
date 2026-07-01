// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The parameter details of each model
*/
export interface ModelParameter {
    /**
    * name of the model
    */
    'modelName': string;
    'modelParameters': model.ModelParameterResponse;

}

export namespace ModelParameter {



    export function getJsonObj(obj: ModelParameter): object {
        const jsonObj = {...obj, ...{
            

                'modelParameters': obj.modelParameters ?
                
                
                model.ModelParameterResponse.getJsonObj(obj.modelParameters) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelParameter): object {
        const jsonObj = {...obj, ...{
            

                    'modelParameters': obj.modelParameters ?
                
                
                model.ModelParameterResponse.getDeserializedJsonObj(obj.modelParameters) : undefined,
         }};

        
        
        return jsonObj;
    }
}
