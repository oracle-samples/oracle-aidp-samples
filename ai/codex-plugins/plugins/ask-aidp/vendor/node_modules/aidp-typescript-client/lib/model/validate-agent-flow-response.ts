// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The response object for validating the agent flow json
*/
export interface ValidateAgentFlowResponse {
    /**
    * true if valid, else false
    */
    'isValidAgentFlow': boolean;
    /**
    * List of validation errors encountered in the diagram.
    */
    'validationError'?: Array<model.ValidationError>;

}

export namespace ValidateAgentFlowResponse {



    export function getJsonObj(obj: ValidateAgentFlowResponse): object {
        const jsonObj = {...obj, ...{
            

                'validationError': obj.validationError ?
                
                obj.validationError.map((item)=>{return model.ValidationError.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ValidateAgentFlowResponse): object {
        const jsonObj = {...obj, ...{
            

                    'validationError': obj.validationError ?
                
                obj.validationError.map((item)=>{return model.ValidationError.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
