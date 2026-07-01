// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Options controlling Agent Flow diagram validation behavior.
*/
export interface AgentFlowValidationOptions {
    /**
    * Whether to skip optional LakeFlow validation when deep validation is requested.
    */
    'shouldSkipLakeFlowValidation'?: boolean;
    /**
    * Whether warning issues should be included in the validation result.
    */
    'shouldIncludeWarnings'?: boolean;

}

export namespace AgentFlowValidationOptions {



    export function getJsonObj(obj: AgentFlowValidationOptions): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AgentFlowValidationOptions): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
