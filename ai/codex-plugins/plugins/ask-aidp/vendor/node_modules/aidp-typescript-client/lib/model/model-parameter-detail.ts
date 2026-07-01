// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The detail summary of each model parameter
*/
export interface ModelParameterDetail {
    /**
    * Internal code-friendly name for the parameter
    */
    'codeGenName'?: string;
    /**
    * Human-readable name for the parameter
    */
    'name'?: string;
    /**
    * Description of the parameter
    */
    'description'?: string;

}

export namespace ModelParameterDetail {




    export function getJsonObj(obj: ModelParameterDetail): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ModelParameterDetail): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
