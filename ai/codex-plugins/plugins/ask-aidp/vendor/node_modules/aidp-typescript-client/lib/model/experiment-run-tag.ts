// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Run tag.
*/
export interface ExperimentRunTag {
    /**
    * Key of the run tag.
    */
    'key'?: string;
    /**
    * Value of the run tag.
    */
    'value'?: string;

}

export namespace ExperimentRunTag {



    export function getJsonObj(obj: ExperimentRunTag): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunTag): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
