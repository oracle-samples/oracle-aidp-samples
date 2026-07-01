// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Key of the ExperimentRun tag.
*/
export interface ExperimentRunTagKey {
    /**
    * Tag key.
    */
    'key': string;

}

export namespace ExperimentRunTagKey {


    export function getJsonObj(obj: ExperimentRunTagKey): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentRunTagKey): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
