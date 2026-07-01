// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for setting tag on an experiment.
*/
export interface SetExperimentTagResponseDetails {

}

export namespace SetExperimentTagResponseDetails {

    export function getJsonObj(obj: SetExperimentTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SetExperimentTagResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
