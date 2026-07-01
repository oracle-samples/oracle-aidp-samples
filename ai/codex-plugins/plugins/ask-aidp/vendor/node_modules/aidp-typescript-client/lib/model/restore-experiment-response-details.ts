// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for restoring an experiment.
*/
export interface RestoreExperimentResponseDetails {

}

export namespace RestoreExperimentResponseDetails {

    export function getJsonObj(obj: RestoreExperimentResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RestoreExperimentResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
