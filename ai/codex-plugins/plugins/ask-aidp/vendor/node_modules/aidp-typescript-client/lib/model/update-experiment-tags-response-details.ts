// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for updating tags of an experiment.
*/
export interface UpdateExperimentTagsResponseDetails {

}

export namespace UpdateExperimentTagsResponseDetails {

    export function getJsonObj(obj: UpdateExperimentTagsResponseDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateExperimentTagsResponseDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
