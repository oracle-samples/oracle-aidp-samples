// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of run.
*/
export interface GetExperimentRunResponseDetails {
    'run': model.ExperimentRun;

}

export namespace GetExperimentRunResponseDetails {


    export function getJsonObj(obj: GetExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'run': obj.run ?
                
                
                model.ExperimentRun.getJsonObj(obj.run) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: GetExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'run': obj.run ?
                
                
                model.ExperimentRun.getDeserializedJsonObj(obj.run) : undefined,
         }};

        
        
        return jsonObj;
    }
}
