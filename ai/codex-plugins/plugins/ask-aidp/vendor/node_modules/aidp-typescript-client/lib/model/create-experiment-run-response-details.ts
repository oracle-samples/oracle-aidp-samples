// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of created run.
*/
export interface CreateExperimentRunResponseDetails {
    'run': model.ExperimentRun;

}

export namespace CreateExperimentRunResponseDetails {


    export function getJsonObj(obj: CreateExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'run': obj.run ?
                
                
                model.ExperimentRun.getJsonObj(obj.run) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'run': obj.run ?
                
                
                model.ExperimentRun.getDeserializedJsonObj(obj.run) : undefined,
         }};

        
        
        return jsonObj;
    }
}
