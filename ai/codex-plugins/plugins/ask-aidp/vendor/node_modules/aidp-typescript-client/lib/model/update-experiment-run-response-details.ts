// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of updated run info.
*/
export interface UpdateExperimentRunResponseDetails {
    'runInfo': model.ExperimentRunInfo;

}

export namespace UpdateExperimentRunResponseDetails {


    export function getJsonObj(obj: UpdateExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_info': obj.runInfo ?
                
                
                model.ExperimentRunInfo.getJsonObj(obj.runInfo) : undefined,
        }};

        delete (jsonObj as Partial<UpdateExperimentRunResponseDetails>).runInfo;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateExperimentRunResponseDetails): object {
        const jsonObj = {...obj, ...{
            
                    'runInfo': (obj as any)["run_info"] ?
                
                
                model.ExperimentRunInfo.getDeserializedJsonObj((obj as any)["run_info"]) : undefined,
         }};

        delete (jsonObj as any)["run_info"];
        
        return jsonObj;
    }
}
