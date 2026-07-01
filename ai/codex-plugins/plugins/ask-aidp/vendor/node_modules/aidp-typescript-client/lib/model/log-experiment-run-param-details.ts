// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of an experiment run param.
*/
export interface LogExperimentRunParamDetails {
    /**
    * Unique identifier for the run.
    */
    'runId': string;
    /**
    * Name of the param.
    */
    'key': string;
    /**
    * Value of the param.
    */
    'value': string;

}

export namespace LogExperimentRunParamDetails {




    export function getJsonObj(obj: LogExperimentRunParamDetails): object {
        const jsonObj = {...obj, ...{
            
                'run_id': obj.runId,



        }};

        delete (jsonObj as Partial<LogExperimentRunParamDetails>).runId;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LogExperimentRunParamDetails): object {
        const jsonObj = {...obj, ...{
            
                'runId': (obj as any)["run_id"],



         }};

        delete (jsonObj as any)["run_id"];
        
        return jsonObj;
    }
}
