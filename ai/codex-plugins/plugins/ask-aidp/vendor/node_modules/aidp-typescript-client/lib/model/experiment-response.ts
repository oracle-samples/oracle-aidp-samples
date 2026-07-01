// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object for getting an experiment.
*/
export interface ExperimentResponse {
    'experiment': model.Experiment;

}

export namespace ExperimentResponse {


    export function getJsonObj(obj: ExperimentResponse): object {
        const jsonObj = {...obj, ...{
            
                'experiment': obj.experiment ?
                
                
                model.Experiment.getJsonObj(obj.experiment) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentResponse): object {
        const jsonObj = {...obj, ...{
            
                    'experiment': obj.experiment ?
                
                
                model.Experiment.getDeserializedJsonObj(obj.experiment) : undefined,
         }};

        
        
        return jsonObj;
    }
}
