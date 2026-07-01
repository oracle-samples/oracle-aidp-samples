// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A tag associated with an experiment.
*/
export interface ExperimentTag {
    /**
    * Key of the experiment tag.
    */
    'key': string;
    /**
    * Value of the experiment tag.
    */
    'value'?: string;

}

export namespace ExperimentTag {



    export function getJsonObj(obj: ExperimentTag): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ExperimentTag): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
