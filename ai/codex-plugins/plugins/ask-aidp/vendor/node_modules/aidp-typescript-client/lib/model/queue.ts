// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Queue configuration for job.
*/
export interface Queue {
    /**
    * True if job queue is enabled.
    */
    'isEnabled': boolean;

}

export namespace Queue {


    export function getJsonObj(obj: Queue): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Queue): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
