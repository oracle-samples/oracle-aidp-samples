// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Log group details.
*/
export interface LogGroup {
    /**
    * The compartment ID of the log group.
    */
    'compartmentId'?: string;
    /**
    * Log group name.
    */
    'groupName'?: string;
    /**
    * Log name.
    */
    'logName'?: string;

}

export namespace LogGroup {




    export function getJsonObj(obj: LogGroup): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LogGroup): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
