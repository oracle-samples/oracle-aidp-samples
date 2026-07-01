// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* event object details
*/
export interface TraceEvent {
    /**
    * event name
    */
    'name'?: string;
    /**
    * event start time
    */
    'timestamp'?: Date;
    /**
    * event attributes
    */
    'attributes'?: { [key: string]: any; };

}

export namespace TraceEvent {




    export function getJsonObj(obj: TraceEvent): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TraceEvent): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
