// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Logged model tag.
*/
export interface LoggedModelTag {
    /**
    * Key of the tag.
    */
    'key': string;
    /**
    * Value of the tag.
    */
    'value': string;

}

export namespace LoggedModelTag {



    export function getJsonObj(obj: LoggedModelTag): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LoggedModelTag): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
