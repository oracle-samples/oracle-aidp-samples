// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Session Variable for each Session Context.
*/
export interface SessionVariable {
    /**
    * Name of the Session Variable.
    */
    'name': string;
    /**
    * Value of this Session Variable for this session
    */
    'value'?: string;

}

export namespace SessionVariable {



    export function getJsonObj(obj: SessionVariable): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SessionVariable): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
