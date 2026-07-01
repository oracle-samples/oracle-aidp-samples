// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* List of libraries to be installed on the cluster.
*/
export interface Libraries {
    /**
    * URI of the jar to be installed.
    */
    'jar'?: string;

}

export namespace Libraries {


    export function getJsonObj(obj: Libraries): object {
        const jsonObj = {...obj, ...{
            

        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Libraries): object {
        const jsonObj = {...obj, ...{
            

         }};

        
        
        return jsonObj;
    }
}
