// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The property of the view.
*/
export interface ViewProperty {
    /**
    * Property name.
    */
    'propertyName': string;
    /**
    * Property value.
    */
    'propertyValue'?: string;

}

export namespace ViewProperty {



    export function getJsonObj(obj: ViewProperty): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ViewProperty): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
