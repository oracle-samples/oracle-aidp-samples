// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Alias of the registered model
*/
export interface RegisteredModelAlias {
    /**
    * The name of the alias.
    */
    'alias'?: string;
    /**
    * The model version number that the alias points to.
    */
    'version'?: string;

}

export namespace RegisteredModelAlias {



    export function getJsonObj(obj: RegisteredModelAlias): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RegisteredModelAlias): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
