// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to rename a registered model.
*/
export interface RenameRegisteredModelDetails {
    /**
    * Current name of the registered model.
    */
    'name': string;
    /**
    * New name for the registered model.
    */
    'newName'?: string;

}

export namespace RenameRegisteredModelDetails {



    export function getJsonObj(obj: RenameRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            

                'new_name': obj.newName,

        }};

        delete (jsonObj as Partial<RenameRegisteredModelDetails>).newName;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RenameRegisteredModelDetails): object {
        const jsonObj = {...obj, ...{
            

                'newName': (obj as any)["new_name"],

         }};

        delete (jsonObj as any)["new_name"];
        
        return jsonObj;
    }
}
