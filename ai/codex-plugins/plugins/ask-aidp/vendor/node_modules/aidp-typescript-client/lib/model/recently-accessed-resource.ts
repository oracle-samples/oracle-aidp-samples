// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The resources which were recently accessed by a user.
* 
*/
export interface RecentlyAccessedResource {
    /**
    * Last active workspaceKey.
    */
    'lastAccessedWorkspaceKey': string;
    /**
    * Last active workspace display name.
    */
    'lastAccessedWorkspaceDisplayName': string;

}

export namespace RecentlyAccessedResource {



    export function getJsonObj(obj: RecentlyAccessedResource): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: RecentlyAccessedResource): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
