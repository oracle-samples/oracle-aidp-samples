// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to detach a workspace object from a cluster.
*/
export interface DetachWorkspaceObjectFromClusterDetails {

}

export namespace DetachWorkspaceObjectFromClusterDetails {

    export function getJsonObj(obj: DetachWorkspaceObjectFromClusterDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DetachWorkspaceObjectFromClusterDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
