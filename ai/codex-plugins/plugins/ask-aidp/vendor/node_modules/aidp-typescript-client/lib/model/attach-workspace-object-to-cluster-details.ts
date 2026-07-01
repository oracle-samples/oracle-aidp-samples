// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details to attach a workspace object to a cluster.
*/
export interface AttachWorkspaceObjectToClusterDetails {

}

export namespace AttachWorkspaceObjectToClusterDetails {

    export function getJsonObj(obj: AttachWorkspaceObjectToClusterDetails): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AttachWorkspaceObjectToClusterDetails): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
