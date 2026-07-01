// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to perform grant/revoke consumption access on a share to/from a recipient.
*/
export interface ManageShareAccessDetails {
    /**
    * The action. Either Grant or Revoke.
    */
    'action': model.ShareAccessAction;
    /**
    * The simplified name of the grantee.
    */
    'recipient': string;

}

export namespace ManageShareAccessDetails {



    export function getJsonObj(obj: ManageShareAccessDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageShareAccessDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
