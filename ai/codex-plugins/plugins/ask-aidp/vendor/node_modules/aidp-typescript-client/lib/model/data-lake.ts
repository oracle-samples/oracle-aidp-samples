// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A DataLake is a description of a DataLake.
* <p>
To use any of the API operations, you must be authorized in an IAM policy. If you're not authorized, talk to
* an administrator. If you're an administrator who needs to write policies to give users access, see
* [Getting Started with Policies]({{DOC_SERVER_URL}}/iaas/Content/Identity/policiesgs/get-started-with-policies.htm).
* 
*/
export interface DataLake {

}

export namespace DataLake {

    export function getJsonObj(obj: DataLake): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DataLake): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        return jsonObj;
    }
}
