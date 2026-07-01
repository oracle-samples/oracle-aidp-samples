// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The data to create/update a file.
*/
export interface UploadFileWithParDetails {
    /**
    * Action type on create or close.
    */
    'action'?: UploadFileWithParDetails.Action;
    /**
    * Etag that needs to be updated.
    */
    'eTag'?: string;
    /**
    * Size of the file needed when closed. Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'size'?: number;
    /**
    * System tags for this resource. Each key is predefined and scoped to a namespace.
* <p>
Example: {@code {\"orcl-cloud\": {\"free-tier-retained\": \"true\"}}}
* 
    */
    'systemTags'?: { [key: string]: { [key: string]: any; }; };

}

export namespace UploadFileWithParDetails {

    export enum Action {
    
    Create = "CREATE",
    Refresh = "REFRESH",
    Update = "UPDATE"

}





    export function getJsonObj(obj: UploadFileWithParDetails): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UploadFileWithParDetails): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
