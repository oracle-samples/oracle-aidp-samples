// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Response object of generate PAR with download API.
*/
export interface DownloadFileWithParResult {
    /**
    * String representing the PAR URL.
* 
    */
    'parUrl'?: string;
    /**
    * Etag after creating or closing a file.
* 
    */
    'eTag': string;
    /**
    * The object storage URI which has bucket and namespace information.
* 
    */
    'locationUri': string;
    /**
    * File size in bytes.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'size': number;
    /**
    * The user who created the file.
* 
    */
    'createdBy'?: string;
    /**
    * The user who last updated the file.
* 
    */
    'updatedBy'?: string;
    /**
    * The time at which the file was created.
* 
    */
    'createdTime'?: string;
    /**
    * The last modified time of the file.
* 
    */
    'updatedTime'?: string;
    /**
    * The file description.
* 
    */
    'description'?: string;
    /**
    * System tags for this resource. Each key is predefined and scoped to a namespace.
* <p>
Example: {@code {\"orcl-cloud\": {\"free-tier-retained\": \"true\"}}}
* 
    */
    'systemTags'?: { [key: string]: { [key: string]: any; }; };

}

export namespace DownloadFileWithParResult {











    export function getJsonObj(obj: DownloadFileWithParResult): object {
        const jsonObj = {...obj, ...{
            










        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DownloadFileWithParResult): object {
        const jsonObj = {...obj, ...{
            










         }};

        
        
        return jsonObj;
    }
}
