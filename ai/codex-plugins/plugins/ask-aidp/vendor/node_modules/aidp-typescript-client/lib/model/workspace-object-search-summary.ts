// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* An object in a Data Lake Workspace
* 
*/
export interface WorkspaceObjectSearchSummary {
    /**
    * The key of the object
    */
    'key': string;
    /**
    * A user-friendly name.
    */
    'displayName': string;
    /**
    * Description of the object
    */
    'description'?: string;
    /**
    * The type of the Workspace Object
    */
    'type': string;
    /**
    * Key of the workspace.
    */
    'workspaceKey'?: string;
    /**
    * Path of workspace object.
    */
    'path'?: string;
    /**
    * The date and time when the object was created, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated': Date;
    /**
    * The date and time when the object was updated, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;
    /**
    * OCID of the user who created this record
    */
    'createdBy'?: string;
    /**
    * Name of the user who created this record
    */
    'createdByName'?: string;
    /**
    * OCID of the user who updated this record
    */
    'updatedBy'?: string;
    /**
    * Name of the user who updated this record
    */
    'updatedByName'?: string;

}

export namespace WorkspaceObjectSearchSummary {













    export function getJsonObj(obj: WorkspaceObjectSearchSummary): object {
        const jsonObj = {...obj, ...{
            












        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: WorkspaceObjectSearchSummary): object {
        const jsonObj = {...obj, ...{
            












         }};

        
        
        return jsonObj;
    }
}
