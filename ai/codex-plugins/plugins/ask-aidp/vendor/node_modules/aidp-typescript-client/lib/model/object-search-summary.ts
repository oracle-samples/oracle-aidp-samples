// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A Search object in a Data Lake
* 
*/
export interface ObjectSearchSummary {
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
    'description': string;
    /**
    * The type of the Object
    */
    'type': string;
    /**
    * Path of object.
    */
    'path': string;
    /**
    * The date and time the object was created, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated'?: Date;
    /**
    * The date and time the object was updated, in the format defined by [RFC 3339](https://tools.ietf.org/html/rfc3339).
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated': Date;
    /**
    * Name of the owner of the record
    */
    'owner'?: string;
    /**
    * Name of the user who created this record
    */
    'createdByName': string;
    /**
    * Name of the user who updated this record
    */
    'updatedByName'?: string;
    /**
    * Key of the workspace.
    */
    'workspaceKey'?: string;
    /**
    * Name of the workspace.
    */
    'workspaceName'?: string;
    /**
    * Key of the catalog.
    */
    'catalogKey'?: string;
    /**
    * Key of the schema.
    */
    'schemaKey'?: string;
    /**
    * Highlights related to this notebook object
    */
    'hitHighlights'?: Array<string>;

}

export namespace ObjectSearchSummary {
















    export function getJsonObj(obj: ObjectSearchSummary): object {
        const jsonObj = {...obj, ...{
            















        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ObjectSearchSummary): object {
        const jsonObj = {...obj, ...{
            















         }};

        
        
        return jsonObj;
    }
}
