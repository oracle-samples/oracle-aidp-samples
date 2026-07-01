// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information of file in the volume
*/
export interface VolumeFileSummary {
    /**
    * The fully qualified path of the volume file.
* Example: /Shared/Folder1/sample.csv
* 
    */
    'path': string;
    /**
    * The name of the volume file. This will be the name of the file/folder in the volume.
* Example: sample.csv, Folder1
* 
    */
    'displayName': string;
    /**
    * The date and time the file was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated': Date;
    /**
    * The date and time the file was updated, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;
    /**
    * The type of volume file.
    */
    'type': VolumeFileSummary.Type;
    /**
    * The description for the file and folder.
    */
    'description'?: string;
    /**
    * Metadata details of file or folder objects.
    */
    'metadata'?: { [key: string]: string; };
    /**
    * Etag combining data and metadata.
    */
    'compositeEtag'?: string;
    /**
    * System tags for this resource. Each key is predefined and scoped to a namespace.
* <p>
Example: {@code {\"orcl-cloud\": {\"free-tier-retained\": \"true\"}}}
* 
    */
    'systemTags'?: { [key: string]: { [key: string]: any; }; };
    /**
    * OCID of the user who created this file.
    */
    'createdBy'?: string;
    /**
    * Name of the user who created this file.
    */
    'createdByName'?: string;

}

export namespace VolumeFileSummary {





    export enum Type {
    
    File = "FILE",
    Folder = "FOLDER",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}








    export function getJsonObj(obj: VolumeFileSummary): object {
        const jsonObj = {...obj, ...{
            











        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: VolumeFileSummary): object {
        const jsonObj = {...obj, ...{
            











         }};

        
        
        return jsonObj;
    }
}
