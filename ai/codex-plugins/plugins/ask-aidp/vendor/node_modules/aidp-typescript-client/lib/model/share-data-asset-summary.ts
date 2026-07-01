// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Summary information about a share data asset.
*/
export interface ShareDataAssetSummary {
    /**
    * The asset type for this update.
    */
    'type': model.ShareDataAssetType;
    /**
    * The data asset name for this operation.
* 
    */
    'name': string;
    /**
    * The ID of the user who created the share data asset.
* 
    */
    'createdBy': string;
    /**
    * Short description or comment.
    */
    'description'?: string;
    /**
    * The data asset catalog for this operation.
* 
    */
    'catalog'?: string;
    /**
    * Partition clause information, only applicable for TABLE.
* 
    */
    'partition'?: string;
    /**
    * The date and time the Delta Share Data Asset was created, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeCreated'?: Date;
    /**
    * The date and time the Delta Share Data Asset was updated, in the format defined by <a href=\"https://tools.ietf.org/html/rfc3339\" target=\"_blank\" rel=\"noopener noreferrer\">RFC 3339</a>.
* Example: {@code 2016-08-25T21:10:29.600Z}
* 
    */
    'timeUpdated'?: Date;

}

export namespace ShareDataAssetSummary {









    export function getJsonObj(obj: ShareDataAssetSummary): object {
        const jsonObj = {...obj, ...{
            








        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ShareDataAssetSummary): object {
        const jsonObj = {...obj, ...{
            








         }};

        
        
        return jsonObj;
    }
}
