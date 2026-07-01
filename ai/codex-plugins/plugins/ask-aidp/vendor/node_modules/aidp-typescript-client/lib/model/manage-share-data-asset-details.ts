// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Information to update data assets on a share.
*/
export interface ManageShareDataAssetDetails {
    /**
    * The action of this update.
    */
    'action': model.ShareDataAssetAction;
    /**
    * The asset type for this update.
    */
    'type': model.ShareDataAssetType;
    /**
    * The data asset name for this operation. For relational assets, it should be fully qualified name. For example, catalog.schema or catalog.schema.table.
* 
    */
    'name': string;
    /**
    * The data asset description for this operation.
* 
    */
    'description'?: string;
    /**
    * Partition clause information, only applicable for TABLE.
* 
    */
    'partition'?: string;
    /**
    * The data asset alias for this operation, only applicable for TABLE and VIEW.
* 
    */
    'alias'?: string;

}

export namespace ManageShareDataAssetDetails {







    export function getJsonObj(obj: ManageShareDataAssetDetails): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ManageShareDataAssetDetails): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
