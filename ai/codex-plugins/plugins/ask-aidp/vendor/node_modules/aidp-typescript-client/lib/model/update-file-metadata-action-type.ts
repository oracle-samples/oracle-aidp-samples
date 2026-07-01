// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Metadata update action.
**/
export enum UpdateFileMetadataActionType {
    Update = "UPDATE",
    Append = "APPEND",
    Replace = "REPLACE",
    Reset = "RESET"
    
}

export namespace UpdateFileMetadataActionType {
    export function getJsonObj(obj: UpdateFileMetadataActionType): UpdateFileMetadataActionType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: UpdateFileMetadataActionType): UpdateFileMetadataActionType {
        return obj;
    }
}

