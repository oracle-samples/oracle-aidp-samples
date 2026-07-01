// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The privilege for a volume
**/
export enum VolumePrivilege {
    Read = "READ",
    Write = "WRITE",
    Admin = "ADMIN"
    
}

export namespace VolumePrivilege {
    export function getJsonObj(obj: VolumePrivilege): VolumePrivilege {
        return obj;
    }
    export function getDeserializedJsonObj(obj: VolumePrivilege): VolumePrivilege {
        return obj;
    }
}

