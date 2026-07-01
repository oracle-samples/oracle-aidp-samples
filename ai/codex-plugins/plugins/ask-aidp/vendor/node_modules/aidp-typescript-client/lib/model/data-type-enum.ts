// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Possible types of data types
**/
export enum DataTypeEnum {
    String = "STRING",
    Date = "DATE",
    Integer = "INTEGER"
    
}

export namespace DataTypeEnum {
    export function getJsonObj(obj: DataTypeEnum): DataTypeEnum {
        return obj;
    }
    export function getDeserializedJsonObj(obj: DataTypeEnum): DataTypeEnum {
        return obj;
    }
}

