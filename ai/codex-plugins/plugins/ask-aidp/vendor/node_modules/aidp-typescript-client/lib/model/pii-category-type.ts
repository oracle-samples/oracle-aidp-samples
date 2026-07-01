// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * PII Category types
**/
export enum PiiCategoryType {
    Person = "PERSON",
    Address = "ADDRESS",
    TelephoneNumber = "TELEPHONE_NUMBER",
    Email = "EMAIL"
    
}

export namespace PiiCategoryType {
    export function getJsonObj(obj: PiiCategoryType): PiiCategoryType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: PiiCategoryType): PiiCategoryType {
        return obj;
    }
}

