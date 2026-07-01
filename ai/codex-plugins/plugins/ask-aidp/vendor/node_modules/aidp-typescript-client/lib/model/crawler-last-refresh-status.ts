// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * The status for an entity refresh.
**/
export enum CrawlerLastRefreshStatus {
    Failed = "FAILED",
    Success = "SUCCESS",
    InProgress = "IN_PROGRESS",
    
    /**
     * This value is used if a service returns a value for this enum that is not recognized by this
     * version of the SDK.
     */
    UnknownValue = "UNKNOWN_VALUE"
}

export namespace CrawlerLastRefreshStatus {
    export function getJsonObj(obj: CrawlerLastRefreshStatus): CrawlerLastRefreshStatus {
        return obj;
    }
    export function getDeserializedJsonObj(obj: CrawlerLastRefreshStatus): CrawlerLastRefreshStatus {
        return obj;
    }
}

