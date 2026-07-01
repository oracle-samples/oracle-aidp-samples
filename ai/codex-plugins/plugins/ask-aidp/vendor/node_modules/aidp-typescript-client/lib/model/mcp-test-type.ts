// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Mcp test type.
**/
export enum McpTestType {
    Connection = "CONNECTION",
    ExternalTool = "EXTERNAL_TOOL"
    
}

export namespace McpTestType {
    export function getJsonObj(obj: McpTestType): McpTestType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: McpTestType): McpTestType {
        return obj;
    }
}

