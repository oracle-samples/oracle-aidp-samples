// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Mcp object type.
**/
export enum McpObjectType {
    Tool = "TOOL",
    Prompt = "PROMPT",
    Resource = "RESOURCE"
    
}

export namespace McpObjectType {
    export function getJsonObj(obj: McpObjectType): McpObjectType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: McpObjectType): McpObjectType {
        return obj;
    }
}

