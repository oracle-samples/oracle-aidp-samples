// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * Tool type.
**/
export enum ToolType {
    Rag = "RAG",
    Sql = "SQL",
    Prompt = "PROMPT",
    Nl2Sql = "NL2SQL",
    Mcp = "MCP",
    Custom = "CUSTOM",
    Http = "HTTP"
    
}

export namespace ToolType {
    export function getJsonObj(obj: ToolType): ToolType {
        return obj;
    }
    export function getDeserializedJsonObj(obj: ToolType): ToolType {
        return obj;
    }
}

