// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");

/**
 * UI position of an input or output port on a node.
**/
export enum NodePortPosition {
    Top = "TOP",
    Bottom = "BOTTOM",
    Right = "RIGHT",
    Left = "LEFT"
    
}

export namespace NodePortPosition {
    export function getJsonObj(obj: NodePortPosition): NodePortPosition {
        return obj;
    }
    export function getDeserializedJsonObj(obj: NodePortPosition): NodePortPosition {
        return obj;
    }
}

