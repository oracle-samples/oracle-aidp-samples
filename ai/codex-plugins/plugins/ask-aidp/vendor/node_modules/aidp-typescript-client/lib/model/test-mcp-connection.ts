// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Test mcp tool connection
*/
export interface TestMcpConnection extends model.TestMcpOperation {

   "testType": string;
}

export namespace TestMcpConnection {

    export function getJsonObj(obj: TestMcpConnection, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestMcpOperation.getJsonObj(obj) as TestMcpConnection, ...{
            
        }};

        
        
        return jsonObj;
    }
    export const testType = 'CONNECTION';
    export function getDeserializedJsonObj(obj: TestMcpConnection, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestMcpOperation.getDeserializedJsonObj(obj) as TestMcpConnection, ...{
            
         }};

        
        
        return jsonObj;
    }
}
