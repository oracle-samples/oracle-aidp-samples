// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Test mcp tool connection
*/
export interface TestMcpExternalTool extends model.TestMcpOperation {
    /**
    * name of external tool to test
    */
    'externalToolName': string;
    'paramValues'?: model.TestToolParamValues;

   "testType": string;
}

export namespace TestMcpExternalTool {



    export function getJsonObj(obj: TestMcpExternalTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestMcpOperation.getJsonObj(obj) as TestMcpExternalTool, ...{
            

                'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getJsonObj(obj.paramValues) : undefined,
        }};

        
        
        return jsonObj;
    }
    export const testType = 'EXTERNAL_TOOL';
    export function getDeserializedJsonObj(obj: TestMcpExternalTool, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.TestMcpOperation.getDeserializedJsonObj(obj) as TestMcpExternalTool, ...{
            

                    'paramValues': obj.paramValues ?
                
                
                model.TestToolParamValues.getDeserializedJsonObj(obj.paramValues) : undefined,
         }};

        
        
        return jsonObj;
    }
}
