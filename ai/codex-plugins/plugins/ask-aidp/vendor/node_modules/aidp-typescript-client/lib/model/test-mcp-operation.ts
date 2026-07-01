// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Represents a type of testing on mcp.
*/
export interface TestMcpOperation {

   "testType": string;
}

export namespace TestMcpOperation {

    export function getJsonObj(obj: TestMcpOperation): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "testType" in obj && obj.testType) {
            switch (obj.testType) {
                case "CONNECTION":
                    return model.TestMcpConnection.getJsonObj(<model.TestMcpConnection>(<object>jsonObj), true);
                case "EXTERNAL_TOOL":
                    return model.TestMcpExternalTool.getJsonObj(<model.TestMcpExternalTool>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.testType}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TestMcpOperation): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "testType" in obj && obj.testType) {
            switch (obj.testType) {
                case "CONNECTION":
                    return model.TestMcpConnection.getDeserializedJsonObj(<model.TestMcpConnection>(<object>jsonObj), true);
                case "EXTERNAL_TOOL":
                    return model.TestMcpExternalTool.getDeserializedJsonObj(<model.TestMcpExternalTool>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.testType}`)
        }
        }
        return jsonObj;
    }
}
