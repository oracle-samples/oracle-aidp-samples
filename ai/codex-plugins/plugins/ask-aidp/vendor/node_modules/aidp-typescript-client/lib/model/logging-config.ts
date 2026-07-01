// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Logging configuration.
*/
export interface LoggingConfig {

   "type": string;
}

export namespace LoggingConfig {

    export function getJsonObj(obj: LoggingConfig): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "OCI_LOGGING":
                    return model.OciLogging.getJsonObj(<model.OciLogging>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: LoggingConfig): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "OCI_LOGGING":
                    return model.OciLogging.getDeserializedJsonObj(<model.OciLogging>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
