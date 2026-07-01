// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Base class for all dependencies, including files, compute resources, and nested jobs.
*/
export interface Dependency {

   "type": string;
}

export namespace Dependency {

    export function getJsonObj(obj: Dependency): object {
        const jsonObj = {...obj, ...{
            
        }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "COMPUTE":
                    return model.ComputeDependency.getJsonObj(<model.ComputeDependency>(<object>jsonObj), true);
                case "FILE":
                    return model.FileDependency.getJsonObj(<model.FileDependency>(<object>jsonObj), true);
                case "JOB":
                    return model.JobDependency.getJsonObj(<model.JobDependency>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)

        }
        }
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Dependency): object {
        const jsonObj = {...obj, ...{
            
         }};

        
        
        if (obj && "type" in obj && obj.type) {
            switch (obj.type) {
                case "COMPUTE":
                    return model.ComputeDependency.getDeserializedJsonObj(<model.ComputeDependency>(<object>jsonObj), true);
                case "FILE":
                    return model.FileDependency.getDeserializedJsonObj(<model.FileDependency>(<object>jsonObj), true);
                case "JOB":
                    return model.JobDependency.getDeserializedJsonObj(<model.JobDependency>(<object>jsonObj), true);
                default:
                    if (common.LOG.logger) common.LOG.logger.info(`Unknown value for: ${obj.type}`)
        }
        }
        return jsonObj;
    }
}
