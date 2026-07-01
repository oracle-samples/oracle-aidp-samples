// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Job dependency. Represents a job with its dependencies. Can be used for both root jobs and nested jobs.
*/
export interface JobDependency extends model.Dependency {
    /**
    * Unique identifier for the job.
    */
    'key': string;
    /**
    * List of dependencies for this job.
    */
    'dependencies': Array<model.Dependency>;

   "type": string;
}

export namespace JobDependency {



    export function getJsonObj(obj: JobDependency, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Dependency.getJsonObj(obj) as JobDependency, ...{
            

                'dependencies': obj.dependencies ?
                
                obj.dependencies.map((item)=>{return model.Dependency.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    export const type = 'JOB';
    export function getDeserializedJsonObj(obj: JobDependency, isParentJsonObj?: boolean): object {
        const jsonObj = {...isParentJsonObj? obj : model.Dependency.getDeserializedJsonObj(obj) as JobDependency, ...{
            

                    'dependencies': obj.dependencies ?
                
                obj.dependencies.map((item)=>{return model.Dependency.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
