// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Specifies the dependency graph of the task. All the tasks mentioned in this field need to be completed before executing this task.
*/
export interface DependsOn {
    /**
    * The name of the task that it depends on.
    */
    'taskKey': string;
    /**
    * Specified on condition task dependencies. The outcome of the dependent task should be met for this task to be executed.
    */
    'outcome'?: string;

}

export namespace DependsOn {



    export function getJsonObj(obj: DependsOn): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DependsOn): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
