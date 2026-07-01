// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Kernel summary
*/
export interface KernelSummary {
    /**
    * UUID of the kernel.
    */
    'id'?: string;
    /**
    * Kernel spec name.
    */
    'name': string;
    /**
    * ISO 8601 timestamp for the last-seen activity on this kernel.
* Use this in combination with execution_state == 'idle' to identify
* which kernels have been idle since a given time.
* Timestamps will be UTC, indicated 'Z' suffix.
* Added in notebook server 5.0.
* 
    */
    'lastActivity'?: string;
    /**
    * The number of active connections to this kernel.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'connections'?: number;
    /**
    * Current execution state of the kernel. Typically 'idle' or 'busy', but may be other values, such as 'starting'.
* 
    */
    'executionState'?: string;

}

export namespace KernelSummary {






    export function getJsonObj(obj: KernelSummary): object {
        const jsonObj = {...obj, ...{
            


                'last_activity': obj.lastActivity,


                'execution_state': obj.executionState,

        }};

        delete (jsonObj as Partial<KernelSummary>).lastActivity;delete (jsonObj as Partial<KernelSummary>).executionState;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: KernelSummary): object {
        const jsonObj = {...obj, ...{
            


                'lastActivity': (obj as any)["last_activity"],


                'executionState': (obj as any)["execution_state"],

         }};

        delete (jsonObj as any)["last_activity"];delete (jsonObj as any)["execution_state"];
        
        return jsonObj;
    }
}
