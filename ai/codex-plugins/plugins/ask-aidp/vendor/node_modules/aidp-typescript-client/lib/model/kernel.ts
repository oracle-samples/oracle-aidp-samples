// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Notebook kernel information.
*/
export interface Kernel {
    /**
    * UUID of kernel.
    */
    'id'?: string;
    /**
    * Kernel spec name. (Example python3)
    */
    'name'?: string;
    /**
    * ISO 8601 timestamp for last-seen activity on this kernel.
* Use this in combination with execution_state == 'idle' to identify
* which kernels have been idle since a given time.
* Timestamps will be UTC, indicated 'Z' suffix.
* 
    */
    'lastActivity'?: string;
    /**
    * The number of active connections to this kernel.
*  Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'connections'?: number;
    /**
    * Current execution state of Kernel (typically 'idle' or 'busy', but may be other values, such as 'starting').
* Added in notebook server 5.0.
* 
    */
    'executionState'?: Kernel.ExecutionState;

}

export namespace Kernel {





    export enum ExecutionState {
    
    Unknown = "unknown",
    Starting = "starting",
    Idle = "idle",
    Busy = "busy",
    Terminating = "terminating",
    Restarting = "restarting",
    Autorestarting = "autorestarting",
    Dead = "dead",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}


    export function getJsonObj(obj: Kernel): object {
        const jsonObj = {...obj, ...{
            


                'last_activity': obj.lastActivity,


                'execution_state': obj.executionState,

        }};

        delete (jsonObj as Partial<Kernel>).lastActivity;delete (jsonObj as Partial<Kernel>).executionState;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: Kernel): object {
        const jsonObj = {...obj, ...{
            


                'lastActivity': (obj as any)["last_activity"],


                'executionState': (obj as any)["execution_state"],

         }};

        delete (jsonObj as any)["last_activity"];delete (jsonObj as any)["execution_state"];
        
        return jsonObj;
    }
}
