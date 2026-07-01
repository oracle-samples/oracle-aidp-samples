// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of attached sessions to a cluster
*/
export interface AttachedSession {
    /**
    * The type of the attached session.
    */
    'type'?: AttachedSession.Type;
    /**
    * The path of the attached file.
    */
    'path'?: string;
    /**
    * The state of the attached file.
    */
    'state'?: AttachedSession.State;
    /**
    * The time of the last command of file was run in this cluster.
* 
    */
    'lastCommandRun'?: string;

}

export namespace AttachedSession {

    export enum Type {
    
    Notebook = "NOTEBOOK",
    File = "FILE",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}



    export enum State {
    
    Active = "ACTIVE",
    Idle = "IDLE",
    /**
    * This value is used if a service returns a value for this enum that is not recognized by this
    * version of the SDK.
    */
    UnknownValue = "UNKNOWN_VALUE"
}



    export function getJsonObj(obj: AttachedSession): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: AttachedSession): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
