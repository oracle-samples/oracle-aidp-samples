// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Session Variable Details.
*/
export interface SessionVariableDetails {
    /**
    * Name of the Session Variable.
    */
    'name': string;
    /**
    * Description of the Session Variable
    */
    'description'?: string;
    /**
    * If this Variable is required or not
    */
    'isRequired'?: boolean;
    /**
    * If we should log this Session Variable or not
    */
    'shouldLog'?: boolean;
    /**
    * True if Session Variable is defined by System
    */
    'isSystem'?: boolean;
    /**
    * Default Value of this Session Variable
    */
    'value'?: string;

}

export namespace SessionVariableDetails {







    export function getJsonObj(obj: SessionVariableDetails): object {
        const jsonObj = {...obj, ...{
            






        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SessionVariableDetails): object {
        const jsonObj = {...obj, ...{
            






         }};

        
        
        return jsonObj;
    }
}
