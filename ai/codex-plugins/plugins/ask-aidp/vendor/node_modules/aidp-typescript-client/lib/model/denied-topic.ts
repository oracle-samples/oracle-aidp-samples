// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Denied topic configuration
*/
export interface DeniedTopic {
    /**
    * Name of the denied topic
    */
    'name': string;
    /**
    * Definition of the denied topic
    */
    'definition': string;
    /**
    * Examples of content that would violate this topic
    */
    'examples'?: Array<string>;

}

export namespace DeniedTopic {




    export function getJsonObj(obj: DeniedTopic): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: DeniedTopic): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
