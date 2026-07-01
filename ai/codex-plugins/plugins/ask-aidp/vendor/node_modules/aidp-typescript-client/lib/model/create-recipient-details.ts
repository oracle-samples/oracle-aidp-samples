// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The details for creating a Delta Share recipient in AI Data Platform Workbench.
*/
export interface CreateRecipientDetails {
    /**
    * A user-friendly name. Has to be unique within the AI Data Platform Workbench instance.
    */
    'displayName': string;
    /**
    * Short description of the Recipient
    */
    'description'?: string;
    /**
    * Key-value pair representing a defined tag key and value.
* Example: {@code { \"CostCenter\": \"42\" }}
* 
    */
    'properties'?: { [key: string]: string; };

}

export namespace CreateRecipientDetails {




    export function getJsonObj(obj: CreateRecipientDetails): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: CreateRecipientDetails): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
