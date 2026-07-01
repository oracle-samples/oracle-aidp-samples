// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of subscription.
*/
export interface SubscriptionDetails {
    /**
    * The notification callback URL.
    */
    'callbackUrl'?: string;
    /**
    * The name of the service.
    */
    'serviceName'?: string;

}

export namespace SubscriptionDetails {



    export function getJsonObj(obj: SubscriptionDetails): object {
        const jsonObj = {...obj, ...{
            


        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SubscriptionDetails): object {
        const jsonObj = {...obj, ...{
            


         }};

        
        
        return jsonObj;
    }
}
