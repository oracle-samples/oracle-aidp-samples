// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Session data retention configuration for agent flow
*/
export interface SessionRetentionConfiguration {
    /**
    * No. of days session data will be kept Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'retentionPeriodInDays'?: number;
    /**
    * Max storage allocated to session data (in MB). Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'sessionSizeLimit'?: number;
    /**
    * Maximum no. of user prompt and agent response pairs per session Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'threadCountLimit'?: number;

}

export namespace SessionRetentionConfiguration {




    export function getJsonObj(obj: SessionRetentionConfiguration): object {
        const jsonObj = {...obj, ...{
            



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SessionRetentionConfiguration): object {
        const jsonObj = {...obj, ...{
            



         }};

        
        
        return jsonObj;
    }
}
