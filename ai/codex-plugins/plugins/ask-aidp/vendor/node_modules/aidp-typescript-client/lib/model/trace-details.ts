// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Trace object
*/
export interface TraceDetails {
    /**
    * trace id
    */
    'traceId': string;
    /**
    * session id
    */
    'parentSessionId': string;
    /**
    * collections of spans
    */
    'spans': Array<model.SpanDetails>;
    /**
    * startTime Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime': number;
    /**
    * endTime Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime': number;
    /**
    * Map of properties
    */
    'resources'?: { [key: string]: any; };

}

export namespace TraceDetails {







    export function getJsonObj(obj: TraceDetails): object {
        const jsonObj = {...obj, ...{
            


                'spans': obj.spans ?
                
                obj.spans.map((item)=>{return model.SpanDetails.getJsonObj(item)})
                
                 : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: TraceDetails): object {
        const jsonObj = {...obj, ...{
            


                    'spans': obj.spans ?
                
                obj.spans.map((item)=>{return model.SpanDetails.getDeserializedJsonObj(item)})
                
                 : undefined,



         }};

        
        
        return jsonObj;
    }
}
