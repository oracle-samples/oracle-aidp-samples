// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Span details
*/
export interface SpanDetails {
    /**
    * trace id
    */
    'parentTraceId': string;
    /**
    * span id
    */
    'spanId': string;
    /**
    * parent span id
    */
    'parentSpanId'?: string;
    /**
    * start time Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'startTime': number;
    /**
    * end time Note: Numbers greater than Number.MAX_SAFE_INTEGER will result in rounding issues.
    */
    'endTime': number;
    /**
    * type of span
    */
    'kind': string;
    /**
    * span name
    */
    'spanName': string;
    /**
    * span attributes
    */
    'attributes': { [key: string]: any; };
    /**
    * collections of trace objects
    */
    'events': Array<model.TraceEvent>;
    'status': model.SpanStatus;

}

export namespace SpanDetails {











    export function getJsonObj(obj: SpanDetails): object {
        const jsonObj = {...obj, ...{
            








                'events': obj.events ?
                
                obj.events.map((item)=>{return model.TraceEvent.getJsonObj(item)})
                
                 : undefined,
                'status': obj.status ?
                
                
                model.SpanStatus.getJsonObj(obj.status) : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SpanDetails): object {
        const jsonObj = {...obj, ...{
            








                    'events': obj.events ?
                
                obj.events.map((item)=>{return model.TraceEvent.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'status': obj.status ?
                
                
                model.SpanStatus.getDeserializedJsonObj(obj.status) : undefined,
         }};

        
        
        return jsonObj;
    }
}
