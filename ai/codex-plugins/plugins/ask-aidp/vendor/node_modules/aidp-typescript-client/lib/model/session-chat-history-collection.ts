// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A paginated collection of chat messages.
*/
export interface SessionChatHistoryCollection {
    /**
    * Collection of chat messages
    */
    'items': Array<model.SessionChatHistorySummary>;

}

export namespace SessionChatHistoryCollection {


    export function getJsonObj(obj: SessionChatHistoryCollection): object {
        const jsonObj = {...obj, ...{
            
                'items': obj.items ?
                
                obj.items.map((item)=>{return model.SessionChatHistorySummary.getJsonObj(item)})
                
                 : undefined,
        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SessionChatHistoryCollection): object {
        const jsonObj = {...obj, ...{
            
                    'items': obj.items ?
                
                obj.items.map((item)=>{return model.SessionChatHistorySummary.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        
        
        return jsonObj;
    }
}
