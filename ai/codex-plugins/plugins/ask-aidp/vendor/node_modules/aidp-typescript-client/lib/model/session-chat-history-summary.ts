// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* A single message in a chat session history. Does not include internal state.
*/
export interface SessionChatHistorySummary {
    /**
    * Unique identifier for the message.
    */
    'key': string;
    /**
    * Identifier of the chat session this message belongs to.
    */
    'sessionKey': string;
    /**
    * Role associated with the message, such as user, assistant, system, or tool.
    */
    'role': string;
    /**
    * Time the message was created.
    */
    'timeCreated': Date;
    /**
    * Message content.
    */
    'content': Array<model.ChatMessage>;
    /**
    * Name of the tool, if this is a tool message.
    */
    'toolName'?: string;
    /**
    * Identifier of the tool call, if applicable.
    */
    'toolCallId'?: string;
    /**
    * Optional key-value metadata associated with the message
    */
    'metadata'?: { [key: string]: any; };

}

export namespace SessionChatHistorySummary {









    export function getJsonObj(obj: SessionChatHistorySummary): object {
        const jsonObj = {...obj, ...{
            




                'content': obj.content ?
                
                obj.content.map((item)=>{return model.ChatMessage.getJsonObj(item)})
                
                 : undefined,



        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: SessionChatHistorySummary): object {
        const jsonObj = {...obj, ...{
            




                    'content': obj.content ?
                
                obj.content.map((item)=>{return model.ChatMessage.getDeserializedJsonObj(item)})
                
                 : undefined,



         }};

        
        
        return jsonObj;
    }
}
