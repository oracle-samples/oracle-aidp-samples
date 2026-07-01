// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* The content of the chat query sent by the user.
*/
export interface ChatMessage {
    /**
    * Type of input.
    */
    'type': ChatMessage.Type;
    /**
    * Text input from the user. Set this parameter when type is input_text.
    */
    'text'?: string;
    /**
    * Image URL for the image user intends to query. Set this parameter when type is input_image.
    */
    'imageUrl'?: string;
    /**
    * File URL for the image user intends to query. Set this parameter when type is input_file.
    */
    'fileUrl'?: string;

}

export namespace ChatMessage {

    export enum Type {
    
    InputText = "INPUT_TEXT",
    InputImage = "INPUT_IMAGE",
    InputFile = "INPUT_FILE"

}





    export function getJsonObj(obj: ChatMessage): object {
        const jsonObj = {...obj, ...{
            




        }};

        
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ChatMessage): object {
        const jsonObj = {...obj, ...{
            




         }};

        
        
        return jsonObj;
    }
}
