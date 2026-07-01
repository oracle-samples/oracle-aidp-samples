// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the registered model tags to update.
*/
export interface UpdateRegisteredModelTagsDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Registered model tags to set.
    */
    'setTags'?: Array<model.RegisteredModelTag>;
    /**
    * Registered model tags to delete.
    */
    'deleteTags'?: Array<model.RegisteredModelTagKey>;

}

export namespace UpdateRegisteredModelTagsDetails {




    export function getJsonObj(obj: UpdateRegisteredModelTagsDetails): object {
        const jsonObj = {...obj, ...{
            

                'set_tags': obj.setTags ?
                
                obj.setTags.map((item)=>{return model.RegisteredModelTag.getJsonObj(item)})
                
                 : undefined,
                'delete_tags': obj.deleteTags ?
                
                obj.deleteTags.map((item)=>{return model.RegisteredModelTagKey.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<UpdateRegisteredModelTagsDetails>).setTags;delete (jsonObj as Partial<UpdateRegisteredModelTagsDetails>).deleteTags;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateRegisteredModelTagsDetails): object {
        const jsonObj = {...obj, ...{
            

                    'setTags': (obj as any)["set_tags"] ?
                
                (obj as any)["set_tags"].map((item: any)=>{return model.RegisteredModelTag.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'deleteTags': (obj as any)["delete_tags"] ?
                
                (obj as any)["delete_tags"].map((item: any)=>{return model.RegisteredModelTagKey.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["set_tags"];delete (jsonObj as any)["delete_tags"];
        
        return jsonObj;
    }
}
