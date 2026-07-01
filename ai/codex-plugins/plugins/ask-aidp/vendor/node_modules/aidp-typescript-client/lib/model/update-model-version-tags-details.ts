// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Details of the model version tags to update.
*/
export interface UpdateModelVersionTagsDetails {
    /**
    * Name of the registered model.
    */
    'name': string;
    /**
    * Model version number.
    */
    'version': string;
    /**
    * Model version tags to set.
    */
    'setTags'?: Array<model.ModelVersionTag>;
    /**
    * Model version tags to delete.
    */
    'deleteTags'?: Array<model.ModelVersionTagKey>;

}

export namespace UpdateModelVersionTagsDetails {





    export function getJsonObj(obj: UpdateModelVersionTagsDetails): object {
        const jsonObj = {...obj, ...{
            


                'set_tags': obj.setTags ?
                
                obj.setTags.map((item)=>{return model.ModelVersionTag.getJsonObj(item)})
                
                 : undefined,
                'delete_tags': obj.deleteTags ?
                
                obj.deleteTags.map((item)=>{return model.ModelVersionTagKey.getJsonObj(item)})
                
                 : undefined,
        }};

        delete (jsonObj as Partial<UpdateModelVersionTagsDetails>).setTags;delete (jsonObj as Partial<UpdateModelVersionTagsDetails>).deleteTags;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: UpdateModelVersionTagsDetails): object {
        const jsonObj = {...obj, ...{
            


                    'setTags': (obj as any)["set_tags"] ?
                
                (obj as any)["set_tags"].map((item: any)=>{return model.ModelVersionTag.getDeserializedJsonObj(item)})
                
                 : undefined,
                    'deleteTags': (obj as any)["delete_tags"] ?
                
                (obj as any)["delete_tags"].map((item: any)=>{return model.ModelVersionTagKey.getDeserializedJsonObj(item)})
                
                 : undefined,
         }};

        delete (jsonObj as any)["set_tags"];delete (jsonObj as any)["delete_tags"];
        
        return jsonObj;
    }
}
