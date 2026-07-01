// Copyright (c) 2026, Oracle and/or its affiliates.  All rights reserved.

import * as model from '../model';
import common = require("oci-common");


/**
* Result of listing artifacts.
*/
export interface ArtifactList {
    /**
    * Root artifact directory for the run.
    */
    'rootUri'?: string;
    /**
    * File location and metadata for artifacts.
    */
    'files'?: Array<model.ArtifactFileInfo>;
    /**
    * Token that can be used to retrieve the next page of artifact results.
    */
    'nextPageToken'?: string;

}

export namespace ArtifactList {




    export function getJsonObj(obj: ArtifactList): object {
        const jsonObj = {...obj, ...{
            
                'root_uri': obj.rootUri,

                'files': obj.files ?
                
                obj.files.map((item)=>{return model.ArtifactFileInfo.getJsonObj(item)})
                
                 : undefined,
                'next_page_token': obj.nextPageToken,

        }};

        delete (jsonObj as Partial<ArtifactList>).rootUri;delete (jsonObj as Partial<ArtifactList>).nextPageToken;
        
        return jsonObj;
    }
    ;
    export function getDeserializedJsonObj(obj: ArtifactList): object {
        const jsonObj = {...obj, ...{
            
                'rootUri': (obj as any)["root_uri"],

                    'files': obj.files ?
                
                obj.files.map((item)=>{return model.ArtifactFileInfo.getDeserializedJsonObj(item)})
                
                 : undefined,
                'nextPageToken': (obj as any)["next_page_token"],

         }};

        delete (jsonObj as any)["root_uri"];delete (jsonObj as any)["next_page_token"];
        
        return jsonObj;
    }
}
