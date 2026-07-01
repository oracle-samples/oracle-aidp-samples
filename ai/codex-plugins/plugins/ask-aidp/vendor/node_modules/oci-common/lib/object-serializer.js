"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.ObjectSerializer = void 0;
var ObjectSerializer;
(function (ObjectSerializer) {
    function serialize(data, type, getJsonObj) {
        if (getJsonObj) {
            if (isList(type)) {
                // constuct List of json object first, such as: List<NonPrimaryType>
                // then return JSON.stringify of it
                return JSON.stringify(data.map((item) => {
                    return getJsonObj(item);
                }));
            }
            else if (isMap(type)) {
                // construct a map of json object, such as { [key: string]: model.NonPrimaryType }
                // Notes: we don't have any spec fall into this case now, put it here just in case it
                // has in the future
                const obj = {};
                Object.keys(data).forEach((key) => {
                    obj[key] = getJsonObj(data[key]);
                });
                return JSON.stringify(obj);
            }
            else {
                return JSON.stringify(getJsonObj(data));
            }
        }
        // for type without getJsonObj callback use native stringify
        return JSON.stringify(data);
    }
    ObjectSerializer.serialize = serialize;
    function deserialize(data, type, bodyModel) {
        if (typeof bodyModel === "object") {
            if (isList(type)) {
                return data.map((item) => {
                    return bodyModel.getDeserializedJsonObj(item);
                });
            }
            else if (isMap(type)) {
                const obj = {};
                Object.keys(data).forEach((key) => {
                    obj[key] = bodyModel.getDeserializedJsonObj(data[key]);
                });
                return obj;
            }
            else {
                return bodyModel.getDeserializedJsonObj(data);
            }
        }
        return data;
    }
    ObjectSerializer.deserialize = deserialize;
    // check if type is List
    function isList(type) {
        return type.lastIndexOf("Array<", 0) === 0;
    }
    const mapRegex = new RegExp("^{\\[\\w*:\\w*]:model.\\w*}$");
    // check if type is a map, such as: { [key: string]: model.ComplexType }
    function isMap(type) {
        // remove whitespace from type for regex match
        return mapRegex.test(type.replace(/\s/g, ""));
    }
})(ObjectSerializer = exports.ObjectSerializer || (exports.ObjectSerializer = {}));
//# sourceMappingURL=object-serializer.js.map