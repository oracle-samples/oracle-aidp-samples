"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.composeResponse = void 0;
const object_serializer_1 = require("./object-serializer");
const helper_1 = require("./helper");
/*
 * Composes an SDKResponse.
 * @param ResponseParams to create a response
 */
function composeResponse(params) {
    const response = params.responseObject;
    let content = params.body;
    if (content) {
        const bodyContent = object_serializer_1.ObjectSerializer.deserialize(content, params.type, params.bodyModel);
        const key = params.bodyKey;
        Object.assign(response, { [key]: bodyContent });
    }
    computeHeaders(params.responseHeaders, response);
    return response;
}
exports.composeResponse = composeResponse;
function computeHeaders(responseHeaders, response) {
    if (responseHeaders) {
        responseHeaders.forEach(header => {
            if (header.value) {
                Object.assign(response, {
                    [header.key]: helper_1.convertStringToType(header.value, header.dataType)
                });
            }
        });
    }
}
//# sourceMappingURL=response-generator.js.map