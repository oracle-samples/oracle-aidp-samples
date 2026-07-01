"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.addUserAgent = exports.addOpcRequestId = void 0;
/* This utility adds extra headers to each request before being sent over wire*/
const package_json_1 = require("../package.json");
const utils_1 = require("./utils");
const uuidv1 = require("uuid/v1");
const OPC_REQUEST_ID_HEADER = "opc-request-id";
const USER_AGENT_HEADER = "User-Agent";
const BROWSER_USER_AGENT_HEADER = "X-Orcl-User-Agent";
const PLATFORM_KEY = "platform";
const RELEASE_KEY = "relase";
const VERSION_KEY = "nodeVersion";
let NODE_PLATFORM_HASH = new Map();
function addOpcRequestId(headers) {
    // if user has not passed opc-request-id
    if (!hasOpcRequestId(headers)) {
        headers.append(OPC_REQUEST_ID_HEADER, generateRequestId());
    }
}
exports.addOpcRequestId = addOpcRequestId;
function hasOpcRequestId(headers) {
    if (headers.has(OPC_REQUEST_ID_HEADER)) {
        return true;
    }
    return false;
}
function generateRequestId() {
    return uuidv1()
        .replace("-", "")
        .toLocaleUpperCase("en-US");
}
function addUserAgent(headers) {
    const appendUserAgent = process.env.OCI_SDK_APPEND_USER_AGENT;
    const clientInfo = `Oracle-TypeScriptSDK/${package_json_1.version}`;
    // Check if the sdk is used in browser or node, populate header accordingly.
    if (!utils_1.isBrowser()) {
        // Hash the values of Platform specific attributes
        if (utils_1.isEmpty(NODE_PLATFORM_HASH)) {
            populatePlatformHash();
        }
        const platform = NODE_PLATFORM_HASH.get(PLATFORM_KEY);
        const release = NODE_PLATFORM_HASH.get(RELEASE_KEY);
        const nodeVersion = NODE_PLATFORM_HASH.get(VERSION_KEY);
        // Formulate User-Agent
        let userAgent = appendUserAgent
            ? `${clientInfo} (${platform}/${release}; Node/${nodeVersion}) ${appendUserAgent}`
            : `${clientInfo} (${platform}/${release}; Node/${nodeVersion})`;
        // Headers will always be replaced if they exist already
        headers.append(USER_AGENT_HEADER, userAgent);
        return;
    }
    headers.append(BROWSER_USER_AGENT_HEADER, clientInfo);
}
exports.addUserAgent = addUserAgent;
function populatePlatformHash() {
    const os = require("os");
    NODE_PLATFORM_HASH.set(PLATFORM_KEY, os.platform);
    NODE_PLATFORM_HASH.set(RELEASE_KEY, os.release);
    NODE_PLATFORM_HASH.set(VERSION_KEY, process.version);
}
//# sourceMappingURL=headers.js.map