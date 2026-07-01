"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadPrivateKeyFromFile = void 0;
const fs_1 = require("fs");
const sshpk_1 = require("sshpk");
function loadPrivateKeyFromFile(privateKeyPath, passphrasePath) {
    try {
        let options = {};
        let passphraseFileContent;
        const privateKeyFileContent = fs_1.readFileSync(privateKeyPath, "utf8");
        if (passphrasePath) {
            passphraseFileContent = fs_1.readFileSync(passphrasePath, "utf8");
            Object.assign(options, { passphrase: passphraseFileContent });
        }
        // parse privateKeyFileContent with passphraseFileContent (if it exist) into a PrivateKey
        const privateKey = sshpk_1.parsePrivateKey(privateKeyFileContent, "auto", options);
        const publicKey = privateKey.toPublic();
        return {
            publicKey: publicKey.toString("pem"),
            privateKey: privateKey.toBuffer("pem", {}).toString()
        };
    }
    catch (e) {
        throw Error(`Failed to read file contents, error: ${e}`);
    }
}
exports.loadPrivateKeyFromFile = loadPrivateKeyFromFile;
//# sourceMappingURL=load-private-key-from-file.js.map