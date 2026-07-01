"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
const key_pair_1 = __importDefault(require("./key-pair"));
const sshpk_1 = require("sshpk");
/**
 * This is a SessionKeySupplier which fakes the ability to refresh its contained key.
 * It is initialised once with fixed values of private key and (optional) passphrase; that key is always returned.
 */
class FixedContentKeySupplier {
    constructor(privateKeyContent, passphrase) {
        this.privateKeyContent = privateKeyContent;
        this.passphrase = passphrase;
        try {
            let options = {};
            // parse privateKeyContent with passphrase (if it exist) into a PrivateKey
            if (passphrase) {
                Object.assign(options, { passphrase: passphrase });
            }
            const privateKey = sshpk_1.parsePrivateKey(privateKeyContent, "auto", options);
            const publicKey = privateKey.toPublic();
            this.keyPair = new key_pair_1.default(publicKey.toString("pem"), privateKey.toBuffer("pem", {}).toString());
        }
        catch (e) {
            throw Error(`Failed to read file contents, error: ${e}`);
        }
    }
    // Getter: KeyPair
    getKeyPair() {
        return this.keyPair;
    }
    // Getter: Public Key
    get publicKey() {
        return this.getKeyPair().getPublic();
    }
    // Getter Private Key
    get privateKey() {
        return this.getKeyPair().getPrivate();
    }
    refreshKeys() { }
}
exports.default = FixedContentKeySupplier;
//# sourceMappingURL=fixed-content-key-supplier.js.map