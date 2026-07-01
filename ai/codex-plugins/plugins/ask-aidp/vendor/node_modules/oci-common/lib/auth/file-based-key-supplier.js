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
const load_private_key_from_file_1 = require("./helpers/load-private-key-from-file");
class FileBasedKeySupplier {
    constructor(privateKeyPath, passphrasePath) {
        this.privateKeyPath = privateKeyPath;
        this.passphrasePath = passphrasePath;
        this.refreshKeys();
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
    refreshKeys() {
        if (!this.privateKeyPath) {
            throw Error("privateKeyPath not set");
        }
        // Try to create a privateKey & publicKey from the given privateKeyPath & passphrasePath
        try {
            const { publicKey, privateKey } = load_private_key_from_file_1.loadPrivateKeyFromFile(this.privateKeyPath, this.passphrasePath);
            this.keyPair = new key_pair_1.default(publicKey, privateKey);
        }
        catch (e) {
            throw Error(`Problem occured trying to create KeyPairs from given paths with error: ${e}`);
        }
    }
}
exports.default = FileBasedKeySupplier;
//# sourceMappingURL=file-based-key-supplier.js.map