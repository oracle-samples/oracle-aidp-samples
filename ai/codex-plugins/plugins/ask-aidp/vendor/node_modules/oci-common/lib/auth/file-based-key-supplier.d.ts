/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
import SessionKeySupplier from "./models/session-key-supplier";
import KeyPair from "./key-pair";
export default class FileBasedKeySupplier implements SessionKeySupplier {
    private privateKeyPath;
    private passphrasePath?;
    private keyPair;
    constructor(privateKeyPath: string, passphrasePath?: string | undefined);
    getKeyPair(): KeyPair;
    get publicKey(): string;
    get privateKey(): string;
    refreshKeys(): void;
}
