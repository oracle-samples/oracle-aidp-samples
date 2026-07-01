"use strict";
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
Object.defineProperty(exports, "__esModule", { value: true });
exports.loadFromFile = void 0;
const fs_1 = require("fs");
function loadFromFile(filePath) {
    try {
        const fileContent = fs_1.readFileSync(filePath, "utf8");
        return fileContent;
    }
    catch (e) {
        throw Error(`Failed to read file contents, error: ${e}`);
    }
}
exports.loadFromFile = loadFromFile;
//# sourceMappingURL=load-from-file.js.map