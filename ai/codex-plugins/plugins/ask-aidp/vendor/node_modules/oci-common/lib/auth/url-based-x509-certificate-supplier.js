"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    function adopt(value) { return value instanceof P ? value : new P(function (resolve) { resolve(value); }); }
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : adopt(result.value).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
var __importDefault = (this && this.__importDefault) || function (mod) {
    return (mod && mod.__esModule) ? mod : { "default": mod };
};
Object.defineProperty(exports, "__esModule", { value: true });
exports.ResourceDetails = exports.URLBasedX509CertificateSupplier = void 0;
/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
const sshpk_1 = require("sshpk");
const http_1 = require("../http");
const certificate_and_privatekey_pair_1 = __importDefault(require("./certificate-and-privatekey-pair"));
const helper_1 = require("../helper");
const node_fetch_1 = __importDefault(require("node-fetch"));
/**
 * A class that retrieves certificate based on metadata service url
 */
class URLBasedX509CertificateSupplier {
    constructor(certificateDetails, privateKeyDetails, privateKeyPassphraseCharacters) {
        this.certificateDetails = certificateDetails;
        this.privateKeyDetails = privateKeyDetails;
        this.privateKeyPassphraseCharacters = privateKeyPassphraseCharacters;
    }
    /**
     * So far we don't care whether the certificate is current or not.
     * @return false always.
     */
    isCurrent() {
        return false;
    }
    /**
     * A method to refresh the X509 certificate and return the certificate
     * @returns Promise<URLBasedX509CertificateSupplier>
     */
    refresh() {
        return __awaiter(this, void 0, void 0, function* () {
            const certificate = yield this.readRawCertificate(this.certificateDetails);
            const privateKey = yield this.readPrivateKey(this.privateKeyDetails, this.privateKeyPassphraseCharacters);
            this.certificateAndKeyPair = new certificate_and_privatekey_pair_1.default(certificate, privateKey);
            return this;
        });
    }
    readRawCertificate(certificateDetails) {
        return __awaiter(this, void 0, void 0, function* () {
            const certificateStream = yield certificateDetails.send();
            // Convert responseStream to actual certificate string
            const certificateString = yield helper_1.getStringFromResponseBody(certificateStream.body);
            const certificate = sshpk_1.parseCertificate(certificateString, "pem");
            return certificate;
        });
    }
    readPrivateKey(privateKeyResourceDetails, privateKeyPassphrase) {
        return __awaiter(this, void 0, void 0, function* () {
            let options = {};
            if (!privateKeyResourceDetails || !privateKeyResourceDetails.getUrl()) {
                return null;
            }
            if (privateKeyPassphrase) {
                Object.assign(options, { passphrase: privateKeyPassphrase });
            }
            try {
                const privateKeyStream = yield privateKeyResourceDetails.send();
                // Convert privateKeyStream to privateKey string
                const privateKeyString = yield helper_1.getStringFromResponseBody(privateKeyStream.body);
                const privateKey = sshpk_1.parsePrivateKey(privateKeyString, "auto", options);
                return privateKey;
            }
            catch (e) {
                throw Error(`Unable to obtain private key, error: , ${e}`);
            }
        });
    }
    getCertificateAndKeyPair() {
        return this.certificateAndKeyPair;
    }
}
exports.URLBasedX509CertificateSupplier = URLBasedX509CertificateSupplier;
class ResourceDetails {
    constructor(url, headers, circuitBreaker) {
        this.url = url;
        this.headers = headers;
        this.circuitBreaker = circuitBreaker;
        this.METADATA_AUTH_HEADERS = "Bearer Oracle";
        this.AUTHORIZATION = "Authorization";
    }
    send() {
        return __awaiter(this, void 0, void 0, function* () {
            const httpClient = new http_1.FetchHttpClient(null, this.circuitBreaker);
            const metaDataHeaders = {};
            metaDataHeaders[this.AUTHORIZATION] = this.METADATA_AUTH_HEADERS;
            const response = yield node_fetch_1.default(this.url, {
                method: "GET",
                headers: metaDataHeaders
            });
            return response;
        });
    }
    getUrl() {
        return this.url;
    }
}
exports.ResourceDetails = ResourceDetails;
//# sourceMappingURL=url-based-x509-certificate-supplier.js.map