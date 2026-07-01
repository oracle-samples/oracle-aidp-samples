/**
 * Copyright (c) 2020, 2021 Oracle and/or its affiliates.  All rights reserved.
 * This software is dual-licensed to you under the Universal Permissive License (UPL) 1.0 as shown at https://oss.oracle.com/licenses/upl or Apache License 2.0 as shown at http://www.apache.org/licenses/LICENSE-2.0. You may choose either license.
 */
export declare class ConfigFileReader {
    /**
     * Default location of the config file.
     */
    static DEFAULT_FILE_PATH: string;
    /**
     * The fallback default location of the config file. If and only if the DEFAULT_FILE_PATH does not exist,
     * this fallback default location will be used.
     */
    static FALLBACK_DEFAULT_FILE_PATH: string;
    static DEFAULT_PROFILE_NAME: string;
    /**
     * Creates a new ConfigFile instance using the configuration at the default location,
     * using the given profile.
     * @param profile The profile name to load, or null if you want to load the
     *            "DEFAULT" profile.
     * @return A new ConfigFile instance.
     * @throws Error, if the file could not be read.
     */
    static parseDefault(profile: string | null): ConfigFile;
    static expandUserHome(path: string): string;
    static correctPath(isWindows: boolean, path: string): string;
    static fileExists(filepath: string): boolean;
    static parseFileFromPath(path: string, profile: string | null): ConfigFile;
    static parse(fileContent: string, profile: string | null): ConfigFile;
}
export declare class ConfigFile {
    profile: string | null;
    accumulator: ConfigAccumulator;
    constructor(acc: ConfigAccumulator, profile: string | null);
    /**
     * Gets the value associated with a given key. The value returned will
     * be the one for the selected profile (if available), else the value in
     * the DEFAULT profile (if specified), else null.
     *
     * @param key, The key to look up.
     * @return The value, or null if it didn't exist.
     */
    get(key: string): string | null;
    get profileCredentials(): ConfigAccumulator;
}
export declare class ConfigAccumulator {
    configurationsByProfile: Map<string, Map<string, string>>;
    "currentProfile": string;
    "foundDefaultProfile": boolean;
    accept(line: string): void;
}
