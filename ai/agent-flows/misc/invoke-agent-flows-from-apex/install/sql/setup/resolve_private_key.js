var Files = Java.type("java.nio.file.Files");
var Paths = Java.type("java.nio.file.Paths");
var StandardCharsets = Java.type("java.nio.charset.StandardCharsets");

function escapeSqlSingleQuotedLiteral(value) {
  return String(value).replace(/'/g, "''");
}

var rawInput = "";
if (typeof args !== "undefined" && args !== null && args.length > 0) {
  // In SQLcl, args[0] can be the script path itself.
  // Prefer the first non-.js argument; otherwise fall back to the last argument.
  for (var ai = 0; ai < args.length; ai++) {
    var candidate = String(args[ai]);
    if (!/\.js$/i.test(candidate)) {
      rawInput = candidate;
      break;
    }
  }

  if (rawInput.length === 0) {
    rawInput = String(args[args.length - 1]);
  }
}

// SQLcl script arguments may arrive wrapped in quotes.
if (
  (rawInput.length >= 2 && rawInput.charAt(0) === '"' && rawInput.charAt(rawInput.length - 1) === '"') ||
  (rawInput.length >= 2 && rawInput.charAt(0) === "'" && rawInput.charAt(rawInput.length - 1) === "'")
) {
  rawInput = rawInput.substring(1, rawInput.length - 1);
}

var resolved = rawInput;
var sourceMode = "INLINE";

try {
  var p = Paths.get(rawInput);
  if (Files.exists(p) && Files.isRegularFile(p)) {
    resolved = String(new java.lang.String(Files.readAllBytes(p), StandardCharsets.UTF_8));
    sourceMode = "FILE";
  }
} catch (e) {
  // If the input is not a valid local path, keep original value.
}

// Normalize PEM content:
// - Omit BEGIN/END wrapper lines
// - Ignore any lines after END PRIVATE KEY
// - Preserve key body line breaks
var normalized = String(resolved).replace(/\r\n/g, "\n").replace(/\r/g, "\n");
var lines = normalized.split("\n");
var keyBodyLines = [];
var sawBegin = false;

for (var i = 0; i < lines.length; i++) {
  var line = String(lines[i]).trim();

  if (!sawBegin && line === "-----BEGIN PRIVATE KEY-----") {
    sawBegin = true;
    continue;
  }

  if (line === "-----END PRIVATE KEY-----") {
    break;
  }

  if (sawBegin) {
    if (line.length > 0) {
      keyBodyLines.push(line);
    }
  } else {
    // No PEM header detected: keep original content as-is.
    keyBodyLines.push(String(lines[i]));
  }
}

if (sawBegin) {
  resolved = keyBodyLines.join("\n");
} else {
  resolved = normalized;
}

resolved = String(resolved)
  .replace(/\n/g, "\\n");

var stmt = "define RESOLVED_CRED_PRIVATE_KEY = '" + escapeSqlSingleQuotedLiteral(resolved) + "'";
sqlcl.setStmt(stmt);
sqlcl.run();

var sourceStmt = "define RESOLVED_CRED_PRIVATE_KEY_SOURCE = '" + escapeSqlSingleQuotedLiteral(sourceMode) + "'";
sqlcl.setStmt(sourceStmt);
sqlcl.run();
