#!/usr/bin/env node

import fs from 'node:fs';
import path from 'node:path';
import {
   RUNTIME_PATH_SIGNAL_TEXTBOX,
   loadRuntimePathRegistry,
   resolveRuntimePathSignal,
   selectSignalTextValue
} from './runtime-path-registry-utils.mjs';

const argv = process.argv.slice(2);

function getArg(flag) {
   const index = argv.indexOf(flag);
   if (index === -1 || index + 1 >= argv.length) {
      return null;
   }
   return argv[index + 1];
}

function fail(message) {
   process.stderr.write(`${message}\n`);
   process.exit(1);
}

function readJson(filePath, label) {
   try {
      return JSON.parse(fs.readFileSync(filePath, 'utf8'));
   } catch (error) {
      fail(`Unable to parse ${label} JSON '${filePath}': ${error?.message || String(error)}`);
   }
}

function isPlainObject(value) {
   return value && typeof value === 'object' && !Array.isArray(value);
}

function toNonEmptyTrimmedString(value) {
   if (typeof value !== 'string') {
      return null;
   }
   const trimmed = value.trim();
   return trimmed === '' ? null : trimmed;
}

function normalizeText(value) {
   return toNonEmptyTrimmedString(value)?.toLowerCase() || null;
}

function unescapeQuotedToken(rawValue, quoteToken) {
   const unescapedQuote = quoteToken === '"' ? '\\"' : "\\'";
   return rawValue
      .replaceAll('\\\\', '\\')
      .replaceAll(unescapedQuote, quoteToken);
}

function quoteIdentifier(value) {
   return `"${value.replaceAll('\\', '\\\\').replaceAll('"', '\\"')}"`;
}

function canonicalizeSubjectAreaToken(rawToken) {
   const normalized = toNonEmptyTrimmedString(rawToken);
   if (!normalized) {
      return null;
   }
   if (/^XSA\([^)]*\)$/i.test(normalized)) {
      return normalized;
   }
   if (normalized.startsWith('"') && normalized.endsWith('"') && normalized.length >= 2) {
      const value = unescapeQuotedToken(normalized.slice(1, -1), '"');
      return quoteIdentifier(value);
   }
   if (normalized.startsWith('\'') && normalized.endsWith('\'') && normalized.length >= 2) {
      const value = unescapeQuotedToken(normalized.slice(1, -1), '\'');
      return quoteIdentifier(value);
   }
   return quoteIdentifier(normalized);
}

function canonicalizeIdentifierToken(rawToken) {
   const normalized = toNonEmptyTrimmedString(rawToken);
   if (!normalized) {
      return null;
   }
   return quoteIdentifier(unescapeQuotedToken(normalized, '"'));
}

function parseDirectColumnExpression(expression) {
   const normalized = toNonEmptyTrimmedString(expression);
   if (!normalized) {
      return null;
   }
   const quotedReferenceMatch = normalized.match(
      /^(XSA\([^)]*\)|"(?:[^"\\]|\\.)+"|'(?:[^'\\]|\\.)+'|[^.]+)\."((?:[^"\\]|\\.)+)"\."((?:[^"\\]|\\.)+)"$/i
   );
   if (quotedReferenceMatch) {
      const [, rawSubjectAreaToken, rawTableToken, rawColumnToken] = quotedReferenceMatch;
      const canonicalSubjectArea = canonicalizeSubjectAreaToken(rawSubjectAreaToken);
      const canonicalTable = canonicalizeIdentifierToken(rawTableToken);
      const canonicalColumn = canonicalizeIdentifierToken(rawColumnToken);
      if (!canonicalSubjectArea || !canonicalTable || !canonicalColumn) {
         return null;
      }
      return `${canonicalSubjectArea}.${canonicalTable}.${canonicalColumn}`;
   }
   return null;
}

function collectCriteriaExpressions(workbookJson) {
   const directExpressions = new Set();
   const directExpressionsLower = new Set();
   const expressionsByColumnID = new Map();
   const columns = Array.isArray(workbookJson?.criteria?.columns?.children)
      ? workbookJson.criteria.columns.children
      : [];
   for (const column of columns) {
      const columnID = toNonEmptyTrimmedString(column?.columnID);
      const expr = toNonEmptyTrimmedString(column?.columnFormula?.expr?.expression);
      if (!expr) {
         continue;
      }
      const columnExpressions = new Set([expr, expr.toLowerCase()]);
      directExpressions.add(expr);
      directExpressionsLower.add(expr.toLowerCase());
      const canonicalExpr = parseDirectColumnExpression(expr);
      if (canonicalExpr) {
         directExpressions.add(canonicalExpr);
         directExpressionsLower.add(canonicalExpr.toLowerCase());
         columnExpressions.add(canonicalExpr);
         columnExpressions.add(canonicalExpr.toLowerCase());
      }
      if (columnID) {
         expressionsByColumnID.set(columnID, columnExpressions);
      }
   }
   return {
      directExpressions,
      directExpressionsLower,
      expressionsByColumnID
   };
}

function collectColumnIDsFromValue(value, collector) {
   if (!value || typeof value !== 'object') {
      return;
   }
   if (Array.isArray(value)) {
      for (const entry of value) {
         collectColumnIDsFromValue(entry, collector);
      }
      return;
   }
   for (const [key, nestedValue] of Object.entries(value)) {
      if (key === 'columnID' && typeof nestedValue === 'string' && nestedValue.trim() !== '') {
         collector.add(nestedValue.trim());
      }
      collectColumnIDsFromValue(nestedValue, collector);
   }
}

function collectEffectiveExpressionsForView(pluginView, criteriaExpressionCatalog) {
   const columnIDs = new Set();
   collectColumnIDsFromValue(pluginView?.dataModels, columnIDs);
   const expressions = new Set();
   for (const columnID of columnIDs) {
      const columnExpressions = criteriaExpressionCatalog.expressionsByColumnID.get(columnID);
      if (!columnExpressions) {
         continue;
      }
      for (const expression of columnExpressions) {
         expressions.add(expression);
      }
   }
   return expressions;
}

function shouldWarnOnlyForEffectiveBindingRole(role) {
   const normalizedRole = normalizeText(role);
   return normalizedRole === 'dimension.secondary';
}

function collectPluginViews(workbookJson, textboxRuntimePathSignal) {
   const views = Array.isArray(workbookJson?.views?.children) ? workbookJson.views.children : [];
   return views
      .filter((view) => isPlainObject(view) && view.type === 'saw:pluginView')
      .map((view, index) => {
         const captionText = toNonEmptyTrimmedString(view?.viewCaption?.caption?.text);
         const textboxText = selectSignalTextValue(view, textboxRuntimePathSignal, { allowLegacyFallback: true }).selectedText;
         return {
            index,
            view,
            viewName: toNonEmptyTrimmedString(view?.viewName) || `plugin_view_${index + 1}`,
            pluginType: toNonEmptyTrimmedString(view?.pluginType) || null,
            titleText: captionText || textboxText || null
         };
      });
}

function classifyFilterOutcome(filter) {
   const token = normalizeText(
      filter?.planningOutcome
      || filter?.status
      || filter?.resolution
      || filter?.disposition
   );
   if (token === 'considered_not_grounded') {
      return 'considered_not_grounded';
   }
   if (token === 'rejected_conflict') {
      return 'rejected_conflict';
   }
   if (token === 'rejected_missing_field') {
      return 'rejected_missing_field';
   }
   return 'applied';
}

const requestPathArg = getArg('--request');
if (!requestPathArg) {
   fail('Usage: node validate-requirements-trace.mjs --request <trace-request.json>');
}

const requestPath = path.resolve(requestPathArg);
if (!fs.existsSync(requestPath)) {
   fail(`Trace request file does not exist: ${requestPath}`);
}

const traceRequest = readJson(requestPath, 'trace request');
if (!isPlainObject(traceRequest)) {
   fail('Trace request must be a JSON object.');
}

const workbookPath = toNonEmptyTrimmedString(traceRequest.workbookPath);
if (!workbookPath) {
   fail('trace request workbookPath is required.');
}
const resolvedWorkbookPath = path.resolve(workbookPath);
if (!fs.existsSync(resolvedWorkbookPath)) {
   fail(`Workbook file does not exist: ${resolvedWorkbookPath}`);
}
const workbookJson = readJson(resolvedWorkbookPath, 'workbook');

const analysisShape = isPlainObject(traceRequest.analysisShape) ? traceRequest.analysisShape : null;
const analysisRequirements = isPlainObject(traceRequest.analysisRequirements) ? traceRequest.analysisRequirements : null;
const generationStrategyApplied = toNonEmptyTrimmedString(traceRequest.generationStrategyApplied) || 'unknown';
const viewAssignments = Array.isArray(traceRequest.viewAssignments) ? traceRequest.viewAssignments : [];
const runtimePathRegistry = loadRuntimePathRegistry({
   registryPath: getArg('--runtime-path-registry') || null,
   targetVersion: toNonEmptyTrimmedString(traceRequest.targetVersion)
});
const textboxRuntimePathSignal = resolveRuntimePathSignal(runtimePathRegistry, RUNTIME_PATH_SIGNAL_TEXTBOX).signal;

const issues = [];
let issueCounter = 0;
function addIssue(id, message, severity, contextPath = null) {
   issueCounter += 1;
   issues.push({
      id,
      order: issueCounter,
      message,
      path: contextPath,
      severity
   });
}

function addMismatch(id, message, contextPath = null) {
   addIssue(id, message, 'error', contextPath);
}

function addWarning(id, message, contextPath = null) {
   addIssue(id, message, 'warning', contextPath);
}

const criteriaExpressionCatalog = collectCriteriaExpressions(workbookJson);
const pluginViews = collectPluginViews(workbookJson, textboxRuntimePathSignal);
const requestedViewToViewName = new Map();
for (const assignment of viewAssignments) {
   const requestedViewID = toNonEmptyTrimmedString(assignment?.requestedViewID);
   const viewName = toNonEmptyTrimmedString(assignment?.viewName);
   if (requestedViewID && viewName && !requestedViewToViewName.has(requestedViewID)) {
      requestedViewToViewName.set(requestedViewID, viewName);
   }
}

const counts = {
   requiredBindingCount: 0,
   resolvedBindingCount: 0,
   requiredCalculationCount: 0,
   resolvedCalculationCount: 0,
   requiredFilterCount: 0,
   appliedFilterCount: 0,
   consideredNotGroundedFilterCount: 0,
   rejectedConflictFilterCount: 0,
   rejectedMissingFieldFilterCount: 0
};

if (generationStrategyApplied === 'compose_ootb' && !analysisRequirements) {
   addMismatch('REQ_MISSING_APPROVED_ARTIFACT', 'compose_ootb requires analysisRequirements for traceability validation.', 'analysisRequirements');
}

if (analysisRequirements) {
   const requirementCanvases = Array.isArray(analysisRequirements.canvases) ? analysisRequirements.canvases : [];
   if (requirementCanvases.length === 0) {
      addMismatch('REQ_CANVASES_EMPTY', 'analysisRequirements.canvases must not be empty.', 'analysisRequirements.canvases');
   }

   const analysisShapeCanvasViewIndex = new Map();
   const shapeCanvases = Array.isArray(analysisShape?.canvases) ? analysisShape.canvases : [];
   for (const canvas of shapeCanvases) {
      const canvasID = toNonEmptyTrimmedString(canvas?.id);
      if (!canvasID) {
         continue;
      }
      const views = Array.isArray(canvas?.views) ? canvas.views : [];
      analysisShapeCanvasViewIndex.set(
         canvasID,
         new Set(views.map((view) => toNonEmptyTrimmedString(view?.id)).filter(Boolean))
      );
   }

   for (let canvasIndex = 0; canvasIndex < requirementCanvases.length; canvasIndex += 1) {
      const canvas = requirementCanvases[canvasIndex];
      const canvasID = toNonEmptyTrimmedString(canvas?.id);
      const canvasPath = `analysisRequirements.canvases[${canvasIndex}]`;
      if (!canvasID) {
         addMismatch('REQ_CANVAS_ID_MISSING', 'Canvas id is required.', `${canvasPath}.id`);
         continue;
      }
      if (!analysisShapeCanvasViewIndex.has(canvasID)) {
         addMismatch('REQ_CANVAS_NOT_IN_ANALYSIS_SHAPE', `Canvas '${canvasID}' is not present in analysisShape.`, `${canvasPath}.id`);
      }
      const views = Array.isArray(canvas?.views) ? canvas.views : [];
      for (let viewIndex = 0; viewIndex < views.length; viewIndex += 1) {
         const view = views[viewIndex];
         const viewID = toNonEmptyTrimmedString(view?.id);
         const viewPath = `${canvasPath}.views[${viewIndex}]`;
         if (!viewID) {
            addMismatch('REQ_VIEW_ID_MISSING', 'View id is required.', `${viewPath}.id`);
            continue;
         }
         const analysisShapeViews = analysisShapeCanvasViewIndex.get(canvasID);
         if (analysisShapeViews && !analysisShapeViews.has(viewID)) {
            addMismatch('REQ_VIEW_NOT_IN_ANALYSIS_SHAPE', `View '${viewID}' in canvas '${canvasID}' is not present in analysisShape.`, `${viewPath}.id`);
         }

         const bindings = isPlainObject(view?.bindings) ? view.bindings : null;
         if (!bindings || Object.keys(bindings).length === 0) {
            addMismatch('REQ_BINDINGS_MISSING', `View '${viewID}' must include at least one binding.`, `${viewPath}.bindings`);
         } else {
            for (const [role, binding] of Object.entries(bindings)) {
               const rolePath = `${viewPath}.bindings.${role}`;
               const expression = toNonEmptyTrimmedString(binding) || toNonEmptyTrimmedString(binding?.expression);
               counts.requiredBindingCount += 1;
               if (!expression) {
                  addMismatch('REQ_BINDING_EXPRESSION_MISSING', `Binding '${role}' in view '${viewID}' is missing expression.`, rolePath);
                  continue;
               }
               const parsedDirect = parseDirectColumnExpression(expression);
               if (!parsedDirect) {
                  addMismatch('REQ_BINDING_EXPRESSION_INVALID', `Binding '${role}' in view '${viewID}' is not a direct subjectArea.table.column expression.`, rolePath);
                  continue;
               }
               if (criteriaExpressionCatalog.directExpressions.has(parsedDirect)
                  || criteriaExpressionCatalog.directExpressionsLower.has(parsedDirect.toLowerCase())) {
                  counts.resolvedBindingCount += 1;
               } else {
                  addMismatch('REQ_BINDING_NOT_IN_CRITERIA', `Binding '${role}' expression in view '${viewID}' is missing from workbook criteria columns.`, rolePath);
                  continue;
               }

               const requestedViewName = requestedViewToViewName.get(viewID);
               const targetView = requestedViewName
                  ? pluginViews.find((entry) => entry.viewName === requestedViewName)
                  : null;
               if (!targetView) {
                  addWarning(
                     'REQ_VIEW_ASSIGNMENT_MISSING',
                     `View '${viewID}' has binding '${role}' but no generated view assignment was available for effective-binding validation.`,
                     rolePath
                  );
                  continue;
               }

               const effectiveExpressions = collectEffectiveExpressionsForView(targetView.view, criteriaExpressionCatalog);
               const hasEffectiveBinding = effectiveExpressions.has(parsedDirect)
                  || effectiveExpressions.has(parsedDirect.toLowerCase());
               if (!hasEffectiveBinding) {
                  const message = `Binding '${role}' in requested view '${viewID}' resolves to '${parsedDirect}', ` +
                     `but generated view '${targetView.viewName}' does not bind that criteria expression.`;
                  if (shouldWarnOnlyForEffectiveBindingRole(role)) {
                     addWarning('REQ_VIEW_EFFECTIVE_BINDING_UNSUPPORTED', message, rolePath);
                  } else {
                     addMismatch('REQ_VIEW_EFFECTIVE_BINDING_MISMATCH', message, rolePath);
                  }
               }
            }
         }

         const calculations = Array.isArray(view?.calculations) ? view.calculations : [];
         for (let calculationIndex = 0; calculationIndex < calculations.length; calculationIndex += 1) {
            const calculation = calculations[calculationIndex];
            const calcPath = `${viewPath}.calculations[${calculationIndex}]`;
            const calcExpression = toNonEmptyTrimmedString(calculation)
               || toNonEmptyTrimmedString(calculation?.expression)
               || toNonEmptyTrimmedString(calculation?.formula)
               || null;
            counts.requiredCalculationCount += 1;
            if (!calcExpression) {
               addMismatch('REQ_CALC_EXPRESSION_MISSING', `Calculation ${calculationIndex + 1} in view '${viewID}' is missing expression/formula.`, calcPath);
               continue;
            }
            const calcNormalized = calcExpression.toLowerCase();
            const resolved = criteriaExpressionCatalog.directExpressionsLower.has(calcNormalized)
               || Array.from(criteriaExpressionCatalog.directExpressionsLower)
                  .some((value) => value.includes(calcNormalized) || calcNormalized.includes(value));
            if (resolved) {
               counts.resolvedCalculationCount += 1;
            } else {
               addMismatch('REQ_CALC_NOT_IN_CRITERIA', `Calculation ${calculationIndex + 1} in view '${viewID}' is not traceable to workbook criteria expressions.`, calcPath);
            }
         }

         const filters = Array.isArray(view?.filters) ? view.filters : [];
         for (let filterIndex = 0; filterIndex < filters.length; filterIndex += 1) {
            const filter = filters[filterIndex];
            const filterPath = `${viewPath}.filters[${filterIndex}]`;
            counts.requiredFilterCount += 1;
            if (!isPlainObject(filter)) {
               addMismatch('REQ_FILTER_INVALID', `Filter ${filterIndex + 1} in view '${viewID}' must be an object.`, filterPath);
               continue;
            }
            if (!toNonEmptyTrimmedString(filter.scope)
               || !toNonEmptyTrimmedString(filter.operator)
               || filter.default === undefined) {
               addMismatch('REQ_FILTER_FIELDS_MISSING', `Filter ${filterIndex + 1} in view '${viewID}' must define scope, operator, and default.`, filterPath);
            }
            const outcome = classifyFilterOutcome(filter);
            if (outcome === 'considered_not_grounded') {
               counts.consideredNotGroundedFilterCount += 1;
            } else if (outcome === 'rejected_conflict') {
               counts.rejectedConflictFilterCount += 1;
            } else if (outcome === 'rejected_missing_field') {
               counts.rejectedMissingFieldFilterCount += 1;
            } else {
               counts.appliedFilterCount += 1;
            }
         }

         const requestedViewName = requestedViewToViewName.get(viewID);
         const targetView = requestedViewName
            ? pluginViews.find((entry) => entry.viewName === requestedViewName)
            : null;
         const requiredTitle = toNonEmptyTrimmedString(view?.labels?.title);
         if (requiredTitle) {
            const normalizedRequiredTitle = normalizeText(requiredTitle);
            const foundTitle = targetView
               ? normalizeText(targetView.titleText) === normalizedRequiredTitle
               : pluginViews.some((entry) => normalizeText(entry.titleText) === normalizedRequiredTitle);
            if (!foundTitle) {
               addWarning('REQ_LABEL_TITLE_MISSING', `Title '${requiredTitle}' for view '${viewID}' is not present in generated plugin view caption/text.`, `${viewPath}.labels.title`);
            }
         }
      }
   }
}

const placeholderPattern = /(placeholder|todo|tbd|__)/i;
const criteriaColumns = Array.isArray(workbookJson?.criteria?.columns?.children)
   ? workbookJson.criteria.columns.children
   : [];
for (let columnIndex = 0; columnIndex < criteriaColumns.length; columnIndex += 1) {
   const heading = toNonEmptyTrimmedString(criteriaColumns[columnIndex]?.columnHeading?.caption?.text);
   if (heading && placeholderPattern.test(heading)) {
      addMismatch(
         'REQ_PLACEHOLDER_HEADING',
         `Placeholder-like heading detected in criteria column '${heading}'.`,
         `workbook.criteria.columns.children[${columnIndex}].columnHeading.caption.text`
      );
      break;
   }
}

const blockingMismatches = issues.filter((issue) => issue.severity !== 'warning');
const warnings = issues.filter((issue) => issue.severity === 'warning');

const result = {
   valid: blockingMismatches.length === 0,
   mismatchCount: blockingMismatches.length,
   blockingMismatchCount: blockingMismatches.length,
   warningCount: warnings.length,
   mismatches: blockingMismatches,
   blockingMismatches,
   warnings,
   counts
};

process.stdout.write(`${JSON.stringify(result, null, 2)}\n`);
