#!/usr/bin/env node
// Reads a mashup.pq path on argv[2], prints the JSON contract on stdout.
// Contract: { ok: true, section_attrs: "..."|null,
//              queries: [ { name, attrs, final, steps, raw?, note? } ] }
//           { ok: false, error: "..." }
// Every step: { name, fn, args: [raw text], nav: [raw text], inputs: [step names], raw }
//
// This file is the ONLY place the powerquery-parser AST is touched. Everything
// downstream depends on the shape above and nothing else.
const PQP = require('@microsoft/powerquery-parser');
const fs = require('fs');

function build(src) {
  const slice = n => (n && n.tokenRange)
    ? src.slice(n.tokenRange.positionStart.codeUnit, n.tokenRange.positionEnd.codeUnit) : null;

  function steps(letExpr) {
    const names = new Set(letExpr.variableList.elements.map(e => e.node.key.literal));
    return letExpr.variableList.elements.map(el => {
      const name = el.node.key.literal, v = el.node.value;
      let fn = null, args = [], nav = [];
      if (v.kind === 'RecursivePrimaryExpression') {
        fn = v.head.identifier ? v.head.identifier.literal : v.head.kind;
        for (const e of (v.recursiveExpressions.elements || [])) {
          // A step is either a call -- Table.SelectColumns(prev, {...}) -- or a
          // navigation chain -- Source{[Id="t"]}[Data].  Keep both: in a Lakehouse
          // read the table name lives only in the navigation keys, and those keys
          // are usually on a LATER step whose `fn` is the previous step's name.
          if (e.kind === 'InvokeExpression') args = (e.content.elements || []).map(x => slice(x.node));
          else nav.push(slice(e));
        }
      } else { fn = v.kind; }
      const inputs = args.filter(a => names.has(String(a).trim()))
        .concat(names.has(String(fn).trim()) ? [String(fn).trim()] : []);
      return { name, fn, args, nav, inputs, raw: slice(v) };
    });
  }

  return async () => {
    const r = await PQP.TaskUtils.tryLexParse(PQP.DefaultSettings, src);
    if (!PQP.TaskUtils.isParseStageOk(r)) {
      return { ok: false, error: String(r.error && r.error.message || 'parse failed') };
    }
    const root = r.ast, queries = [];
    // The section's OWN literal attributes, ahead of `section Section1;`.
    // `[DefaultOutputDestinationSettings = [DestinationDefinition =
    // [Kind = "Reference", QueryName = "DefaultDestination"], ...]]` lives
    // here and nowhere else -- queryMetadata.json carries only loadEnabled.
    // Without it a member marked `[BindToDefaultDestination = true]` has
    // nothing anywhere saying where it writes.
    let sectionAttrs = null;
    if (root.kind === 'Section') {
      sectionAttrs = slice(root.literalAttributes);
      for (const m of root.sectionMembers.elements) {
        const nm = m.namePairedExpression.key.literal, val = m.namePairedExpression.value;
        // The write target is declared here, as a literal attribute record on the
        // member -- NOT in queryMetadata.json, which carries only loadEnabled.
        const attrs = slice(m.literalAttributes);
        // `source` is the member exactly as written -- attributes, `shared`,
        // name and body -- and exists only so a report can show a reviewer
        // what was translated. It is deliberately NOT `raw`: `raw` means "the
        // text of a non-`let` member" and is parsed downstream as a single
        // value expression, so a `let` carrying one would classify differently.
        if (val.kind === 'LetExpression') queries.push({
          name: nm, attrs, steps: steps(val), final: slice(val.expression),
          source: slice(m)
        });
        // Keep the source text of a non-`let` member: a parameter query
        // (`shared d = #date(2024,1,1) meta [...]`) is the only place a
        // generated date table's range is written down.
        else queries.push({ name: nm, attrs, steps: [], raw: slice(val),
                            source: slice(m),
                            note: `unsupported member kind ${val.kind}` });
      }
    } else if (root.kind === 'LetExpression') {
      queries.push({ name: 'Query', attrs: null, steps: steps(root),
                     final: slice(root.expression), source: slice(root) });
    } else {
      return { ok: false, error: `unsupported root ${root.kind}` };
    }
    return { ok: true, section_attrs: sectionAttrs, queries };
  };
}

(async () => {
  // A UTF-8 BOM (Windows editors, some Git tooling) is not M; the parser
  // rejected the whole file and every query of the Dataflow vanished.
  const src = fs.readFileSync(process.argv[2], 'utf8').replace(/^﻿/, '');
  try { console.log(JSON.stringify(await build(src)(), null, 2)); }
  catch (e) { console.log(JSON.stringify({ ok: false, error: String(e && e.message || e) })); }
})();
