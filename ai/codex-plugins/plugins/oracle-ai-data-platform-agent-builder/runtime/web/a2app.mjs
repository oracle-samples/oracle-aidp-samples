function el(tag, text, css) {
    const item = document.createElement(tag);
    if (text !== undefined)
        item.textContent = text;
    if (css)
        item.className = css;
    return item;
}
export async function a2appClient(appId, target, chart, initialCatalog = 'https://aidp.local/catalogs/a2app-v1') {
    let definition, revision = '', process = {}, selected = '', pending = false;
    let failure = '', warning = '', timer;
    let backgroundRequest;
    let last = {}, chatAnswer = '';
    const surfaces = new Map(), models = new Map(), roots = new Map();
    let lastRequest = {}, lastUserAction = {};
    function pathKeys(path) {
        const keys = path.split('/').filter(Boolean).map(k => k.replaceAll('~1', '/').replaceAll('~0', '~'));
        if (keys.some(k => ['__proto__', 'constructor', 'prototype'].includes(k)))
            throw new Error('Unsafe data path');
        return keys;
    }
    function read(model, path) { return pathKeys(path).reduce((value, key) => value?.[key], model); }
    function write(model, path, value) {
        const keys = pathKeys(path);
        const last = keys.pop();
        if (last === undefined)
            throw new Error('Expected a data field');
        let parent = model;
        for (const key of keys)
            parent = parent[key] ??= ({});
        parent[last] = value;
    }
    function initializeBindings(value, model) {
        if (!value || typeof value !== 'object')
            return;
        const key = Object.keys(value).find(k => k.startsWith('literal'));
        if (typeof value.path === 'string' && key)
            write(model, value.path, structuredClone(value[key]));
        else
            for (const child of Object.values(value))
                initializeBindings(child, model);
    }
    function entries(contents) {
        const value = {};
        for (const item of contents) {
            pathKeys('/' + item.key);
            const key = Object.keys(item).find(k => k.startsWith('value'));
            if (!key)
                throw new Error('Invalid dataModelUpdate');
            value[item.key] = key === 'valueMap' ? entries(item[key]) : item[key];
        }
        return value;
    }
    function bound(value, model) {
        if (typeof value?.path === 'string')
            return read(model, value.path);
        const key = Object.keys(value || {}).find(k => k.startsWith('literal'));
        return key ? value[key] : undefined;
    }
    const base = '/api/apps/' + encodeURIComponent(appId) + '/a2app';
    function editing() {
        const active = document.activeElement;
        return !!active && target.contains(active) && active.matches('input, select, textarea');
    }
    async function send(packet, background = false) {
        const request = packet.kind ? packet : { protocol: 'aidp.a2app', protocolVersion: 1, requestId: crypto.randomUUID(),
            channel: 'ui', ...packet };
        lastRequest = request;
        if (request.kind === 'data' && request.data?.userAction)
            lastUserAction = request;
        const response = await fetch(base, { method: 'POST', headers: { 'content-type': 'application/json' }, body: JSON.stringify(request) });
        const value = await response.json();
        if (!response.ok)
            throw new Error(value.error || 'A2App transport failed');
        revision = value.definitionRevision;
        if (value.result?.definition)
            definition = value.result.definition;
        if (value.process)
            process = value.process;
        // A status response arriving during typing must not reset input bindings.
        for (const message of background && editing() ? [] : value.presentation?.a2ui || []) {
            if (message.surfaceUpdate) {
                const update = message.surfaceUpdate, previous = surfaces.get(update.surfaceId);
                const nodes = new Map((previous?.components || []).map((n) => [n.id, n]));
                const model = models.get(update.surfaceId) || {};
                for (const n of update.components) {
                    nodes.set(n.id, n);
                    initializeBindings(n.component, model);
                }
                surfaces.set(update.surfaceId, { ...update, components: [...nodes.values()] });
                models.set(update.surfaceId, model);
            }
            else if (message.dataModelUpdate) {
                const update = message.dataModelUpdate, model = models.get(update.surfaceId) || {}, values = entries(update.contents);
                if (!update.path || update.path === '/')
                    models.set(update.surfaceId, values);
                else {
                    write(model, update.path, values);
                    models.set(update.surfaceId, model);
                }
            }
            else if (message.beginRendering) {
                if (message.beginRendering.catalogId !== definition.catalogId)
                    throw new Error('Unexpected A2UI catalog');
                roots.set(message.beginRendering.surfaceId, message.beginRendering.root);
            }
            else if (message.deleteSurface) {
                const id = message.deleteSurface.surfaceId;
                surfaces.delete(id);
                models.delete(id);
                roots.delete(id);
            }
            else
                throw new Error('Unsupported A2UI message');
        }
        warning = value.telemetry?.taskFinalizationWarning || '';
        last = value;
        if (value.status === 'error')
            throw new Error(value.error.message + (value.error.suggestions.length ? ' · Try: ' + value.error.suggestions.join(', ') : ''));
        return value;
    }
    function poll(componentId = selected) {
        clearTimeout(timer);
        timer = setTimeout(() => {
            if (pending || backgroundRequest || editing()) {
                poll(componentId);
                return;
            }
            backgroundRequest = invoke(componentId, 'status', {}, false)
                .finally(() => { backgroundRequest = undefined; });
        }, 2500);
    }
    async function invoke(componentId, actionId, args = {}, human = true, event) {
        if (pending && human)
            return;
        if (human) {
            pending = true;
            failure = '';
            clearTimeout(timer);
            render();
        }
        try {
            // Serialize the click behind a status read, then use its latest version.
            if (human) {
                await backgroundRequest;
                clearTimeout(timer);
            }
            // A component without status can always refresh its display.
            const action = definition.components.find(c => c.id === componentId)?.actions[actionId];
            if (!action && !human)
                actionId = 'display';
            const metadata = { protocolVersion: 1, requestId: crypto.randomUUID(), channel: 'ui', definitionRevision: revision,
                idempotencyKey: crypto.randomUUID(), process: { runId: process.runId, expectedStateVersion: process.stateVersion },
                presentation: { a2uiVersion: '0.8', catalogId: definition.catalogId } };
            const value = await send(event ? { kind: 'data', data: { userAction: event },
                metadata: { mimeType: 'application/json+a2ui', 'aidp.a2app': metadata } } :
                { protocol: 'aidp.a2app', type: 'command', ...metadata, componentId, actionId, arguments: args }, !human);
            if (value.result?.markdown) {
                const url = URL.createObjectURL(new Blob([value.result.markdown], { type: 'text/markdown;charset=utf-8' }));
                const a = el('a');
                a.href = url;
                a.download = value.result.filename || 'report.md';
                a.click();
                setTimeout(() => URL.revokeObjectURL(url), 1000);
            }
            if (value.status === 'accepted' || (!human && editing()))
                poll(componentId);
        }
        catch (e) {
            failure = e instanceof Error ? e.message : String(e);
        }
        finally {
            if (human)
                pending = false;
            if (human || (!pending && !editing()))
                render();
        }
    }
    function render() {
        if (!definition)
            return;
        target.replaceChildren();
        const nav = el('nav');
        for (const c of definition.components) {
            const b = el('button', c.label, selected === c.id ? '' : 'secondary');
            b.disabled = pending;
            b.onclick = () => { selected = c.id; render(); void invoke(c.id, 'display'); };
            nav.append(b);
        }
        target.append(nav);
        if (failure) {
            const alert = el('p', failure, 'error');
            alert.setAttribute('role', 'alert');
            target.append(alert);
        }
        if (pending || last.status === 'accepted') {
            const status = el('p', `Agent is working${process.completed !== undefined ? ` · ${process.completed} / ${process.total}` : ''}…`, 'status');
            status.setAttribute('role', 'status');
            target.append(status);
        }
        const cdef = definition.components.find(c => c.id === selected);
        const surface = surfaces.get(selected);
        if (surface) {
            const nodes = new Map(surface.components.map((n) => [n.id, n]));
            const path = new Set(), model = models.get(selected) || {};
            function component(id) {
                if (path.has(id) || path.size > 12 || !nodes.has(id))
                    throw new Error('Invalid A2UI component graph');
                path.add(id);
                const [kind, p] = Object.entries(nodes.get(id).component)[0];
                let box;
                if (kind === 'Column' || kind === 'Row') {
                    box = el('div', undefined, kind.toLowerCase());
                    for (const child of p.children.explicitList)
                        box.append(component(child));
                }
                else if (kind === 'Text' || kind === 'RuleSection')
                    box = el('p', String(bound(p.text, model) ?? ''), kind === 'RuleSection' && p.highlight === true ? 'rule-highlight' : '');
                else if (kind === 'TextField' || kind === 'CheckBox' || kind === 'MultipleChoice') {
                    box = el('label');
                    const binding = kind === 'CheckBox' ? p.value : kind === 'MultipleChoice' ? p.selections : p.text;
                    const label = kind === 'MultipleChoice' ? 'Select one' : String(bound(p.label, model) ?? '');
                    box.append(document.createTextNode(label));
                    if (kind === 'MultipleChoice') {
                        const input = el('select');
                        input.setAttribute('aria-label', label);
                        for (const choice of p.options) {
                            const option = el('option', String(bound(choice.label, model)));
                            option.value = choice.value;
                            input.append(option);
                        }
                        input.value = bound(binding, model)?.[0] ?? '';
                        input.onchange = () => write(model, binding.path, [input.value]);
                        box.append(input);
                    }
                    else {
                        const input = el('input');
                        input.type = kind === 'CheckBox' ? 'checkbox' : p.textFieldType === 'number' ? 'number' : 'text';
                        input.setAttribute('aria-label', label);
                        if (kind === 'CheckBox') {
                            input.checked = bound(binding, model) === true;
                            input.onchange = () => write(model, binding.path, input.checked);
                        }
                        else {
                            input.value = String(bound(binding, model) ?? '');
                            input.oninput = () => write(model, binding.path, p.textFieldType === 'number' && input.value !== '' ? Number(input.value) : input.value);
                        }
                        box.append(input);
                    }
                }
                else if (kind === 'Button') {
                    const name = p.action.name, split = name.lastIndexOf('.'), cid = name.slice(0, split), aid = name.slice(split + 1);
                    if (cid !== selected || !cdef.actions[aid])
                        throw new Error('View references an undeclared action');
                    const button = el('button');
                    button.append(component(p.child));
                    button.disabled = pending;
                    button.onclick = () => {
                        const args = {};
                        for (const entry of p.action.context || []) {
                            let value = bound(entry.value, model);
                            const kind = cdef.actions[aid].inputSchema.properties[entry.key]?.type;
                            if (kind === 'boolean' && typeof value === 'string' && /^(true|false)$/i.test(value))
                                value = value.toLowerCase() === 'true';
                            else if (['integer', 'number'].includes(kind) && typeof value === 'string' && value.trim() !== '' && Number.isFinite(Number(value)))
                                value = Number(value);
                            args[entry.key] = value;
                        }
                        void invoke(cid, aid, args, true, { name, surfaceId: selected, sourceComponentId: id, timestamp: new Date().toISOString(), context: args });
                    };
                    box = button;
                }
                else if (kind === 'Markdown') {
                    box = el('pre', p.text, 'a2app-markdown');
                }
                else if (kind === 'Metric') {
                    box = el('section', undefined, 'card');
                    box.append(el('p', p.label), el('h2', p.format === 'usd'
                        ? new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(p.value) : String(p.value)));
                }
                else if (kind === 'Action')
                    box = actionControl(p.componentId, p.actionId, p.arguments, p.defaults);
                else if (kind === 'BarChart') {
                    box = chart(p);
                }
                else if (kind === 'Table') {
                    box = el('section', undefined, 'card batch-table');
                    const table = el('table'), head = el('tr');
                    for (const col of p.columns)
                        head.append(el('th', col.label));
                    if (p.rowAction || p.actionChildren)
                        head.append(el('th', ''));
                    const thead = el('thead');
                    thead.append(head);
                    table.append(thead);
                    const tbody = el('tbody');
                    for (const [index, row] of p.rows.entries()) {
                        const tr = el('tr');
                        for (const col of p.columns)
                            tr.append(el('td', String(row[col.key] ?? '—')));
                        if (p.actionChildren) {
                            const td = el('td');
                            td.append(component(p.actionChildren.explicitList[index]));
                            tr.append(td);
                        }
                        else if (p.rowAction) {
                            const args = {};
                            for (const [key, val] of Object.entries(p.rowAction.arguments))
                                args[key] = typeof val === 'string' && val.startsWith('$row.') ? row[val.slice(5)] : val;
                            const td = el('td');
                            td.append(actionControl(p.rowAction.componentId, p.rowAction.actionId, args));
                            tr.append(td);
                        }
                        tbody.append(tr);
                    }
                    table.append(tbody);
                    box.append(table);
                }
                else
                    throw new Error('Unsupported A2App visual component: ' + kind);
                path.delete(id);
                return box;
            }
            target.append(component(roots.get(selected) || 'root'));
        }
        function actionControl(cid, aid, bound, defaults = {}) {
            const action = definition.components.find(c => c.id === cid)?.actions[aid];
            if (!action)
                throw new Error('View references an undeclared action');
            const form = el('form'), inputs = new Map();
            for (const [key, schema] of Object.entries(action.inputSchema.properties)) {
                if (key in bound)
                    continue;
                const control = schema.enum ? el('select') : el('input');
                if (schema.enum)
                    for (const value of schema.enum) {
                        const option = el('option', value);
                        option.value = value;
                        control.append(option);
                    }
                else if (control instanceof HTMLInputElement) {
                    control.type = schema.type === 'boolean' ? 'checkbox' : (['integer', 'number'].includes(schema.type) ? 'number' : 'text');
                    control.placeholder = schema.description || key;
                    if (schema.maxLength)
                        control.maxLength = schema.maxLength;
                }
                if (schema.type === 'boolean' && control instanceof HTMLInputElement)
                    control.checked = defaults[key] === true;
                else if (key in defaults)
                    control.value = String(defaults[key]);
                control.required = schema.type !== 'boolean' && !!action.inputSchema.required?.includes(key);
                control.setAttribute('aria-label', key);
                const label = el('label', key.replaceAll(/([A-Z])/g, ' $1'));
                label.append(control);
                form.append(label);
                inputs.set(key, control);
            }
            const b = el('button', action.label);
            b.type = 'submit';
            b.disabled = pending;
            form.append(b);
            form.onsubmit = e => { e.preventDefault(); const args = { ...bound }; for (const [key, input] of inputs) {
                const kind = action.inputSchema.properties[key].type;
                if (kind === 'boolean') {
                    args[key] = input instanceof HTMLInputElement ? input.checked : input.value === 'true';
                    continue;
                }
                if (!input.value && !input.required)
                    continue;
                args[key] = ['integer', 'number'].includes(kind) ? Number(input.value) : input.value;
            } void invoke(cid, aid, args); };
            return form;
        }
        const chat = el('details');
        chat.append(el('summary', 'Chat with this agent · read-only'));
        const form = el('form'), input = el('input');
        input.placeholder = 'Ask about the saved results or available capabilities';
        input.required = true;
        input.maxLength = 4000;
        input.setAttribute('aria-label', 'Chat message');
        const ask = el('button', 'Send');
        ask.type = 'submit';
        ask.disabled = pending;
        form.append(input, ask);
        form.onsubmit = e => {
            e.preventDefault();
            if (pending)
                return;
            pending = true;
            failure = '';
            const text = input.value;
            clearTimeout(timer);
            render();
            void (async () => { await backgroundRequest; clearTimeout(timer); return send({ type: 'chat', definitionRevision: revision, text, context: { componentId: selected, runId: process.runId } }); })()
                .then(v => { chatAnswer = v.message; if (v.status === 'accepted')
                poll(); }).catch(e => { failure = String(e); }).finally(() => { pending = false; render(); });
        };
        chat.append(form);
        if (chatAnswer) {
            chat.open = true;
            chat.append(el('p', chatAnswer));
        }
        target.append(chat);
        const info = el('details');
        info.append(el('summary', 'A2App contract and runtime details'), el('pre', JSON.stringify({ definitionRevision: revision, process, telemetry: last.telemetry, lastRequest, lastUserAction, definition }, null, 2)));
        if (warning)
            info.append(el('p', warning, 'error'));
        target.append(info);
    }
    try {
        await send({ type: 'command', componentId: '@app', actionId: 'initialize', arguments: {}, presentation: { a2uiVersion: '0.8', catalogId: initialCatalog } });
        selected = definition.components[0].id;
        render();
        if (last.status === 'accepted')
            poll();
    }
    catch (e) {
        target.replaceChildren(el('p', e instanceof Error ? e.message : String(e), 'error'));
    }
}
