/** Deterministic renderer for the shared catalog's BarChart component. */
function el(tag, text, css) {
  const item = document.createElement(tag);
  if (text !== undefined) item.textContent = String(text);
  if (css) item.className = css;
  return item;
}

export function chart(props) {
  if (!Array.isArray(props.rows) || typeof props.label !== 'string' || typeof props.value !== 'string') {
    throw new Error('A chart requires rows, label and value fields.');
  }
  const section = el('section', undefined, 'card chart');
  section.append(el('h2', props.title || 'Chart'));
  const {rows, label, value} = props;
  const numbers = rows.map(row => {
    const number = row[value];
    if (!['number', 'string'].includes(typeof number) || (typeof number === 'string' && !number.trim())) {
      throw new Error('Chart values must be finite numbers.');
    }
    return Number(number);
  });
  if (!numbers.every(Number.isFinite)) throw new Error('Chart values must be finite numbers.');
  const formatter = new Intl.NumberFormat('en-US', props.format === 'usd'
    ? {style: 'currency', currency: 'USD', maximumFractionDigits: 2} : {});
  const format = number => formatter.format(number);
  if (!rows.length) {
    section.append(el('p', 'No data for this scope.'));
    return section;
  }
  const maximum = Math.max(...numbers.map(Math.abs), 1);
  if (rows.length > 15) {
    const negative = numbers.some(number => number < 0);
    const plot = el('div', undefined, negative ? 'plot mixed' : 'plot');
    for (const [index, row] of rows.entries()) {
      const column = el('div', undefined, 'column');
      const number = numbers[index];
      const bar = el('div', undefined, number < 0 ? 'bar negative' : 'bar');
      bar.style.height = `${Math.abs(number) / maximum * (negative ? 48 : 95)}%`;
      bar.title = `${row[label]}: ${format(number)}`;
      bar.setAttribute('role', 'img');
      bar.setAttribute('aria-label', bar.title);
      column.append(bar);
      plot.append(column);
    }
    const ticks = el('div', undefined, 'ticks');
    for (const index of [0, Math.floor(rows.length / 2), rows.length - 1]) {
      ticks.append(el('span', rows[index][label]));
    }
    section.append(plot, ticks);
  } else {
    for (const [index, row] of rows.entries()) {
      const number = numbers[index], line = el('div', undefined, 'rank-row');
      const text = el('span', row[label], 'value-label');
      text.title = String(row[label]);
      const track = el('div', undefined, 'track');
      const bar = el('div', undefined, number < 0 ? 'rank-bar negative' : 'rank-bar');
      bar.style.width = `${Math.abs(number) / maximum * 100}%`;
      track.append(bar);
      line.append(text, track, el('strong', format(number)));
      section.append(line);
    }
  }
  const details = el('details');
  details.append(el('summary', `View ${rows.length} values`));
  const table = el('table'), headings = el('tr');
  headings.append(el('th', props.labelLabel || label), el('th', props.valueLabel || value));
  const head = el('thead'); head.append(headings); table.append(head);
  const body = el('tbody');
  for (const [index, row] of rows.entries()) {
    const tr = el('tr');
    tr.append(el('td', row[label]), el('td', format(numbers[index])));
    body.append(tr);
  }
  table.append(body); details.append(table); section.append(details);
  return section;
}
