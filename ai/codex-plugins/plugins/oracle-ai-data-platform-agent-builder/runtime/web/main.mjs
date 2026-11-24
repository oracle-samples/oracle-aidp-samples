import {a2appClient} from './a2app.mjs';
import {chart} from './charts.mjs';

const target = document.querySelector('#app');
try {
  const response = await fetch('/api/config', {credentials: 'same-origin'});
  if (!response.ok) throw new Error('The local client configuration is unavailable.');
  const config = await response.json();
  if (typeof config.agentKey !== 'string' || typeof config.catalogId !== 'string') {
    throw new Error('The local client configuration is incomplete.');
  }
  const title = typeof config.title === 'string' ? config.title : 'Agent application';
  document.title = title;
  document.querySelector('#title').textContent = title;
  await a2appClient(config.agentKey, target, chart, config.catalogId);
} catch (error) {
  const alert = document.createElement('p');
  alert.className = 'error';
  alert.setAttribute('role', 'alert');
  alert.textContent = error instanceof Error ? error.message : String(error);
  target.replaceChildren(alert);
}
