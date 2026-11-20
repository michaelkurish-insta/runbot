// Run with: node tests/test_review_ui.mjs
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../runbase/review/static/app.js', import.meta.url), 'utf8');
new vm.Script(source);
const between = (start, end) => {
    const begin = source.indexOf(start);
    const finish = source.indexOf(end, begin + start.length);
    assert.ok(begin >= 0 && finish > begin, `Missing function boundaries: ${start}, ${end}`);
    return source.slice(begin, finish);
};
const splitFunctions = between('    function buildIntervalTables(', '    function renderSummary(')
    + between('    function wireIntervalEditing(', '    function parseDurationPrecise(')
    + between('    function refreshIntervalsTable(', '    function buildEditForm(');
const options = source.match(/    const IV_ZONE_OPTIONS = .*;/)[0]
    + source.match(/    const IV_ZONE_LABELS = .*;/)[0];
const calls = [];
const data = { splits: [{id: 10, activity_id: 1, rep_number: 1, pace_zone: 'E',
    source: 'mile_split', gps_measured_distance_mi: 1, duration_s: 600, intensity_points: 2}], intervals: [], laps: [], summary: [] };
class Element {
    constructor(tag) {
        this.tag = tag; this.dataset = {}; this.children = []; this.attributes = {};
        this.disabled = false; this.hidden = false;
    }
    addEventListener(type, callback) { this[type] = callback; }
    setAttribute(name, value) { this.attributes[name] = value; }
    getAttribute(name) { return this.attributes[name]; }
    appendChild(child) { child.parent = this; this.children.push(child); }
    remove() { this.parent.children = this.parent.children.filter(child => child !== this); }
    focus() { this.focused = true; }
    matches(selector) {
        if (selector === '[data-table-aid="1"]') return this.dataset.tableAid === '1';
        if (selector.startsWith('.')) return (this.className || '').split(' ').includes(selector.slice(1));
        return selector.split(', ').includes(this.tag);
    }
    closest(selector) { return this.matches(selector) ? this : this.parent?.closest(selector); }
    querySelectorAll(selector) {
        return this.children.flatMap(child => [
            ...(child.matches(selector) ? [child] : []), ...child.querySelectorAll(selector),
        ]);
    }
    querySelector(selector) { return this.querySelectorAll(selector)[0] || null; }
}
const table = new Element('div');
table.dataset.tableAid = '1';
table.className = 'activity-interval-tables';
table.renders = 0;
Object.defineProperty(table, 'innerHTML', {set(value) {
    this.html = value; this.renders++;
    // Mount the next rendered row so subsequent edits use fresh controls.
    this.children = [];
    const cell = new Element('td');
    const trigger = new Element('button');
    trigger.className = 'iv-zone-trigger';
    trigger.dataset = {intervalId: '10', activityId: '1', zone: data.splits[0].pace_zone,
        overridden: String(!!data.splits[0].zone_overridden)};
    trigger.setAttribute('aria-label', 'Edit intensity for split 1');
    cell.appendChild(trigger); this.appendChild(cell);
}});
const container = new Element('div');
container.appendChild(table);
const context = vm.createContext({ modalNeedsEnrich: false, updateModalButtons() {},
    document: {createElement: tag => new Element(tag)},
    wireWalkingButtons() {}, wireIntervalCheckboxes() {},
    saveIntervalEdit: async (...args) => {
        calls.push(args); data.splits[0].pace_zone = args[2]; data.splits[0].zone_overridden = true;
        data.splits[0].intensity_points = args[2] === 'M' ? 4 : 2;
        return {ok: true};
    }, fetch: async () => ({ok: true, json: async () => data}) });
vm.runInContext(options + splitFunctions, context);
table.innerHTML = context.buildIntervalTables(data);
table.renders = 0;
assert.ok(table.html.includes('iv-zone-trigger'));
assert.ok(!table.html.includes('<select'), 'intensity controls should be hidden until clicked');
assert.ok(table.html.includes('>Points</th>'));
assert.ok(table.html.includes('<td class="iv-points">2.00</td>'));
assert.ok(!table.html.includes('split-note'));
context.wireIntervalEditing(container);
const clickIntensity = () => {
    const trigger = table.querySelector('.iv-zone-trigger');
    trigger.click({stopPropagation() {}});
    assert.equal(trigger.hidden, true);
    return table.querySelector('select');
};
let select = clickIntensity();
assert.equal(select.value, '', 'unmodified zones should default to Auto');
select.value = 'M';
await select.change();
await select.blur();
assert.equal(table.renders, 1);
assert.ok(table.html.includes('<h3>Mile splits</h3>'));
assert.ok(table.html.includes('data-zone="M" data-overridden="true"'));
assert.ok(table.html.includes('<td class="iv-points">4.00</td>'));
select = clickIntensity();
assert.equal(select.value, 'M');
select.value = 'E';
await select.change();
assert.equal(table.renders, 2, 'a second edit must refresh the table itself');
assert.ok(table.html.includes('<td class="iv-points">2.00</td>'));
assert.deepEqual(calls, [['10', 'pace_zone', 'M'], ['10', 'pace_zone', 'E']]);
select = clickIntensity();
select.value = 'T';
select.keydown({key: 'Escape', stopPropagation() {}, preventDefault() {}});
await select.blur();
assert.equal(table.querySelector('select'), null);
assert.equal(table.querySelector('.iv-zone-trigger').hidden, false);
assert.equal(calls.length, 2, 'Escape must cancel without saving on blur');
console.log('Split intensity UI: hidden until click, compact columns, consecutive edits, and cancel pass.');

const saveFunctions = between('    function hasFieldChanges(', '    function confirmDiscard(')
    + between('    function wireActivityFlags(', '    // ── Render charts');
function saveHarness() {
    const error = new Element('p');
    const modal = new Element('div');
    const flag = new Element('input');
    flag.className = 'flag-cb'; flag.dataset = {aid: '1', flag: 'exclude_from_vdot'};
    flag.checked = false;
    const save = new Element('button'), saveClose = new Element('button');
    modal.appendChild(flag); modal.appendChild(save); modal.appendChild(saveClose);
    const events = [];
    const context = vm.createContext({
        modalDirtyState: {}, modalClearQueue: {}, modalVdotState: {}, modalFlagState: {},
        modalNeedsEnrich: false, modalNeedsRefresh: false, modalSaving: false,
        modalActivityIds: [1], modalDate: '2026-11-07', modalEl: modal,
        modalBtnSave: save, modalBtnSaveClose: saveClose,
        document: {getElementById: () => error, querySelector: () => null, querySelectorAll: () => []},
        console: {error() {}}, refreshFooterStats() {}, refreshSevenDayMA() {},
        closeModal() { events.push('close'); },
        location: {reload() { events.push('reload'); }},
        sessionStorage: {setItem() {}},
        fetch: async url => { events.push(url); return {ok: true, json: async () => ({ok: true})}; },
    });
    vm.runInContext(saveFunctions, context);
    context.wireActivityFlags(modal);
    const toggle = checked => { flag.checked = checked; flag.change(); };
    return {context, flag, save, saveClose, error, events, toggle};
}

{
    const h = saveHarness();
    h.toggle(true);
    assert.deepEqual(h.events, [], 'flags should be staged until Save');
    assert.equal(h.saveClose.disabled, false);
    let resolveWrite;
    const write = new Promise(resolve => { resolveWrite = resolve; });
    h.context.fetch = async (url, options) => {
        h.events.push(url);
        assert.deepEqual(JSON.parse(options.body), {exclude_from_vdot: true});
        return write;
    };
    const saving = h.context.saveModalChanges(true);
    assert.deepEqual(h.events, ['/api/activity/1/flags']);
    assert.equal(h.flag.disabled, true);
    assert.equal(h.saveClose.disabled, true);
    await h.context.saveModalChanges(true); // second click cannot start another save
    resolveWrite({ok: true, json: async () => ({ok: true})});
    await saving;
    assert.deepEqual(h.events, ['/api/activity/1/flags', 'close', 'reload']);
    assert.equal(h.context.modalNeedsEnrich, false, 'exclusions must not rewrite activity metrics');
}
{
    const h = saveHarness();
    h.toggle(true);
    h.context.fetch = async () => ({ok: false, json: async () => ({error: 'Database unavailable'})});
    await h.context.saveModalChanges(true);
    assert.deepEqual(h.events, [], 'failed flag write cannot close or reload');
    assert.equal(h.error.hidden, false);
    assert.equal(h.error.textContent, 'Database unavailable');
    assert.equal(h.context.isDirty(), true);
    assert.equal(h.saveClose.disabled, false, 'failed save must be retryable');
    h.context.fetch = async () => ({ok: true, json: async () => ({ok: true})});
    await h.context.saveModalChanges(true);
    assert.deepEqual(h.events, ['close', 'reload']);
}
{
    const h = saveHarness();
    h.toggle(true);
    await h.context.saveModalChanges(false);
    assert.deepEqual(h.events, ['/api/activity/1/flags']);
    assert.equal(h.saveClose.disabled, false, 'Save followed by Save & Close must still refresh the page');
    h.toggle(false);
    await h.context.saveModalChanges(true);
    assert.deepEqual(h.events, ['/api/activity/1/flags', '/api/activity/1/flags', 'close', 'reload']);
}
{
    const h = saveHarness();
    h.toggle(true); h.toggle(false);
    assert.equal(h.context.isDirty(), false, 'undoing a staged flag change should leave no writes');
    assert.equal(h.saveClose.disabled, true);
}
{
    const h = saveHarness();
    h.flag.dataset.flag = 'suppress_hr';
    h.toggle(true);
    h.context.fetch = async url => {
        h.events.push(url);
        return {ok: true, json: async () => url === '/api/enrich'
            ? {ok: true, results: [{id: 1, ok: false, error: 'Calculation failed'}]}
            : {ok: true}};
    };
    await h.context.saveModalChanges(true);
    assert.deepEqual(h.events, ['/api/activity/1/flags', '/api/enrich']);
    assert.equal(h.error.textContent, 'Calculation failed');
    assert.equal(h.saveClose.disabled, false);
    assert.equal(h.context.modalNeedsEnrich, true);
    h.context.fetch = async url => {
        h.events.push(url);
        return {ok: true, json: async () => ({ok: true})};
    };
    await h.context.saveModalChanges(true);
    assert.deepEqual(h.events.slice(-4), ['/api/activity/1/flags', '/api/enrich', 'close', 'reload']);
}
console.log('Save & Close: staged flags, write ordering, fresh reload, reenabling, and failure recovery pass.');

{
    const label = {};
    const checkbox = {checked: false, dataset: {}, closest: () => label};
    const context = vm.createContext({modalFlagState: {}, modalEl: {querySelector: () => checkbox}});
    vm.runInContext(between('    function syncVdotExclusion(', '    function wireActivityFlags('), context);
    context.syncVdotExclusion(1, true, 'strides');
    assert.equal(checkbox.checked, true);
    assert.equal(checkbox.dataset.savedChecked, 'true');
    assert.ok(label.title.includes('40%'));
    context.modalFlagState[1] = {exclude_from_vdot: false};
    checkbox.checked = false;
    context.syncVdotExclusion(1, true, 'strides');
    assert.equal(checkbox.checked, false, 'a pending manual inclusion must not be replaced');
    context.modalFlagState = {};
    context.syncVdotExclusion(1, false, null);
    assert.equal(checkbox.checked, false);
    assert.equal(checkbox.dataset.savedChecked, 'false');
    assert.equal(label.title, '');
}
console.log('Stride default UI: automatic exclusion is shown without replacing a staged user choice.');

const timeContext = vm.createContext({});
vm.runInContext(between('    function formatActivityStartTime(', '    // ── Weather display helper'), timeContext);
for (const [value, expected] of [
    ['2026-11-14T09:22:32', '9:22 AM'],
    ['2026-06-24T09:22:32', '9:22 AM'],
    ['2026-11-14 15:22:32.123', '3:22 PM'],
    ['2026-11-14T00:05:00', '12:05 AM'],
    ['2026-11-14T12:05:00', '12:05 PM'],
    ['2026-11-01T01:30:00', '1:30 AM'], // DST transition: keep the stored local clock
    ['2026-11-14T14:22:32Z', '9:22 AM'],
    ['2026-06-24T13:22:32+00:00', '9:22 AM'],
    ['2026-11-14T09:22:32-05:00', '9:22 AM'],
    [null, '—'], ['', '—'], ['not-a-date', '—'], ['2026-11-14T25:22:32', '—'],
]) {
    assert.equal(timeContext.formatActivityStartTime(value), expected, `Start time: ${value}`);
}
// Local wall times must not depend on the browser's clock or time zone at all.
timeContext.Date = class { constructor() { throw new Error('Unexpected time zone conversion'); } };
assert.equal(timeContext.formatActivityStartTime('2026-11-14T09:22:32'), '9:22 AM');
console.log('Activity time: local clocks, explicit offsets, DST, noon/midnight, and missing values pass.');

const shoePage = {};
const shoeContext = vm.createContext({
    document: {querySelector: () => shoePage},
    fetch: async url => ({json: async () => url.endsWith('/monthly') ? [] : [
        {id: 1, name: 'Daily', total_miles: 12, activity_count: 2, avg_pace: 480,
            avg_cvd: 57.1428, cvd_count: 2, retired: false},
        {id: 2, name: 'No HR', total_miles: 5, activity_count: 1, avg_pace: 600,
            avg_cvd: null, cvd_count: 0, retired: false},
    ]}),
    escapeHtml: text => text,
    formatPaceLabel: value => value === 480 ? '8:00' : '10:00',
    SHOE_COLORS: ['blue', 'red'], drawCumulativeMileageChart() {}, drawMonthlyShoeChart() {},
});
vm.runInContext(between('    function loadShoesTab(', '    function drawCumulativeMileageChart('), shoeContext);
await shoeContext.loadShoesTab();
assert.ok(shoePage.innerHTML.includes('Avg CVD</th>'));
assert.ok(shoePage.innerHTML.includes('<td>8:00</td>'), 'existing pace remains visible');
assert.ok(shoePage.innerHTML.includes('title="2 activities included in CVD">57.1</td>'));
assert.ok(shoePage.innerHTML.includes('title="0 activities included in CVD">—</td>'));
console.log('Shoes UI: average CVD, eligible activity count, and missing-data display pass.');
