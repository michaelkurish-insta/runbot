// Run with: node tests/test_trends_ui.mjs
import assert from 'node:assert/strict';
import fs from 'node:fs';
import vm from 'node:vm';

const source = fs.readFileSync(new URL('../runbase/review/static/app.js', import.meta.url), 'utf8');
new vm.Script(source);
function between(start, end) {
    const a = source.indexOf(start), b = source.indexOf(end, a + start.length);
    assert.ok(a >= 0 && b > a);
    return source.slice(a, b);
}
class Element {
    constructor(tag) { this.tag = tag; this.children = []; this.attributes = {}; this.dataset = {}; }
    appendChild(child) { child.parentElement = this; this.children.push(child); }
    remove() { this.parentElement.children = this.parentElement.children.filter(c => c !== this); }
    setAttribute(key, value) { this.attributes[key] = value; }
    querySelector(selector) {
        return this.children.find(c => selector[0] === '.' ? c.className === selector.slice(1) : c.tag === selector) || null;
    }
}
function makeCanvas(id, width = 530) {
    const parent = new Element('div');
    const heading = new Element('h3'); heading.textContent = id; parent.appendChild(heading);
    const canvas = new Element('canvas');
    canvas.id = id;
    parent.appendChild(canvas);
    canvas.calls = [];
    const ctx = new Proxy({}, { get(target, name) {
        if (name in target) return target[name];
        if (name === 'measureText') return text => ({width: text.length * 6});
        return (...args) => {
            for (const n of args.filter(a => typeof a === 'number')) assert.ok(Number.isFinite(n), `${name}: nonfinite coordinate`);
            canvas.calls.push({name, args});
            if (name === 'getImageData') return {width: args[2], height: args[3]};
        };
    }});
    canvas.getContext = () => ctx;
    canvas.getBoundingClientRect = () => ({left: 10, top: 20, width, height: 200});
    canvas.focus = () => { canvas.focused = true; };
    canvas.setPointerCapture = id => { canvas.capture = id; };
    canvas.hasPointerCapture = id => canvas.capture === id;
    canvas.releasePointerCapture = () => { canvas.capture = null; };
    return canvas;
}
const ids = ['volume', 'pace', 'hr', 'fitness', 'vdot', 'predicted'];
const canvases = Object.fromEntries(ids.map(id => [id, makeCanvas(id, id === 'predicted' ? 1100 : 530)]));
const context = vm.createContext({
    window: {devicePixelRatio: 2},
    document: {getElementById: id => canvases[id], createElement: tag => new Element(tag)},
    escapeHtml: value => value,
});
vm.runInContext(between('    function formatPaceLabel(', '    function dateXPositioner(')
    + between('    function trendsDateTime(', '    // ═══════════════════════════════════════════════════════════════\n    // ── Race Prediction Tab'), context);

const weekly = ['2026-10-04', '2026-10-11', '2026-10-18', '2026-10-25', '2026-11-01', '2026-11-08', '2026-11-15']
    .map((week, i) => ({week, distance: 40 + i * 3, avg_pace: 470 + i, avg_hr: 145 + i}));
const axis = context.trendsDateAxis({date_range: {start: '2025-11-19', end: '2026-11-19'}, weekly}, 530);
assert.equal(new Date(axis.start).toISOString().slice(0, 10), '2025-11-19');
assert.ok(axis.ticks.length >= 5);
for (const tick of axis.ticks) {
    const date = new Date(tick.time);
    assert.equal(date.getUTCDate(), 1);
    assert.equal(date.getUTCHours(), 0);
    assert.match(tick.label, /^\d{2}\/\d{2}$/);
    assert.equal(Number(tick.label.slice(0, 2)), date.getUTCMonth() + 1);
}
const monthly = context.trendsDateAxis({date_range: {start: '2026-02-01', end: '2026-05-01'}}, 530);
assert.deepEqual(Array.from(monthly.ticks, t => t.label), ['02/26', '03/26', '04/26', '05/26']);
assert.equal(monthly.ticks[2].time - monthly.ticks[1].time, 31 * 86400000, 'DST must not move calendar ticks');
const partialWeek = context.trendsDateAxis({date_range: {start: '2026-11-19', end: '2026-11-19'}, weekly: [{week: '2026-11-15'}]}, 530);
assert.equal(new Date(partialWeek.start).toISOString().slice(0, 10), '2026-11-15', 'retain the first partial weekly bucket');
const singleDay = context.trendsDateAxis({date_range: {start: '2026-11-19', end: '2026-11-19'}}, 530);
assert.ok(singleDay.end > singleDay.start);
assert.equal(context.formatPaceLabel(479.6), '8:00');

context.drawTrendsBarChart('volume', weekly, {axis, maWindow: 6});
context.drawTrendsLineChart('pace', [{label: 'Pace', color: '#4a7ab5', fill: true,
    data: weekly.map(w => ({date: w.week, value: w.avg_pace}))}],
    {axis, weekly: true, invertY: true, formatY: context.formatPaceLabel, formatValue: v => `${context.formatPaceLabel(v)}/mi`});
context.drawTrendsLineChart('hr', [{label: 'HR', color: '#c05050',
    data: weekly.map(w => ({date: w.week, value: w.avg_hr}))}], {axis});
context.drawTrendsLineChart('fitness', [
    {label: 'CTL', color: '#4a7ab5', data: [{date: '2026-11-10', value: 50}]},
    {label: 'ATL', color: '#d9a030', data: [{date: '2026-11-10', value: 70}]},
    {label: 'TSB', color: '#50a050', data: [{date: '2026-11-10', value: -20}]},
], {axis, zeroLine: true, legend: true});
context.drawScatterChart('vdot', [{date: '2026-04-12', value: 55, label: 'Spring race'}], axis);
context.drawPredictedVdotChart('predicted', [{date: '2026-11-10', value: 58}], [{date: '2026-11-10', value: 61}], axis);

function event(canvas, x, y, extras = {}) {
    const rect = canvas.getBoundingClientRect();
    return {button: 0, buttons: 1, pointerId: 7, clientX: rect.left + x, clientY: rect.top + y,
        preventDefault() { this.prevented = true; }, ...extras};
}
function readout(canvas) { return canvas.parentElement.querySelector('.trends-chart-readout').textContent; }
const monthLabels = canvas => canvas.calls.filter(c => c.name === 'fillText' && /^\d{2}\/\d{2}$/.test(c.args[0]));
for (const canvas of Object.values(canvases)) {
    const labels = monthLabels(canvas);
    assert.deepEqual(labels.map(c => c.args[0]), Array.from(axis.ticks, t => t.label), `${canvas.id}: same month labels`);
    for (let i = 0; i < labels.length; i++) {
        const fraction = (labels[i].args[1] - 52) / (canvas.getBoundingClientRect().width - 64);
        assert.ok(Math.abs(fraction - (axis.ticks[i].time - axis.start) / (axis.end - axis.start)) < 1e-10);
    }
    assert.equal(canvas.width, canvas.getBoundingClientRect().width * 2, 'retina resolution');
    assert.equal(canvas.height, 400);
    assert.equal(canvas.tabIndex, 0);
    canvas.onpointerdown(event(canvas, 200, 80));
    assert.equal(canvas.capture, 7, 'drag must retain pointer outside the canvas');
    assert.ok(!readout(canvas).includes('Click and drag'));
    canvas.onpointerup(event(canvas, 200, 80, {buttons: 0}));
    assert.equal(canvas.capture, null);
    const held = readout(canvas);
    canvas.onpointermove(event(canvas, 450, 90, {buttons: 0}));
    assert.equal(readout(canvas), held, 'selection persists after release');
}
assert.match(readout(canvases.volume), /Week of .*Miles: .*mi/);
assert.match(readout(canvases.pace), /Week of .*Pace: .*\/mi/);
assert.match(readout(canvases.vdot), /04\/12\/26 · VDOT: 55.0 · Spring race/);

// Verify both axis guides, exact values, series picking, capture, cancellation, and keyboard movement.
const canvas = makeCanvas('interaction');
const chart = context.initTrendsChart(canvas, axis);
chart.yPos = value => 170 - value;
context.wireTrendsCrosshair(canvas, chart, [
    {date: '2026-03-01', value: 50, series: 'CTL'},
    {date: '2026-03-01', value: 70, series: 'ATL'},
    {date: '2026-04-01', value: 60, series: 'CTL'},
]);
const marchX = chart.xPos(context.trendsDateTime('2026-03-01'));
const aprilX = chart.xPos(context.trendsDateTime('2026-04-01'));
canvas.onpointerdown(event(canvas, marchX, 100));
assert.match(readout(canvas), /03\/01\/26 · ATL: 70.0/);
assert.ok(canvas.calls.some(c => c.name === 'moveTo' && c.args[0] === 52 && c.args[1] === 100), 'horizontal guide');
assert.ok(canvas.calls.some(c => c.name === 'lineTo' && c.args[0] === marchX && c.args[1] === 170), 'vertical guide');
assert.ok(canvas.calls.some(c => c.name === 'fillText' && c.args[0] === '70.0'), 'exact y-axis label');
assert.ok(canvas.calls.some(c => c.name === 'fillText' && c.args[0] === '03/01/26'), 'exact x-axis label');
canvas.onpointermove(event(canvas, aprilX, 110, {pointerId: 8}));
assert.match(readout(canvas), /ATL/, 'ignore a different pointer');
canvas.onpointermove(event(canvas, aprilX, 110));
assert.match(readout(canvas), /04\/01\/26 · CTL: 60.0/);
canvas.onpointermove(event(canvas, 1000, 110));
assert.match(readout(canvas), /04\/01\/26/, 'outside drag clamps to last data point');
canvas.onpointerup(event(canvas, 1000, 110, {buttons: 0}));
canvas.onkeydown({key: 'ArrowLeft', preventDefault() {}});
assert.match(readout(canvas), /03\/01\/26 · CTL: 50.0/, 'keyboard retains selected series');
canvas.onkeydown({key: 'Escape'});
assert.equal(readout(canvas), 'Click and drag to inspect');
canvas.onpointerdown(event(canvas, marchX, 120));
canvas.onpointercancel();
canvas.onpointermove(event(canvas, aprilX, 110));
assert.match(readout(canvas), /03\/01\/26/, 'cancel ends dragging');
canvas.onpointerdown(event(canvas, aprilX, 110, {button: 2}));
assert.match(readout(canvas), /03\/01\/26/, 'right click does not change selection');

// Missing data should still display the common calendar axis and an honest empty state.
context.drawTrendsLineChart('hr', [], {axis});
assert.equal(readout(canvases.hr), 'No data in this range');
assert.equal(canvases.hr.tabIndex, -1);
assert.equal(canvases.hr.parentElement.children.filter(c => c.className === 'trends-chart-readout').length, 1);
context.drawPredictedVdotChart('predicted', [], [{date: '2026-11-10', value: 61}], axis);
canvases.predicted.onpointerdown(event(canvases.predicted, 400, 100));
assert.match(readout(canvases.predicted), /Race Actual: 61.0/, 'race scatter remains available without prediction data');
// Exercise real loading/wiring: every chart follows one cursor even with daily,
// weekly, and sparse race data, including the chart drawn at twice the width.
const container = new Element('div');
container.querySelectorAll = () => ids.map(id => canvases[id]);
const lookup = {
    'trends-charts-container': container, 'trends-volume-chart': canvases.volume,
    'trends-pace-chart': canvases.pace, 'trends-hr-chart': canvases.hr,
    'trends-fitness-chart': canvases.fitness, 'trends-vdot-chart': canvases.vdot,
    'trends-predicted-vdot-chart': canvases.predicted,
};
context.document.getElementById = id => lookup[id];
const pending = [];
context.fetch = () => new Promise(resolve => pending.push(resolve));
vm.runInContext(between('    function loadTrendsData(', '    function trendsDateTime('), context);
const oldRequest = context.loadTrendsData('6m');
const request = context.loadTrendsData('1y');
const payload = {
    date_range: {start: '2025-11-19', end: '2026-11-19'}, weekly,
    atl_ctl: [{date: '2026-11-10', ctl: 50, atl: 60, tsb: -10}, {date: '2026-11-11', ctl: 51, atl: 59, tsb: -8}],
    vdot_timeline: [{effective_date: '2026-04-12', vdot: 55, source: 'race'}],
    predicted_vdot: [{date: '2026-11-10', value: 58}, {date: '2026-11-11', value: 59}],
};
pending[1]({ok: true, json: async () => payload});
await request;
assert.ok(container.innerHTML.includes('Weekly Volume'), 'all charts render successfully');
const callsAfterLatest = canvases.volume.calls.length;
pending[0]({ok: true, json: async () => ({})});
await oldRequest;
assert.equal(canvases.volume.calls.length, callsAfterLatest, 'stale range response must not replace current charts');

const sharedX = date => 52 + (context.trendsDateTime(date) - axis.start) / (axis.end - axis.start) * (530 - 64);
const exactDateLabel = canvas => canvas.calls.filter(c => c.name === 'fillText' && /^\d{2}\/\d{2}\/\d{2}$/.test(c.args[0])).at(-1);
function assertSynced(date) {
    const fraction = (context.trendsDateTime(date) - axis.start) / (axis.end - axis.start);
    for (const canvas of Object.values(canvases)) {
        const label = exactDateLabel(canvas);
        assert.equal(label.args[0], `${date.slice(5, 7)}/${date.slice(8, 10)}/${date.slice(2, 4)}`, `${canvas.id}: same cursor date`);
        const lastRestore = canvas.calls.findLastIndex(c => c.name === 'putImageData');
        const guide = canvas.calls.slice(lastRestore).find(c => c.name === 'moveTo');
        assert.ok(Math.abs(guide.args[0] - (52 + fraction * (canvas.getBoundingClientRect().width - 64))) < 1e-8,
            `${canvas.id}: same proportional x position`);
    }
}
canvases.fitness.onpointerdown(event(canvases.fitness, sharedX('2026-11-10'), 40));
assertSynced('2026-11-10');
assert.match(readout(canvases.pace), /Week of 11\/08\/26.*nearest point/, 'weekly readout identifies the actual week');
assert.match(readout(canvases.vdot), /04\/12\/26.*nearest point/, 'sparse race readout identifies the actual race date');
canvases.fitness.onpointermove(event(canvases.fitness, sharedX('2026-11-11'), 40));
assertSynced('2026-11-11');
canvases.fitness.onpointerup(event(canvases.fitness, sharedX('2026-11-11'), 40, {buttons: 0}));
const frozen = Object.values(canvases).map(readout);
canvases.fitness.onpointermove(event(canvases.fitness, sharedX('2026-11-10'), 40, {buttons: 0}));
assert.deepEqual(Object.values(canvases).map(readout), frozen);
canvases.pace.onpointerdown(event(canvases.pace, sharedX('2026-11-15'), 50));
assertSynced('2026-11-15');
canvases.pace.onkeydown({key: 'ArrowLeft', preventDefault() {}});
assertSynced('2026-11-08');
canvases.pace.onkeydown({key: 'Escape'});
assert.ok(Object.values(canvases).every(c => readout(c) === 'Click and drag to inspect'), 'Escape clears every chart');

console.log('Trends chart axes, dragging, and synchronized crosshair tests passed');
