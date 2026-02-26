/* ── RunBase Review UI ────────────────────────────────────────── */

(function () {
    "use strict";

    const maps = {};
    const loaded = {};
    const mainContent = document.querySelector(".main-content");

    // ── Row click → expand/collapse ─────────────────────────────

    document.querySelectorAll(".activity-row").forEach(row => {
        row.addEventListener("click", () => {
            const id = row.dataset.id;
            const detailRow = document.querySelector(`.detail-row[data-id="${id}"]`);
            if (!detailRow) return;

            const isVisible = detailRow.style.display !== "none";
            document.querySelectorAll(".detail-row").forEach(r => r.style.display = "none");
            document.querySelectorAll(".activity-row").forEach(r => r.classList.remove("expanded"));

            if (!isVisible) {
                detailRow.style.display = "table-row";
                row.classList.add("expanded");
                if (!loaded[id]) {
                    const activityIds = (row.dataset.activityIds || id).split(",");
                    loadDetail(id, activityIds, row.dataset.hasStreams === "true");
                    loaded[id] = true;
                }
            }
        });
    });

    // ── Double-click to edit workout name ──────────────────────

    document.querySelectorAll(".activity-row .col-name").forEach(cell => {
        cell.addEventListener("dblclick", (e) => {
            e.stopPropagation();
            if (cell.querySelector(".inline-edit-name")) return;

            const row = cell.closest(".activity-row");
            const id = row.dataset.id;
            const currentValue = cell.textContent.trim();

            const input = document.createElement("input");
            input.type = "text";
            input.className = "inline-edit-name";
            input.value = currentValue;

            cell.textContent = "";
            cell.appendChild(input);
            input.focus();
            input.select();

            function finish(save) {
                const newValue = input.value.trim();
                input.remove();
                if (save && newValue !== currentValue) {
                    cell.textContent = newValue;
                    saveOverride(id, "workout_name", newValue).then(ok => {
                        if (ok) cell.classList.add("overridden");
                    });
                } else {
                    cell.textContent = currentValue;
                }
            }

            input.addEventListener("keydown", (ev) => {
                ev.stopPropagation();
                if (ev.key === "Enter") finish(true);
                if (ev.key === "Escape") finish(false);
            });
            input.addEventListener("blur", () => finish(true));
        });
    });

    // ── Double-click to edit workout type zone ─────────────────

    const ZONE_OPTIONS = ["", "E", "M", "T", "I", "R", "FR"];

    document.querySelectorAll(".activity-row .col-type").forEach(cell => {
        cell.addEventListener("dblclick", (e) => {
            e.stopPropagation();
            if (cell.querySelector("select")) return;

            const row = cell.closest(".activity-row");
            const id = row.dataset.id;
            const currentValue = cell.textContent.trim();

            const select = document.createElement("select");
            select.className = "inline-edit-name";
            for (const z of ZONE_OPTIONS) {
                const opt = document.createElement("option");
                opt.value = z;
                opt.textContent = z || "--";
                if (z === currentValue) opt.selected = true;
                select.appendChild(opt);
            }

            cell.textContent = "";
            cell.appendChild(select);
            select.focus();

            function finish(save) {
                const newValue = select.value;
                select.remove();
                if (save && newValue !== currentValue) {
                    cell.textContent = newValue;
                    cell.className = "col-type" + (newValue ? ` zone-${newValue}` : "");
                    if (newValue) {
                        saveOverride(id, "workout_type_zone", newValue);
                    } else {
                        clearOverride(id, "workout_type_zone");
                    }
                } else {
                    cell.textContent = currentValue;
                }
            }

            select.addEventListener("change", () => finish(true));
            select.addEventListener("keydown", (ev) => {
                ev.stopPropagation();
                if (ev.key === "Escape") finish(false);
            });
            select.addEventListener("blur", () => finish(true));
        });
    });

    // ── Click future blank row → planned activity entry ────────

    document.querySelectorAll(".future-row, .planned-row").forEach(row => {
        row.addEventListener("click", (e) => {
            if (row.querySelector(".planned-input")) return;
            const dateStr = row.dataset.date;
            if (!dateStr) return;

            // Read existing planned values if any
            const existingDist = row.querySelector(".planned-dist");
            const existingName = row.querySelector(".planned-name");
            const oldDist = existingDist ? existingDist.textContent.trim() : "";
            const oldName = existingName ? existingName.textContent.trim() : "";

            // Find the blank colspan cell or replace planned cells
            const cells = row.querySelectorAll("td");
            // Remove all cells after date (index 2+)
            while (cells.length > 2 && row.children.length > 2) {
                row.removeChild(row.lastChild);
            }

            // Create dist input cell
            const distTd = document.createElement("td");
            distTd.className = "col-dist";
            const distInput = document.createElement("input");
            distInput.type = "number";
            distInput.step = "0.1";
            distInput.className = "planned-input";
            distInput.placeholder = "mi";
            distInput.value = oldDist;
            distTd.appendChild(distInput);
            row.appendChild(distTd);

            // Create name input cell
            const nameTd = document.createElement("td");
            nameTd.className = "col-name";
            const nameInput = document.createElement("input");
            nameInput.type = "text";
            nameInput.className = "planned-input";
            nameInput.placeholder = "Workout name";
            nameInput.value = oldName;
            nameTd.appendChild(nameInput);
            row.appendChild(nameTd);

            // Remaining colspan
            const restTd = document.createElement("td");
            restTd.colSpan = 9;
            row.appendChild(restTd);

            distInput.focus();

            function savePlanned() {
                const dist = distInput.value.trim();
                const name = nameInput.value.trim();
                if (!dist && !name) {
                    if (oldDist || oldName) {
                        fetch(`/api/planned/${dateStr}`, { method: "DELETE" })
                            .then(r => r.json())
                            .then(() => {
                                restoreBlanks();
                                row.classList.remove("planned-row");
                                if (!row.classList.contains("future-row")) row.classList.add("future-row");
                                refreshSevenDayMA(dateStr);
                            });
                    } else {
                        restoreBlanks();
                    }
                    return;
                }
                fetch(`/api/planned/${dateStr}`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        distance_mi: dist ? parseFloat(dist) : null,
                        workout_name: name,
                    }),
                }).then(r => r.json()).then(d => {
                    if (d.ok) {
                        // Update row in place
                        while (row.children.length > 2) row.removeChild(row.lastChild);
                        const dTd = document.createElement("td");
                        dTd.className = "col-dist planned-dist";
                        dTd.textContent = dist ? parseFloat(dist).toFixed(1) : "";
                        row.appendChild(dTd);
                        const nTd = document.createElement("td");
                        nTd.className = "col-name planned-name";
                        nTd.textContent = name;
                        row.appendChild(nTd);
                        const rTd = document.createElement("td");
                        rTd.colSpan = 9;
                        row.appendChild(rTd);
                        row.classList.add("planned-row");
                        row.classList.remove("future-row");
                        refreshSevenDayMA(dateStr);
                    }
                });
            }

            function restoreBlanks() {
                while (row.children.length > 2) row.removeChild(row.lastChild);
                const td = document.createElement("td");
                td.colSpan = 11;
                row.appendChild(td);
                row.classList.remove("planned-row");
                if (!row.classList.contains("future-row")) row.classList.add("future-row");
            }

            [distInput, nameInput].forEach(input => {
                input.addEventListener("keydown", (ev) => {
                    ev.stopPropagation();
                    if (ev.key === "Enter") savePlanned();
                    if (ev.key === "Escape") restoreBlanks();
                    if (ev.key === "Tab" && input === distInput) {
                        ev.preventDefault();
                        nameInput.focus();
                    }
                });
                input.addEventListener("click", (ev) => ev.stopPropagation());
            });

            nameInput.addEventListener("blur", (ev) => {
                // Only save on blur if focus isn't moving to the other input
                setTimeout(() => {
                    if (!row.contains(document.activeElement) || !document.activeElement.classList.contains("planned-input")) {
                        savePlanned();
                    }
                }, 100);
            });
            distInput.addEventListener("blur", (ev) => {
                setTimeout(() => {
                    if (!row.contains(document.activeElement) || !document.activeElement.classList.contains("planned-input")) {
                        savePlanned();
                    }
                }, 100);
            });
        });
    });

    // ── Scroll spy ──────────────────────────────────────────────

    function updateActiveMonth() {
        if (!mainContent) return;
        const sections = document.querySelectorAll(".month-section");
        let activeMonth = null;
        for (const sec of sections) {
            const rect = sec.getBoundingClientRect();
            const mainRect = mainContent.getBoundingClientRect();
            if (rect.top <= mainRect.top + 60) activeMonth = sec.id;
        }
        document.querySelectorAll(".month-cell").forEach(cell => {
            cell.classList.toggle("active", activeMonth === `month-${cell.dataset.month}`);
        });
    }

    if (mainContent) {
        mainContent.addEventListener("scroll", updateActiveMonth);
        updateActiveMonth();
    }

    document.querySelectorAll(".month-cell").forEach(cell => {
        cell.addEventListener("click", (e) => {
            e.preventDefault();
            const target = document.getElementById(`month-${cell.dataset.month}`);
            if (target && mainContent) {
                target.scrollIntoView({ behavior: "smooth", block: "start" });
            }
        });
    });

    // ── Load detail ─────────────────────────────────────────────

    function loadDetail(id, activityIds, hasStreams) {
        // Fetch intervals from all activities
        Promise.all(activityIds.map(aid =>
            fetch(`/api/activity/${aid}/intervals`).then(r => r.json())
        )).then(results => {
            // Merge intervals and laps from all activities
            const allIntervals = results.flatMap(r => r.intervals || []);
            const allLaps = results.flatMap(r => r.laps || []);
            const allSummary = results.flatMap(r => r.summary || []);
            renderIntervals(id, allIntervals, allLaps, allSummary);
        });

        // Fetch chart data from all activities
        Promise.all(activityIds.map(aid =>
            fetch(`/api/activity/${aid}/chart`).then(r => r.json())
        )).then(results => {
            const allPace = results.flatMap(r => r.pace || []);
            const allHr = results.flatMap(r => r.hr || []);
            if (allPace.length || allHr.length) {
                renderCharts(id, allPace, allHr);
            }
        });

        // Fetch streams for map
        if (hasStreams) {
            Promise.all(activityIds.map(aid =>
                fetch(`/api/activity/${aid}/streams`).then(r => r.json())
            )).then(results => {
                renderMap(id, results.flat());
            });
        }

        populateShoeDropdown(id);
        wireEditButtons(id);
    }

    // ── Render intervals + laps ─────────────────────────────────

    function buildTable(rows, showSource) {
        if (!rows.length) return "";

        let html = `<table class="intervals-table">
            <thead><tr>
                <th>#</th><th>Distance</th><th>Duration</th><th>Pace</th>
                <th>HR</th><th>Zone</th>`;
        if (showSource) html += `<th>Source</th>`;
        html += `<th></th>`;  // walking toggle column
        html += `</tr></thead><tbody>`;

        for (const iv of rows) {
            const zone = iv.pace_zone || "";
            const zoneClass = zone ? `zone-${zone}` : "";
            const bgClass = zone ? `zone-bg-${zone}` : "";
            let rowClass = "";
            if (iv.is_walking) rowClass += " is-walking";
            if (iv.is_recovery) rowClass += " is-recovery";
            if (iv.is_stride) rowClass += " is-stride";

            const setLabel = iv.set_number ? ` S${iv.set_number}` : "";
            const locLabel = iv.location_type ? ` [${iv.location_type}]` : "";

            // Walking toggle button
            let walkBtn = "";
            if (iv.is_walking) {
                walkBtn = `<button class="btn-unscrub" data-interval-id="${iv.id}" title="Mark as not walking">&#x21A9;</button>`;
            } else if (iv.source && iv.source !== "pace_segment") {
                walkBtn = `<button class="btn-scrub" data-interval-id="${iv.id}" title="Mark as walking">&#x1F6B6;</button>`;
            }

            html += `<tr class="${rowClass} ${bgClass}" data-interval-id="${iv.id}">
                <td>${iv.rep_number || ""}</td>
                <td>${iv.display_distance}${locLabel}</td>
                <td>${iv.is_recovery ? "" : iv.display_duration}</td>
                <td>${iv.is_recovery ? "" : iv.display_pace}</td>
                <td>${iv.is_recovery ? "" : iv.display_hr}</td>
                <td class="${zoneClass}">${zone}${setLabel}</td>`;
            if (showSource) html += `<td>${iv.source || ""}</td>`;
            html += `<td class="walk-toggle-cell">${walkBtn}</td>`;
            html += `</tr>`;
        }

        html += "</tbody></table>";
        return html;
    }

    function renderSummary(summary) {
        if (!summary || !summary.length) return "";
        let html = `<div class="rep-summary"><table class="summary-table">
            <thead><tr>
                <th>Distance</th><th>Reps</th><th>Avg Time</th><th>Avg Pace</th><th>Avg HR</th>
            </tr></thead><tbody>`;
        for (const s of summary) {
            html += `<tr>
                <td>${s.distance}</td>
                <td>${s.count}</td>
                <td>${s.avg_duration}</td>
                <td>${s.avg_pace}</td>
                <td>${s.avg_hr}</td>
            </tr>`;
        }
        html += `</tbody></table></div>`;
        return html;
    }

    function renderIntervals(id, intervals, laps, summary) {
        const container = document.getElementById(`intervals-${id}`);
        if (!intervals.length && !laps.length) {
            container.innerHTML = "<p class='loading'>No intervals.</p>";
            return;
        }

        let html = "";

        if (intervals.length) {
            html += `<h3>Intervals</h3>`;
            html += buildTable(intervals, true);
        }

        if (laps.length) {
            html += `<h3 style="margin-top:10px">Laps</h3>`;
            html += buildTable(laps, false);
        }

        if (summary && summary.length) {
            html += `<h3 style="margin-top:10px">Summary</h3>`;
            html += renderSummary(summary);
        }

        container.innerHTML = html;

        // Wire walking toggle buttons
        container.querySelectorAll(".btn-unscrub, .btn-scrub").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                const intervalId = btn.dataset.intervalId;
                const setWalking = btn.classList.contains("btn-scrub");
                toggleWalking(intervalId, setWalking).then(ok => {
                    if (ok) {
                        const tr = btn.closest("tr");
                        if (setWalking) {
                            tr.classList.add("is-walking");
                            btn.className = "btn-unscrub";
                            btn.innerHTML = "&#x21A9;";
                            btn.title = "Mark as not walking";
                        } else {
                            tr.classList.remove("is-walking");
                            btn.className = "btn-scrub";
                            btn.innerHTML = "&#x1F6B6;";
                            btn.title = "Mark as walking";
                        }
                    }
                });
            });
        });
    }

    // ── Render charts (canvas) ──────────────────────────────────

    const chartRegistry = {};

    function renderCharts(id, paceData, hrData) {
        const section = document.getElementById(`chart-${id}`);
        if (!section) return;
        section.style.display = "";

        if (paceData.length) {
            drawChart(`pace-chart-${id}`, paceData, {
                color: "#4a7ab5",
                fillColor: "rgba(74,122,181,0.15)",
                formatY: v => { const m = Math.floor(v / 60); const s = Math.round(v % 60); return `${m}:${s < 10 ? "0" : ""}${s}`; },
                invertY: true, // lower pace = faster = top
                partnerId: `hr-chart-${id}`,
            });
        }

        if (hrData.length) {
            drawChart(`hr-chart-${id}`, hrData, {
                color: "#c05050",
                fillColor: "rgba(192,80,80,0.15)",
                formatY: v => Math.round(v).toString(),
                invertY: false,
                partnerId: `pace-chart-${id}`,
            });
        }

        wireChartCrosshair(`pace-chart-${id}`);
        wireChartCrosshair(`hr-chart-${id}`);
    }

    function drawChart(canvasId, data, opts) {
        const canvas = document.getElementById(canvasId);
        if (!canvas || !data.length) return;

        const ctx = canvas.getContext("2d");
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);

        const w = rect.width;
        const h = rect.height;
        const pad = { top: 8, right: 8, bottom: 18, left: 40 };
        const cw = w - pad.left - pad.right;
        const ch = h - pad.top - pad.bottom;

        const tMin = data[0].t;
        const tMax = data[data.length - 1].t;
        const tRange = tMax - tMin || 1;

        const values = data.map(d => d.v);
        let vMin = Math.min(...values);
        let vMax = Math.max(...values);
        const vPad = (vMax - vMin) * 0.05 || 1;
        vMin -= vPad;
        vMax += vPad;
        const vRange = vMax - vMin || 1;

        function xPos(t) { return pad.left + ((t - tMin) / tRange) * cw; }
        function yPos(v) {
            const norm = (v - vMin) / vRange;
            return opts.invertY ? pad.top + norm * ch : pad.top + (1 - norm) * ch;
        }
        function tFromX(x) { return tMin + ((x - pad.left) / cw) * tRange; }

        // Fill
        ctx.beginPath();
        ctx.moveTo(xPos(data[0].t), yPos(data[0].v));
        for (let i = 1; i < data.length; i++) {
            ctx.lineTo(xPos(data[i].t), yPos(data[i].v));
        }
        ctx.lineTo(xPos(data[data.length - 1].t), pad.top + ch);
        ctx.lineTo(xPos(data[0].t), pad.top + ch);
        ctx.closePath();
        ctx.fillStyle = opts.fillColor;
        ctx.fill();

        // Line
        ctx.beginPath();
        ctx.moveTo(xPos(data[0].t), yPos(data[0].v));
        for (let i = 1; i < data.length; i++) {
            ctx.lineTo(xPos(data[i].t), yPos(data[i].v));
        }
        ctx.strokeStyle = opts.color;
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Y-axis labels
        ctx.fillStyle = "#999";
        ctx.font = "9px -apple-system, sans-serif";
        ctx.textAlign = "right";
        const ySteps = 4;
        for (let i = 0; i <= ySteps; i++) {
            const v = vMin + (vRange * i) / ySteps;
            const y = yPos(v);
            ctx.fillText(opts.formatY(v), pad.left - 4, y + 3);
            ctx.beginPath();
            ctx.moveTo(pad.left, y);
            ctx.lineTo(w - pad.right, y);
            ctx.strokeStyle = "#eee";
            ctx.lineWidth = 0.5;
            ctx.stroke();
        }

        // X-axis labels (time in minutes)
        ctx.textAlign = "center";
        ctx.fillStyle = "#999";
        const totalMin = tRange / 60;
        const xStep = totalMin <= 15 ? 5 : totalMin <= 45 ? 10 : 15;
        for (let min = 0; min <= totalMin; min += xStep) {
            const t = tMin + min * 60;
            const x = xPos(t);
            ctx.fillText(`${min}m`, x, h - 4);
        }

        // Save clean image and metadata for crosshair overlay
        const cleanImage = ctx.getImageData(0, 0, canvas.width, canvas.height);
        chartRegistry[canvasId] = {
            data, opts, canvas, cleanImage, dpr,
            xPos, yPos, tFromX,
            pad, w, h, tMin, tMax, tRange,
        };
    }

    function drawCrosshair(canvasId, time, frozen) {
        const reg = chartRegistry[canvasId];
        if (!reg) return;
        const { data, opts, canvas, cleanImage, dpr, xPos, yPos, pad, w, h } = reg;
        const ctx = canvas.getContext("2d");

        // Restore clean chart (putImageData ignores transform, writes raw pixels)
        ctx.putImageData(cleanImage, 0, 0);
        // Set transform absolutely to DPR scale (don't stack on existing)
        ctx.save();
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        // Find nearest data point to time
        let nearest = data[0];
        let minDiff = Infinity;
        for (const d of data) {
            const diff = Math.abs(d.t - time);
            if (diff < minDiff) { minDiff = diff; nearest = d; }
        }

        const x = xPos(nearest.t);
        const y = yPos(nearest.v);

        // Vertical time line
        ctx.beginPath();
        ctx.moveTo(x, pad.top);
        ctx.lineTo(x, h - pad.bottom);
        ctx.strokeStyle = "rgba(0,0,0,0.2)";
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.stroke();
        ctx.setLineDash([]);

        // Dot on curve
        ctx.beginPath();
        ctx.arc(x, y, frozen ? 5 : 4, 0, Math.PI * 2);
        ctx.fillStyle = opts.color;
        ctx.fill();
        ctx.strokeStyle = "#fff";
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // Value + time label (only when frozen)
        if (frozen) {
            const min = Math.floor((nearest.t - data[0].t) / 60);
            const sec = Math.round((nearest.t - data[0].t) % 60);
            const label = `${opts.formatY(nearest.v)}  ${min}:${sec < 10 ? "0" : ""}${sec}`;

            ctx.font = "bold 10px -apple-system, sans-serif";
            const metrics = ctx.measureText(label);
            const lw = metrics.width + 8;
            const lh = 16;
            let lx = x + 8;
            if (lx + lw > w - pad.right) lx = x - lw - 8;
            let ly = y - lh - 4;
            if (ly < pad.top) ly = y + 8;

            ctx.fillStyle = "rgba(255,255,255,0.92)";
            ctx.strokeStyle = "rgba(0,0,0,0.15)";
            ctx.lineWidth = 1;
            ctx.beginPath();
            if (ctx.roundRect) {
                ctx.roundRect(lx, ly, lw, lh, 3);
            } else {
                ctx.rect(lx, ly, lw, lh);
            }
            ctx.fill();
            ctx.stroke();

            ctx.fillStyle = "#333";
            ctx.textAlign = "left";
            ctx.fillText(label, lx + 4, ly + 12);
        }

        ctx.restore();
    }

    function wireChartCrosshair(canvasId) {
        const reg = chartRegistry[canvasId];
        if (!reg) return;
        const { canvas, tFromX } = reg;
        let tracking = false;
        let frozenTime = null;

        function getTime(e) {
            const rect = canvas.getBoundingClientRect();
            return tFromX(e.clientX - rect.left);
        }

        canvas.addEventListener("mousedown", (e) => {
            e.preventDefault();
            e.stopPropagation();
            if (frozenTime !== null) {
                // Clear existing frozen crosshair, start fresh
                frozenTime = null;
            }
            tracking = true;
            const time = getTime(e);
            drawCrosshair(canvasId, time, false);
            syncPartner(canvasId, time, false);
        });

        canvas.addEventListener("mousemove", (e) => {
            if (!tracking) return;
            const time = getTime(e);
            drawCrosshair(canvasId, time, false);
            syncPartner(canvasId, time, false);
        });

        canvas.addEventListener("mouseup", (e) => {
            if (!tracking) return;
            tracking = false;
            const time = getTime(e);
            frozenTime = time;
            drawCrosshair(canvasId, time, true);
            syncPartner(canvasId, time, true);
        });

        // If mouse leaves during drag, freeze at last position
        canvas.addEventListener("mouseleave", (e) => {
            if (tracking) {
                tracking = false;
                const time = getTime(e);
                frozenTime = time;
                drawCrosshair(canvasId, time, true);
                syncPartner(canvasId, time, true);
            }
        });
    }

    function syncPartner(canvasId, time, frozen) {
        const reg = chartRegistry[canvasId];
        if (!reg || !reg.opts.partnerId) return;
        const partnerId = reg.opts.partnerId;
        if (chartRegistry[partnerId]) {
            drawCrosshair(partnerId, time, frozen);
        }
    }

    function clearCrosshair(canvasId) {
        const reg = chartRegistry[canvasId];
        if (!reg) return;
        const ctx = reg.canvas.getContext("2d");
        ctx.putImageData(reg.cleanImage, 0, 0);
    }

    // ── Render map ──────────────────────────────────────────────

    function renderMap(id, points) {
        const container = document.getElementById(`map-${id}`);
        if (!container || !points.length) return;

        setTimeout(() => {
            if (maps[id]) { maps[id].invalidateSize(); return; }

            const map = L.map(container, { attributionControl: false });
            L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
                maxZoom: 19,
            }).addTo(map);

            const latlngs = points.map(p => [p.lat, p.lon]);
            const polyline = L.polyline(latlngs, {
                color: "#4A90D9", weight: 3, opacity: 0.8,
            }).addTo(map);

            map.fitBounds(polyline.getBounds(), { padding: [20, 20] });
            maps[id] = map;
        }, 100);
    }

    // ── Shoe dropdown ───────────────────────────────────────────

    function populateShoeDropdown(id) {
        // Try both the primary id and query by detail panel
        let select = document.querySelector(`#edit-${id} select[data-field="shoe_id"]`);
        if (!select) {
            // Fallback: find within the detail row
            const detailRow = document.querySelector(`.detail-row[data-id="${id}"]`);
            if (detailRow) select = detailRow.querySelector(`select[data-field="shoe_id"]`);
        }
        if (!select || !window.SHOES) return;
        if (select.options.length > 1) return; // already populated

        for (const [shoeId, name] of Object.entries(SHOES)) {
            const opt = document.createElement("option");
            opt.value = shoeId;
            opt.textContent = name;
            select.appendChild(opt);
        }

        const row = document.querySelector(`.activity-row[data-id="${id}"]`);
        const shoeCell = row ? row.querySelector(".col-shoe") : null;
        if (shoeCell) {
            const currentShoe = shoeCell.textContent.trim();
            for (const opt of select.options) {
                if (opt.textContent === currentShoe) { opt.selected = true; break; }
            }
        }
    }

    // ── Edit buttons ────────────────────────────────────────────

    function wireEditButtons(id) {
        const form = document.getElementById(`edit-${id}`);
        if (!form) return;

        form.querySelectorAll(".btn-save").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                const field = btn.dataset.field;
                const input = form.querySelector(`[data-field="${field}"]`);
                const value = input.value;
                if (value === "" || value === undefined) return;

                saveOverride(id, field, value).then(result => {
                    if (result) {
                        btn.textContent = "Saved!";
                        setTimeout(() => btn.textContent = "Save", 1500);
                        const clearBtn = form.querySelector(`.btn-clear[data-field="${field}"]`);
                        if (clearBtn) clearBtn.style.display = "";
                        const row = document.querySelector(`.activity-row[data-id="${id}"]`);
                        const colClass = fieldToColClass(field);
                        const cell = row ? row.querySelector(`.col-${colClass}`) : null;
                        if (cell) {
                            cell.classList.add("overridden");
                            updateCellDisplay(cell, field, value);
                        }
                        // Refresh aggregates when distance changes
                        if (field === "distance_mi" && result.date) {
                            refreshSevenDayMA(result.date);
                            refreshFooterStats();
                        }
                    }
                });
            });
        });

        form.querySelectorAll(".btn-clear").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                const field = btn.dataset.field;
                clearOverride(id, field).then(ok => {
                    if (ok) {
                        btn.style.display = "none";
                        const row = document.querySelector(`.activity-row[data-id="${id}"]`);
                        const cell = row.querySelector(`.col-${fieldToColClass(field)}`);
                        if (cell) cell.classList.remove("overridden");
                    }
                });
            });
        });

        form.addEventListener("click", e => e.stopPropagation());
    }

    function fieldToColClass(field) {
        const map = {
            distance_mi: "dist", duration_s: "dur", avg_pace_s_per_mi: "pace",
            workout_name: "name", workout_category: "name",
            shoe_id: "shoe", notes: "notes", strides: "strides",
            workout_type_zone: "type",
        };
        return map[field] || field;
    }

    function updateCellDisplay(cell, field, value) {
        if (field === "distance_mi") {
            cell.textContent = parseFloat(value).toFixed(1);
        } else if (field === "workout_name") {
            cell.textContent = value;
        } else if (field === "strides") {
            cell.textContent = value;
        } else if (field === "notes") {
            cell.textContent = value;
            cell.title = value;
        } else if (field === "shoe_id" && window.SHOES) {
            cell.textContent = SHOES[value] || "";
        } else if (field === "duration_s") {
            const s = parseInt(value, 10);
            const h = Math.floor(s / 3600);
            const m = Math.floor((s % 3600) / 60);
            const sec = s % 60;
            cell.textContent = h ? `${h}h ${m}m` : `${m}:${sec < 10 ? "0" : ""}${sec}`;
        } else if (field === "avg_pace_s_per_mi") {
            const s = parseFloat(value);
            const m = Math.floor(s / 60);
            const sec = Math.round(s % 60);
            cell.textContent = `${m}:${sec < 10 ? "0" : ""}${sec}`;
        }
    }

    // ── API ─────────────────────────────────────────────────────

    function saveOverride(activityId, field, value) {
        return fetch(`/api/activity/${activityId}/override`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ field_name: field, override_value: value }),
        }).then(r => r.json()).then(d => d.ok ? d : false).catch(() => false);
    }

    function clearOverride(activityId, field) {
        return fetch(`/api/activity/${activityId}/override/${field}`, {
            method: "DELETE",
        }).then(r => r.json()).then(d => d.ok).catch(() => false);
    }

    function toggleWalking(intervalId, isWalking) {
        return fetch(`/api/interval/${intervalId}/walking`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ is_walking: isWalking }),
        }).then(r => r.json()).then(d => d.ok).catch(() => false);
    }

    // ── Keyboard nav ────────────────────────────────────────────

    document.addEventListener("keydown", (e) => {
        if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA" || e.target.tagName === "SELECT") return;
        if (e.key === "Escape") {
            document.querySelectorAll(".detail-row").forEach(r => r.style.display = "none");
            document.querySelectorAll(".activity-row").forEach(r => r.classList.remove("expanded"));
        }
    });

    // ── Weekly mileage sidebar chart ────────────────────────────

    function drawWeeklyMileageChart() {
        const canvas = document.getElementById("weekly-mileage-chart");
        if (!canvas || !window.WEEKLY_CHART_DATA || !WEEKLY_CHART_DATA.length) return;

        const data = WEEKLY_CHART_DATA;
        const ctx = canvas.getContext("2d");
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);

        const w = rect.width;
        const h = rect.height;
        const pad = { top: 10, right: 8, bottom: 20, left: 30 };
        const cw = w - pad.left - pad.right;
        const ch = h - pad.top - pad.bottom;

        const distances = data.map(d => d.distance);
        const vMax = Math.max(...distances) * 1.1 || 1;

        function xPos(i) { return pad.left + (i / (data.length - 1 || 1)) * cw; }
        function yPos(v) { return pad.top + (1 - v / vMax) * ch; }

        // Grid lines
        ctx.strokeStyle = "#eee";
        ctx.lineWidth = 0.5;
        const ySteps = 4;
        ctx.fillStyle = "#aaa";
        ctx.font = "9px -apple-system, sans-serif";
        ctx.textAlign = "right";
        for (let i = 0; i <= ySteps; i++) {
            const v = (vMax * i) / ySteps;
            const y = yPos(v);
            ctx.beginPath();
            ctx.moveTo(pad.left, y);
            ctx.lineTo(w - pad.right, y);
            ctx.stroke();
            ctx.fillText(Math.round(v).toString(), pad.left - 3, y + 3);
        }

        // Fill area under line
        ctx.beginPath();
        ctx.moveTo(xPos(0), yPos(data[0].distance));
        for (let i = 1; i < data.length; i++) {
            ctx.lineTo(xPos(i), yPos(data[i].distance));
        }
        ctx.lineTo(xPos(data.length - 1), pad.top + ch);
        ctx.lineTo(xPos(0), pad.top + ch);
        ctx.closePath();
        ctx.fillStyle = "rgba(74,122,181,0.12)";
        ctx.fill();

        // Weekly mileage line
        ctx.beginPath();
        ctx.moveTo(xPos(0), yPos(data[0].distance));
        for (let i = 1; i < data.length; i++) {
            ctx.lineTo(xPos(i), yPos(data[i].distance));
        }
        ctx.strokeStyle = "#4a7ab5";
        ctx.lineWidth = 1.5;
        ctx.stroke();

        // 6-week moving average (dotted line)
        const maWindow = 6;
        if (data.length >= maWindow) {
            ctx.beginPath();
            let started = false;
            for (let i = maWindow - 1; i < data.length; i++) {
                let sum = 0;
                for (let j = i - maWindow + 1; j <= i; j++) {
                    sum += data[j].distance;
                }
                const avg = sum / maWindow;
                if (!started) {
                    ctx.moveTo(xPos(i), yPos(avg));
                    started = true;
                } else {
                    ctx.lineTo(xPos(i), yPos(avg));
                }
            }
            ctx.strokeStyle = "#c05050";
            ctx.lineWidth = 1.5;
            ctx.setLineDash([4, 3]);
            ctx.stroke();
            ctx.setLineDash([]);
        }

        // X-axis labels (show ~4 evenly spaced)
        ctx.textAlign = "center";
        ctx.fillStyle = "#aaa";
        ctx.font = "8px -apple-system, sans-serif";
        const labelStep = Math.max(1, Math.floor(data.length / 4));
        for (let i = 0; i < data.length; i += labelStep) {
            ctx.fillText(data[i].label, xPos(i), h - 4);
        }
        // Always label the last point
        if (data.length > 1) {
            ctx.fillText(data[data.length - 1].label, xPos(data.length - 1), h - 4);
        }
    }

    drawWeeklyMileageChart();

    // ── Import button ───────────────────────────────────────────

    const importBtn = document.getElementById("btn-import");
    const importStatus = document.getElementById("import-status");

    if (importBtn) {
        importBtn.addEventListener("click", () => {
            importBtn.disabled = true;
            importBtn.textContent = "Running...";
            importStatus.textContent = "";

            fetch("/api/import", { method: "POST" })
                .then(r => r.json())
                .then(d => {
                    if (!d.ok) {
                        importBtn.disabled = false;
                        importBtn.textContent = "Import";
                        importStatus.textContent = d.error || "Error";
                        return;
                    }
                    // Poll for completion
                    const poll = setInterval(() => {
                        fetch("/api/import/status").then(r => r.json()).then(s => {
                            if (!s.running) {
                                clearInterval(poll);
                                importBtn.disabled = false;
                                importBtn.textContent = "Import";
                                if (s.success) {
                                    // Parse "Pipeline complete: N new activities" from output
                                    const match = s.output.match(/(\d+) new activit/);
                                    const newCount = match ? parseInt(match[1], 10) : 0;
                                    if (newCount > 0) {
                                        importStatus.textContent = `Done! ${newCount} new`;
                                        importStatus.className = "import-status success";
                                        setTimeout(() => location.reload(), 1500);
                                    } else {
                                        importStatus.textContent = "No new activities";
                                        importStatus.className = "import-status success";
                                    }
                                } else {
                                    importStatus.textContent = "Failed";
                                    importStatus.className = "import-status error";
                                    importStatus.title = s.output;
                                }
                            }
                        });
                    }, 2000);
                })
                .catch(() => {
                    importBtn.disabled = false;
                    importBtn.textContent = "Import";
                    importStatus.textContent = "Error";
                });
        });
    }

    // ── Refresh 7d MA after planned activity change ────────────

    function refreshSevenDayMA(changedDate) {
        // The changed date affects 7d MA for itself and the next 6 days
        const d = new Date(changedDate + "T00:00:00");
        const start = changedDate;
        const endD = new Date(d);
        endD.setDate(endD.getDate() + 6);
        const end = endD.toISOString().slice(0, 10);

        fetch(`/api/seven_day_ma?start=${start}&end=${end}`)
            .then(r => r.json())
            .then(maData => {
                // Patch all matching rows in the DOM
                for (const [dateStr, maValue] of Object.entries(maData)) {
                    const rows = mainContent.querySelectorAll(`[data-date="${dateStr}"]`);
                    for (const row of rows) {
                        const maCell = row.querySelector(".col-7d");
                        if (maCell) maCell.textContent = maValue.toFixed(1);
                    }
                }
            });
    }

    function refreshFooterStats() {
        // Extract current year from the page
        const yearEl = document.querySelector(".cal-year");
        const year = yearEl ? yearEl.textContent.trim() : new Date().getFullYear();

        fetch(`/api/footer_stats?year=${year}`)
            .then(r => r.json())
            .then(data => {
                // Update stat cards
                const cards = document.querySelectorAll(".stat-card");
                const cardMap = {};
                cards.forEach(card => {
                    const label = card.querySelector(".stat-label").textContent.trim();
                    cardMap[label] = card.querySelector(".stat-value");
                });
                if (cardMap["Miles"]) cardMap["Miles"].textContent = data.yearly_distance.toFixed(1);
                if (cardMap["Runs"]) cardMap["Runs"].textContent = data.yearly_count;
                if (cardMap["Time"]) cardMap["Time"].textContent = data.yearly_duration;
                if (cardMap["Avg Pace"]) cardMap["Avg Pace"].textContent = data.yearly_avg_pace;
                if (cardMap["Longest"]) cardMap["Longest"].textContent = data.longest_run.toFixed(1);

                // Update monthly table
                const monthRows = document.querySelectorAll(".monthly-table tbody tr");
                const monthByName = {};
                data.monthly.forEach(s => { monthByName[s.name] = s; });
                monthRows.forEach(tr => {
                    const cells = tr.querySelectorAll("td");
                    if (cells.length < 6) return;
                    const name = cells[0].textContent.trim().slice(0, 3);
                    const s = monthByName[name];
                    if (!s) return;
                    cells[1].textContent = s.count;
                    cells[2].textContent = s.display_distance;
                    cells[3].textContent = s.avg_weekly;
                    cells[4].textContent = s.display_duration;
                    cells[5].textContent = s.display_pace;
                });

                // Update monthly bar chart
                const bars = document.querySelectorAll(".chart-col");
                bars.forEach(col => {
                    const label = col.querySelector(".chart-label");
                    if (!label) return;
                    const name = label.textContent.trim();
                    const s = monthByName[name];
                    if (!s) return;
                    const val = col.querySelector(".chart-value");
                    const bar = col.querySelector(".chart-bar");
                    if (val) val.textContent = s.display_distance;
                    if (bar) bar.style.height = `${(s.distance / data.max_month_dist * 100)}%`;
                });
            });
    }

    // ── Scroll helpers ─────────────────────────────────────────

    function scrollToDate(targetDate) {
        if (!mainContent) return;
        const allRows = mainContent.querySelectorAll("[data-date]");
        let best = null;
        let bestDiff = Infinity;
        for (const row of allRows) {
            const d = row.dataset.date;
            if (!d) continue;
            const diff = Math.abs(Date.parse(d) - Date.parse(targetDate));
            if (diff < bestDiff) { bestDiff = diff; best = row; }
        }
        if (best) best.scrollIntoView({ block: "center" });
    }

    function reloadToDate(dateStr) {
        // Preserve scroll position by storing target date, then reload
        sessionStorage.setItem("runbase_scroll_to", dateStr);
        location.reload();
    }

    // On load: scroll to stored date (from reload) or current week
    const scrollTarget = sessionStorage.getItem("runbase_scroll_to");
    if (scrollTarget) {
        sessionStorage.removeItem("runbase_scroll_to");
        scrollToDate(scrollTarget);
    } else if (mainContent) {
        const today = new Date();
        const weekAgo = new Date(today);
        weekAgo.setDate(weekAgo.getDate() - 7);
        scrollToDate(weekAgo.toISOString().slice(0, 10));
    }

})();

