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
                <td>${iv.display_duration}</td>
                <td>${iv.display_pace}</td>
                <td>${iv.display_hr}</td>
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
            });
        }

        if (hrData.length) {
            drawChart(`hr-chart-${id}`, hrData, {
                color: "#c05050",
                fillColor: "rgba(192,80,80,0.15)",
                formatY: v => Math.round(v).toString(),
                invertY: false,
            });
        }
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
        // Add 5% padding
        const vPad = (vMax - vMin) * 0.05 || 1;
        vMin -= vPad;
        vMax += vPad;
        const vRange = vMax - vMin || 1;

        function xPos(t) { return pad.left + ((t - tMin) / tRange) * cw; }
        function yPos(v) {
            const norm = (v - vMin) / vRange;
            return opts.invertY
                ? pad.top + norm * ch       // higher value (slower) at bottom
                : pad.top + (1 - norm) * ch;
        }

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
            // Grid line
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
        const select = document.querySelector(`#edit-${id} select[data-field="shoe_id"]`);
        if (!select || !window.SHOES) return;

        for (const [shoeId, name] of Object.entries(SHOES)) {
            const opt = document.createElement("option");
            opt.value = shoeId;
            opt.textContent = name;
            select.appendChild(opt);
        }

        const row = document.querySelector(`.activity-row[data-id="${id}"]`);
        const shoeCell = row.querySelector(".col-shoe");
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

                saveOverride(id, field, value).then(ok => {
                    if (ok) {
                        btn.textContent = "Saved!";
                        setTimeout(() => btn.textContent = "Save", 1500);
                        const clearBtn = form.querySelector(`.btn-clear[data-field="${field}"]`);
                        if (clearBtn) clearBtn.style.display = "";
                        const row = document.querySelector(`.activity-row[data-id="${id}"]`);
                        const cell = row.querySelector(`.col-${fieldToColClass(field)}`);
                        if (cell) cell.classList.add("overridden");
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
        };
        return map[field] || field;
    }

    // ── API ─────────────────────────────────────────────────────

    function saveOverride(activityId, field, value) {
        return fetch(`/api/activity/${activityId}/override`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ field_name: field, override_value: value }),
        }).then(r => r.json()).then(d => d.ok).catch(() => false);
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
                                    importStatus.textContent = "Done!";
                                    importStatus.className = "import-status success";
                                    setTimeout(() => location.reload(), 1500);
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

})();

