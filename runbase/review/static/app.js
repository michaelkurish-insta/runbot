/* ── RunBase Review UI ────────────────────────────────────────── */

(function () {
    "use strict";

    const mainContent = document.querySelector(".main-content");

    // ── Modal DOM references ────────────────────────────────────
    const modalEl = document.getElementById("activity-modal");
    const modalTitleEl = document.getElementById("modal-title");
    const modalChartSectionEl = document.getElementById("modal-chart-section");
    const modalIntervalsEl = document.getElementById("modal-intervals-container");
    const modalEditSectionEl = document.getElementById("modal-edit-section");
    const modalMapContainerEl = document.getElementById("modal-map-container");
    const modalBtnSave = document.getElementById("modal-btn-save");
    const modalBtnSaveClose = document.getElementById("modal-btn-save-close");

    // ── Modal state ─────────────────────────────────────────────
    let modalMap = null;
    const modalChartRegistry = {};
    let modalDirtyState = {};   // { aid: { field: newValue, ... } }
    let modalOriginalValues = {}; // { aid: { field: originalValue, ... } }
    let modalClearQueue = {};   // { aid: Set(field, ...) } — overrides to delete on save
    let modalActivityIds = [];  // all activity IDs shown in the current modal
    let modalVdotState = {};    // { aid: { vdot, race_distance_m, race_time_s, tag_race, apply_timeline } }

    // ── Row click → open modal ──────────────────────────────────

    document.querySelectorAll(".activity-row").forEach(row => {
        row.addEventListener("click", () => {
            const id = row.dataset.id;
            const activityIds = (row.dataset.activityIds || id).split(",");
            const hasStreams = row.dataset.hasStreams === "true";
            openModal(id, activityIds, hasStreams, row);
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

            const existingDist = row.querySelector(".planned-dist");
            const existingName = row.querySelector(".planned-name");
            const oldDist = existingDist ? existingDist.textContent.trim() : "";
            const oldName = existingName ? existingName.textContent.trim() : "";

            const cells = row.querySelectorAll("td");
            while (cells.length > 2 && row.children.length > 2) {
                row.removeChild(row.lastChild);
            }

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

            const nameTd = document.createElement("td");
            nameTd.className = "col-name";
            const nameInput = document.createElement("input");
            nameInput.type = "text";
            nameInput.className = "planned-input";
            nameInput.placeholder = "Workout name";
            nameInput.value = oldName;
            nameTd.appendChild(nameInput);
            row.appendChild(nameTd);

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

            nameInput.addEventListener("blur", () => {
                setTimeout(() => {
                    if (!row.contains(document.activeElement) || !document.activeElement.classList.contains("planned-input")) {
                        savePlanned();
                    }
                }, 100);
            });
            distInput.addEventListener("blur", () => {
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

    // ── Modal open/close ────────────────────────────────────────

    function openModal(id, activityIds, hasStreams, row) {
        // Reset state
        modalDirtyState = {};
        modalOriginalValues = {};
        modalClearQueue = {};
        modalVdotState = {};
        modalActivityIds = activityIds.map(Number);

        // Clean up old chart registry entries
        for (const key of Object.keys(modalChartRegistry)) {
            delete modalChartRegistry[key];
        }

        // Set title from row data
        const dateCell = row.querySelector(".date-cell");
        const nameCell = row.querySelector(".col-name");
        const dateText = dateCell ? dateCell.textContent.trim() : "";
        const nameText = nameCell ? nameCell.textContent.trim() : "";
        modalTitleEl.textContent = `${dateText}${nameText ? " \u2014 " + nameText : ""}`;

        // Clear containers
        modalChartSectionEl.innerHTML = "";
        modalIntervalsEl.innerHTML = '<p class="loading">Loading...</p>';
        modalEditSectionEl.innerHTML = "";
        modalMapContainerEl.innerHTML = "";

        // Show modal
        modalEl.style.display = "flex";

        // Buttons always visible but disabled until dirty
        modalBtnSave.disabled = true;
        modalBtnSaveClose.disabled = true;

        // Load data
        loadDetailIntoModal(id, activityIds, hasStreams);
    }

    function closeModal() {
        modalEl.style.display = "none";

        // Clean up leaflet map
        if (modalMap) {
            modalMap.remove();
            modalMap = null;
        }

        // Clean up chart registry
        for (const key of Object.keys(modalChartRegistry)) {
            delete modalChartRegistry[key];
        }
    }

    function isDirty() {
        for (const aid of Object.keys(modalDirtyState)) {
            if (Object.keys(modalDirtyState[aid]).length > 0) return true;
        }
        for (const aid of Object.keys(modalClearQueue)) {
            if (modalClearQueue[aid] && modalClearQueue[aid].size > 0) return true;
        }
        for (const aid of Object.keys(modalVdotState)) {
            if (modalVdotState[aid]) return true;
        }
        return false;
    }

    function confirmDiscard() {
        if (!isDirty()) return true;
        return confirm("You have unsaved changes. Discard them?");
    }

    // Wire close buttons
    document.getElementById("modal-close-x").addEventListener("click", () => {
        if (confirmDiscard()) closeModal();
    });
    document.getElementById("modal-btn-close").addEventListener("click", () => {
        if (confirmDiscard()) closeModal();
    });

    // Overlay click
    modalEl.addEventListener("click", (e) => {
        if (e.target === e.currentTarget) {
            if (confirmDiscard()) closeModal();
        }
    });

    // Save button
    modalBtnSave.addEventListener("click", () => {
        saveModalChanges(false);
    });

    // Save & Close button
    modalBtnSaveClose.addEventListener("click", () => {
        saveModalChanges(true);
    });

    // Escape key
    document.addEventListener("keydown", (e) => {
        if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA" || e.target.tagName === "SELECT") return;
        if (e.key === "Escape") {
            if (modalEl.style.display !== "none") {
                if (confirmDiscard()) closeModal();
            }
        }
    });

    // ── Load data into modal ────────────────────────────────────

    function loadDetailIntoModal(id, activityIds, hasStreams) {
        const isMulti = activityIds.length > 1;

        // Fetch intervals + metadata from all activities
        Promise.all(activityIds.map(aid =>
            Promise.all([
                fetch(`/api/activity/${aid}/intervals`).then(r => r.json()),
                fetch(`/api/activity/${aid}/meta`).then(r => r.json()),
            ]).then(([intervals, meta]) => ({ aid: parseInt(aid), meta, ...intervals }))
        )).then(results => {
            // Build all content per-activity into a single stream:
            // title → charts → intervals → edit form, then next activity
            let html = "";

            for (let i = 0; i < results.length; i++) {
                const r = results[i];
                const meta = r.meta;
                const intervals = r.intervals || [];
                const laps = r.laps || [];
                const summary = r.summary || [];

                if (i > 0) html += '<hr class="activity-divider">';

                if (isMulti) {
                    html += `<div class="activity-section" data-activity-id="${r.aid}">`;
                    html += `<div class="activity-section-header">`;
                    html += `<span class="activity-title">${escapeHtml(meta.workout_name || "Activity #" + r.aid)}</span>`;
                    const hrPart = meta.display_hr ? ` &middot; ${meta.display_hr}bpm` : "";
                    html += `<span class="activity-meta">${meta.display_distance}mi &middot; ${meta.display_pace}/mi &middot; ${meta.display_duration}${hrPart}</span>`;
                    html += `</div>`;
                }

                // Chart placeholder (renderChartsInModal will fill this)
                html += `<div class="modal-chart-placeholder" data-chart-aid="${r.aid}"></div>`;

                if (intervals.length) {
                    html += `<h3>Intervals</h3>`;
                    html += buildTable(intervals);
                }
                if (laps.length) {
                    html += `<h3 style="margin-top:10px">Laps</h3>`;
                    html += buildTable(laps);
                }
                if (summary && summary.length) {
                    html += `<h3 style="margin-top:10px">Summary</h3>`;
                    html += renderSummary(summary);
                }

                // Edit form inline with this activity's content
                html += buildEditForm(meta);

                if (isMulti) html += `</div>`;
            }

            // Put everything into the intervals container; leave chart/edit sections empty
            modalChartSectionEl.innerHTML = "";
            modalEditSectionEl.innerHTML = "";
            modalIntervalsEl.innerHTML = html || "<p class='loading'>No intervals.</p>";

            // Wire interval-level interactions (immediate saves)
            wireWalkingButtons(modalIntervalsEl);
            wireIntervalCheckboxes(modalIntervalsEl);
            wireIntervalEditing(modalIntervalsEl);

            // Wire edit forms for dirty tracking
            for (const r of results) {
                wireEditForm(modalIntervalsEl, r.meta);
            }
        });

        // Fetch chart data per activity
        activityIds.forEach(aid => {
            fetch(`/api/activity/${aid}/chart`).then(r => r.json()).then(data => {
                const pace = data.pace || [];
                const hr = data.hr || [];
                if (pace.length || hr.length) {
                    renderChartsInModal(parseInt(aid), pace, hr, isMulti ? data.name : null);
                }
            });
        });

        // Fetch streams for map
        if (hasStreams) {
            Promise.all(activityIds.map(aid =>
                fetch(`/api/activity/${aid}/streams`).then(r => r.json())
            )).then(results => {
                renderMapInModal(results.flat());
            });
        }
    }

    // ── Render charts in modal ──────────────────────────────────

    function renderChartsInModal(activityId, paceData, hrData, label) {
        // Render into the per-activity placeholder if it exists, else fall back to chart section
        const placeholder = document.querySelector(`.modal-chart-placeholder[data-chart-aid="${activityId}"]`);
        const target = placeholder || modalChartSectionEl;
        if (!target) return;

        const paceId = `modal-pace-chart-${activityId}`;
        const hrId = `modal-hr-chart-${activityId}`;

        const chartDiv = document.createElement("div");
        chartDiv.className = "chart-section";
        const labelHtml = label ? `<div class="chart-activity-label">${escapeHtml(label)}</div>` : "";
        chartDiv.innerHTML = `${labelHtml}<div class="chart-row">
            <div class="chart-wrap"><h3>Pace</h3><canvas id="${paceId}" height="100"></canvas></div>
            <div class="chart-wrap"><h3>Heart Rate</h3><canvas id="${hrId}" height="100"></canvas></div>
        </div>`;
        target.appendChild(chartDiv);

        if (paceData.length) {
            drawChart(paceId, paceData, {
                color: "#4a7ab5",
                fillColor: "rgba(74,122,181,0.15)",
                formatY: v => { const m = Math.floor(v / 60); const s = Math.round(v % 60); return `${m}:${s < 10 ? "0" : ""}${s}`; },
                invertY: true,
                partnerId: hrId,
            }, modalChartRegistry);
        }

        if (hrData.length) {
            drawChart(hrId, hrData, {
                color: "#c05050",
                fillColor: "rgba(192,80,80,0.15)",
                formatY: v => Math.round(v).toString(),
                invertY: false,
                partnerId: paceId,
            }, modalChartRegistry);
        }

        wireChartCrosshair(paceId, modalChartRegistry);
        wireChartCrosshair(hrId, modalChartRegistry);
    }

    // ── Render map in modal ─────────────────────────────────────

    function renderMapInModal(points) {
        if (!modalMapContainerEl || !points.length) return;

        modalMapContainerEl.innerHTML = '<h3>Map</h3><div class="map-container" id="modal-map"></div>';
        const mapEl = document.getElementById("modal-map");

        setTimeout(() => {
            if (modalMap) {
                modalMap.remove();
                modalMap = null;
            }

            const map = L.map(mapEl, { attributionControl: false });
            L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
                maxZoom: 19,
            }).addTo(map);

            const latlngs = points.map(p => [p.lat, p.lon]);
            const polyline = L.polyline(latlngs, {
                color: "#4A90D9", weight: 3, opacity: 0.8,
            }).addTo(map);

            map.fitBounds(polyline.getBounds(), { padding: [20, 20] });
            modalMap = map;
        }, 150);
    }

    // ── Render intervals + laps ─────────────────────────────────

    function buildTable(rows) {
        if (!rows.length) return "";

        let html = `<table class="intervals-table">
            <thead><tr>
                <th>#</th><th>Distance</th><th>Duration</th><th>Pace</th>
                <th>HR</th><th>Zone</th>`;
        html += `<th title="Stride">S</th><th title="Recovery">Rec</th>`;
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

            const rawDist = iv.canonical_distance_mi || iv.gps_measured_distance_mi || iv.prescribed_distance_mi || 0;

            let walkBtn = "";
            if (iv.is_walking) {
                walkBtn = `<button class="btn-unscrub" data-interval-id="${iv.id}" title="Mark as not walking">&#x21A9;</button>`;
            } else if (iv.source && iv.source !== "pace_segment") {
                walkBtn = `<button class="btn-scrub" data-interval-id="${iv.id}" title="Mark as walking">&#x1F6B6;</button>`;
            }

            html += `<tr class="${rowClass} ${bgClass}" data-interval-id="${iv.id}">
                <td>${iv.rep_number || ""}</td>
                <td class="iv-editable" data-field="distance" data-raw="${rawDist}">${iv.display_distance}${locLabel}</td>
                <td class="iv-editable" data-field="duration_s" data-raw="${iv.duration_s || ""}">${iv.display_duration}</td>
                <td>${iv.display_pace}</td>
                <td class="iv-editable" data-field="avg_hr" data-raw="${iv.avg_hr || ""}">${iv.display_hr}</td>
                <td class="iv-editable ${zoneClass}" data-field="pace_zone" data-raw="${zone}">${zone}${setLabel}</td>`;
            const strideChecked = iv.is_stride ? "checked" : "";
            const recoveryChecked = iv.is_recovery ? "checked" : "";
            html += `<td class="iv-checkbox-cell"><input type="checkbox" class="iv-stride-cb" data-interval-id="${iv.id}" ${strideChecked}></td>`;
            html += `<td class="iv-checkbox-cell"><input type="checkbox" class="iv-recovery-cb" data-interval-id="${iv.id}" ${recoveryChecked}></td>`;
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

    function wireWalkingButtons(container) {
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

    function wireIntervalCheckboxes(container) {
        container.querySelectorAll(".iv-stride-cb, .iv-recovery-cb").forEach(cb => {
            cb.addEventListener("change", (e) => {
                e.stopPropagation();
                const intervalId = cb.dataset.intervalId;
                const field = cb.classList.contains("iv-stride-cb") ? "is_stride" : "is_recovery";
                const value = cb.checked;
                fetch(`/api/interval/${intervalId}/flag`, {
                    method: "PUT",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ field, value }),
                }).then(r => r.json()).then(d => {
                    if (!d.ok) {
                        cb.checked = !value;
                    } else {
                        const tr = cb.closest("tr");
                        if (field === "is_stride") {
                            tr.classList.toggle("is-stride", value);
                            // Update the grid row's strides cell
                            if (d.activity_id && d.strides !== undefined) {
                                const gridRow = document.querySelector(`.activity-row[data-id="${d.activity_id}"]`)
                                    || document.querySelector(`.activity-row[data-activity-ids*="${d.activity_id}"]`);
                                if (gridRow) {
                                    const cell = gridRow.querySelector(".col-strides");
                                    if (cell) cell.textContent = d.strides || "";
                                }
                            }
                        } else {
                            tr.classList.toggle("is-recovery", value);
                        }
                    }
                }).catch(() => { cb.checked = !value; });
            });
        });
    }

    const IV_ZONE_OPTIONS = ["", "E", "M", "T", "I", "R", "FR"];

    function wireIntervalEditing(container) {
        container.querySelectorAll(".iv-editable").forEach(cell => {
            cell.addEventListener("dblclick", (e) => {
                e.stopPropagation();
                if (cell.querySelector("input, select")) return;

                const tr = cell.closest("tr");
                const intervalId = tr.dataset.intervalId;
                const field = cell.dataset.field;
                const rawValue = cell.dataset.raw;
                const originalHtml = cell.innerHTML;

                if (field === "pace_zone") {
                    const select = document.createElement("select");
                    select.className = "iv-inline-edit";
                    for (const z of IV_ZONE_OPTIONS) {
                        const opt = document.createElement("option");
                        opt.value = z;
                        opt.textContent = z || "--";
                        if (z === rawValue) opt.selected = true;
                        select.appendChild(opt);
                    }
                    cell.textContent = "";
                    cell.appendChild(select);
                    select.focus();

                    function finish(save) {
                        const newVal = select.value;
                        select.remove();
                        if (save && newVal !== rawValue) {
                            saveIntervalEdit(intervalId, field, newVal).then(result => {
                                if (result) {
                                    const iv = result.interval;
                                    const z = iv.pace_zone || "";
                                    const setLabel = iv.set_number ? ` S${iv.set_number}` : "";
                                    cell.className = "iv-editable" + (z ? ` zone-${z}` : "");
                                    cell.textContent = z + setLabel;
                                    cell.dataset.raw = z;
                                    tr.className = tr.className.replace(/zone-bg-\w+/g, "");
                                    if (z) tr.classList.add(`zone-bg-${z}`);
                                } else {
                                    cell.innerHTML = originalHtml;
                                }
                            });
                        } else {
                            cell.innerHTML = originalHtml;
                        }
                    }

                    select.addEventListener("change", () => finish(true));
                    select.addEventListener("keydown", (ev) => {
                        ev.stopPropagation();
                        if (ev.key === "Escape") finish(false);
                    });
                    select.addEventListener("blur", () => finish(true));

                } else {
                    const input = document.createElement("input");
                    input.className = "iv-inline-edit";

                    if (field === "distance") {
                        const dist = parseFloat(rawValue);
                        if (dist && dist < 1.0) {
                            input.type = "number";
                            input.step = "1";
                            input.value = Math.round(dist * 1609.344);
                            input.dataset.unit = "m";
                        } else {
                            input.type = "number";
                            input.step = "0.01";
                            input.value = dist ? dist.toFixed(2) : "";
                            input.dataset.unit = "mi";
                        }
                    } else if (field === "duration_s") {
                        input.type = "text";
                        const dur = parseFloat(rawValue);
                        if (dur) {
                            const total = Math.floor(dur);
                            const tenths = Math.round((dur - total) * 10) % 10;
                            const m = Math.floor(total / 60);
                            const s = total % 60;
                            input.value = `${m}:${s < 10 ? "0" : ""}${s}.${tenths}`;
                        } else {
                            input.value = "";
                        }
                        input.placeholder = "m:ss.t";
                    } else if (field === "avg_hr") {
                        input.type = "number";
                        input.step = "1";
                        input.value = rawValue ? Math.round(parseFloat(rawValue)) : "";
                    }

                    cell.textContent = "";
                    cell.appendChild(input);
                    input.focus();
                    input.select();

                    function finish(save) {
                        const val = input.value.trim();
                        input.remove();
                        if (save && val !== "") {
                            let sendValue;
                            if (field === "distance") {
                                if (input.dataset.unit === "m") {
                                    sendValue = parseFloat(val) / 1609.344;
                                } else {
                                    sendValue = parseFloat(val);
                                }
                                if (isNaN(sendValue) || sendValue <= 0) {
                                    cell.innerHTML = originalHtml;
                                    return;
                                }
                            } else if (field === "duration_s") {
                                sendValue = parseDurationPrecise(val);
                                if (sendValue === null) {
                                    cell.innerHTML = originalHtml;
                                    return;
                                }
                            } else if (field === "avg_hr") {
                                sendValue = parseFloat(val);
                                if (isNaN(sendValue)) {
                                    cell.innerHTML = originalHtml;
                                    return;
                                }
                            }

                            saveIntervalEdit(intervalId, field, sendValue).then(result => {
                                if (result) {
                                    const iv = result.interval;
                                    if (field === "distance") {
                                        cell.textContent = iv.display_distance;
                                        cell.dataset.raw = iv.canonical_distance_mi || iv.gps_measured_distance_mi || iv.prescribed_distance_mi || 0;
                                        const paceCell = tr.children[3];
                                        if (paceCell) paceCell.textContent = iv.display_pace;
                                    } else if (field === "duration_s") {
                                        cell.textContent = iv.display_duration;
                                        cell.dataset.raw = iv.duration_s || "";
                                        const paceCell = tr.children[3];
                                        if (paceCell) paceCell.textContent = iv.display_pace;
                                    } else if (field === "avg_hr") {
                                        cell.textContent = iv.display_hr;
                                        cell.dataset.raw = iv.avg_hr || "";
                                    }
                                } else {
                                    cell.innerHTML = originalHtml;
                                }
                            });
                        } else {
                            cell.innerHTML = originalHtml;
                        }
                    }

                    input.addEventListener("keydown", (ev) => {
                        ev.stopPropagation();
                        if (ev.key === "Enter") finish(true);
                        if (ev.key === "Escape") finish(false);
                    });
                    input.addEventListener("blur", () => finish(true));
                }
            });
        });
    }

    function parseDurationPrecise(str) {
        if (!str) return null;
        const match = str.match(/^(\d+):(\d{1,2})(?::(\d{1,2}))?(?:\.(\d))?$/);
        if (!match) return null;
        let result;
        if (match[3] !== undefined) {
            result = parseInt(match[1]) * 3600 + parseInt(match[2]) * 60 + parseInt(match[3]);
        } else {
            result = parseInt(match[1]) * 60 + parseInt(match[2]);
        }
        if (match[4] !== undefined) {
            result += parseInt(match[4]) / 10;
        }
        return result;
    }

    function saveIntervalEdit(intervalId, field, value) {
        return fetch(`/api/interval/${intervalId}/edit`, {
            method: "PUT",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ field, value }),
        }).then(r => r.json()).then(d => d.ok ? d : false).catch(() => false);
    }

    function escapeHtml(text) {
        const div = document.createElement("div");
        div.textContent = text;
        return div.innerHTML;
    }

    // ── VDOT calculator (Daniels-Gilbert) ─────────────────────

    function computeVDOT(distanceM, timeS) {
        const timeMin = timeS / 60.0;
        const velocity = distanceM / timeMin;
        const vo2 = -4.60 + 0.182258 * velocity + 0.000104 * velocity * velocity;
        const pctVO2max = 0.8 + 0.1894393 * Math.exp(-0.012778 * timeMin)
                        + 0.2989558 * Math.exp(-0.1932605 * timeMin);
        return Math.round((vo2 / pctVO2max) * 100) / 100;
    }

    // ── Per-activity edit form builder (no per-field save buttons) ──

    function formatDurationInput(seconds) {
        if (seconds == null) return "";
        const s = Math.round(seconds);
        const h = Math.floor(s / 3600);
        const m = Math.floor((s % 3600) / 60);
        const sec = s % 60;
        if (h > 0) return `${h}:${m < 10 ? "0" : ""}${m}:${sec < 10 ? "0" : ""}${sec}`;
        return `${m}:${sec < 10 ? "0" : ""}${sec}`;
    }

    function formatPaceInput(secPerMile) {
        if (secPerMile == null) return "";
        const s = Math.round(secPerMile);
        const m = Math.floor(s / 60);
        const sec = s % 60;
        return `${m}:${sec < 10 ? "0" : ""}${sec}`;
    }

    function parseDuration(str) {
        if (!str) return null;
        const parts = str.split(":").map(Number);
        if (parts.some(isNaN)) return null;
        if (parts.length === 3) return parts[0] * 3600 + parts[1] * 60 + parts[2];
        if (parts.length === 2) return parts[0] * 60 + parts[1];
        return null;
    }

    function buildEditForm(meta) {
        const aid = meta.id;
        const ov = meta.overridden_fields || [];
        const cats = ["", "easy", "long", "tempo", "interval", "race", "recovery", "other"];

        let shoeOptions = '<option value="">--</option>';
        if (window.SHOES) {
            for (const [shoeId, name] of Object.entries(window.SHOES)) {
                const sel = meta.shoe_id && parseInt(shoeId) === meta.shoe_id ? " selected" : "";
                shoeOptions += `<option value="${shoeId}"${sel}>${escapeHtml(name)}</option>`;
            }
        }

        let catOptions = "";
        for (const cat of cats) {
            const sel = meta.workout_category === cat ? " selected" : "";
            catOptions += `<option value="${cat}"${sel}>${cat || "--"}</option>`;
        }

        const durVal = formatDurationInput(meta.duration_s);
        const paceVal = formatPaceInput(meta.avg_pace_s_per_mi);

        // Reset button for overridden fields
        function resetBtn(field) {
            if (!ov.includes(field)) return "";
            return `<button class="btn-reset-override" data-field="${field}" data-aid="${aid}" title="Clear override (revert to original)">&#x21A9;</button>`;
        }

        return `
        <h3 style="margin-top:10px">Edit — Activity #${aid}</h3>
        <div class="edit-form" data-edit-aid="${aid}">
            <div class="edit-row" data-field-row="workout_name">
                <label>Name</label>
                <input type="text" data-field="workout_name" value="${escapeHtml(meta.workout_name)}" placeholder="${escapeHtml(meta.workout_name)}">
                ${resetBtn("workout_name")}
            </div>
            <div class="edit-row" data-field-row="workout_category">
                <label>Category</label>
                <select data-field="workout_category">${catOptions}</select>
                ${resetBtn("workout_category")}
            </div>
            <div class="edit-row" data-field-row="distance_mi">
                <label>Distance (mi)</label>
                <input type="number" step="0.01" data-field="distance_mi" value="${meta.adjusted_distance_mi != null ? parseFloat(meta.adjusted_distance_mi).toFixed(2) : (meta.distance_mi != null ? parseFloat(meta.distance_mi).toFixed(2) : "")}" placeholder="${meta.display_distance || ""}">
                ${resetBtn("distance_mi")}
            </div>
            <div class="edit-row" data-field-row="duration_s">
                <label>Duration</label>
                <input type="text" data-field="duration_s" data-type="duration" value="${durVal}" placeholder="${durVal || "m:ss"}">
                ${resetBtn("duration_s")}
            </div>
            <div class="edit-row" data-field-row="avg_pace_s_per_mi">
                <label>Pace (/mi)</label>
                <input type="text" data-field="avg_pace_s_per_mi" data-type="pace" value="${paceVal}" placeholder="${paceVal || "m:ss"}">
                ${resetBtn("avg_pace_s_per_mi")}
            </div>
            <div class="edit-row" data-field-row="avg_hr">
                <label>Avg HR</label>
                <input type="number" step="1" data-field="avg_hr" value="${meta.avg_hr != null ? Math.round(meta.avg_hr) : ""}" placeholder="${meta.avg_hr != null ? Math.round(meta.avg_hr) : ""}">
                ${resetBtn("avg_hr")}
            </div>
            <div class="edit-row" data-field-row="avg_cadence">
                <label>Cadence</label>
                <input type="number" step="1" data-field="avg_cadence" value="${meta.avg_cadence != null ? Math.round(meta.avg_cadence) : ""}" placeholder="${meta.avg_cadence != null ? Math.round(meta.avg_cadence) : ""}">
                ${resetBtn("avg_cadence")}
            </div>
            <div class="edit-row" data-field-row="shoe_id">
                <label>Shoe</label>
                <select data-field="shoe_id">${shoeOptions}</select>
                ${resetBtn("shoe_id")}
            </div>
            <div class="edit-row" data-field-row="strides">
                <label>Strides</label>
                <input type="number" step="1" data-field="strides" value="${meta.strides || ""}" placeholder="${meta.strides || ""}">
                ${resetBtn("strides")}
            </div>
            <div class="edit-row" data-field-row="vdot">
                <label>VDOT</label>
                <input type="number" step="0.1" data-field="vdot" value="${meta.vdot != null ? meta.vdot : ""}" placeholder="${meta.vdot != null ? meta.vdot : ""}">
            </div>
            <div class="edit-row race-entry-row" data-field-row="race">
                <label>Race</label>
                <select class="race-distance-select">
                    <option value="">-- distance --</option>
                    <option value="800">800m</option>
                    <option value="1500">1500m</option>
                    <option value="1609.344">Mile</option>
                    <option value="3000">3000m</option>
                    <option value="3200">3200m</option>
                    <option value="3218.688">2 Mile</option>
                    <option value="5000">5K</option>
                    <option value="8000">8K</option>
                    <option value="10000">10K</option>
                    <option value="21097.5">Half Marathon</option>
                    <option value="42195">Marathon</option>
                    <option value="custom">Custom (m)</option>
                </select>
                <input type="number" step="1" class="race-custom-dist" placeholder="meters" style="display:none;width:70px">
                <input type="text" class="race-time-input" placeholder="m:ss" style="width:70px">
                <span class="race-vdot-computed"></span>
                <label style="margin-left:6px;width:auto">Adj:</label>
                <input type="number" step="0.1" class="race-vdot-adjusted" placeholder="VDOT" style="width:60px">
                <label style="margin-left:4px;width:auto"><input type="checkbox" class="race-tag-cb"> Tag Race</label>
            </div>
            <div class="edit-row" data-field-row="notes">
                <label>Notes</label>
                <textarea data-field="notes" rows="2" placeholder="${escapeHtml(meta.notes)}">${escapeHtml(meta.notes)}</textarea>
                ${resetBtn("notes")}
            </div>
        </div>`;
    }

    function wireEditForm(container, meta) {
        const aid = meta.id;
        const form = container.querySelector(`[data-edit-aid="${aid}"]`);
        if (!form) return;

        // Store original values
        const originals = {};
        form.querySelectorAll("[data-field]").forEach(input => {
            const field = input.dataset.field;
            originals[field] = input.value;
        });
        modalOriginalValues[aid] = originals;
        modalDirtyState[aid] = {};
        modalClearQueue[aid] = new Set();

        // Track changes on all inputs
        form.querySelectorAll("[data-field]").forEach(input => {
            const field = input.dataset.field;
            const handler = () => {
                const current = input.value;
                const original = modalOriginalValues[aid][field];
                const editRow = input.closest(".edit-row");

                if (current !== original) {
                    modalDirtyState[aid][field] = current;
                    if (editRow) editRow.classList.add("dirty");
                } else {
                    delete modalDirtyState[aid][field];
                    if (editRow) editRow.classList.remove("dirty");
                }
                updateModalButtons();
            };
            input.addEventListener("input", handler);
            input.addEventListener("change", handler);
        });

        // Reset override buttons (queue clear on save)
        form.querySelectorAll(".btn-reset-override").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                const field = btn.dataset.field;
                const btnAid = parseInt(btn.dataset.aid);
                if (!modalClearQueue[btnAid]) modalClearQueue[btnAid] = new Set();
                modalClearQueue[btnAid].add(field);
                btn.style.opacity = "0.2";
                btn.title = "Will be cleared on save";
                const editRow = btn.closest(".edit-row");
                if (editRow) editRow.classList.add("dirty");
                updateModalButtons();
            });
        });

        // Race entry: auto-compute VDOT
        const raceDistSelect = form.querySelector(".race-distance-select");
        const raceCustomDist = form.querySelector(".race-custom-dist");
        const raceTimeInput = form.querySelector(".race-time-input");
        const raceComputedSpan = form.querySelector(".race-vdot-computed");
        const raceAdjustedInput = form.querySelector(".race-vdot-adjusted");
        const raceTagCb = form.querySelector(".race-tag-cb");
        let userEditedAdjusted = false;

        function getRaceDistM() {
            if (!raceDistSelect) return NaN;
            if (raceDistSelect.value === "custom") {
                return parseFloat(raceCustomDist.value);
            }
            return parseFloat(raceDistSelect.value);
        }

        function updateRaceVDOT() {
            const distM = getRaceDistM();
            const timeS = parseDuration(raceTimeInput ? raceTimeInput.value.trim() : "");
            if (!distM || isNaN(distM) || distM <= 0 || !timeS || timeS <= 0) {
                if (raceComputedSpan) raceComputedSpan.textContent = "";
                return;
            }
            const vdot = computeVDOT(distM, timeS);
            if (raceComputedSpan) raceComputedSpan.textContent = `= ${vdot.toFixed(1)}`;
            if (raceAdjustedInput && !userEditedAdjusted) {
                raceAdjustedInput.value = vdot.toFixed(1);
            }
            // Mark as dirty if we have race data
            updateRaceDirty();
        }

        function updateRaceDirty() {
            const distM = getRaceDistM();
            const timeS = parseDuration(raceTimeInput ? raceTimeInput.value.trim() : "");
            const hasRace = distM > 0 && timeS > 0;
            const editRow = form.querySelector('.race-entry-row');

            if (hasRace) {
                // Store VDOT state for batch save
                let vdotToSave = raceAdjustedInput ? parseFloat(raceAdjustedInput.value) : NaN;
                if (isNaN(vdotToSave) || vdotToSave <= 0) {
                    vdotToSave = computeVDOT(distM, timeS);
                }
                modalVdotState[aid] = {
                    vdot: vdotToSave,
                    race_distance_m: distM,
                    race_time_s: timeS,
                    tag_race: raceTagCb ? raceTagCb.checked : false,
                };
                if (editRow) editRow.classList.add("dirty");
            } else {
                delete modalVdotState[aid];
                if (editRow) editRow.classList.remove("dirty");
            }
            updateModalButtons();
        }

        if (raceDistSelect && raceCustomDist) {
            raceDistSelect.addEventListener("change", () => {
                raceCustomDist.style.display = raceDistSelect.value === "custom" ? "" : "none";
                userEditedAdjusted = false;
                updateRaceVDOT();
            });
            raceCustomDist.addEventListener("input", () => { userEditedAdjusted = false; updateRaceVDOT(); });
        }
        if (raceTimeInput) {
            raceTimeInput.addEventListener("input", () => { userEditedAdjusted = false; updateRaceVDOT(); });
        }
        if (raceAdjustedInput) {
            raceAdjustedInput.addEventListener("input", () => {
                userEditedAdjusted = true;
                updateRaceDirty();
            });
        }
        if (raceTagCb) {
            raceTagCb.addEventListener("change", updateRaceDirty);
        }

        // VDOT field dirty tracking (separate from race entry)
        const vdotInput = form.querySelector('[data-field="vdot"]');
        if (vdotInput) {
            const vdotHandler = () => {
                const current = vdotInput.value;
                const original = modalOriginalValues[aid]["vdot"];
                const editRow = vdotInput.closest(".edit-row");
                if (current !== original) {
                    // Track VDOT change in vdotState (without race data)
                    const v = parseFloat(current);
                    if (!isNaN(v) && v > 0) {
                        if (!modalVdotState[aid]) {
                            modalVdotState[aid] = { vdot: v };
                        } else {
                            modalVdotState[aid].vdot = v;
                        }
                    }
                    if (editRow) editRow.classList.add("dirty");
                } else {
                    // Only remove if no race data
                    if (modalVdotState[aid] && !modalVdotState[aid].race_distance_m) {
                        delete modalVdotState[aid];
                    }
                    if (editRow) editRow.classList.remove("dirty");
                }
                updateModalButtons();
            };
            vdotInput.addEventListener("input", vdotHandler);
            vdotInput.addEventListener("change", vdotHandler);
        }

        form.addEventListener("click", e => e.stopPropagation());
    }

    function updateModalButtons() {
        // Save and Save & Close are always visible; disabled when nothing to save
        const hasDirty = isDirty();
        modalBtnSave.disabled = !hasDirty;
        modalBtnSaveClose.disabled = !hasDirty;
    }

    // ── Batch save ──────────────────────────────────────────────

    async function saveModalChanges(andClose) {
        modalBtnSave.disabled = true;
        modalBtnSaveClose.disabled = true;

        try {
            // 1. Save dirty field overrides
            for (const [aidStr, fields] of Object.entries(modalDirtyState)) {
                const aid = parseInt(aidStr);
                for (const [field, rawValue] of Object.entries(fields)) {
                    if (field === "vdot") continue; // handled separately
                    let value = rawValue;
                    // Parse duration/pace
                    const form = document.querySelector(`[data-edit-aid="${aid}"]`);
                    const input = form ? form.querySelector(`[data-field="${field}"]`) : null;
                    if (input && value !== "" && (input.dataset.type === "duration" || input.dataset.type === "pace")) {
                        const parsed = parseDuration(value);
                        if (parsed === null) continue;
                        value = parsed;
                    }
                    await saveOverride(aid, field, value);

                    // Update grid row
                    const row = document.querySelector(`.activity-row[data-id="${aid}"]`)
                             || document.querySelector(`.activity-row[data-activity-ids*="${aid}"]`);
                    if (row) {
                        const colClass = fieldToColClass(field);
                        const cell = row.querySelector(`.col-${colClass}`);
                        if (cell) {
                            cell.classList.add("overridden");
                            updateCellDisplay(cell, field, value);
                        }
                    }
                }
            }

            // 2. Clear queued overrides
            for (const [aidStr, fields] of Object.entries(modalClearQueue)) {
                const aid = parseInt(aidStr);
                for (const field of fields) {
                    await clearOverride(aid, field);
                    const row = document.querySelector(`.activity-row[data-id="${aid}"]`)
                             || document.querySelector(`.activity-row[data-activity-ids*="${aid}"]`);
                    if (row) {
                        const cell = row.querySelector(`.col-${fieldToColClass(field)}`);
                        if (cell) cell.classList.remove("overridden");
                    }
                }
            }

            // 3. Save VDOT changes
            for (const [aidStr, vdotData] of Object.entries(modalVdotState)) {
                const aid = parseInt(aidStr);
                if (!vdotData || !vdotData.vdot) continue;

                const payload = { vdot: vdotData.vdot };
                if (vdotData.race_distance_m) {
                    payload.race_distance_m = vdotData.race_distance_m;
                    payload.race_time_s = vdotData.race_time_s;
                    payload.tag_race = vdotData.tag_race || false;
                }
                // For VDOT timeline: prompt user
                const applyTimeline = confirm(
                    "Apply this VDOT to all activities from this date until the next race?\n\n" +
                    "OK = yes (updates VDOT timeline + re-enriches)\n" +
                    "Cancel = this activity only"
                );
                payload.apply_timeline = applyTimeline;

                await fetch(`/api/activity/${aid}/vdot`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(payload),
                }).then(r => r.json());
            }

            // Reset dirty state
            modalDirtyState = {};
            modalClearQueue = {};
            modalVdotState = {};
            // Remove dirty classes
            document.querySelectorAll("#modal-edit-section .edit-row.dirty").forEach(el => {
                el.classList.remove("dirty");
            });
            updateModalButtons();

            // Refresh footer stats
            refreshFooterStats();

            // Refresh 7d MA for affected dates
            for (const aid of modalActivityIds) {
                const row = document.querySelector(`.activity-row[data-id="${aid}"]`)
                         || document.querySelector(`.activity-row[data-activity-ids*="${aid}"]`);
                if (row && row.dataset.date) {
                    refreshSevenDayMA(row.dataset.date);
                }
            }

            if (andClose) {
                // Enrich all activity IDs then close
                await fetch("/api/enrich", {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({ activity_ids: modalActivityIds }),
                }).then(r => r.json());
                closeModal();
            }
        } catch (err) {
            console.error("Save error:", err);
            // Re-enable so user can retry
            updateModalButtons();
        }
    }

    // ── Render charts (canvas) ──────────────────────────────────

    function drawChart(canvasId, data, opts, registry) {
        if (!registry) registry = modalChartRegistry;
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

        // X-axis labels
        ctx.textAlign = "center";
        ctx.fillStyle = "#999";
        const totalMin = tRange / 60;
        const xStep = totalMin <= 15 ? 5 : totalMin <= 45 ? 10 : 15;
        for (let min = 0; min <= totalMin; min += xStep) {
            const t = tMin + min * 60;
            const x = xPos(t);
            ctx.fillText(`${min}m`, x, h - 4);
        }

        const cleanImage = ctx.getImageData(0, 0, canvas.width, canvas.height);
        registry[canvasId] = {
            data, opts, canvas, cleanImage, dpr,
            xPos, yPos, tFromX,
            pad, w, h, tMin, tMax, tRange,
        };
    }

    function drawCrosshair(canvasId, time, frozen, registry) {
        if (!registry) registry = modalChartRegistry;
        const reg = registry[canvasId];
        if (!reg) return;
        const { data, opts, canvas, cleanImage, dpr, xPos, yPos, pad, w, h } = reg;
        const ctx = canvas.getContext("2d");

        ctx.putImageData(cleanImage, 0, 0);
        ctx.save();
        ctx.setTransform(dpr, 0, 0, dpr, 0, 0);

        let nearest = data[0];
        let minDiff = Infinity;
        for (const d of data) {
            const diff = Math.abs(d.t - time);
            if (diff < minDiff) { minDiff = diff; nearest = d; }
        }

        const x = xPos(nearest.t);
        const y = yPos(nearest.v);

        ctx.beginPath();
        ctx.moveTo(x, pad.top);
        ctx.lineTo(x, h - pad.bottom);
        ctx.strokeStyle = "rgba(0,0,0,0.2)";
        ctx.lineWidth = 1;
        ctx.setLineDash([3, 3]);
        ctx.stroke();
        ctx.setLineDash([]);

        ctx.beginPath();
        ctx.arc(x, y, frozen ? 5 : 4, 0, Math.PI * 2);
        ctx.fillStyle = opts.color;
        ctx.fill();
        ctx.strokeStyle = "#fff";
        ctx.lineWidth = 1.5;
        ctx.stroke();

        {
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

    function wireChartCrosshair(canvasId, registry) {
        if (!registry) registry = modalChartRegistry;
        const reg = registry[canvasId];
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
                frozenTime = null;
            }
            tracking = true;
            const time = getTime(e);
            drawCrosshair(canvasId, time, false, registry);
            syncPartner(canvasId, time, false, registry);
        });

        canvas.addEventListener("mousemove", (e) => {
            if (!tracking) return;
            const time = getTime(e);
            drawCrosshair(canvasId, time, false, registry);
            syncPartner(canvasId, time, false, registry);
        });

        canvas.addEventListener("mouseup", (e) => {
            if (!tracking) return;
            tracking = false;
            const time = getTime(e);
            frozenTime = time;
            drawCrosshair(canvasId, time, true, registry);
            syncPartner(canvasId, time, true, registry);
        });

        canvas.addEventListener("mouseleave", (e) => {
            if (tracking) {
                tracking = false;
                const time = getTime(e);
                frozenTime = time;
                drawCrosshair(canvasId, time, true, registry);
                syncPartner(canvasId, time, true, registry);
            }
        });
    }

    function syncPartner(canvasId, time, frozen, registry) {
        if (!registry) registry = modalChartRegistry;
        const reg = registry[canvasId];
        if (!reg || !reg.opts.partnerId) return;
        const partnerId = reg.opts.partnerId;
        if (registry[partnerId]) {
            drawCrosshair(partnerId, time, frozen, registry);
        }
    }

    // ── Field helpers ───────────────────────────────────────────

    function fieldToColClass(field) {
        const map = {
            distance_mi: "dist", duration_s: "dur", avg_pace_s_per_mi: "pace",
            workout_name: "name", workout_category: "name",
            notes: "notes", strides: "strides",
            workout_type_zone: "type", avg_hr: "hr", max_hr: "hr",
            avg_cadence: "cad",
        };
        return map[field] || field;
    }

    function updateCellDisplay(cell, field, value) {
        const empty = value === "" || value === null || value === undefined;
        if (field === "distance_mi") {
            cell.textContent = empty ? "" : parseFloat(value).toFixed(2);
        } else if (field === "workout_name") {
            cell.textContent = value || "";
        } else if (field === "strides") {
            cell.textContent = value || "";
        } else if (field === "notes") {
            cell.textContent = value || "";
            cell.title = value || "";
        } else if (field === "duration_s") {
            if (empty) { cell.textContent = ""; return; }
            const s = parseInt(value, 10);
            const h = Math.floor(s / 3600);
            const m = Math.floor((s % 3600) / 60);
            const sec = s % 60;
            cell.textContent = h ? `${h}:${m < 10 ? "0" : ""}${m}:${sec < 10 ? "0" : ""}${sec}` : `${m}:${sec < 10 ? "0" : ""}${sec}`;
        } else if (field === "avg_pace_s_per_mi") {
            if (empty) { cell.textContent = ""; return; }
            const s = parseFloat(value);
            const m = Math.floor(s / 60);
            const sec = Math.round(s % 60);
            cell.textContent = `${m}:${sec < 10 ? "0" : ""}${sec}`;
        } else if (field === "avg_hr" || field === "max_hr" || field === "avg_cadence") {
            cell.textContent = empty ? "" : Math.round(parseFloat(value));
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

    // ── Weekly mileage sidebar chart ────────────────────────────

    const sidebarChartRegistry = {};

    function drawWeeklyMileageChart() {
        const canvas = document.getElementById("weekly-mileage-chart");
        if (!canvas || !window.WEEKLY_CHART_DATA || !window.WEEKLY_CHART_DATA.length) return;

        const data = window.WEEKLY_CHART_DATA;
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

        ctx.beginPath();
        ctx.moveTo(xPos(0), yPos(data[0].distance));
        for (let i = 1; i < data.length; i++) {
            ctx.lineTo(xPos(i), yPos(data[i].distance));
        }
        ctx.strokeStyle = "#4a7ab5";
        ctx.lineWidth = 1.5;
        ctx.stroke();

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

        ctx.textAlign = "center";
        ctx.fillStyle = "#aaa";
        ctx.font = "8px -apple-system, sans-serif";
        const labelStep = Math.max(1, Math.floor(data.length / 4));
        for (let i = 0; i < data.length; i += labelStep) {
            ctx.fillText(data[i].label, xPos(i), h - 4);
        }
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
                    const poll = setInterval(() => {
                        fetch("/api/import/status").then(r => r.json()).then(s => {
                            if (!s.running) {
                                clearInterval(poll);
                                importBtn.disabled = false;
                                importBtn.textContent = "Import";
                                if (s.success) {
                                    const match = s.output.match(/(\d+) new/);
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
        const d = new Date(changedDate + "T00:00:00");
        const start = changedDate;
        const endD = new Date(d);
        endD.setDate(endD.getDate() + 6);
        const end = endD.toISOString().slice(0, 10);

        fetch(`/api/seven_day_ma?start=${start}&end=${end}`)
            .then(r => r.json())
            .then(maData => {
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
        const yearEl = document.querySelector(".cal-year");
        const year = yearEl ? yearEl.textContent.trim() : new Date().getFullYear();

        fetch(`/api/footer_stats?year=${year}`)
            .then(r => r.json())
            .then(data => {
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
