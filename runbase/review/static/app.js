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
    const modalBtnNew = document.getElementById("modal-btn-new");

    // ── Plan undo stack (Cmd/Ctrl+Z for planning edits) ────────
    window.planUndoHistory = [];
    function pushPlanUndo(entry) {
        window.planUndoHistory.push(entry);
        if (window.planUndoHistory.length > 50) window.planUndoHistory.shift();
    }

    // ── Modal state ─────────────────────────────────────────────
    let modalMap = null;
    const modalChartRegistry = {};
    let modalDirtyState = {};   // { aid: { field: newValue, ... } }
    let modalOriginalValues = {}; // { aid: { field: originalValue, ... } }
    let modalClearQueue = {};   // { aid: Set(field, ...) } — overrides to delete on save
    let modalActivityIds = [];  // all activity IDs shown in the current modal
    let modalVdotState = {};    // { aid: { vdot, race_distance_m, race_time_s, tag_race, apply_timeline } }
    let modalDate = null;       // date of the currently open modal's activity
    let modalNewActivityIds = new Set();  // IDs of activities created during this modal session
    let modalNeedsEnrich = false;  // set when stride/recovery/walking toggles fire
    let modalFlagState = {};    // quality flags are saved with the other edits
    let modalNeedsRefresh = false;
    let modalSaving = false;

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
                    pushPlanUndo({ type: "override", activityId: id, field: "workout_name", previous: currentValue, cell: cell });
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

    const ZONE_OPTIONS = ["", "E", "M", "T", "CV", "I", "R", "FR", "T/CV", "CV/I", "T/I", "T/R", "T/FR", "I/R", "I/FR", "R/FR"];

    function zoneClass(zone) {
        if (!zone) return "";
        const parts = zone.split("/");
        return "zone-" + parts[0];
    }

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
                    pushPlanUndo({ type: "override", activityId: id, field: "workout_type_zone", previous: currentValue, cell: cell });
                    cell.textContent = newValue;
                    cell.className = "col-type" + (newValue ? ` ${zoneClass(newValue)}` : "");
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

    // ── Click past blank row → open modal for that date ────────

    document.querySelectorAll(".blank-row:not(.future-row):not(.planned-row)").forEach(row => {
        row.addEventListener("click", () => {
            const dateStr = row.dataset.date;
            if (!dateStr) return;
            openModal(null, [], false, row);
        });
    });

    // ── Click future/planned blank row → planned activity entry ──

    document.querySelectorAll(".future-row, .planned-row").forEach(row => {
        row.addEventListener("click", (e) => {
            if (row.querySelector(".planned-input")) return;
            const dateStr = row.dataset.date;
            if (!dateStr) return;

            const existingDist = row.querySelector(".planned-dist");
            const existingName = row.querySelector(".planned-name");
            const existingType = row.querySelector(".planned-type");
            const existingStrides = row.querySelector(".planned-strides");
            const oldDist = existingDist ? existingDist.textContent.trim() : "";
            const oldName = existingName ? existingName.textContent.trim() : "";
            const oldType = existingType ? existingType.textContent.trim() : "";
            const oldStrides = existingStrides ? existingStrides.textContent.trim() : "";

            const cells = row.querySelectorAll("td");
            while (cells.length > 5 && row.children.length > 5) {
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

            const typeTd = document.createElement("td");
            typeTd.className = "col-type";
            const typeSelect = document.createElement("select");
            typeSelect.className = "planned-input";
            for (const z of ZONE_OPTIONS) {
                const opt = document.createElement("option");
                opt.value = z;
                opt.textContent = z || "--";
                if (z === oldType) opt.selected = true;
                typeSelect.appendChild(opt);
            }
            typeTd.appendChild(typeSelect);
            row.appendChild(typeTd);

            const stridesTd = document.createElement("td");
            stridesTd.className = "col-strides";
            const stridesInput = document.createElement("input");
            stridesInput.type = "text";
            stridesInput.className = "planned-input";
            stridesInput.placeholder = "str";
            stridesInput.value = oldStrides;
            stridesTd.appendChild(stridesInput);
            row.appendChild(stridesTd);

            const restTd = document.createElement("td");
            restTd.colSpan = 7;
            row.appendChild(restTd);

            distInput.focus();

            function savePlanned() {
                const dist = distInput.value.trim();
                const name = nameInput.value.trim();
                // Build previous state for undo
                const prevData = (oldDist || oldName || oldType || oldStrides)
                    ? { distance_mi: oldDist ? parseFloat(oldDist) : null, workout_name: oldName || null, workout_type_zone: oldType || null, strides: oldStrides ? parseInt(oldStrides) : null }
                    : null;
                if (!dist && !name) {
                    if (oldDist || oldName) {
                        pushPlanUndo({ type: "planned", date: dateStr, previous: prevData });
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
                const type = typeSelect.value.trim().toUpperCase();
                const stridesVal = stridesInput.value.trim();
                pushPlanUndo({ type: "planned", date: dateStr, previous: prevData });
                fetch(`/api/planned/${dateStr}`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        distance_mi: dist ? parseFloat(dist) : null,
                        workout_name: name,
                        workout_type_zone: type || null,
                        strides: stridesVal ? parseInt(stridesVal) : null,
                    }),
                }).then(r => r.json()).then(d => {
                    if (d.ok) {
                        while (row.children.length > 5) row.removeChild(row.lastChild);
                        const dTd = document.createElement("td");
                        dTd.className = "col-dist planned-dist";
                        dTd.textContent = dist ? parseFloat(dist).toFixed(1) : "";
                        row.appendChild(dTd);
                        const nTd = document.createElement("td");
                        nTd.className = "col-name planned-name";
                        nTd.textContent = name;
                        row.appendChild(nTd);
                        const tTd = document.createElement("td");
                        tTd.className = "col-type planned-type" + (d.workout_type_zone ? " " + zoneClass(d.workout_type_zone) : "");
                        tTd.textContent = d.workout_type_zone || "";
                        row.appendChild(tTd);
                        const sTd = document.createElement("td");
                        sTd.className = "col-strides planned-strides";
                        sTd.textContent = d.strides || "";
                        row.appendChild(sTd);
                        for (let i = 0; i < 4; i++) {
                            row.appendChild(document.createElement("td"));
                        }
                        const iTd = document.createElement("td");
                        iTd.className = "col-iscore planned-iscore";
                        iTd.textContent = d.estimated_iscore ? d.estimated_iscore.toFixed(1) : "";
                        row.appendChild(iTd);
                        for (let j = 0; j < 4; j++) {
                            row.appendChild(document.createElement("td"));
                        }
                        row.classList.add("planned-row");
                        row.classList.remove("future-row");
                        refreshSevenDayMA(dateStr);
                    }
                });
            }

            function restoreBlanks() {
                while (row.children.length > 5) row.removeChild(row.lastChild);
                const td = document.createElement("td");
                td.colSpan = 13;
                row.appendChild(td);
                row.classList.remove("planned-row");
                if (!row.classList.contains("future-row")) row.classList.add("future-row");
            }

            const allInputs = [distInput, nameInput, typeSelect, stridesInput];
            allInputs.forEach((input, idx) => {
                input.addEventListener("keydown", (ev) => {
                    ev.stopPropagation();
                    if (ev.key === "Enter") savePlanned();
                    if (ev.key === "Escape") restoreBlanks();
                    if (ev.key === "Tab" && idx < allInputs.length - 1) {
                        ev.preventDefault();
                        allInputs[idx + 1].focus();
                    } else if (ev.key === "Tab" && idx === allInputs.length - 1) {
                        ev.preventDefault();
                        savePlanned();
                    }
                });
                input.addEventListener("click", (ev) => ev.stopPropagation());
                input.addEventListener("blur", () => {
                    setTimeout(() => {
                        if (!row.contains(document.activeElement) || !document.activeElement.classList.contains("planned-input")) {
                            savePlanned();
                        }
                    }, 100);
                });
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
        modalDate = row.dataset.date || null;
        modalNewActivityIds = new Set();
        modalNeedsEnrich = false;
        modalFlagState = {};
        modalNeedsRefresh = false;
        modalSaving = false;
        showModalSaveError("");

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
        modalEditSectionEl.innerHTML = "";
        modalMapContainerEl.innerHTML = "";

        // Show modal
        modalEl.style.display = "flex";

        // Buttons always visible but disabled until dirty
        modalBtnSave.disabled = true;
        modalBtnSaveClose.disabled = true;

        // Load data or show empty state
        if (activityIds.length === 0) {
            modalIntervalsEl.innerHTML = '<p class="loading">No activities on this day. Click "+ New Activity" to add one.</p>';
        } else {
            modalIntervalsEl.innerHTML = '<p class="loading">Loading...</p>';
            loadDetailIntoModal(id, activityIds, hasStreams);
        }
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

    function hasFieldChanges() {
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

    function isDirty() {
        return hasFieldChanges() || Object.values(modalFlagState).some(flags => Object.keys(flags).length > 0);
    }

    function confirmDiscard() {
        if (modalSaving) return false;
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

    // + New Activity button
    modalBtnNew.addEventListener("click", () => {
        if (!modalDate) return;
        modalBtnNew.disabled = true;
        fetch("/api/activity", {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify({ date: modalDate }),
        }).then(r => r.json()).then(d => {
            modalBtnNew.disabled = false;
            if (!d.ok) return;
            const newId = d.activity_id;
            modalNewActivityIds.add(newId);
            modalActivityIds.push(newId);

            // Reload modal content with updated activity list
            modalChartSectionEl.innerHTML = "";
            modalIntervalsEl.innerHTML = '<p class="loading">Loading...</p>';
            modalEditSectionEl.innerHTML = "";

            const hasStreams = modalActivityIds.some(aid => {
                const row = document.querySelector(`.activity-row[data-id="${aid}"]`)
                         || document.querySelector(`.activity-row[data-activity-ids*="${aid}"]`);
                return row && row.dataset.hasStreams === "true";
            });
            loadDetailIntoModal(modalActivityIds[0], modalActivityIds.map(String), hasStreams);
        }).catch(() => { modalBtnNew.disabled = false; });
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

    // ── Cmd/Ctrl+Z: undo planning edits ──────────────────────────
    document.addEventListener("keydown", (e) => {
        if ((e.metaKey || e.ctrlKey) && e.key === "z" && !e.shiftKey) {
            if (e.target.tagName === "INPUT" || e.target.tagName === "TEXTAREA" || e.target.tagName === "SELECT") return;
            if (window.planUndoHistory.length === 0) return;
            e.preventDefault();
            planUndo();
        }
    });

    function planUndo() {
        const entry = window.planUndoHistory.pop();
        if (!entry) return;

        if (entry.type === "planned") {
            if (!entry.previous) {
                // Was empty before → delete the planned activity
                fetch(`/api/planned/${entry.date}`, { method: "DELETE" })
                    .then(r => r.json())
                    .then(() => restorePlannedRow(entry.date, null));
            } else {
                // Had data before → restore it
                fetch(`/api/planned/${entry.date}`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify(entry.previous),
                }).then(r => r.json())
                  .then(d => { if (d.ok) restorePlannedRow(entry.date, d); });
            }
        } else if (entry.type === "weekly") {
            // Restore weekly plan fields
            fetch(`/api/planning/${entry.weekStart}`, {
                method: "POST",
                headers: { "Content-Type": "application/json" },
                body: JSON.stringify(entry.previous),
            }).then(r => r.json())
              .then(() => restoreWeeklyRow(entry.weekStart, entry.previous));
        } else if (entry.type === "override") {
            const prev = entry.previous;
            const cell = entry.cell;
            if (prev) {
                saveOverride(entry.activityId, entry.field, prev).then(ok => {
                    if (!ok) return;
                    cell.textContent = prev;
                    if (entry.field === "workout_type_zone") {
                        cell.className = "col-type" + (prev ? ` ${zoneClass(prev)}` : "");
                    }
                });
            } else {
                clearOverride(entry.activityId, entry.field).then(ok => {
                    if (!ok) return;
                    cell.textContent = "";
                    cell.classList.remove("overridden");
                    if (entry.field === "workout_type_zone") {
                        cell.className = "col-type";
                    }
                });
            }
        }
    }

    function restorePlannedRow(dateStr, data) {
        const row = document.querySelector(`tr[data-date="${dateStr}"]`);
        if (!row) return;
        // Remove everything after the first 5 columns (date, dow, wk-mi, 7d-mi, 30d-mi)
        while (row.children.length > 5) row.removeChild(row.lastChild);
        if (data && (data.distance_mi || data.workout_name)) {
            const dTd = document.createElement("td");
            dTd.className = "col-dist planned-dist";
            dTd.textContent = data.distance_mi ? parseFloat(data.distance_mi).toFixed(1) : "";
            row.appendChild(dTd);
            const nTd = document.createElement("td");
            nTd.className = "col-name planned-name";
            nTd.textContent = data.workout_name || "";
            row.appendChild(nTd);
            const tTd = document.createElement("td");
            tTd.className = "col-type planned-type" + (data.workout_type_zone ? " " + zoneClass(data.workout_type_zone) : "");
            tTd.textContent = data.workout_type_zone || "";
            row.appendChild(tTd);
            const sTd = document.createElement("td");
            sTd.className = "col-strides planned-strides";
            sTd.textContent = data.strides || "";
            row.appendChild(sTd);
            for (let i = 0; i < 4; i++) row.appendChild(document.createElement("td"));
            const iTd = document.createElement("td");
            iTd.className = "col-iscore planned-iscore";
            iTd.textContent = data.estimated_iscore ? data.estimated_iscore.toFixed(1) : "";
            row.appendChild(iTd);
            for (let j = 0; j < 4; j++) row.appendChild(document.createElement("td"));
            row.classList.add("planned-row");
            row.classList.remove("future-row");
        } else {
            const td = document.createElement("td");
            td.colSpan = 13;
            row.appendChild(td);
            row.classList.remove("planned-row");
            if (!row.classList.contains("future-row")) row.classList.add("future-row");
        }
        refreshSevenDayMA(dateStr);
    }

    function restoreWeeklyRow(weekStart, previous) {
        const row = document.querySelector(`tr[data-week-start="${weekStart}"]`);
        if (!row) return;
        row.querySelectorAll(".planning-editable").forEach(cell => {
            const f = cell.dataset.field;
            const val = previous[f];
            if (f === "target_mileage" || f === "target_intensity") {
                cell.dataset.target = val != null ? val : "";
                const actual = parseFloat(cell.dataset.actual) || 0;
                if (val != null) {
                    if (actual > 0) {
                        cell.innerHTML = `<span class="planning-target">${parseFloat(val).toFixed(1)}</span>`
                            + `<span class="planning-actual">(${actual.toFixed(1)})</span>`;
                    } else {
                        cell.textContent = parseFloat(val).toFixed(1);
                    }
                } else {
                    cell.textContent = actual > 0 ? actual.toFixed(1) : "";
                }
            } else {
                cell.textContent = val || "";
            }
        });
    }

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

            // Collect manual_split intervals per activity for the edit form
            const manualSplitsMap = {};
            for (const r of results) {
                const allIvs = (r.intervals || []).concat(r.laps || []);
                manualSplitsMap[r.aid] = allIvs.filter(iv => iv.source === "manual_split");
            }

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
                    const gapPart = meta.display_gap ? ` &middot; GAP ${meta.display_gap}` : "";
                    html += `<span class="activity-meta">${meta.display_distance}mi &middot; ${meta.display_pace}/mi${gapPart} &middot; ${meta.display_duration}${hrPart}</span>`;
                    html += `</div>`;
                }

                // Chart placeholder (renderChartsInModal will fill this)
                html += `<div class="modal-chart-placeholder" data-chart-aid="${r.aid}"></div>`;

                html += `<div class="activity-interval-tables" data-table-aid="${r.aid}">${buildIntervalTables(r)}</div>`;

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

            // Wire edit forms for dirty tracking + splits
            for (const r of results) {
                wireEditForm(modalIntervalsEl, r.meta);

                // If manual splits exist, auto-expand the splits section
                const splits = manualSplitsMap[r.aid] || [];
                if (splits.length > 0) {
                    const placeholder = modalIntervalsEl.querySelector(`[data-splits-placeholder-aid="${r.aid}"]`);
                    if (placeholder) {
                        placeholder.style.display = "";
                        placeholder.innerHTML = buildSplitsSection(r.aid, splits);
                        wireSplitsSection(modalIntervalsEl, r.aid);
                        // Hide the "Add Splits" button
                        const btn = modalIntervalsEl.querySelector(`.btn-add-splits[data-aid="${r.aid}"]`);
                        if (btn) btn.style.display = "none";
                    }
                }
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

    function buildIntervalTables(data) {
        let html = "";
        for (const [key, label] of [["intervals", "Intervals"], ["laps", "Laps"], ["splits", "Mile splits"]]) {
            const rows = data[key] || [];
            if (!rows.length) continue;
            const fullMiles = rows.slice(0, -1).every(iv => {
                const distance = iv.gps_measured_distance_mi || iv.canonical_distance_mi || 0;
                return distance >= 0.98 && distance <= 1.02;
            });
            const title = key === "splits" && !fullMiles ? "Automatic splits" : label;
            html += `<h3>${title}</h3>${buildTable(rows)}`;
        }
        if (data.summary && data.summary.length) html += `<h3>Summary</h3>${renderSummary(data.summary)}`;
        return html;
    }

    function buildTable(rows) {
        if (!rows.length) return "";

        let html = `<table class="intervals-table">
            <thead><tr>
                <th>#</th><th>Distance</th><th>Duration</th><th>Pace</th>
                <th>HR</th><th>Intensity</th><th class="iv-points" title="Contribution to activity intensity score">Points</th>`;
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
            const manualLabel = iv.source === "manual_split" ? " (manual)" : "";

            const rawDist = iv.canonical_distance_mi || iv.gps_measured_distance_mi || iv.prescribed_distance_mi || 0;
            const intensityTitle = "Click to edit intensity" + (iv.is_partial_split
                ? ". Partial mile: excluded from workout type." : "");

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
                <td class="iv-intensity ${zoneClass}"><button type="button" class="iv-zone-trigger" data-interval-id="${iv.id}" data-activity-id="${iv.activity_id}" data-zone="${zone}" data-overridden="${!!iv.zone_overridden}" title="${intensityTitle}" aria-label="Edit intensity for split ${iv.rep_number || ''}: ${zone || 'Auto'}">${zone || "--"}</button>${setLabel}${manualLabel}</td>
                <td class="iv-points">${(iv.intensity_points || 0).toFixed(2)}</td>`;
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
                        modalNeedsEnrich = true;
                        updateModalButtons();
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
                        syncVdotExclusion(d.activity_id, d.exclude_from_vdot, d.vdot_exclusion_source);
                        modalNeedsEnrich = true;
                        updateModalButtons();
                    }
                }).catch(() => { cb.checked = !value; });
            });
        });
    }

    const IV_ZONE_OPTIONS = ["", "E", "M", "T", "CV", "I", "R", "FR"];
    const IV_ZONE_LABELS = { E: "Easy", M: "Marathon", T: "Threshold", CV: "Critical velocity", I: "Interval", R: "Repetition", FR: "Fast repetition" };

    function wireIntervalEditing(container) {
        container.querySelectorAll(".iv-zone-trigger").forEach(trigger => {
            trigger.addEventListener("click", event => {
                event.stopPropagation();
                const cell = trigger.closest("td");
                if (cell.querySelector("select")) return;
                const aid = trigger.dataset.activityId;
                const table = trigger.closest(".activity-interval-tables");
                const originalValue = trigger.dataset.overridden === "true" ? trigger.dataset.zone : "";
                const select = document.createElement("select");
                select.className = "iv-zone-select";
                select.setAttribute("aria-label", trigger.getAttribute("aria-label"));
                for (const zone of IV_ZONE_OPTIONS) {
                    const option = document.createElement("option");
                    option.value = zone;
                    option.textContent = zone ? `${IV_ZONE_LABELS[zone]} (${zone})` : `Auto (${trigger.dataset.zone || "--"})`;
                    select.appendChild(option);
                }
                select.value = originalValue;
                trigger.hidden = true;
                cell.appendChild(select);
                let finished = false;

                async function finish(save) {
                    if (finished) return;
                    finished = true;
                    if (!save || select.value === originalValue) {
                        select.remove();
                        trigger.hidden = false;
                        return;
                    }
                    select.disabled = true;
                    try {
                        const result = await saveIntervalEdit(trigger.dataset.intervalId, "pace_zone", select.value);
                        if (!result) throw new Error("Intensity could not be saved. Please try again.");
                        modalNeedsEnrich = true;
                        updateModalButtons();
                        const response = await fetch(`/api/activity/${aid}/intervals`);
                        if (!response.ok) throw new Error("Intensity saved. Reopen this run to refresh the splits.");
                        refreshIntervalsTable(container, aid, await response.json());
                    } catch (error) {
                        let message = table.querySelector(".split-save-error");
                        if (!message) {
                            message = document.createElement("p");
                            message.className = "split-save-error";
                            message.setAttribute("role", "alert");
                            table.appendChild(message);
                        }
                        message.textContent = error.message;
                        select.disabled = false;
                        finished = false;
                    }
                }
                select.addEventListener("change", () => finish(true));
                select.addEventListener("blur", () => finish(true));
                select.addEventListener("keydown", event => {
                    event.stopPropagation();
                    if (event.key === "Escape") {
                        event.preventDefault();
                        finish(false);
                        trigger.focus();
                    }
                });
                select.focus();
                // Open the native menu on the same click where supported.
                try { select.showPicker?.(); } catch (_) { /* keyboard selection still works */ }
            });
        });
        container.querySelectorAll(".iv-editable").forEach(cell => {
            cell.addEventListener("dblclick", (e) => {
                e.stopPropagation();
                if (cell.querySelector("input, select")) return;

                const tr = cell.closest("tr");
                const intervalId = tr.dataset.intervalId;
                const field = cell.dataset.field;
                const rawValue = cell.dataset.raw;
                const originalHtml = cell.innerHTML;

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
                                modalNeedsEnrich = true;
                                updateModalButtons();
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

    function formatActivityStartTime(value) {
        if (!value) return "\u2014";
        const timestamp = value.trim().replace(" ", "T");
        // Naive timestamps are already activity-local wall time. Preserve that
        // clock even if the browser is in another time zone.
        const local = timestamp.match(/^\d{4}-\d{2}-\d{2}T(\d{2}):(\d{2})(?::\d{2}(?:\.\d+)?)?$/);
        if (local) {
            const hour = Number(local[1]);
            if (hour > 23 || Number(local[2]) > 59) return "\u2014";
            return `${hour % 12 || 12}:${local[2]} ${hour < 12 ? "AM" : "PM"}`;
        }
        // FIT files without a local clock retain an explicit UTC offset.
        if (!/(?:Z|[+-]\d{2}:?\d{2})$/i.test(timestamp)) return "\u2014";
        const instant = new Date(timestamp);
        if (Number.isNaN(instant.getTime())) return "\u2014";
        return instant.toLocaleTimeString("en-US", {
            hour: "numeric", minute: "2-digit", timeZone: "America/New_York",
        });
    }

    // ── Weather display helper ──────────────────────────────────
    function formatWeather(meta) {
        if (meta.temperature_f == null) return "\u2014";
        const t = Math.round(meta.temperature_f);
        const parts = [`${t}\u00B0F`];
        if (meta.humidity_pct != null) {
            parts.push(`${Math.round(meta.humidity_pct)}% RH`);
        }
        // Dew point (Magnus formula, temp in °C)
        if (meta.humidity_pct != null) {
            const tc = (meta.temperature_f - 32) * 5 / 9;
            const rh = meta.humidity_pct;
            const a = 17.27, b = 237.7;
            const alpha = (a * tc) / (b + tc) + Math.log(rh / 100);
            const dpC = (b * alpha) / (a - alpha);
            const dpF = dpC * 9 / 5 + 32;
            parts.push(`DP ${Math.round(dpF)}\u00B0`);
            // Wet bulb (Stull 2011 approximation, temp in °C)
            const wbC = tc * Math.atan(0.151977 * Math.sqrt(rh + 8.313659))
                + Math.atan(tc + rh) - Math.atan(rh - 1.676331)
                + 0.00391838 * Math.pow(rh, 1.5) * Math.atan(0.023101 * rh)
                - 4.686035;
            const wbF = wbC * 9 / 5 + 32;
            parts.push(`WB ${Math.round(wbF)}\u00B0`);
        }
        if (meta.cloud_cover_pct != null) {
            const sun = 100 - Math.round(meta.cloud_cover_pct);
            parts.push(`\u2600 ${sun}%`);
        }
        if (meta.weather_conditions) {
            parts.push(meta.weather_conditions);
        }
        return parts.join("  ");
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

    function formatSplitDistance(dist) {
        if (!dist) return "";
        if (dist < 1.0) return `${Math.round(dist * 1609.344)}m`;
        return `${dist.toFixed(2)}mi`;
    }

    function formatSplitDuration(s) {
        if (!s) return "";
        const total = Math.round(s);
        const m = Math.floor(total / 60);
        const sec = total % 60;
        return `${m}:${sec < 10 ? "0" : ""}${sec}`;
    }

    function buildSplitsSection(aid, splits) {
        let html = `<div class="splits-section" data-splits-aid="${aid}">`;
        html += `<div class="splits-list">`;
        if (splits && splits.length) {
            for (const sp of splits) {
                const zone = sp.pace_zone || "";
                const zoneClass = zone ? `zone-${zone}` : "";
                html += `<div class="split-row" data-split-id="${sp.id}">`;
                html += `<span>${formatSplitDistance(sp.canonical_distance_mi || sp.gps_measured_distance_mi || sp.prescribed_distance_mi)}</span>`;
                html += `<span>${formatSplitDuration(sp.duration_s)}</span>`;
                html += `<span>${sp.display_pace || ""}</span>`;
                html += `<span class="split-zone ${zoneClass}">${zone}</span>`;
                html += `<button class="split-delete" data-split-id="${sp.id}" data-aid="${aid}">&times;</button>`;
                html += `</div>`;
            }
        } else {
            html += `<div style="font-size:11px;color:#999">No splits yet</div>`;
        }
        html += `</div>`;

        // Add row
        html += `<div class="splits-add-row">`;
        html += `<input type="number" step="0.01" class="split-dist-input" placeholder="mi" style="width:60px">`;
        html += `<input type="text" class="split-time-input" placeholder="m:ss" style="width:60px">`;
        html += `<select class="split-zone-select" style="width:50px">`;
        html += `<option value="">--</option>`;
        for (const z of ["E", "M", "T", "CV", "I", "R", "FR"]) {
            html += `<option value="${z}">${z}</option>`;
        }
        html += `</select>`;
        html += `<button class="btn-add-split" data-aid="${aid}">+ Add</button>`;
        html += `</div>`;
        html += `</div>`;
        return html;
    }

    function wireSplitsSection(container, aid) {
        const section = container.querySelector(`[data-splits-aid="${aid}"]`);
        if (!section) return;

        // Wire delete buttons
        section.querySelectorAll(".split-delete").forEach(btn => {
            btn.addEventListener("click", (e) => {
                e.stopPropagation();
                const splitId = btn.dataset.splitId;
                const splitAid = btn.dataset.aid;
                fetch(`/api/activity/${splitAid}/splits/${splitId}`, { method: "DELETE" })
                    .then(r => r.json())
                    .then(d => {
                        if (d.ok) {
                            refreshSplitsSection(container, parseInt(splitAid));
                        }
                    });
            });
        });

        // Wire add button
        const addBtn = section.querySelector(".btn-add-split");
        if (addBtn) {
            addBtn.addEventListener("click", (e) => {
                e.stopPropagation();
                const distInput = section.querySelector(".split-dist-input");
                const timeInput = section.querySelector(".split-time-input");
                const zoneSelect = section.querySelector(".split-zone-select");

                const distMi = parseFloat(distInput.value);
                const durS = parseDuration(timeInput.value.trim());
                const zone = zoneSelect.value;

                if (!distMi || distMi <= 0 || !durS || durS <= 0) return;

                addBtn.disabled = true;
                fetch(`/api/activity/${aid}/splits`, {
                    method: "POST",
                    headers: { "Content-Type": "application/json" },
                    body: JSON.stringify({
                        distance_mi: distMi,
                        duration_s: durS,
                        pace_zone: zone || null,
                    }),
                }).then(r => r.json()).then(d => {
                    addBtn.disabled = false;
                    if (d.ok) {
                        distInput.value = "";
                        timeInput.value = "";
                        zoneSelect.value = "";
                        refreshSplitsSection(container, aid);
                    }
                }).catch(() => { addBtn.disabled = false; });
            });

            // Enter key to submit
            const inputs = section.querySelectorAll(".split-dist-input, .split-time-input, .split-zone-select");
            inputs.forEach(inp => {
                inp.addEventListener("keydown", (ev) => {
                    ev.stopPropagation();
                    if (ev.key === "Enter") addBtn.click();
                });
                inp.addEventListener("click", (ev) => ev.stopPropagation());
            });
        }
    }

    function refreshSplitsSection(container, aid) {
        // Re-fetch intervals and rebuild the splits section
        fetch(`/api/activity/${aid}/intervals`).then(r => r.json()).then(data => {
            const allIntervals = (data.intervals || []).concat(data.laps || []);
            const splits = allIntervals.filter(iv => iv.source === "manual_split");

            const section = container.querySelector(`[data-splits-aid="${aid}"]`);
            if (!section) return;

            // Rebuild inline
            const newSection = document.createElement("div");
            newSection.innerHTML = buildSplitsSection(aid, splits);
            const newContent = newSection.firstElementChild;
            section.replaceWith(newContent);

            wireSplitsSection(container, aid);

            // Also update the intervals table if visible
            refreshIntervalsTable(container, aid, data);
        });
    }

    function refreshIntervalsTable(container, aid, data) {
        const selector = `[data-table-aid="${aid}"]`;
        const table = container.matches(selector) ? container : container.querySelector(selector);
        if (!table) return;
        table.innerHTML = buildIntervalTables(data);
        wireWalkingButtons(table);
        wireIntervalCheckboxes(table);
        wireIntervalEditing(table);
    }

    function buildEditForm(meta) {
        const aid = meta.id;
        const ov = meta.overridden_fields || [];
        let shoeOptions = '<option value="">--</option>';
        if (window.SHOES) {
            for (const [shoeId, name] of Object.entries(window.SHOES)) {
                const sel = meta.shoe_id && parseInt(shoeId) === meta.shoe_id ? " selected" : "";
                shoeOptions += `<option value="${shoeId}"${sel}>${escapeHtml(name)}</option>`;
            }
        }

        const zones = ZONE_OPTIONS;
        let zoneOptions = "";
        for (const z of zones) {
            const sel = meta.workout_type_zone === z ? " selected" : "";
            zoneOptions += `<option value="${z}"${sel}>${z || "--"}</option>`;
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
            <div class="edit-row" data-field-row="workout_type_zone">
                <label>Type</label>
                <select data-field="workout_type_zone">${zoneOptions}</select>
                ${resetBtn("workout_type_zone")}
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
            <div class="edit-row">
                <label>GAP</label>
                <span style="padding:3px 6px;font-size:12px">${meta.display_gap || "\u2014"}</span>
            </div>
            <div class="edit-row">
                <label>Elevation</label>
                <span style="padding:3px 6px;font-size:12px">${meta.total_ascent_ft != null || meta.total_descent_ft != null ? `\u25B2${Math.round(meta.total_ascent_ft || 0)}ft \u25BC${Math.round(meta.total_descent_ft || 0)}ft` : "\u2014"}</span>
            </div>
            <div class="edit-row">
                <label>Weather</label>
                <span style="padding:3px 6px;font-size:12px">${formatWeather(meta)}</span>
            </div>
            <div class="edit-row">
                <label>Time</label>
                <span style="padding:3px 6px;font-size:12px">${formatActivityStartTime(meta.start_time)}</span>
            </div>
            <div class="edit-row" data-field-row="avg_hr">
                <label>Avg HR</label>
                <input type="number" step="0.1" data-field="avg_hr" value="${meta.avg_hr != null ? parseFloat(meta.avg_hr).toFixed(1) : ""}" placeholder="${meta.avg_hr != null ? parseFloat(meta.avg_hr).toFixed(1) : ""}">
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
            <div class="edit-row" data-field-row="notes" style="align-items:start">
                <label style="margin-top:4px">Notes</label>
                <div class="notes-wrap" style="flex:1;display:flex;flex-direction:column">
                    <textarea data-field="notes" rows="2" placeholder="${escapeHtml(meta.notes)}" style="resize:none;${localStorage.getItem('notesHeight') ? 'height:' + localStorage.getItem('notesHeight') + 'px' : ''}">${escapeHtml(meta.notes)}</textarea>
                    <div class="notes-resize-handle"></div>
                </div>
                ${resetBtn("notes")}
            </div>
            <div class="edit-row data-quality-row">
                <label>Data</label>
                <span style="display:flex;gap:12px;align-items:center;font-size:12px">
                    <label style="width:auto;display:flex;align-items:center;gap:3px"><input type="checkbox" class="flag-cb" data-flag="suppress_hr" data-aid="${aid}" ${meta.suppress_hr ? "checked" : ""}> Suppress HR</label>
                    <label style="width:auto;display:flex;align-items:center;gap:3px"><input type="checkbox" class="flag-cb" data-flag="suppress_cadence" data-aid="${aid}" ${meta.suppress_cadence ? "checked" : ""}> Suppress Cadence</label>
                    <label title="${meta.vdot_exclusion_source === 'strides' ? 'Automatically excluded: strides make up at least 40% of the activity distance. Uncheck to include this activity.' : ''}" style="width:auto;display:flex;align-items:center;gap:3px"><input type="checkbox" class="flag-cb" data-flag="exclude_from_vdot" data-aid="${aid}" ${meta.exclude_from_vdot ? "checked" : ""}> Exclude from VDOT</label>
                </span>
            </div>
            <div class="edit-row" data-field-row="splits">
                <label>Splits</label>
                <button class="btn-add-splits" data-aid="${aid}">+ Add Splits</button>
            </div>
            <div class="splits-placeholder" data-splits-placeholder-aid="${aid}" style="display:none"></div>
            <div class="edit-row edit-row-delete">
                <button class="btn-delete-activity" data-aid="${aid}">Delete Activity</button>
            </div>
        </div>`;
    }

    function wireEditForm(container, meta) {
        const aid = meta.id;
        const form = container.querySelector(`[data-edit-aid="${aid}"]`);
        if (!form) return;

        // Drag-to-resize notes textarea
        const notesTA = form.querySelector('textarea[data-field="notes"]');
        const resizeHandle = form.querySelector('.notes-resize-handle');
        if (notesTA && resizeHandle) {
            resizeHandle.addEventListener('mousedown', (e) => {
                e.preventDefault();
                e.stopPropagation();
                const startY = e.clientY;
                const startH = notesTA.getBoundingClientRect().height;
                resizeHandle.classList.add('dragging');
                document.body.style.userSelect = 'none';
                document.body.style.cursor = 'ns-resize';
                const onMove = (ev) => {
                    ev.preventDefault();
                    const newH = Math.max(36, startH + (ev.clientY - startY));
                    notesTA.style.height = newH + 'px';
                };
                const onUp = () => {
                    resizeHandle.classList.remove('dragging');
                    document.body.style.userSelect = '';
                    document.body.style.cursor = '';
                    localStorage.setItem('notesHeight', Math.round(notesTA.getBoundingClientRect().height));
                    document.removeEventListener('mousemove', onMove);
                    document.removeEventListener('mouseup', onUp);
                };
                document.addEventListener('mousemove', onMove);
                document.addEventListener('mouseup', onUp);
            });
        }

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

        // Save quality flags in the same sequence as the rest of the editor.
        wireActivityFlags(form);

        // "Add Splits" button — reveals the splits section
        const splitsBtn = form.querySelector(".btn-add-splits");
        if (splitsBtn) {
            splitsBtn.addEventListener("click", (e) => {
                e.stopPropagation();
                const btnAid = parseInt(splitsBtn.dataset.aid);
                const placeholder = container.querySelector(`[data-splits-placeholder-aid="${btnAid}"]`);
                if (placeholder && placeholder.style.display === "none") {
                    placeholder.style.display = "";
                    placeholder.innerHTML = buildSplitsSection(btnAid, []);
                    wireSplitsSection(container, btnAid);
                    splitsBtn.style.display = "none";
                }
            });
        }

        // Delete activity button
        const deleteBtn = form.querySelector(".btn-delete-activity");
        if (deleteBtn) {
            deleteBtn.addEventListener("click", (e) => {
                e.stopPropagation();
                if (!confirm(`Delete activity #${aid}? This cannot be undone.`)) return;
                fetch(`/api/activity/${aid}`, { method: "DELETE" })
                    .then(r => r.json())
                    .then(d => {
                        if (!d.ok) return;
                        // Remove from modalActivityIds
                        modalActivityIds = modalActivityIds.filter(id => id !== aid);

                        if (modalActivityIds.length === 0) {
                            // Last activity deleted — close modal and reload
                            closeModal();
                            sessionStorage.setItem("runbase_scroll_to", modalDate);
                            location.reload();
                            return;
                        }

                        // Multi-activity: remove this activity's section from DOM
                        const section = form.closest(".activity-section");
                        if (section) {
                            // Remove adjacent divider (before or after)
                            const prev = section.previousElementSibling;
                            const next = section.nextElementSibling;
                            if (prev && prev.classList.contains("activity-divider")) {
                                prev.remove();
                            } else if (next && next.classList.contains("activity-divider")) {
                                next.remove();
                            }
                            section.remove();
                        }

                        // Clean up dirty/vdot state for deleted activity
                        delete modalDirtyState[aid];
                        delete modalOriginalValues[aid];
                        delete modalClearQueue[aid];
                        delete modalVdotState[aid];
                        delete modalFlagState[aid];
                        updateModalButtons();
                    })
                    .catch(err => {
                        console.error("Delete failed:", err);
                        alert("Failed to delete activity. Check console for details.");
                    });
            });
        }

        form.addEventListener("click", e => e.stopPropagation());
    }

    function syncVdotExclusion(aid, excluded, source) {
        if (excluded === undefined || "exclude_from_vdot" in (modalFlagState[aid] || {})) return;
        const checkbox = modalEl.querySelector(`.flag-cb[data-aid="${aid}"][data-flag="exclude_from_vdot"]`);
        if (!checkbox) return;
        checkbox.checked = !!excluded;
        checkbox.dataset.savedChecked = String(checkbox.checked);
        checkbox.closest("label").title = source === "strides"
            ? "Automatically excluded: strides make up at least 40% of the activity distance. Uncheck to include this activity."
            : "";
    }

    function wireActivityFlags(form) {
        form.querySelectorAll(".flag-cb").forEach(cb => {
            cb.dataset.savedChecked = String(cb.checked);
            const pending = modalFlagState[cb.dataset.aid] || {};
            if (cb.dataset.flag in pending) cb.checked = pending[cb.dataset.flag];
            cb.addEventListener("change", () => {
                const {flag, aid} = cb.dataset;
                if (!modalFlagState[aid]) modalFlagState[aid] = {};
                if (String(cb.checked) === cb.dataset.savedChecked) {
                    delete modalFlagState[aid][flag];
                } else {
                    modalFlagState[aid][flag] = cb.checked;
                }
                updateModalButtons();
            });
        });
    }

    function showModalSaveError(message) {
        const element = document.getElementById("modal-save-error");
        element.textContent = message;
        element.hidden = !message;
    }

    async function postModalChange(url, body) {
        const response = await fetch(url, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(body),
        });
        const data = await response.json();
        const failed = (data.results || []).find(result => !result.ok);
        if (!response.ok || !data.ok || failed) {
            throw new Error(data.error || (failed && failed.error) || "Could not save changes. Please try again.");
        }
        return data;
    }

    function updateModalButtons() {
        // Save and Save & Close are always visible; disabled when nothing to save
        const hasDirty = isDirty();
        modalBtnSave.disabled = modalSaving || !hasDirty;
        // Save & Close also enabled when enrichment needed (stride/recovery/walking toggled)
        modalBtnSaveClose.disabled = modalSaving || !(hasDirty || modalNeedsEnrich || modalNeedsRefresh);
    }

    // ── Batch save ──────────────────────────────────────────────

    async function saveModalChanges(andClose) {
        if (modalSaving) return;
        modalSaving = true;
        showModalSaveError("");
        const controls = [...modalEl.querySelectorAll("input, select, textarea, button")];
        const disabledBefore = controls.map(control => control.disabled);
        controls.forEach(control => { control.disabled = true; });
        updateModalButtons();
        modalNeedsEnrich = modalNeedsEnrich || hasFieldChanges()
            || Object.values(modalFlagState).some(flags => "suppress_hr" in flags);
        modalNeedsRefresh = modalNeedsRefresh || isDirty() || modalNeedsEnrich;

        try {
            // Finish flag writes before recalculating or reloading CVD/AVD.
            for (const [aid, flags] of Object.entries(modalFlagState)) {
                if (Object.keys(flags).length) {
                    await postModalChange(`/api/activity/${aid}/flags`, flags);
                }
            }

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
                        if (parsed === null) throw new Error(`Invalid ${field.replaceAll("_", " ")}. Please correct it and save again.`);
                        value = parsed;
                    }
                    if (!await saveOverride(aid, field, value)) {
                        throw new Error(`Could not save ${field.replaceAll("_", " ")}. Please try again.`);
                    }

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
                    if (!await clearOverride(aid, field)) {
                        throw new Error(`Could not clear ${field.replaceAll("_", " ")}. Please try again.`);
                    }
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

                await postModalChange(`/api/activity/${aid}/vdot`, payload);
            }

            if (andClose && modalNeedsEnrich && modalActivityIds.length) {
                await postModalChange("/api/enrich", { activity_ids: modalActivityIds });
                modalNeedsEnrich = false;
            }

            // Reset dirty state
            modalDirtyState = {};
            modalClearQueue = {};
            modalVdotState = {};
            modalFlagState = {};
            modalEl.querySelectorAll(".flag-cb").forEach(cb => {
                cb.dataset.savedChecked = String(cb.checked);
            });
            // Remove dirty classes
            document.querySelectorAll("#modal-edit-section .edit-row.dirty").forEach(el => {
                el.classList.remove("dirty");
            });
            // Also check inside modal-intervals-container for inline edit forms
            document.querySelectorAll("#modal-intervals-container .edit-row.dirty").forEach(el => {
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
                closeModal();
                sessionStorage.setItem("runbase_scroll_to", modalDate);
                location.reload();
            }
        } catch (err) {
            console.error("Save error:", err);
            showModalSaveError(err.message || "Could not save changes. Please try again.");
        } finally {
            modalSaving = false;
            controls.forEach((control, index) => { control.disabled = disabledBefore[index]; });
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
            workout_name: "name",
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
        } else if (field === "workout_type_zone") {
            cell.textContent = value || "";
            cell.className = "col-type" + (value ? ` ${zoneClass(value)}` : "");
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
    const importDetails = document.getElementById("import-details");
    const importOutput = document.getElementById("import-output");
    let importPoll = null;
    let observedImportRunning = false;

    async function refreshSystemStatus() {
        const summary = document.getElementById("system-summary");
        const output = document.getElementById("system-output");
        try {
            const response = await fetch("/api/system/status");
            if (!response.ok) throw new Error("Could not check system status");
            const report = await response.json();
            const issues = report.checks.filter(c => c.status === "warning" || c.status === "error");
            summary.textContent = issues.length ? `System status (${issues.length} issues)` : "System status";
            output.textContent = report.checks.map(c => `${c.name} — ${c.status.toUpperCase()}\n${c.message}`).join("\n\n");
        } catch (error) {
            summary.textContent = "System status unavailable";
            output.textContent = error.message;
        }
    }
    if (document.getElementById("system-details")) {
        refreshSystemStatus();
        document.getElementById("system-details").addEventListener("toggle", event => {
            if (event.target.open) refreshSystemStatus();
        });
    }

    function renderImportStatus(s) {
        importBtn.disabled = s.running;
        importBtn.textContent = s.running ? "Running..." : "Import";
        const elapsed = `${Math.floor(s.elapsed_s / 60)}:${String(s.elapsed_s % 60).padStart(2, "0")}`;
        const warnings = s.warnings || [];
        const message = s.running
            ? `${s.message || "Starting import..."} (${elapsed})`
            : warnings.length && s.success
                ? `${s.message} ${warnings.join(" ")}`
                : s.success === false ? `Import stopped: ${s.message}` : s.message || "";
        importStatus.textContent = message;
        importStatus.title = message;
        importStatus.className = "import-status" + (s.running ? ""
            : s.success === false ? " error" : warnings.length ? " warning"
            : s.success ? " success" : "");
        importOutput.textContent = s.output || s.message || "";
        importDetails.hidden = !importOutput.textContent;
        if (!s.running && (s.success === false || warnings.length)) importDetails.open = true;
    }

    async function pollImportStatus() {
        try {
            const response = await fetch("/api/import/status");
            if (!response.ok) throw new Error("Could not read import status");
            const s = await response.json();
            renderImportStatus(s);
            if (s.running) {
                observedImportRunning = true;
                importPoll = setTimeout(pollImportStatus, 1000);
            } else {
                importPoll = null;
                if (observedImportRunning && s.success !== null) {
                    observedImportRunning = false;
                    // A retry may enrich existing rows without importing new
                    // ones. Reload those results too; status remains on server.
                    setTimeout(() => location.reload(), 500);
                }
            }
        } catch (error) {
            importStatus.textContent = "Cannot reach the server; retrying import status...";
            importStatus.className = "import-status error";
            importPoll = setTimeout(pollImportStatus, 3000);
        }
    }

    if (importBtn) {
        // Reconnect to a running import after a page refresh or year change.
        pollImportStatus();
        importBtn.addEventListener("click", async () => {
            clearTimeout(importPoll);
            observedImportRunning = true;
            importBtn.disabled = true;
            importBtn.textContent = "Running...";
            importStatus.textContent = "Starting import...";
            importStatus.className = "import-status";
            importDetails.open = true;
            try {
                const response = await fetch("/api/import", { method: "POST" });
                const result = await response.json();
                if (!result.ok && response.status !== 409) throw new Error(result.error || "Import could not start");
                pollImportStatus();
            } catch (error) {
                importStatus.textContent = `${error.message}. Checking server status...`;
                importStatus.className = "import-status error";
                importPoll = setTimeout(pollImportStatus, 1000);
            }
        });
    }

    // ── Refresh 7d MA after planned activity change ────────────

    function refreshSevenDayMA(changedDate) {
        fetch(`/api/training_load?from=${changedDate}`)
            .then(r => r.json())
            .then(data => {
                for (const [dateStr, vals] of Object.entries(data)) {
                    const rows = mainContent.querySelectorAll(`[data-date="${dateStr}"]`);
                    for (const row of rows) {
                        const c7d = row.querySelector(".col-7d");
                        if (c7d) c7d.textContent = vals.seven_d.toFixed(1);
                        const c7di = row.querySelector(".col-7di");
                        if (c7di) c7di.textContent = vals.seven_di.toFixed(1);
                        const catl = row.querySelector(".col-atl");
                        if (catl) catl.textContent = vals.atl.toFixed(1);
                        const cctl = row.querySelector(".col-ctl");
                        if (cctl) cctl.textContent = vals.ctl.toFixed(1);
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

    // ═══════════════════════════════════════════════════════════════
    // ── Tab Navigation ─────────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    const tabButtons = document.querySelectorAll(".tab-btn");
    const tabContents = document.querySelectorAll(".tab-content");
    const tabLoaded = { calendar: true, planning: false, shoes: false, trends: false, "race-prediction": false };

    function switchTab(tabName) {
        tabButtons.forEach(btn => btn.classList.toggle("active", btn.dataset.tab === tabName));
        tabContents.forEach(div => div.classList.toggle("active", div.id === `tab-${tabName}`));
        sessionStorage.setItem("runbase_active_tab", tabName);

        if (tabName === "planning" && !tabLoaded.planning) {
            tabLoaded.planning = true;
            loadPlanningTab();
        }
        if (tabName === "shoes" && !tabLoaded.shoes) {
            tabLoaded.shoes = true;
            loadShoesTab();
        }
        if (tabName === "trends" && !tabLoaded.trends) {
            tabLoaded.trends = true;
            loadTrendsTab("1y");
        }
        if (tabName === "race-prediction" && !tabLoaded["race-prediction"]) {
            tabLoaded["race-prediction"] = true;
            loadRacePredictionTab();
        }
    }

    tabButtons.forEach(btn => {
        btn.addEventListener("click", () => switchTab(btn.dataset.tab));
    });

    // Restore tab state
    const savedTab = sessionStorage.getItem("runbase_active_tab");
    if (savedTab && savedTab !== "calendar") {
        switchTab(savedTab);
    }

    // ═══════════════════════════════════════════════════════════════
    // ── Shared Chart Helpers ───────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    const SHOE_COLORS = [
        "#4a7ab5", "#c05050", "#50a050", "#d9a030", "#9060b0",
        "#50b0b0", "#d06080", "#808040", "#b07040", "#6080c0",
        "#40b070", "#c07090",
    ];

    function formatPaceLabel(v) {
        const seconds = Math.round(v);
        const m = Math.floor(seconds / 60);
        const s = seconds % 60;
        return `${m}:${s < 10 ? "0" : ""}${s}`;
    }

    function initCanvas(canvas) {
        const ctx = canvas.getContext("2d");
        const dpr = window.devicePixelRatio || 1;
        const rect = canvas.getBoundingClientRect();
        canvas.width = rect.width * dpr;
        canvas.height = rect.height * dpr;
        ctx.scale(dpr, dpr);
        return { ctx, w: rect.width, h: rect.height, dpr };
    }

    function dateXPositioner(dates, pad, cw) {
        const first = dates[0], last = dates[dates.length - 1];
        const range = last - first || 1;
        return d => pad.left + ((d - first) / range) * cw;
    }

    function drawDateXLabels(ctx, dates, xPos, h, pad) {
        ctx.fillStyle = "#aaa";
        ctx.font = "9px -apple-system, sans-serif";
        ctx.textAlign = "center";
        // Show ~6 labels evenly spaced
        const count = Math.min(dates.length, 6);
        const step = Math.max(1, Math.floor(dates.length / count));
        const shown = new Set();
        for (let i = 0; i < dates.length; i += step) {
            const d = new Date(dates[i]);
            const label = `${d.getMonth() + 1}/${d.getDate()}`;
            if (!shown.has(label)) {
                ctx.fillText(label, xPos(dates[i]), h - 4);
                shown.add(label);
            }
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // ── Shoes Tab ──────────────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    function loadShoesTab() {
        const page = document.querySelector(".shoes-page");
        page.innerHTML = '<p class="loading">Loading shoe data...</p>';

        return Promise.all([
            fetch("/api/shoes").then(r => r.json()),
            fetch("/api/shoes/monthly").then(r => r.json()),
        ]).then(([shoes, monthly]) => {
            let html = "";

            // ── Shoe Summary Table ──
            html += '<h3 style="margin-bottom:8px">Shoe Summary</h3>';
            html += `<table class="shoes-table"><thead><tr>
                <th></th><th>Shoe</th><th>Miles</th><th>Runs</th>
                <th title="Distance-weighted recorded running pace">Avg Pace</th>
                <th title="Duration-weighted CVD; excludes activities excluded from VDOT or with suppressed HR">Avg CVD</th>
                <th>First Use</th><th>Last Use</th>
            </tr></thead><tbody>`;

            // Sort: active first by last_use desc, then retired
            shoes.sort((a, b) => {
                if (a.retired !== b.retired) return a.retired ? 1 : -1;
                return (b.last_use || "").localeCompare(a.last_use || "");
            });

            for (const s of shoes) {
                if (!s.activity_count) continue;
                const statusClass = s.retired ? "retired" : "active";
                const pace = s.avg_pace ? formatPaceLabel(s.avg_pace) : "";
                html += `<tr>
                    <td><span class="shoe-status ${statusClass}"></span></td>
                    <td>${escapeHtml(s.name || "Unknown")}</td>
                    <td>${s.total_miles.toFixed(1)}</td>
                    <td>${s.activity_count}</td>
                    <td>${pace}</td>
                    <td title="${s.cvd_count || 0} activities included in CVD">${s.avg_cvd != null ? s.avg_cvd.toFixed(1) : "\u2014"}</td>
                    <td>${s.first_use || ""}</td>
                    <td>${s.last_use || ""}</td>
                </tr>`;
            }
            html += "</tbody></table>";

            // ── Charts ──
            html += '<div class="shoes-charts">';
            html += '<div class="shoes-chart-wrap"><h3>Cumulative Mileage</h3><canvas id="shoes-cumulative-chart" height="250"></canvas><div id="shoes-cumulative-legend" class="chart-legend"></div></div>';
            html += '<div class="shoes-chart-wrap"><h3>Monthly Shoe Mileage</h3><canvas id="shoes-monthly-chart" height="250"></canvas><div id="shoes-monthly-legend" class="chart-legend"></div></div>';
            html += '</div>';

            page.innerHTML = html;

            // Assign colors to shoes
            const activeShoes = shoes.filter(s => s.activity_count > 0);
            const shoeColorMap = {};
            activeShoes.forEach((s, i) => { shoeColorMap[s.id] = SHOE_COLORS[i % SHOE_COLORS.length]; });

            drawCumulativeMileageChart(shoes, shoeColorMap);
            drawMonthlyShoeChart(monthly, shoeColorMap, shoes);
        });
    }

    function drawCumulativeMileageChart(shoes, colorMap) {
        const canvas = document.getElementById("shoes-cumulative-chart");
        if (!canvas) return;
        const { ctx, w, h } = initCanvas(canvas);
        const pad = { top: 10, right: 10, bottom: 22, left: 40 };
        const cw = w - pad.left - pad.right;
        const ch = h - pad.top - pad.bottom;

        // Build cumulative data per shoe from /api/shoes/monthly
        // We need per-shoe monthly data — reuse the shoes summary for cumulative
        // Actually, fetch cumulative from monthly endpoint data already loaded
        // For cumulative, we need activity-level data. Use monthly as proxy.
        // Better: build from shoes summary — we have total_miles and first/last use
        // Simplest: just fetch monthly and accumulate

        fetch("/api/shoes/monthly").then(r => r.json()).then(monthly => {
            // Group by shoe
            const byShoe = {};
            for (const row of monthly) {
                if (!byShoe[row.shoe_id]) byShoe[row.shoe_id] = { name: row.shoe_name, data: [] };
                byShoe[row.shoe_id].data.push(row);
            }

            // Get all months sorted
            const allMonths = [...new Set(monthly.map(r => r.month))].sort();
            if (!allMonths.length) return;

            const allDates = allMonths.map(m => new Date(m + "-15").getTime());
            const xPos = dateXPositioner(allDates, pad, cw);

            // Build cumulative series
            let maxCum = 0;
            const series = [];
            for (const [shoeId, info] of Object.entries(byShoe)) {
                const monthMap = {};
                for (const d of info.data) monthMap[d.month] = d.miles;
                let cum = 0;
                const pts = [];
                for (const m of allMonths) {
                    cum += monthMap[m] || 0;
                    pts.push({ date: new Date(m + "-15").getTime(), value: cum });
                }
                if (cum > 0) {
                    series.push({ id: parseInt(shoeId), name: info.name, pts, total: cum });
                    if (cum > maxCum) maxCum = cum;
                }
            }

            if (!maxCum) return;
            const yPos = v => pad.top + (1 - v / (maxCum * 1.1)) * ch;

            // Y grid
            ctx.fillStyle = "#aaa";
            ctx.font = "9px -apple-system, sans-serif";
            ctx.textAlign = "right";
            for (let i = 0; i <= 4; i++) {
                const v = (maxCum * 1.1 * i) / 4;
                const y = yPos(v);
                ctx.beginPath();
                ctx.moveTo(pad.left, y);
                ctx.lineTo(w - pad.right, y);
                ctx.strokeStyle = "#eee";
                ctx.lineWidth = 0.5;
                ctx.stroke();
                ctx.fillText(Math.round(v).toString(), pad.left - 4, y + 3);
            }

            // Draw lines
            series.sort((a, b) => b.total - a.total);
            for (const s of series) {
                const color = colorMap[s.id] || "#999";
                ctx.beginPath();
                for (let i = 0; i < s.pts.length; i++) {
                    const x = xPos(s.pts[i].date);
                    const y = yPos(s.pts[i].value);
                    if (i === 0) ctx.moveTo(x, y);
                    else ctx.lineTo(x, y);
                }
                ctx.strokeStyle = color;
                ctx.lineWidth = 2;
                ctx.stroke();
            }

            // X labels
            drawDateXLabels(ctx, allDates, xPos, h, pad);

            // Legend
            const legend = document.getElementById("shoes-cumulative-legend");
            if (legend) {
                legend.innerHTML = series.map(s =>
                    `<span class="chart-legend-item"><span class="chart-legend-swatch" style="background:${colorMap[s.id] || '#999'}"></span>${escapeHtml(s.name)}</span>`
                ).join("");
            }
        });
    }

    function drawMonthlyShoeChart(monthly, colorMap, shoes) {
        const canvas = document.getElementById("shoes-monthly-chart");
        if (!canvas || !monthly.length) return;
        const { ctx, w, h } = initCanvas(canvas);
        const pad = { top: 10, right: 10, bottom: 22, left: 40 };
        const cw = w - pad.left - pad.right;
        const ch = h - pad.top - pad.bottom;

        // Group by month
        const allMonths = [...new Set(monthly.map(r => r.month))].sort();
        const byMonth = {};
        for (const m of allMonths) byMonth[m] = {};
        for (const r of monthly) {
            byMonth[r.month][r.shoe_id] = r.miles;
        }

        // Get shoe IDs that appear
        const shoeIds = [...new Set(monthly.map(r => r.shoe_id))];

        // Max stacked height
        let maxStack = 0;
        for (const m of allMonths) {
            let sum = 0;
            for (const sid of shoeIds) sum += byMonth[m][sid] || 0;
            if (sum > maxStack) maxStack = sum;
        }
        if (!maxStack) return;

        const barWidth = Math.max(4, Math.min(20, (cw / allMonths.length) * 0.7));
        const gap = (cw - barWidth * allMonths.length) / (allMonths.length + 1);
        const yPos = v => pad.top + (1 - v / (maxStack * 1.1)) * ch;

        // Y grid
        ctx.fillStyle = "#aaa";
        ctx.font = "9px -apple-system, sans-serif";
        ctx.textAlign = "right";
        for (let i = 0; i <= 4; i++) {
            const v = (maxStack * 1.1 * i) / 4;
            const y = yPos(v);
            ctx.beginPath();
            ctx.moveTo(pad.left, y);
            ctx.lineTo(w - pad.right, y);
            ctx.strokeStyle = "#eee";
            ctx.lineWidth = 0.5;
            ctx.stroke();
            ctx.fillText(Math.round(v).toString(), pad.left - 4, y + 3);
        }

        // Draw stacked bars
        for (let mi = 0; mi < allMonths.length; mi++) {
            const m = allMonths[mi];
            const x = pad.left + gap + mi * (barWidth + gap);
            let stackY = pad.top + ch; // bottom

            for (const sid of shoeIds) {
                const miles = byMonth[m][sid] || 0;
                if (miles <= 0) continue;
                const barH = (miles / (maxStack * 1.1)) * ch;
                const color = colorMap[sid] || "#999";
                ctx.fillStyle = color;
                ctx.fillRect(x, stackY - barH, barWidth, barH);
                stackY -= barH;
            }
        }

        // X labels
        ctx.fillStyle = "#aaa";
        ctx.font = "8px -apple-system, sans-serif";
        ctx.textAlign = "center";
        const labelStep = Math.max(1, Math.floor(allMonths.length / 8));
        for (let i = 0; i < allMonths.length; i += labelStep) {
            const x = pad.left + gap + i * (barWidth + gap) + barWidth / 2;
            const parts = allMonths[i].split("-");
            ctx.fillText(`${parseInt(parts[1])}/${parts[0].slice(2)}`, x, h - 4);
        }

        // Legend
        const legend = document.getElementById("shoes-monthly-legend");
        if (legend) {
            const shoeMap = {};
            shoes.forEach(s => { shoeMap[s.id] = s.name; });
            legend.innerHTML = shoeIds.map(sid =>
                `<span class="chart-legend-item"><span class="chart-legend-swatch" style="background:${colorMap[sid] || '#999'}"></span>${escapeHtml(shoeMap[sid] || "?")}</span>`
            ).join("");
        }
    }

    // ═══════════════════════════════════════════════════════════════
    // ── Trends Tab ─────────────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    function loadTrendsTab(range) {
        const page = document.querySelector(".trends-page");

        // Build range selector if not present
        if (!page.querySelector(".trends-range-selector")) {
            let html = '<div class="trends-range-selector">';
            for (const r of ["6m", "1y", "2y", "all"]) {
                html += `<button class="trends-range-btn ${r === range ? "active" : ""}" data-range="${r}">${r.toUpperCase()}</button>`;
            }
            html += '</div>';
            html += '<div id="trends-charts-container"><p class="loading">Loading trends...</p></div>';
            page.innerHTML = html;

            // Wire range buttons
            page.querySelectorAll(".trends-range-btn").forEach(btn => {
                btn.addEventListener("click", () => {
                    page.querySelectorAll(".trends-range-btn").forEach(b => b.classList.remove("active"));
                    btn.classList.add("active");
                    loadTrendsData(btn.dataset.range);
                });
            });
        }

        loadTrendsData(range);
    }

    function loadTrendsData(range) {
        const container = document.getElementById("trends-charts-container");
        if (!container) return;
        const requestId = String(Number(container.dataset.requestId || 0) + 1);
        container.dataset.requestId = requestId;
        container.innerHTML = '<p class="loading">Loading trends...</p>';

        return fetch(`/api/trends?range=${range}`).then(r => {
            if (!r.ok) throw new Error("Could not load trends");
            return r.json();
        }).then(data => {
            if (container.dataset.requestId !== requestId) return;
            let html = "";

            // Row 1: Weekly Volume + Pace Trend
            html += '<div class="trends-row">';
            html += '<div class="trends-chart-wrap"><h3>Weekly Volume</h3><canvas id="trends-volume-chart" height="200"></canvas></div>';
            html += '<div class="trends-chart-wrap"><h3 title="Weekly distance-weighted recorded running pace; activities without pace or running distance are omitted">Pace Trend</h3><canvas id="trends-pace-chart" height="200"></canvas></div>';
            html += '</div>';

            // Row 2: HR Trend + ATL/CTL/TSB
            html += '<div class="trends-row">';
            html += '<div class="trends-chart-wrap"><h3>Heart Rate Trend</h3><canvas id="trends-hr-chart" height="200"></canvas></div>';
            html += '<div class="trends-chart-wrap"><h3>ATL / CTL / TSB</h3><canvas id="trends-fitness-chart" height="200"></canvas></div>';
            html += '</div>';

            // Row 3: Race table + VDOT
            html += '<div class="trends-row">';
            html += '<div class="trends-chart-wrap">';
            if (data.races && data.races.length) {
                html += '<h3>Race Performances</h3>';
                html += `<table class="trends-race-table"><thead><tr>
                    <th>Date</th><th>Race</th><th>Distance</th><th>Time</th><th>Pace</th><th>VDOT</th>
                </tr></thead><tbody>`;
                for (const r of data.races) {
                    const dist = r.distance_mi < 1 ? `${(r.distance_mi * 1609.344).toFixed(0)}m` : `${r.distance_mi.toFixed(2)}mi`;
                    const dur = r.duration_s ? formatDurationInput(r.duration_s) : "";
                    const pace = r.avg_pace_s_per_mi ? formatPaceLabel(r.avg_pace_s_per_mi) : "";
                    html += `<tr><td>${r.date}</td><td>${escapeHtml(r.workout_name || "")}</td><td>${dist}</td><td>${dur}</td><td>${pace}</td><td>${r.vdot ? r.vdot.toFixed(1) : ""}</td></tr>`;
                }
                html += "</tbody></table>";
            } else {
                html += '<h3>Race Performances</h3><p style="color:#999;font-size:12px">No races found</p>';
            }
            html += '</div>';
            html += '<div class="trends-chart-wrap"><h3>VDOT Progression</h3><canvas id="trends-vdot-chart" height="200"></canvas></div>';
            html += '</div>';

            // Row 4: Predicted Race VDOT
            html += '<div class="trends-row">';
            html += '<div class="trends-chart-wrap"><h3>Current Race VDOT</h3><canvas id="trends-predicted-vdot-chart" height="200"></canvas></div>';
            html += '</div>';

            container.innerHTML = html;

            // Every chart uses the same dates and month ticks, including sparse race data.
            const widths = [...container.querySelectorAll("canvas")].map(c => c.getBoundingClientRect().width);
            const axis = trendsDateAxis(data, Math.min(...widths));
            const crosshair = createTrendsCrosshairGroup();

            // Draw charts
            drawTrendsBarChart("trends-volume-chart", data.weekly || [], { axis, crosshair, barColor: "#4a7ab5", maWindow: 6 });
            drawTrendsLineChart("trends-pace-chart", [{ label: "Pace", data: (data.weekly || []).filter(w => w.avg_pace).map(w => ({ date: w.week, value: w.avg_pace })), color: "#4a7ab5", fill: true }], { axis, crosshair, weekly: true, invertY: true, formatY: formatPaceLabel, formatValue: v => `${formatPaceLabel(v)}/mi` });
            drawTrendsLineChart("trends-hr-chart", [{ label: "HR", data: (data.weekly || []).filter(w => w.avg_hr).map(w => ({ date: w.week, value: w.avg_hr })), color: "#c05050", fill: true }], { axis, crosshair, weekly: true, formatY: v => Math.round(v).toString(), formatValue: v => `${v.toFixed(1)} bpm` });

            // ATL/CTL/TSB
            const atlCtl = data.atl_ctl || [];
            drawTrendsLineChart("trends-fitness-chart", [
                { label: "CTL", data: atlCtl.map(d => ({ date: d.date, value: d.ctl })), color: "#4a7ab5" },
                { label: "ATL", data: atlCtl.map(d => ({ date: d.date, value: d.atl })), color: "#d9a030" },
                { label: "TSB", data: atlCtl.map(d => ({ date: d.date, value: d.tsb })), color: "#50a050" },
            ], { axis, crosshair, formatY: v => v.toFixed(0), zeroLine: true, legend: true });

            // VDOT scatter
            const vdotPts = (data.vdot_timeline || []).map(v => ({ date: v.effective_date, value: v.vdot, label: v.notes || "" }));
            drawScatterChart("trends-vdot-chart", vdotPts, axis, crosshair);

            // Predicted Race VDOT line + race actuals as scatter overlay
            const predVdot = (data.predicted_vdot || []);
            const raceScatter = (data.vdot_timeline || [])
                .filter(v => v.source === "race")
                .map(v => ({ date: v.effective_date, value: v.vdot }));
            drawPredictedVdotChart("trends-predicted-vdot-chart", predVdot, raceScatter, axis, crosshair);
        }).catch(() => {
            if (container.dataset.requestId === requestId) {
                container.innerHTML = '<p class="loading">Could not load trends. Select a range to try again.</p>';
            }
        });
    }

    function trendsDateTime(date) {
        // Date-only records are calendar dates, independent of timezone and DST.
        return Date.parse(`${date}T00:00:00Z`);
    }

    function trendsDateAxis(data, width) {
        const dates = [data.date_range?.start, data.date_range?.end,
            ...(data.weekly || []).map(d => d.week),
            ...(data.atl_ctl || []).map(d => d.date),
            ...(data.vdot_timeline || []).map(d => d.effective_date),
            ...(data.predicted_vdot || []).map(d => d.date),
        ].filter(Boolean).map(trendsDateTime).filter(Number.isFinite);
        const end = dates.length ? Math.max(...dates) : trendsDateTime(new Date().toISOString().slice(0, 10));
        const start = dates.length ? Math.min(...dates) : end - 365 * 86400000;
        const axis = { start, end: Math.max(end, start + 86400000), ticks: [] };
        const first = new Date(axis.start);
        const last = new Date(axis.end);
        const firstMonth = first.getUTCFullYear() * 12 + first.getUTCMonth();
        const lastMonth = last.getUTCFullYear() * 12 + last.getUTCMonth();
        // Use the narrowest chart's label density for every chart, even the wide one.
        const slots = Math.max(2, Math.floor(((width || 500) - 64) / 48));
        const needed = Math.ceil((lastMonth - firstMonth + 1) / slots);
        const step = [1, 2, 3, 6, 12, 24, 36, 60, 120].find(n => n >= needed) || needed;
        for (let month = Math.ceil(firstMonth / step) * step; month <= lastMonth; month += step) {
            const time = Date.UTC(Math.floor(month / 12), month % 12, 1);
            if (time < axis.start || time > axis.end) continue;
            const date = new Date(time);
            axis.ticks.push({ time, label: `${String(date.getUTCMonth() + 1).padStart(2, "0")}/${String(date.getUTCFullYear()).slice(-2)}` });
        }
        return axis;
    }

    function initTrendsChart(canvas, axis) {
        const chart = initCanvas(canvas);
        const { ctx, w, h } = chart;
        const pad = { top: 12, right: 12, bottom: 30, left: 52 };
        const cw = w - pad.left - pad.right;
        const ch = h - pad.top - pad.bottom;
        const xPos = time => pad.left + (time - axis.start) / (axis.end - axis.start) * cw;
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = "center";
        for (const tick of axis.ticks) {
            const x = xPos(tick.time);
            ctx.beginPath();
            ctx.moveTo(x, pad.top);
            ctx.lineTo(x, h - pad.bottom + 4);
            ctx.strokeStyle = "#eee";
            ctx.lineWidth = 0.5;
            ctx.stroke();
            ctx.fillStyle = "#888";
            const half = ctx.measureText(tick.label).width / 2;
            ctx.fillText(tick.label, Math.max(half + 2, Math.min(w - half - 2, x)), h - 9);
        }
        return { ...chart, pad, cw, ch, xPos };
    }

    function drawTrendsYGrid(chart, yPos, min, max, formatY) {
        const { ctx, w, pad } = chart;
        ctx.fillStyle = "#888";
        ctx.font = "10px -apple-system, sans-serif";
        ctx.textAlign = "right";
        for (let i = 0; i <= 4; i++) {
            const value = min + (max - min) * i / 4;
            const y = yPos(value);
            ctx.beginPath();
            ctx.moveTo(pad.left, y);
            ctx.lineTo(w - pad.right, y);
            ctx.strokeStyle = "#eee";
            ctx.lineWidth = 0.5;
            ctx.stroke();
            ctx.fillText(formatY(value), pad.left - 5, y + 3);
        }
    }

    function createTrendsCrosshairGroup() {
        const charts = [];
        return {
            register(chart) { charts.push(chart); },
            select(point, source) {
                for (const chart of charts) chart.showDate(point?.date || null, chart === source ? point : null);
            },
        };
    }

    function wireTrendsCrosshair(canvas, chart, points, opts = {}) {
        const { ctx, w, h, dpr, pad, xPos } = chart;
        const cleanImage = ctx.getImageData(0, 0, canvas.width, canvas.height);
        const readout = document.createElement("div");
        readout.className = "trends-chart-readout";
        readout.setAttribute("role", "status");
        readout.setAttribute("aria-live", "polite");
        canvas.parentElement.querySelector(".trends-chart-readout")?.remove();
        canvas.parentElement.appendChild(readout);
        const hint = points.length ? "Click and drag to inspect" : "No data in this range";
        readout.textContent = hint;
        canvas.tabIndex = points.length ? 0 : -1;
        canvas.setAttribute("aria-label", `${canvas.parentElement.querySelector("h3")?.textContent || "Trend chart"}. ${hint}. Arrow keys move between dates; Escape clears the selection.`);
        const plotPoints = points.map(p => ({ ...p, time: trendsDateTime(p.date), x: xPos(trendsDateTime(p.date)), y: chart.yPos(p.value) }))
            .sort((a, b) => a.x - b.x);
        let activePointer = null;
        let selected = null;

        const formatDate = date => `${date.slice(5, 7)}/${date.slice(8, 10)}/${date.slice(2, 4)}`;

        function drawSelection(point, cursorDate) {
            selected = point;
            ctx.putImageData(cleanImage, 0, 0);
            if (!cursorDate) { readout.textContent = hint; return; }
            // Keep the date guide aligned even when a chart has only weekly/race points.
            const x = xPos(trendsDateTime(cursorDate));
            const date = formatDate(cursorDate);
            if (point) {
                const value = (opts.formatValue || (v => v.toFixed(1)))(point.value);
                readout.textContent = `${opts.weekly ? "Week of " : ""}${formatDate(point.date)} · ${point.series}: ${value}${point.date !== cursorDate ? " (nearest point)" : ""}${point.label ? ` · ${point.label}` : ""}`;
            } else {
                readout.textContent = `${date} · No data in this range`;
            }
            ctx.save();
            ctx.setTransform(dpr, 0, 0, dpr, 0, 0);
            ctx.beginPath();
            ctx.moveTo(x, pad.top);
            ctx.lineTo(x, h - pad.bottom);
            if (point) {
                ctx.moveTo(pad.left, point.y);
                ctx.lineTo(w - pad.right, point.y);
            }
            ctx.strokeStyle = "#556575";
            ctx.lineWidth = 1;
            ctx.setLineDash([4, 3]);
            ctx.stroke();
            ctx.setLineDash([]);
            if (point) {
                ctx.beginPath();
                ctx.arc(point.x, point.y, 4, 0, Math.PI * 2);
                ctx.fillStyle = point.color || "#4a7ab5";
                ctx.fill();
                ctx.strokeStyle = "#fff";
                ctx.lineWidth = 1.5;
                ctx.stroke();
            }

            // Exact values sit on their axes; the ordinary ticks remain MM/YY.
            ctx.font = "10px -apple-system, sans-serif";
            ctx.textAlign = "center";
            const dateWidth = ctx.measureText(date).width + 10;
            const dateLeft = Math.max(1, Math.min(w - dateWidth - 1, x - dateWidth / 2));
            ctx.fillStyle = "#344454";
            ctx.fillRect(dateLeft, h - pad.bottom + 5, dateWidth, 19);
            ctx.fillStyle = "#fff";
            ctx.fillText(date, dateLeft + dateWidth / 2, h - pad.bottom + 18);
            if (point) {
                const axisValue = (opts.formatSelectionY || (v => v.toFixed(1)))(point.value);
                const valueWidth = Math.max(30, ctx.measureText(axisValue).width + 10);
                ctx.fillStyle = "#344454";
                ctx.fillRect(pad.left - valueWidth - 1, point.y - 9, valueWidth, 18);
                ctx.fillStyle = "#fff";
                ctx.fillText(axisValue, pad.left - valueWidth / 2 - 1, point.y + 4);
            }
            ctx.restore();
        }

        const linkedChart = {
            showDate(date, sourcePoint) {
                if (!date) { drawSelection(null, null); return; }
                let nearest = sourcePoint;
                if (!nearest) {
                    const preferredSeries = selected?.series || points[0]?.series;
                    const time = trendsDateTime(date);
                    let best = Infinity;
                    for (const point of plotPoints) {
                        if (point.series !== preferredSeries) continue;
                        const distance = Math.abs(point.time - time);
                        if (distance < best) { nearest = point; best = distance; }
                    }
                }
                drawSelection(nearest, date);
            },
        };
        opts.crosshair?.register(linkedChart);

        function select(point) {
            if (opts.crosshair) opts.crosshair.select(point, linkedChart);
            else linkedChart.showDate(point?.date || null, point);
        }

        function selectAt(event) {
            const rect = canvas.getBoundingClientRect();
            const x = (event.clientX - rect.left) * w / rect.width;
            const y = (event.clientY - rect.top) * h / rect.height;
            let closest = null, bestX = Infinity, bestY = Infinity;
            for (const point of plotPoints) {
                const dx = Math.abs(point.x - x), dy = Math.abs(point.y - y);
                if (dx < bestX - 0.01 || (Math.abs(dx - bestX) <= 0.01 && dy < bestY)) {
                    closest = point; bestX = dx; bestY = dy;
                }
            }
            select(closest);
        }

        canvas.onpointerdown = event => {
            if (event.button !== 0 || !plotPoints.length) return;
            event.preventDefault();
            canvas.focus({ preventScroll: true });
            activePointer = event.pointerId;
            canvas.setPointerCapture(event.pointerId);
            selectAt(event);
        };
        canvas.onpointermove = event => {
            if (event.pointerId === activePointer && (event.buttons & 1)) selectAt(event);
        };
        canvas.onpointerup = event => {
            if (event.pointerId !== activePointer) return;
            selectAt(event);
            activePointer = null;
            if (canvas.hasPointerCapture(event.pointerId)) canvas.releasePointerCapture(event.pointerId);
        };
        canvas.onpointercancel = canvas.onlostpointercapture = () => { activePointer = null; };
        canvas.onkeydown = event => {
            if (event.key === "Escape") { select(null); return; }
            if (!["ArrowLeft", "ArrowRight"].includes(event.key) || !plotPoints.length) return;
            event.preventDefault();
            const sameSeries = selected ? plotPoints.filter(p => p.series === selected.series) : plotPoints;
            const index = selected ? sameSeries.indexOf(selected) + (event.key === "ArrowLeft" ? -1 : 1) : 0;
            select(sameSeries[Math.max(0, Math.min(sameSeries.length - 1, index))]);
        };
    }

    function drawTrendsBarChart(canvasId, data, opts) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const chart = initTrendsChart(canvas, opts.axis);
        const { ctx, w, pad, cw, ch, xPos } = chart;
        const maxV = Math.max(0, ...data.map(d => d.distance)) * 1.1 || 1;
        const yPos = chart.yPos = v => pad.top + (1 - v / maxV) * ch;
        drawTrendsYGrid(chart, yPos, 0, maxV, v => v.toFixed(0));
        const barWidth = Math.max(1, Math.min(16, cw * 7 * 86400000 / (opts.axis.end - opts.axis.start) * 0.75));
        const color = opts.barColor || "#4a7ab5";
        const points = data.map(d => ({ date: d.week, value: d.distance, series: "Miles", color }));
        ctx.fillStyle = color;
        for (const point of points) {
            const x = xPos(trendsDateTime(point.date));
            const left = Math.max(pad.left, x - barWidth / 2);
            const right = Math.min(w - pad.right, x + barWidth / 2);
            ctx.fillRect(left, yPos(point.value), right - left, pad.top + ch - yPos(point.value));
        }
        if (opts.maWindow && data.length >= opts.maWindow) {
            ctx.beginPath();
            for (let i = opts.maWindow - 1; i < data.length; i++) {
                const date = trendsDateTime(data[i].week);
                let sum = 0;
                for (let j = i - opts.maWindow + 1; j <= i; j++) sum += data[j].distance;
                const value = sum / opts.maWindow;
                const x = xPos(date), y = yPos(value);
                if (i === opts.maWindow - 1) ctx.moveTo(x, y);
                else ctx.lineTo(x, y);
                points.push({ date: data[i].week, value, series: `${opts.maWindow}-week average`, color: "#c05050" });
            }
            ctx.strokeStyle = "#c05050";
            ctx.lineWidth = 1.5;
            ctx.setLineDash([4, 3]);
            ctx.stroke();
            ctx.setLineDash([]);
        }
        wireTrendsCrosshair(canvas, chart, points, { crosshair: opts.crosshair, weekly: true, formatValue: v => `${v.toFixed(1)} mi` });
    }

    function drawTrendsLineChart(canvasId, series, opts) {
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const validSeries = series.map(s => ({ ...s, data: (s.data || [])
            .filter(d => Number.isFinite(d.value) && Number.isFinite(trendsDateTime(d.date)))
            .sort((a, b) => a.date.localeCompare(b.date)) })).filter(s => s.data.length);
        const chart = initTrendsChart(canvas, opts.axis);
        const { ctx, w, pad, ch, xPos } = chart;
        const values = validSeries.flatMap(s => s.data.map(d => d.value));
        let vMin = values.length ? Math.min(...values) : 0;
        let vMax = values.length ? Math.max(...values) : 1;
        const vPad = (vMax - vMin) * 0.1 || 1;
        vMin -= vPad;
        vMax += vPad;
        if (opts.zeroLine) { vMin = Math.min(vMin, -vPad); vMax = Math.max(vMax, vPad); }
        const yPos = chart.yPos = v => {
            const norm = (v - vMin) / (vMax - vMin);
            return pad.top + (opts.invertY ? norm : 1 - norm) * ch;
        };
        drawTrendsYGrid(chart, yPos, vMin, vMax, opts.formatY || (v => v.toFixed(0)));
        if (opts.zeroLine) {
            ctx.beginPath();
            ctx.moveTo(pad.left, yPos(0));
            ctx.lineTo(w - pad.right, yPos(0));
            ctx.strokeStyle = "rgba(0,0,0,0.15)";
            ctx.lineWidth = 1;
            ctx.stroke();
        }
        for (const s of validSeries) {
            const pts = s.data.map(d => ({ x: xPos(trendsDateTime(d.date)), y: yPos(d.value) }));
            if (s.fill) {
                ctx.beginPath();
                ctx.moveTo(pts[0].x, pts[0].y);
                for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y);
                ctx.lineTo(pts[pts.length - 1].x, pad.top + ch);
                ctx.lineTo(pts[0].x, pad.top + ch);
                ctx.closePath();
                ctx.save();
                ctx.globalAlpha = 0.08;
                ctx.fillStyle = s.color;
                ctx.fill();
                ctx.restore();
            }
            if (s.line !== false) {
                ctx.beginPath();
                ctx.moveTo(pts[0].x, pts[0].y);
                for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y);
                ctx.strokeStyle = s.lineColor || s.color;
                ctx.lineWidth = 1.5;
                ctx.stroke();
            }
            if (s.radius || pts.length === 1) {
                for (const p of pts) {
                    ctx.beginPath();
                    ctx.arc(p.x, p.y, s.radius || 3, 0, Math.PI * 2);
                    ctx.fillStyle = s.color;
                    ctx.fill();
                    ctx.strokeStyle = "#fff";
                    ctx.lineWidth = 1.5;
                    ctx.stroke();
                }
            }
        }
        if (opts.legend) {
            let legend = canvas.parentElement.querySelector(".chart-legend");
            if (!legend) {
                legend = document.createElement("div");
                legend.className = "chart-legend";
                canvas.parentElement.appendChild(legend);
            }
            legend.innerHTML = validSeries.map(s => `<span class="chart-legend-item"><span class="chart-legend-swatch" style="background:${s.color}"></span>${escapeHtml(s.label)}</span>`).join("");
        }
        wireTrendsCrosshair(canvas, chart, validSeries.flatMap(s => s.data.map(d => ({ ...d, series: s.label, color: s.color }))), {
            ...opts, formatSelectionY: opts.invertY ? opts.formatY : undefined,
        });
    }

    function drawScatterChart(canvasId, points, axis, crosshair) {
        drawTrendsLineChart(canvasId, [{ label: "VDOT", data: points, color: "#4a7ab5", lineColor: "rgba(74,122,181,0.4)", radius: 4 }], {
            axis, crosshair, formatY: v => v.toFixed(1),
        });
    }

    function drawPredictedVdotChart(canvasId, linePts, scatterPts, axis, crosshair) {
        drawTrendsLineChart(canvasId, [
            { label: "Predicted", data: linePts, color: "#4a7ab5", fill: true },
            { label: "Race Actual", data: scatterPts, color: "#c05050", line: false, radius: 5 },
        ], { axis, crosshair, formatY: v => v.toFixed(1), legend: true });
    }

    // ═══════════════════════════════════════════════════════════════
    // ── Race Prediction Tab ───────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    async function loadRacePredictionTab() {
        const page = document.querySelector(".race-prediction-page");
        page.innerHTML = "<p style='color:#888;font-size:12px'>Loading…</p>";

        const resp = await fetch("/api/race-predictions");
        const data = await resp.json();

        const fmtVdot = v => v != null ? v.toFixed(1) : "\u2014";
        const fmtErr = v => {
            if (v == null) return "\u2014";
            const sign = v >= 0 ? "+" : "";
            const cls = v >= 0 ? "rp-error-pos" : "rp-error-neg";
            return `<span class="${cls}">${sign}${v.toFixed(2)}</span>`;
        };

        let html = "";

        // ── Model Description ──
        html += `<div class="rp-section">
            <h3>Model</h3>
            <p class="rp-desc">
                Predict race VDOT from training data using per-activity computed VDOT (CVD)
                values derived from grade-adjusted pace and heart rate via the
                Daniels-Gilbert / Runalyze formula.<br><br>
                <strong>Step 1:</strong> Compute a duration-weighted exponential moving average
                of CVD values over the ${data.window_days} days before the race.
                Half-life = ${data.half_life} days (3 half-lives span the window).<br>
                <strong>Step 2:</strong> Multiply by a calibration coefficient (least-squares
                optimized to minimize prediction error across all races).
            </p>
        </div>`;

        // ── Calibrated Parameters ──
        html += `<div class="rp-section">
            <h3>Calibrated Parameters</h3>
            <div class="rp-params">
                <div class="rp-param">
                    <div class="rp-param-label">Window</div>
                    <div class="rp-param-value">${data.window_days}d</div>
                </div>
                <div class="rp-param">
                    <div class="rp-param-label">Half-life</div>
                    <div class="rp-param-value">${data.half_life}d</div>
                </div>
                <div class="rp-param">
                    <div class="rp-param-label">Multiplier</div>
                    <div class="rp-param-value">${data.multiplier.toFixed(4)}</div>
                </div>
                <div class="rp-param">
                    <div class="rp-param-label">RMSE</div>
                    <div class="rp-param-value">${data.rmse != null ? data.rmse.toFixed(2) : "\u2014"}</div>
                </div>
                <div class="rp-param">
                    <div class="rp-param-label">MAE</div>
                    <div class="rp-param-value">${data.mae != null ? data.mae.toFixed(2) : "\u2014"}</div>
                </div>
            </div>
        </div>`;

        // ── Predictions Table ──
        html += `<div class="rp-section">
            <h3>Predictions vs Actual</h3>
            <table class="rp-table">
                <thead>
                    <tr>
                        <th>Date</th>
                        <th>Race</th>
                        <th style="text-align:right">Prior Runs</th>
                        <th style="text-align:right">21d EWA</th>
                        <th style="text-align:right">Predicted</th>
                        <th style="text-align:right">Actual</th>
                        <th style="text-align:right">Error</th>
                    </tr>
                </thead>
                <tbody>`;

        for (const r of data.races) {
            html += `<tr>
                <td>${r.date}</td>
                <td>${r.name}</td>
                <td class="num">${r.prior_count}</td>
                <td class="num">${fmtVdot(r.ewa)}</td>
                <td class="num">${fmtVdot(r.predicted)}</td>
                <td class="num">${fmtVdot(r.race_vdot)}</td>
                <td class="num">${fmtErr(r.error)}</td>
            </tr>`;
        }

        html += `</tbody></table></div>`;

        // ── Chart: Predicted vs Actual scatter ──
        html += `<div class="rp-section">
            <h3>Predicted vs Actual</h3>
            <canvas id="rp-scatter" width="500" height="400" style="max-width:100%"></canvas>
        </div>`;

        page.innerHTML = html;

        // Draw scatter chart
        const valid = data.races.filter(r => r.predicted != null);
        if (valid.length > 0) {
            drawRPScatter(valid);
        }

        // ── Weather-Adjusted CVD Model ──
        try {
            const wrResp = await fetch("/api/weather-regression");
            if (wrResp.ok) {
                const wr = await wrResp.json();
                let wrHtml = `<hr style="margin:32px 0;border-color:#333">`;

                // Model description
                wrHtml += `<div class="rp-section">
                    <h3>Weather-Adjusted CVD Model</h3>
                    <p class="rp-desc">
                        A shallow random forest (max depth 4, 100 trees) predicting per-activity
                        computed VDOT from weather and fitness features. Captures nonlinear effects
                        like the heat/humidity performance threshold without hand-engineered features.<br><br>
                        <strong>Features:</strong> temperature, humidity, wet-bulb temp (Stull 2011),
                        precipitation flag, 7-day avg CVD (duration-weighted), and distance.
                        Excludes activities under 3 miles (noisy CVD from short reps).
                        Train/test split is 80/20 with fixed seed for reproducibility.
                    </p>
                </div>`;

                // Diagnostics cards
                wrHtml += `<div class="rp-section">
                    <h3>Model Diagnostics</h3>
                    <div class="rp-params">
                        <div class="rp-param">
                            <div class="rp-param-label">N (train)</div>
                            <div class="rp-param-value">${wr.n_train}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">N (test)</div>
                            <div class="rp-param-value">${wr.n_test}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">Train R²</div>
                            <div class="rp-param-value">${wr.train.r2.toFixed(4)}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">Test R²</div>
                            <div class="rp-param-value">${wr.test.r2.toFixed(4)}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">Train RMSE</div>
                            <div class="rp-param-value">${wr.train.rmse.toFixed(2)}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">Test RMSE</div>
                            <div class="rp-param-value">${wr.test.rmse.toFixed(2)}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">Train MAE</div>
                            <div class="rp-param-value">${wr.train.mae.toFixed(2)}</div>
                        </div>
                        <div class="rp-param">
                            <div class="rp-param-label">Test MAE</div>
                            <div class="rp-param-value">${wr.test.mae.toFixed(2)}</div>
                        </div>
                    </div>
                </div>`;

                // Feature importance table
                wrHtml += `<div class="rp-section">
                    <h3>Feature Importances</h3>
                    <table class="rp-table">
                        <thead>
                            <tr>
                                <th>Feature</th>
                                <th style="text-align:right">Importance</th>
                                <th style="width:200px"></th>
                            </tr>
                        </thead>
                        <tbody>`;

                const maxImp = Math.max(...wr.feature_importances.map(f => f.importance));
                for (const fi of wr.feature_importances) {
                    const barW = maxImp > 0 ? (fi.importance / maxImp * 100) : 0;
                    wrHtml += `<tr>
                        <td>${fi.name}</td>
                        <td class="num">${(fi.importance * 100).toFixed(1)}%</td>
                        <td><div style="background:#4a7ab5;height:12px;width:${barW}%;border-radius:2px"></div></td>
                    </tr>`;
                }

                wrHtml += `</tbody></table></div>`;

                // Scatter chart
                wrHtml += `<div class="rp-section">
                    <h3>Predicted vs Actual CVD</h3>
                    <canvas id="wr-scatter" width="500" height="400" style="max-width:100%"></canvas>
                </div>`;

                page.innerHTML += wrHtml;

                if (wr.activities.length > 0) {
                    drawWRScatter(wr.activities);
                }
            }
        } catch (e) {
            console.warn("Weather regression fetch failed:", e);
        }
    }

    function drawRPScatter(races) {
        const canvas = document.getElementById("rp-scatter");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");
        const w = canvas.width, h = canvas.height;
        const pad = { top: 20, right: 20, bottom: 40, left: 50 };

        const actuals = races.map(r => r.race_vdot);
        const preds = races.map(r => r.predicted);
        const all = actuals.concat(preds);
        const lo = Math.floor(Math.min(...all) - 2);
        const hi = Math.ceil(Math.max(...all) + 2);

        const xPos = v => pad.left + (v - lo) / (hi - lo) * (w - pad.left - pad.right);
        const yPos = v => h - pad.bottom - (v - lo) / (hi - lo) * (h - pad.top - pad.bottom);

        ctx.clearRect(0, 0, w, h);

        // Perfect prediction line
        ctx.beginPath();
        ctx.moveTo(xPos(lo), yPos(lo));
        ctx.lineTo(xPos(hi), yPos(hi));
        ctx.strokeStyle = "#ddd";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.stroke();
        ctx.setLineDash([]);

        // Grid lines
        ctx.fillStyle = "#aaa";
        ctx.font = "10px monospace";
        ctx.textAlign = "center";
        for (let v = Math.ceil(lo); v <= hi; v += 2) {
            ctx.beginPath();
            ctx.moveTo(xPos(v), h - pad.bottom);
            ctx.lineTo(xPos(v), pad.top);
            ctx.strokeStyle = "#f0f0f0";
            ctx.stroke();
            ctx.fillText(v, xPos(v), h - pad.bottom + 14);
        }
        ctx.textAlign = "right";
        for (let v = Math.ceil(lo); v <= hi; v += 2) {
            ctx.beginPath();
            ctx.moveTo(pad.left, yPos(v));
            ctx.lineTo(w - pad.right, yPos(v));
            ctx.strokeStyle = "#f0f0f0";
            ctx.stroke();
            ctx.fillText(v, pad.left - 6, yPos(v) + 3);
        }

        // Axis labels
        ctx.fillStyle = "#888";
        ctx.font = "11px sans-serif";
        ctx.textAlign = "center";
        ctx.fillText("Actual VDOT", (pad.left + w - pad.right) / 2, h - 4);
        ctx.save();
        ctx.translate(12, (pad.top + h - pad.bottom) / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.fillText("Predicted VDOT", 0, 0);
        ctx.restore();

        // Points
        for (let i = 0; i < races.length; i++) {
            const x = xPos(races[i].race_vdot);
            const y = yPos(races[i].predicted);
            ctx.beginPath();
            ctx.arc(x, y, 5, 0, Math.PI * 2);
            ctx.fillStyle = "#4a7ab5";
            ctx.fill();
            ctx.strokeStyle = "#fff";
            ctx.lineWidth = 1.5;
            ctx.stroke();
        }
    }

    function drawWRScatter(activities) {
        const canvas = document.getElementById("wr-scatter");
        if (!canvas) return;
        const ctx = canvas.getContext("2d");
        const w = canvas.width, h = canvas.height;
        const pad = { top: 20, right: 20, bottom: 40, left: 50 };

        const all = activities.map(a => a.actual_cvd).concat(activities.map(a => a.predicted_cvd));
        const lo = Math.floor(Math.min(...all) - 2);
        const hi = Math.ceil(Math.max(...all) + 2);

        const xPos = v => pad.left + (v - lo) / (hi - lo) * (w - pad.left - pad.right);
        const yPos = v => h - pad.bottom - (v - lo) / (hi - lo) * (h - pad.top - pad.bottom);

        ctx.clearRect(0, 0, w, h);

        // Perfect prediction line
        ctx.beginPath();
        ctx.moveTo(xPos(lo), yPos(lo));
        ctx.lineTo(xPos(hi), yPos(hi));
        ctx.strokeStyle = "#ddd";
        ctx.lineWidth = 1;
        ctx.setLineDash([4, 4]);
        ctx.stroke();
        ctx.setLineDash([]);

        // Grid lines
        ctx.fillStyle = "#aaa";
        ctx.font = "10px monospace";
        ctx.textAlign = "center";
        for (let v = Math.ceil(lo); v <= hi; v += 5) {
            ctx.beginPath();
            ctx.moveTo(xPos(v), h - pad.bottom);
            ctx.lineTo(xPos(v), pad.top);
            ctx.strokeStyle = "#f0f0f0";
            ctx.stroke();
            ctx.fillText(v, xPos(v), h - pad.bottom + 14);
        }
        ctx.textAlign = "right";
        for (let v = Math.ceil(lo); v <= hi; v += 5) {
            ctx.beginPath();
            ctx.moveTo(pad.left, yPos(v));
            ctx.lineTo(w - pad.right, yPos(v));
            ctx.strokeStyle = "#f0f0f0";
            ctx.stroke();
            ctx.fillText(v, pad.left - 6, yPos(v) + 3);
        }

        // Axis labels
        ctx.fillStyle = "#888";
        ctx.font = "11px sans-serif";
        ctx.textAlign = "center";
        ctx.fillText("Actual CVD", (pad.left + w - pad.right) / 2, h - 4);
        ctx.save();
        ctx.translate(12, (pad.top + h - pad.bottom) / 2);
        ctx.rotate(-Math.PI / 2);
        ctx.fillText("Predicted CVD", 0, 0);
        ctx.restore();

        // Draw train points first (behind), then test points
        const train = activities.filter(a => a.split === "train");
        const test = activities.filter(a => a.split === "test");

        for (const a of train) {
            ctx.beginPath();
            ctx.arc(xPos(a.actual_cvd), yPos(a.predicted_cvd), 3, 0, Math.PI * 2);
            ctx.fillStyle = "rgba(74, 122, 181, 0.5)";
            ctx.fill();
        }
        for (const a of test) {
            ctx.beginPath();
            ctx.arc(xPos(a.actual_cvd), yPos(a.predicted_cvd), 4, 0, Math.PI * 2);
            ctx.fillStyle = "rgba(230, 140, 50, 0.8)";
            ctx.fill();
            ctx.strokeStyle = "#fff";
            ctx.lineWidth = 1;
            ctx.stroke();
        }

        // Legend
        ctx.font = "11px sans-serif";
        ctx.textAlign = "left";
        const lx = w - pad.right - 110, ly = pad.top + 10;
        ctx.fillStyle = "rgba(74, 122, 181, 0.5)";
        ctx.beginPath(); ctx.arc(lx, ly, 3, 0, Math.PI * 2); ctx.fill();
        ctx.fillStyle = "#aaa";
        ctx.fillText("Train", lx + 8, ly + 4);

        ctx.fillStyle = "rgba(230, 140, 50, 0.8)";
        ctx.beginPath(); ctx.arc(lx, ly + 18, 4, 0, Math.PI * 2); ctx.fill();
        ctx.fillStyle = "#aaa";
        ctx.fillText("Test", lx + 8, ly + 22);
    }

    // ═══════════════════════════════════════════════════════════════
    // ── Planning Tab ─────────────────────────────────────────────
    // ═══════════════════════════════════════════════════════════════

    async function loadPlanningTab(year) {
        const page = document.querySelector(".planning-page");
        if (!year) {
            const params = new URLSearchParams(window.location.search);
            year = parseInt(params.get("year")) || new Date().getFullYear();
        }
        page.innerHTML = "<p style='color:#888;font-size:12px'>Loading\u2026</p>";

        const resp = await fetch(`/api/planning?year=${year}`);
        const data = await resp.json();
        const weeks = data.weeks;

        const todayStr = new Date().toISOString().slice(0, 10);

        let html = "";

        // Year nav
        html += `<div class="planning-year-nav">
            <button class="cal-arrow" data-planning-year="${year - 1}">&larr;</button>
            <span class="cal-year">${year}</span>
            <button class="cal-arrow" data-planning-year="${year + 1}">&rarr;</button>
        </div>`;

        // Month color palette (week-2 shade from calendar)
        const MONTH_COLORS = {
            1:  "#FFA8A8", 2:  "#F0A0F0", 3:  "#90E890", 4:  "#FFFF99",
            5:  "#B0D0F0", 6:  "#FCD098", 7:  "#90E8B0", 8:  "#D0D0D0",
            9:  "#FFE098", 10: "#F0D0B8", 11: "#E0D0B8", 12: "#B0D8B0",
        };

        // Table
        html += `<table class="planning-table">
            <thead>
                <tr>
                    <th>Week</th>
                    <th style="text-align:right">Miles</th>
                    <th style="text-align:right">Intensity</th>
                    <th style="text-align:right">Runs</th>
                    <th>Phase</th>
                    <th>Note</th>
                </tr>
            </thead>
            <tbody>`;

        let lastMonth = null;
        for (const w of weeks) {
            // Month separator
            const month = w.month; // 1-indexed from backend
            if (lastMonth !== null && month !== lastMonth) {
                const monthNames = ["","January","February","March","April","May","June",
                    "July","August","September","October","November","December"];
                html += `<tr class="planning-month-sep"><td colspan="6">${monthNames[month]}</td></tr>`;
            }
            lastMonth = month;

            // Determine if this is the current week
            const isCurrent = w.week_start <= todayStr && w.week_end >= todayStr;
            const isFuture = w.is_future;
            let rowClass = "";
            if (isCurrent) rowClass = "planning-current-week";
            else if (isFuture) rowClass = "planning-future";

            const milesEditable = isFuture ? ' class="planning-editable num" data-field="target_mileage"' : ' class="num"';
            const intensityEditable = isFuture ? ' class="planning-editable num" data-field="target_intensity"' : ' class="num"';
            const phaseEditable = ' class="planning-editable" data-field="phase"';
            const noteEditable = ' class="planning-editable" data-field="note"';
            const dateColor = MONTH_COLORS[month] || "";

            // Show actual mileage for past, target for future (with actual in parens if both)
            let milesDisplay = "";
            if (!isFuture) {
                milesDisplay = w.total_mileage > 0 ? w.total_mileage.toFixed(1) : "";
            } else if (w.target_mileage != null) {
                if (w.total_mileage > 0) {
                    milesDisplay = `<span class="planning-target">${w.target_mileage.toFixed(1)}</span>`
                        + `<span class="planning-actual">(${w.total_mileage.toFixed(1)})</span>`;
                } else {
                    milesDisplay = w.target_mileage.toFixed(1);
                }
            } else {
                milesDisplay = w.total_mileage > 0 ? w.total_mileage.toFixed(1) : "";
            }

            // Intensity display: same target/actual pattern as miles
            let intensityDisplay = "";
            if (!isFuture) {
                intensityDisplay = w.total_intensity > 0 ? w.total_intensity.toFixed(1) : "";
            } else if (w.target_intensity != null) {
                if (w.total_intensity > 0) {
                    intensityDisplay = `<span class="planning-target">${w.target_intensity.toFixed(1)}</span>`
                        + `<span class="planning-actual">(${w.total_intensity.toFixed(1)})</span>`;
                } else {
                    intensityDisplay = w.target_intensity.toFixed(1);
                }
            } else {
                intensityDisplay = w.total_intensity > 0 ? w.total_intensity.toFixed(1) : "";
            }

            html += `<tr class="${rowClass}" data-week-start="${w.week_start}">
                <td class="planning-week-label" style="background:${dateColor}">${escapeHtml(w.label)}</td>
                <td${milesEditable} data-target="${w.target_mileage != null ? w.target_mileage : ""}" data-actual="${w.total_mileage}">${milesDisplay}</td>
                <td${intensityEditable} data-target="${w.target_intensity != null ? w.target_intensity : ""}" data-actual="${w.total_intensity}">${intensityDisplay}</td>
                <td class="num">${w.run_count || ""}</td>
                <td${phaseEditable}>${escapeHtml(w.phase || "")}</td>
                <td${noteEditable}>${escapeHtml(w.note || "")}</td>
            </tr>`;
        }

        html += "</tbody></table>";
        page.innerHTML = html;

        // Year nav click handlers
        page.querySelectorAll("[data-planning-year]").forEach(btn => {
            btn.addEventListener("click", () => {
                const y = parseInt(btn.dataset.planningYear);
                tabLoaded.planning = true;
                loadPlanningTab(y);
            });
        });

        // Inline editing for phase/note
        page.querySelectorAll(".planning-editable").forEach(cell => {
            cell.addEventListener("dblclick", () => startPlanningEdit(cell));
        });
    }

    function startPlanningEdit(cell) {
        if (cell.querySelector(".planning-inline-input")) return;

        const field = cell.dataset.field;
        const row = cell.closest("tr");
        const weekStart = row.dataset.weekStart;
        const oldHtml = cell.innerHTML;

        // Capture full row state before any edits (for undo)
        const prevRowState = {};
        row.querySelectorAll(".planning-editable").forEach(c => {
            const f = c.dataset.field;
            if (f === "target_mileage" || f === "target_intensity") {
                prevRowState[f] = c.dataset.target ? parseFloat(c.dataset.target) : null;
            } else {
                prevRowState[f] = c.textContent.trim() || null;
            }
        });

        // For target_mileage/target_intensity, use data-target as the editable value
        const isTargetField = field === "target_mileage" || field === "target_intensity";
        const editValue = isTargetField
            ? (cell.dataset.target || "")
            : cell.textContent.trim();

        const input = document.createElement("input");
        input.type = "text";
        input.className = "planning-inline-input";
        input.value = editValue;
        cell.innerHTML = "";
        cell.appendChild(input);
        input.focus();
        input.select();

        function save() {
            const newValue = input.value.trim();
            // Push undo entry before mutating DOM (only if value changed)
            if (newValue !== editValue) {
                pushPlanUndo({ type: "weekly", weekStart: weekStart, previous: prevRowState });
            }
            if (isTargetField) {
                const numVal = newValue ? parseFloat(newValue) : null;
                cell.dataset.target = numVal != null && !isNaN(numVal) ? numVal : "";
                const actual = parseFloat(cell.dataset.actual) || 0;
                if (numVal != null && !isNaN(numVal)) {
                    if (actual > 0) {
                        cell.innerHTML = `<span class="planning-target">${numVal.toFixed(1)}</span>`
                            + `<span class="planning-actual">(${actual.toFixed(1)})</span>`;
                    } else {
                        cell.textContent = numVal.toFixed(1);
                    }
                } else {
                    cell.textContent = actual > 0 ? actual.toFixed(1) : "";
                }
            } else {
                cell.textContent = newValue;
            }
            savePlanningField(weekStart, row);
        }

        function cancel() {
            cell.innerHTML = oldHtml;
        }

        function advanceToNext() {
            // Tab: save and move to next editable cell
            save();
            const cells = Array.from(row.querySelectorAll(".planning-editable"));
            const idx = cells.indexOf(cell);
            if (idx >= 0 && idx < cells.length - 1) {
                startPlanningEdit(cells[idx + 1]);
            }
        }

        input.addEventListener("keydown", e => {
            if (e.key === "Enter") { e.preventDefault(); save(); }
            else if (e.key === "Escape") { e.preventDefault(); cancel(); }
            else if (e.key === "Tab") { e.preventDefault(); advanceToNext(); }
        });

        input.addEventListener("blur", () => {
            // Only save on blur if the input is still in the DOM (not already handled)
            if (cell.contains(input)) save();
        });
    }

    function savePlanningField(weekStart, row) {
        // Read current values from the row's editable cells
        const cells = row.querySelectorAll(".planning-editable");
        const payload = {};
        cells.forEach(c => {
            const f = c.dataset.field;
            if (f === "target_mileage" || f === "target_intensity") {
                const v = c.dataset.target;
                payload[f] = v ? parseFloat(v) : null;
            } else {
                payload[f] = c.textContent.trim() || null;
            }
        });

        fetch(`/api/planning/${weekStart}`, {
            method: "POST",
            headers: { "Content-Type": "application/json" },
            body: JSON.stringify(payload),
        }).catch(() => {});
    }

})();
