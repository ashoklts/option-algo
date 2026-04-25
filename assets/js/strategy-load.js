(function () {
    'use strict';

    function getApiUrl(routeName, suffix) {
        if (typeof window.buildNamedApiUrl === 'function') {
            return window.buildNamedApiUrl(routeName, suffix);
        }
        var baseUrl = (window.APP_CONFIG && window.APP_CONFIG.algoApiBaseUrl)
            || (typeof window.getBackendUrl === 'function' ? window.getBackendUrl() : '')
            || window.APP_ALGO_API_BASE_URL
            || '';
        var routeMap = window.APP_API_ROUTES || {};
        var routePath = routeMap[routeName] || routeName || '';
        var normalizedRoute = String(routePath).replace(/\/+$/, '');
        var normalizedSuffix = String(suffix || '').replace(/^\/+/, '');
        return baseUrl.replace(/\/+$/, '') + '/' + (normalizedSuffix ? normalizedRoute + '/' + normalizedSuffix : normalizedRoute);
    }

    function setToggleState(btn, enabled) {
        if (!btn) return;
        btn.setAttribute('aria-checked', enabled ? 'true' : 'false');
        btn.classList.toggle('bg-secondaryBlue-500', enabled);
        btn.classList.toggle('bg-primaryBlack-600', !enabled);
        var knob = btn.querySelector('span');
        if (knob) {
            knob.classList.toggle('translate-x-5', enabled);
            knob.classList.toggle('translate-x-1', !enabled);
        }
    }

    function setSectionContentState(container, enabled) {
        if (!container) return;
        var content = container.lastElementChild;
        if (!content || content.querySelector('button[role="switch"]')) return;
        content.classList.toggle('pointer-events-none', !enabled);
        content.classList.toggle('opacity-40', !enabled);
        if (enabled) {
            content.classList.remove('opacity-50');
            content.classList.remove('opacity-80');
        }
    }

    function createSelectHtml(options, selectedValue, extraClass) {
        var html = '<div class="relative w-max"><select class="' + (extraClass || 'w-max cursor-pointer appearance-none rounded py-2 text-xs border border-primaryBlack-500 text-primaryBlack-750') + '">';
        options.forEach(function (option) {
            var value = option.value;
            var label = option.label;
            var selected = String(value) === String(selectedValue) ? ' selected' : '';
            html += '<option value="' + escapeHtml(value) + '"' + selected + '>' + escapeHtml(label) + '</option>';
        });
        html += '</select><svg xmlns="http://www.w3.org/2000/svg" fill="none" viewBox="0 0 24 24" stroke="currentColor" class="pointer-events-none absolute right-0 top-2.5 mr-3 h-4 w-4 bg-primaryBlack-0"><path stroke-linecap="round" stroke-linejoin="round" stroke-width="2" d="M19 9l-7 7-7-7"></path></svg></div>';
        return html;
    }

    function createNumberInputHtml(value) {
        return '<div class="relative w-max"><input type="number" class="relative w-20 rounded border border-primaryBlack-350 px-1 py-2 text-xs" value="' + escapeHtml(String(value == null ? 0 : value)) + '"></div>';
    }

    function escapeHtml(value) {
        return String(value == null ? '' : value)
            .replace(/&/g, '&amp;')
            .replace(/</g, '&lt;')
            .replace(/>/g, '&gt;')
            .replace(/"/g, '&quot;');
    }

    function getStrikeOptions() {
        var options = [];
        for (var itm = 20; itm >= 1; itm -= 1) options.push({ value: 'StrikeType.ITM' + itm, label: 'ITM' + itm });
        options.push({ value: 'StrikeType.ATM', label: 'ATM' });
        for (var otm = 1; otm <= 30; otm += 1) options.push({ value: 'StrikeType.OTM' + otm, label: 'OTM' + otm });
        return options;
    }

    function getMomentumOptions() {
        return [
            { value: 'MomentumType.PointsUp', label: 'Points (Pts) ↑' },
            { value: 'MomentumType.PointsDown', label: 'Points (Pts) ↓' },
            { value: 'MomentumType.PercentageUp', label: 'Percent (%) ↑' },
            { value: 'MomentumType.PercentageDown', label: 'Percent (%) ↓' },
            { value: 'MomentumType.UnderlyingPointsUp', label: 'Underlying Pts ↑' },
            { value: 'MomentumType.UnderlyingPointsDown', label: 'Underlying Pts ↓' },
            { value: 'MomentumType.UnderlyingPercentageUp', label: 'Underlying % ↑' },
            { value: 'MomentumType.UnderlyingPercentageDown', label: 'Underlying % ↓' }
        ];
    }

    function getTrailOptions() {
        return [
            { value: 'TrailStopLossType.Points', label: 'Points' },
            { value: 'TrailStopLossType.Percentage', label: 'Percent' }
        ];
    }

    function getTargetStoplossOptions() {
        return [
            { value: 'LegTgtSLType.Points', label: 'Points' },
            { value: 'LegTgtSLType.Percentage', label: 'Percent' },
            { value: 'LegTgtSLType.UnderlyingPoints', label: 'Underlying Pts' },
            { value: 'LegTgtSLType.UnderlyingPercentage', label: 'Underlying %' }
        ];
    }

    function getReentryOptions() {
        return [
            { value: 'ReentryType.Immediate', label: 'RE ASAP' },
            { value: 'ReentryType.ImmediateReverse', label: 'RE ASAP ↩' },
            { value: 'ReentryType.LikeOriginal', label: 'RE MOMENTUM' },
            { value: 'ReentryType.LikeOriginalReverse', label: 'RE MOMENTUM ↩' },
            { value: 'ReentryType.AtCost', label: 'RE COST' },
            { value: 'ReentryType.AtCostReverse', label: 'RE COST ↩' },
            { value: 'ReentryType.NextLeg', label: 'Lazy Leg' }
        ];
    }

    function getOverallReentryOptions() {
        return [
            { value: 'ReentryType.Immediate', label: 'RE ASAP' },
            { value: 'ReentryType.ImmediateReverse', label: 'RE ASAP ↩' },
            { value: 'ReentryType.LikeOriginal', label: 'RE MOMENTUM' },
            { value: 'ReentryType.LikeOriginalReverse', label: 'RE MOMENTUM ↩' }
        ];
    }

    function setStrikeValueContent(legEl, entryType, strikeParam) {
        var labelEl = legEl.querySelector('[data-strike-value-label]');
        var contentEl = legEl.querySelector('[data-strike-value-content]');
        if (!contentEl) return;

        var label = 'Value';
        var html = '';

        if (entryType === 'EntryType.EntryByStrikeType' || entryType === 'EntryType.EntryBySyntheticFuture') {
            label = 'Strike Type';
            html = createSelectHtml(getStrikeOptions(), strikeParam || 'StrikeType.ATM');
        } else if (entryType === 'EntryType.EntryByPremiumRange' || entryType === 'EntryType.EntryByDeltaRange') {
            label = entryType === 'EntryType.EntryByPremiumRange' ? 'Premium Range' : 'Delta Range';
            html = '<div class="flex items-center gap-2">' + createNumberInputHtml(strikeParam && strikeParam.LowerRange) + createNumberInputHtml(strikeParam && strikeParam.UpperRange) + '</div>';
        } else if (entryType === 'EntryType.EntryByPremium' || entryType === 'EntryType.EntryByPremiumGEQ' || entryType === 'EntryType.EntryByPremiumLEQ') {
            label = 'Premium';
            html = createNumberInputHtml(strikeParam);
        } else if (entryType === 'EntryType.EntryByDelta') {
            label = 'Delta';
            html = createNumberInputHtml(strikeParam);
        } else if (entryType === 'EntryType.EntryByStraddlePrice') {
            label = 'Straddle Width';
            html = '<div class="flex items-center gap-2">'
                + createSelectHtml([
                    { value: 'AdjustmentType.Minus', label: '-' },
                    { value: 'AdjustmentType.Plus', label: '+' }
                ], strikeParam && strikeParam.Adjustment || 'AdjustmentType.Plus', 'w-max cursor-pointer appearance-none rounded py-2 text-xs border border-primaryBlack-500 text-primaryBlack-750')
                + createNumberInputHtml(strikeParam && strikeParam.Multiplier)
                + '</div>';
        } else if (entryType === 'EntryType.EntryByAtmMultiplier') {
            label = '% of ATM';
            var atmPercent = 0;
            var atmSign = '+';
            if (typeof strikeParam === 'number') {
                atmSign = strikeParam >= 1 ? '+' : '-';
                atmPercent = Math.round(Math.abs(strikeParam - 1) * 100);
            }
            html = '<div class="flex items-center gap-2">'
                + createSelectHtml([
                    { value: '+', label: '+' },
                    { value: '-', label: '-' }
                ], atmSign, 'w-max cursor-pointer appearance-none rounded py-2 text-xs border border-primaryBlack-500 text-primaryBlack-750')
                + createNumberInputHtml(atmPercent)
                + '</div>';
        } else if (entryType === 'EntryType.EntryByPremiumCloseToStraddle') {
            label = 'ATM Straddle Premium %';
            html = createNumberInputHtml(strikeParam && strikeParam.Multiplier ? Math.round(strikeParam.Multiplier * 100) : 0);
        } else {
            label = 'Strike Type';
            html = createSelectHtml(getStrikeOptions(), 'StrikeType.ATM');
        }

        if (labelEl) labelEl.textContent = label;
        contentEl.innerHTML = html;
    }

    function applyTargetOrStop(container, cfg) {
        if (!container) return;
        var enabled = cfg && cfg.Type && cfg.Type !== 'None';
        setToggleState(container.querySelector('button[role="switch"]'), enabled);
        setSectionContentState(container, enabled);
        if (!enabled) return;
        var selects = container.querySelectorAll('select');
        var inputs = container.querySelectorAll('input[type="number"]');
        if (selects[0]) selects[0].value = cfg.Type;
        if (inputs[0]) inputs[0].value = cfg.Value || 0;
    }

    function applyTrail(container, cfg) {
        if (!container) return;
        var enabled = cfg && cfg.Type && cfg.Type !== 'None';
        setToggleState(container.querySelector('button[role="switch"]'), enabled);
        setSectionContentState(container, enabled);
        if (!enabled) return;
        var selects = container.querySelectorAll('select');
        var inputs = container.querySelectorAll('input[type="number"]');
        if (selects[0]) selects[0].value = cfg.Type;
        if (inputs[0]) inputs[0].value = cfg.Value && cfg.Value.InstrumentMove || 0;
        if (inputs[1]) inputs[1].value = cfg.Value && cfg.Value.StopLossMove || 0;
    }

    function applyMomentum(container, cfg) {
        if (!container) return;
        var enabled = cfg && cfg.Type && cfg.Type !== 'None';
        setToggleState(container.querySelector('button[role="switch"]'), enabled);
        setSectionContentState(container, enabled);
        if (!enabled) return;
        var selects = container.querySelectorAll('select');
        var inputs = container.querySelectorAll('input[type="number"]');
        if (selects[0]) selects[0].value = cfg.Type;
        if (inputs[0]) inputs[0].value = cfg.Value || 0;
    }

    function applyReentry(container, cfg) {
        if (!container) return;
        var enabled = cfg && cfg.Type && cfg.Type !== 'None';
        setToggleState(container.querySelector('button[role="switch"]'), enabled);
        setSectionContentState(container, enabled);
        if (!enabled) return;
        var selectEls = container.querySelectorAll('select');
        if (selectEls[0]) selectEls[0].value = cfg.Type;
        if (cfg.Type === 'ReentryType.NextLeg') {
            var noteId = 'lazy-ref-note';
            var note = container.querySelector('[data-' + noteId + ']');
            if (!note) {
                note = document.createElement('div');
                note.setAttribute('data-' + noteId, 'true');
                note.className = 'text-xs text-primaryBlue-500';
                container.appendChild(note);
            }
            note.textContent = 'Lazy Leg: ' + ((cfg.Value && cfg.Value.NextLegRef) || '-');
        } else if (selectEls[1]) {
            selectEls[1].value = String(cfg.Value && cfg.Value.ReentryCount || 1);
        }
    }

    function buildLegCard(template, cfg, labelText) {
        var legEl = template.cloneNode(true);
        legEl.removeAttribute('id');
        legEl.style.display = 'flex';
        legEl.dataset.loadedLeg = 'true';

        var badge = legEl.querySelector('.absolute.-left-0.top-0 h3, .absolute.-left-0.top-0 h3, .absolute.-left-0.top-0 .p-1\\.5');
        if (badge) badge.textContent = labelText;

        var lotInput = legEl.querySelector('input[type="number"]');
        if (lotInput && cfg.LotConfig) lotInput.value = cfg.LotConfig.Value || 1;

        var selects = legEl.querySelectorAll('select[title="Position"], select[title="Option Type"], select[title="Expiry"], [data-strike-criteria-select]');
        if (selects[0] && cfg.PositionType) selects[0].value = cfg.PositionType;
        if (selects[1] && cfg.InstrumentKind) selects[1].value = cfg.InstrumentKind;
        if (selects[2] && cfg.ExpiryKind) selects[2].value = cfg.ExpiryKind;
        if (selects[3] && cfg.EntryType) selects[3].value = cfg.EntryType;

        setStrikeValueContent(legEl, cfg.EntryType, cfg.StrikeParameter);

        applyTargetOrStop(legEl.querySelector('[id^="TargetProfit_"]'), cfg.LegTarget);
        applyTargetOrStop(legEl.querySelector('[id^="StopLoss_"]'), cfg.LegStopLoss);
        applyTrail(legEl.querySelector('[id^="TrailSL_"]'), cfg.LegTrailSL);
        applyReentry(legEl.querySelector('[id*="Re-entry on Tgt"]') && legEl.querySelector('[id*="Re-entry on Tgt"]').closest('.flex.w-fit.flex-col.gap-2'), cfg.LegReentryTP);
        applyReentry(legEl.querySelector('[id*="Re-entry on SL"]') && legEl.querySelector('[id*="Re-entry on SL"]').closest('.flex.w-fit.flex-col.gap-2'), cfg.LegReentrySL);
        applyMomentum(legEl.querySelector('[id^="simple_momentum_"]'), cfg.LegMomentum);

        return legEl;
    }

    function ensureLoadedLegContainer() {
        var existing = document.getElementById('loaded-legs-container');
        if (existing) return existing;

        var template = document.getElementById('backtest');
        if (!template || !template.parentNode) return null;

        var container = document.createElement('div');
        container.id = 'loaded-legs-container';
        container.className = 'flex flex-col gap-4 mt-4';
        template.parentNode.insertBefore(container, template);
        return container;
    }

    function applyOverallSimpleBlock(root, index, cfg) {
        var buttons = root.querySelectorAll('[data-test-id="toggle-handle_Overall Stop Loss_overall-stoploss"]');
        var button = buttons[index];
        if (!button) return;
        var wrap = button.closest('[id^="OverallStop"]');
        var enabled = !!(cfg && cfg.Type && cfg.Type !== 'None');
        setToggleState(button, enabled);
        setSectionContentState(wrap, enabled);
        if (!cfg || cfg.Type === 'None') return;
        if (!wrap) return;
        var selects = wrap.querySelectorAll('select');
        var inputs = wrap.querySelectorAll('input[type="number"]');
        if (selects[0]) selects[0].value = cfg.Type;
        if (inputs[0]) inputs[0].value = cfg.Value || 0;
    }

    function applyOverallReentry(root, index, cfg) {
        var buttons = root.querySelectorAll('[data-test-id="toggle-handle_rentry_undefined_Overall Re-entry on SL"]');
        var button = buttons[index];
        if (!button) return;
        setToggleState(button, !!(cfg && cfg.Type && cfg.Type !== 'None'));
        var wrap = button.closest('.flex.w-fit.flex-col.gap-2');
        setSectionContentState(wrap, !!(cfg && cfg.Type && cfg.Type !== 'None'));
        if (!cfg || cfg.Type === 'None') return;
        if (!wrap) return;
        var selects = wrap.querySelectorAll('select');
        if (selects[0]) selects[0].value = cfg.Type;
        if (selects[1]) selects[1].value = String(cfg.Value && cfg.Value.ReentryCount || 1);
    }

    function applyOverallMomentum(root, cfg) {
        var button = root.querySelector('[data-test-id="toggle-handle_Overall Momentum_OverallMomentum_None"]');
        if (!button) return;
        setToggleState(button, !!(cfg && cfg.Type && cfg.Type !== 'None'));
        var wrap = button.closest('[id^="OverallMomentum"]');
        setSectionContentState(wrap, !!(cfg && cfg.Type && cfg.Type !== 'None'));
        if (!cfg || cfg.Type === 'None') return;
        if (!wrap) return;
        var selects = wrap.querySelectorAll('select');
        var inputs = wrap.querySelectorAll('input[type="number"]');
        if (selects[0]) selects[0].value = cfg.Type;
        if (inputs[0]) inputs[0].value = cfg.Value || 0;
    }

    function applyTrailingOptions(root, strategy) {
        var button = root.querySelector('[data-test-id="toggle-handle_Trailing Options_trailing-options"]');
        if (!button) return;

        var lockCfg = strategy.LockAndTrail;
        var overallTrailCfg = strategy.OverallTrailSL;
        var enabled = !!(
            (lockCfg && lockCfg.Type && lockCfg.Type !== 'None') ||
            (overallTrailCfg && overallTrailCfg.Type && overallTrailCfg.Type !== 'None')
        );
        setToggleState(button, enabled);
        var wrap = button.closest('[id^="TrailingOptions"]');
        setSectionContentState(wrap, enabled);
        if (!enabled) return;

        if (!wrap) return;
        var selectEl = wrap.querySelector('select');
        if (!selectEl) return;

        if (overallTrailCfg && overallTrailCfg.Type && overallTrailCfg.Type !== 'None') {
            selectEl.value = 'TrailingOption.OverallTrailSL';
            var typeSel = document.getElementById('overall-trail-sl-type');
            var input1 = document.getElementById('overall-trail-sl-input1');
            var input2 = document.getElementById('overall-trail-sl-input2');
            if (typeSel) typeSel.value = overallTrailCfg.Type;
            if (input1) input1.value = overallTrailCfg.Value && overallTrailCfg.Value.InstrumentMove || 0;
            if (input2) input2.value = overallTrailCfg.Value && overallTrailCfg.Value.StopLossMove || 0;
            return;
        }

        selectEl.value = lockCfg.Type;
        var profitReaches = document.getElementById('trailing-profit-reaches');
        var lockProfit = document.getElementById('trailing-lock-profit');
        var increaseBy = document.getElementById('trailing-increase-by');
        var trailBy = document.getElementById('trailing-trail-by');
        if (profitReaches) profitReaches.value = lockCfg.Value && lockCfg.Value.ProfitReaches || 0;
        if (lockProfit) lockProfit.value = lockCfg.Value && lockCfg.Value.LockProfit || 0;
        if (increaseBy) increaseBy.value = lockCfg.Value && lockCfg.Value.IncreaseInProfitBy || 0;
        if (trailBy) trailBy.value = lockCfg.Value && lockCfg.Value.TrailProfitBy || 0;
    }

    function applyOverallSettings(cfg) {
        var strategy = (cfg && cfg.strategy) || {};
        applyOverallSimpleBlock(document, 0, strategy.OverallSL);
        applyOverallSimpleBlock(document, 1, strategy.OverallTgt);
        applyOverallReentry(document, 0, strategy.OverallReentrySL);
        applyOverallReentry(document, 1, strategy.OverallReentryTgt);
        applyOverallMomentum(document, strategy.OverallMomentum);
        applyTrailingOptions(document, strategy);
    }

    function applyDatesAndTimes(cfg) {
        var dateInputs = document.querySelectorAll('[title="date-input"]');
        if (dateInputs[0] && cfg.start_date) dateInputs[0].value = cfg.start_date;
        if (dateInputs[1] && cfg.end_date) dateInputs[1].value = cfg.end_date;

        var indicatorSettings = document.getElementById('indicator-settings');
        if (!indicatorSettings) return;

        var timeInputs = indicatorSettings.querySelectorAll('input[type="time"]');
        var entryParams = cfg.strategy && cfg.strategy.EntryIndicators && cfg.strategy.EntryIndicators.Value &&
            cfg.strategy.EntryIndicators.Value[0] && cfg.strategy.EntryIndicators.Value[0].Value &&
            cfg.strategy.EntryIndicators.Value[0].Value.Parameters;
        var exitParams = cfg.strategy && cfg.strategy.ExitIndicators && cfg.strategy.ExitIndicators.Value &&
            cfg.strategy.ExitIndicators.Value[0] && cfg.strategy.ExitIndicators.Value[0].Value &&
            cfg.strategy.ExitIndicators.Value[0].Value.Parameters;

        if (timeInputs[0] && entryParams) {
            timeInputs[0].value = String(entryParams.Hour || 9).padStart(2, '0') + ':' + String(entryParams.Minute || 16).padStart(2, '0');
        }
        if (timeInputs[1] && exitParams) {
            timeInputs[1].value = String(exitParams.Hour || 15).padStart(2, '0') + ':' + String(exitParams.Minute || 15).padStart(2, '0');
        }
    }

    function applyConfig(cfg) {
        if (!cfg || !cfg.strategy) return;

        applyDatesAndTimes(cfg);
        applyOverallSettings(cfg);

        var container = ensureLoadedLegContainer();
        var heading = document.getElementById('legs-heading');
        var template = document.getElementById('backtest');
        if (!container || !template) return;

        container.innerHTML = '';
        if (heading) heading.style.display = '';

        var regularLegs = cfg.strategy.ListOfLegConfigs || [];
        regularLegs.forEach(function (legCfg, index) {
            container.appendChild(buildLegCard(template, legCfg, '# ' + (index + 1)));
        });

        var idleLegConfigs = cfg.strategy.IdleLegConfigs || {};
        var idleNames = Object.keys(idleLegConfigs);
        if (idleNames.length) {
            var idleTitle = document.createElement('h6');
            idleTitle.className = 'text-base font-medium leading-7 text-primaryBlack-750 mt-2';
            idleTitle.textContent = 'Lazy Legs';
            container.appendChild(idleTitle);

            idleNames.forEach(function (name) {
                container.appendChild(buildLegCard(template, idleLegConfigs[name], 'Lazy - ' + name));
            });
        }
    }

    function showEditButtons() {
        var saveBtn = document.getElementById('save-strategy-btn');
        var updateBtn = document.getElementById('update-strategy-btn');
        var saveAsBtn = document.getElementById('save-as-strategy-btn');

        if (saveBtn) saveBtn.style.display = 'none';
        if (updateBtn) updateBtn.style.display = '';
        if (saveAsBtn) saveAsBtn.style.display = '';
    }

    function loadFromUrl() {
        var params = new URLSearchParams(window.location.search);
        var strategyId = params.get('strategy_id');
        if (!strategyId) return;

        fetch(getApiUrl('strategyById', encodeURIComponent(strategyId)))
            .then(function (res) {
                if (!res.ok) throw new Error('Strategy not found (status ' + res.status + ')');
                return res.json();
            })
            .then(function (doc) {
                var fullConfig = doc && doc.full_config;
                if (!fullConfig) throw new Error('Strategy has no full_config');
                applyConfig(fullConfig);
                showEditButtons();
            })
            .catch(function (err) {
                console.error('[strategy-load] Failed to load strategy:', err);
            });
    }

    function init() {
        loadFromUrl();
    }

    if (document.readyState === 'loading') {
        document.addEventListener('DOMContentLoaded', init, { once: true });
    } else {
        init();
    }
})();
