// static/js/account_detail.js

// Ensure utilities are loaded first via HTML script order
if (typeof formatCurrency !== 'function' || typeof formatDate !== 'function' || typeof formatValue !== 'function' || typeof getHealthColorInfo !== 'function') {
    console.error("ERROR: One or more utility functions from dashboard_utils.js seem to be missing!");
    const errorDiv = document.getElementById('pageErrorIndicator');
    if(errorDiv) {
        errorDiv.textContent = "Critical error: UI helper functions not loaded. Please contact support.";
        errorDiv.style.display = 'block';
    }
}

// ----- Distributor → colour map  ------------------------------------------
const DISTRIBUTOR_COLORS = {
    // Primaries
    PALKO     : '#f1c40f',
    THRESHOLD : '#2ca02c',
    UNFI      : '#e6550d',
    KEHE      : '#9467bd',
    DIRECT    : '#d62728',
    INFRA     : '#17becf',
    UNKNOWN   : '#636363', // Fallback
  };
function ensureDistributorColor(name) {
    const key = (name || 'UNKNOWN').toUpperCase();
    if (!DISTRIBUTOR_COLORS[key]) {
        // Generate a soft pastel colour deterministically from the name
        let hash = 0; for (let i = 0; i < key.length; i++) hash = key.charCodeAt(i) + ((hash << 5) - hash);
        DISTRIBUTOR_COLORS[key] = `hsl(${hash % 360}, 45%, 60%)`;
    }
    return DISTRIBUTOR_COLORS[key];
}


document.addEventListener('DOMContentLoaded', function () {
    console.log("Account Detail JS Initializing...");

    // --- Get Account Code ---
    const accountCodeEl = document.getElementById('accountCanonicalCode');
    const accountCode = accountCodeEl ? accountCodeEl.value : null;

    // --- DOM References ---
    const pageLoadingIndicator = document.getElementById('pageLoadingIndicator');
    const pageErrorIndicator = document.getElementById('pageErrorIndicator');
    const accountDetailContent = document.getElementById('accountDetailContent');
    const accountNameEl = document.getElementById('accountName');
    const detailBaseCardCodeEl = document.getElementById('detailBaseCardCode');
    const detailFullAddressEl = document.getElementById('detailFullAddress');
    const detailSalesRepNameEl = document.getElementById('detailSalesRepName');
    //const detailDistributorNameEl = document.getElementById('detailDistributorName');
    const detailPyYearLabelPerfEl = document.getElementById('detailPyYearLabelPerf');
    const detailPyTotalSalesLabelPerfEl = document.getElementById('detailPyTotalSalesLabelPerf');
    const detailPyTotalRevenuePerfEl = document.getElementById('detailPyTotalRevenuePerf');
    const detailCyYearLabelPerfEl = document.getElementById('detailCyYearLabelPerf');
    const detailCytdRevenueValuePerfEl = document.getElementById('detailCytdRevenueValuePerf');
    const detailYepRevenueValuePerfEl = document.getElementById('detailYepRevenueValuePerf');
    const ctaTop30CoverageEl = document.getElementById('ctaTop30Coverage');
    const btnGenerateMarketingEmailEl = document.getElementById('btnGenerateMarketingEmail');
    const btnGenerateDiscountEmailEl = document.getElementById('btnGenerateDiscountEmail');
    const btnGenerateReminderEmailEl = document.getElementById('btnGenerateReminderEmail');
    const detailGrowthPacePctEl = document.getElementById('detailGrowthPacePct');
    const cadenceChartCanvas = document.getElementById('cadenceChart');
    const cadenceChartLoadingEl = document.getElementById('cadenceChartLoading');
    const cadenceChartErrorEl = document.getElementById('cadenceChartError');
    const revenueChartCanvas = document.getElementById('revenueChart');
    const revenueChartLoadingEl = document.getElementById('revenueChartLoading');
    const revenueChartErrorEl = document.getElementById('revenueChartError');
    const revenueTrendEl = document.getElementById('revenueTrend');
    const revenueTrendR2El = document.getElementById('revenueTrendR2');
    const revenueForecastEl = document.getElementById('revenueForecast');
    const productHistoryYearSelectEl = document.getElementById('productHistoryYearSelect');
    const productHistoryLoadingEl = document.getElementById('productHistoryLoading');
    const productHistoryErrorEl = document.getElementById('productHistoryError');
    const productHistoryContentEl = document.getElementById('productHistoryContent');
    const growthOpportunityCardEl = document.getElementById('growthOpportunityCard');
    const growthEngineStatusBadgeEl = document.getElementById('growthEngineStatusBadge');
    const growthEngineMessageEl = document.getElementById('growthEngineMessage');
    const growthEngineDetailsEl = document.getElementById('growthEngineDetails');
    const targetYepDisplayEl = document.getElementById('targetYepDisplay');
    const additionalRevNeededDisplayContainerEl = document.getElementById('additionalRevNeededDisplayContainer');
    const additionalRevNeededDisplayEl = document.getElementById('additionalRevNeededDisplay');
    const suggestedNextPurchaseAmountDisplayEl = document.getElementById('suggestedNextPurchaseAmountDisplay');
    const growthEngineRecommendedProductsEl = document.getElementById('growthEngineRecommendedProducts');
    const yearlyProductSummaryYearSelectEl = document.getElementById('yearlyProductSummaryYearSelect');
    const yearlyProductSummaryLoadingEl = document.getElementById('yearlyProductSummaryLoading');
    const yearlyProductSummaryErrorEl = document.getElementById('yearlyProductSummaryError');
    const yearlyProductSummaryTableEl = document.getElementById('yearlyProductSummaryTable');
    const yearlyProductSummaryTableBodyEl = yearlyProductSummaryTableEl ? yearlyProductSummaryTableEl.querySelector('tbody') : null;
    const yearlyProductSummaryTableHeadEl = yearlyProductSummaryTableEl ? yearlyProductSummaryTableEl.querySelector('thead') : null;
    const yearlyTotalsForYearEl = document.getElementById('yearlyTotalsForYear');
    const yearlyTotalQuantityCellEl = document.getElementById('yearlyTotalQuantityCell');
    const yearlyTotalRevenueCellEl = document.getElementById('yearlyTotalRevenueCell');
    const yearlyProductSummaryTableTotalsRowEl = document.getElementById('yearlyProductSummaryTableTotalsRow');
    const rollingAnalysisLoadingEl = document.getElementById('rollingAnalysisLoading');
    const rollingAnalysisErrorEl = document.getElementById('rollingAnalysisError');
    const rollingAnalysisContentEl = document.getElementById('rollingAnalysisContent');
    const rollingSkuAnalysisTableEl = document.getElementById('rollingSkuAnalysisTable');
    const rollingSkuAnalysisTableBodyEl = rollingSkuAnalysisTableEl ? rollingSkuAnalysisTableEl.querySelector('tbody') : null;
    const rollingSkuAnalysisTableHeadEl = rollingSkuAnalysisTableEl ? rollingSkuAnalysisTableEl.querySelector('thead') : null;
    const top30OpportunitiesListEl = document.getElementById('top30OpportunitiesList');
    const dataFreshnessIndicatorEl = document.getElementById('dataFreshnessIndicator');
    const distributorStatusContainerEl = document.getElementById('distributorStatusContainer');

    const modalElement = document.getElementById('purchaseDetailModal');
    const purchaseDetailModal = modalElement ? new bootstrap.Modal(modalElement) : null;
    const modalTitleEl = document.getElementById('purchaseDetailModalLabel');
    const modalBodyEl = document.getElementById('purchaseDetailModalBody');

    // Chart instances
    let cadenceChartInstance = null;
    let revenueChartInstance = null;

    // State variables for sorting
    let currentYearlyProductData = [];
    let yearlyProductSortColumn = 'total_revenue_year';
    let yearlyProductSortDirection = 'desc';
    let currentRollingSkuData = [];
    let rollingSkuSortColumn = 'current_12m_rev';
    let rollingSkuSortDirection = 'desc';

    // Helper Functions
    function normalizeDateToCurrentYear(dateString) {
        if (!dateString) return null;
        try {
            // --- FIX: Parse the date string by splitting it to avoid timezone issues ---
            // "2024-06-24T00:00:00" -> ["2024", "06", "24"]
            const parts = dateString.split('T')[0].split('-');
            // new Date(year, monthIndex, day) - month is 0-indexed!
            const originalDate = new Date(parts[0], parts[1] - 1, parts[2]);
            // --- END FIX ---
            
            if (isNaN(originalDate.getTime())) return null;
    
            const currentYear = new Date().getFullYear();
            originalDate.setFullYear(currentYear); // This correctly shifts the year
            return originalDate;
        } catch (e) {
            console.error("Error normalizing date:", dateString, e);
            return null;
        }
    }
    function calculateAverage(values) {
        const validValues = values.filter(val => val !== null && val !== undefined && !isNaN(val));
        if (validValues.length === 0) return NaN;
        const sum = validValues.reduce((total, val) => total + val, 0);
        return sum / validValues.length;
    }
    function preprocessRevenueData(revenueData) {
        if (!revenueData || !Array.isArray(revenueData.years) || !Array.isArray(revenueData.revenues)) return null;
        const processedData = JSON.parse(JSON.stringify(revenueData));
        const currentYear = new Date().getFullYear().toString();
        const currentYearIndex = processedData.years.indexOf(currentYear);
        processedData.currentYear = currentYear;
        processedData.currentYearIndex = currentYearIndex;
        processedData.previousYear = (parseInt(currentYear) - 1).toString();
        processedData.previousYearIndex = processedData.years.indexOf(processedData.previousYear);
        processedData.growthRates = [];
        for (let i = 1; i < processedData.revenues.length; i++) {
            const currentRevenue = processedData.revenues[i];
            const prevRevenue = processedData.revenues[i - 1];
            if (prevRevenue && prevRevenue > 0 && currentRevenue !== null) {
                const growthPercent = ((currentRevenue - prevRevenue) / prevRevenue) * 100;
                processedData.growthRates.push(growthPercent);
            } else { processedData.growthRates.push(null); }
        }
        processedData.growthRates.unshift(null);
        return processedData;
    }
    function showLoading(element, isLoading) {
        if (element) element.style.display = isLoading ? 'flex' : 'none';
        else console.warn("showLoading called on a null element.");
    }
    function showError(element, message) {
         if (element) {
             element.textContent = message || "Error loading data.";
             element.style.display = 'block';
         } else console.error("showError called on a null element for message:", message);
    }
    function hideError(element) { if(element) element.style.display = 'none'; }


    // --- Main Data Fetch Function ---
    async function fetchAccountDetails() {
        if (!accountCode) {
            showError(pageErrorIndicator, "Account identifier is missing or invalid.");
            showLoading(pageLoadingIndicator, false); return;
        }
        showLoading(pageLoadingIndicator, true);
        if(cadenceChartLoadingEl) showLoading(cadenceChartLoadingEl, true);
        if(revenueChartLoadingEl) showLoading(revenueChartLoadingEl, true);
        if(productHistoryLoadingEl) showLoading(productHistoryLoadingEl, true);
        if(yearlyProductSummaryLoadingEl) showLoading(yearlyProductSummaryLoadingEl, true);
        if(rollingAnalysisLoadingEl) showLoading(rollingAnalysisLoadingEl, true);

        if(pageErrorIndicator) hideError(pageErrorIndicator);
        if(cadenceChartErrorEl) hideError(cadenceChartErrorEl);
        if(revenueChartErrorEl) hideError(revenueChartErrorEl);
        if(productHistoryErrorEl) hideError(productHistoryErrorEl);
        if(yearlyProductSummaryErrorEl) hideError(yearlyProductSummaryErrorEl);
        if(rollingAnalysisErrorEl) hideError(rollingAnalysisErrorEl);

        if(accountDetailContent) accountDetailContent.style.display = 'none';
        if (growthOpportunityCardEl) growthOpportunityCardEl.style.display = 'none';
        if(rollingAnalysisContentEl) rollingAnalysisContentEl.style.display = 'none';
        if(dataFreshnessIndicatorEl) dataFreshnessIndicatorEl.style.display = 'none';

        const detailApiUrl = `/api/strategic/accounts/${accountCode}/details`;
        try {
            const response = await fetch(detailApiUrl);
            if (!response.ok) {
                let errorMsg = `Error ${response.status}`;
                try { const errData = await response.json(); errorMsg += `: ${errData.error || response.statusText}`; }
                catch(e){ errorMsg += `: ${response.statusText}`; }
                throw new Error(errorMsg);
            }
            const data = await response.json();
            window.fullFetchedData = data;

            if (data.distributor_uploads && data.distributor_uploads.length > 0) {
                renderDataFreshness(data.distributor_uploads);
            }

            if (!data || typeof data !== 'object' || !data.prediction || typeof data.prediction !== 'object') {
                throw new Error("Received invalid or incomplete account data from server.");
            }

            populatePredictionMetrics(data.prediction, data.historical_summary, data.chart_data?.detailed_product_history_by_quarter);
            
            if(data.analysis) {
                populateAnalysisMetrics(data.analysis);
            } else {
                 if (revenueTrendEl) revenueTrendEl.innerHTML = 'N/A';
                 if (revenueTrendR2El) revenueTrendR2El.textContent = '';
                 if (revenueForecastEl) revenueForecastEl.textContent = 'N/A';
            }

            if (data.growth_engine) {
                if (growthOpportunityCardEl) growthOpportunityCardEl.style.display = 'block';
                populateGrowthEngine(data.growth_engine);
            } else {
                if (growthOpportunityCardEl) growthOpportunityCardEl.style.display = 'block';
                if (growthEngineMessageEl) {
                    growthEngineMessageEl.textContent = "Growth opportunity data not available for this account.";
                    growthEngineMessageEl.className = 'mb-2 alert alert-secondary small';
                    growthEngineMessageEl.style.display = 'block';
                }
                if (growthEngineDetailsEl) growthEngineDetailsEl.style.display = 'none';
                if (growthEngineStatusBadgeEl) growthEngineStatusBadgeEl.style.display = 'none';
            }

            if (data.chart_data) {
                renderDetailedPurchaseTimelineChart(data.chart_data?.cy_purchase_timeline, data.chart_data?.py_purchase_timeline, data.prediction?.next_expected_purchase_date);
                const processedRevenueData = preprocessRevenueData(data.chart_data.revenue_history);
                renderRevenueChart(processedRevenueData, data.analysis, data.prediction.yep_revenue);

                let initialYearToDisplay = null;
                let availableYears = [];

                if (data.chart_data.detailed_product_history_by_quarter &&
                    Object.keys(data.chart_data.detailed_product_history_by_quarter).length > 0) {
                    availableYears = Object.keys(data.chart_data.detailed_product_history_by_quarter).sort((a,b) => parseInt(b) - parseInt(a));
                } else if (data.chart_data.yearly_product_summary_table_data &&
                           Object.keys(data.chart_data.yearly_product_summary_table_data).length > 0) {
                    availableYears = Object.keys(data.chart_data.yearly_product_summary_table_data).sort((a,b) => parseInt(b) - parseInt(a));
                }

                if (availableYears.length > 0) {
                    initialYearToDisplay = availableYears[0];
                    populateSharedYearDropdowns(availableYears, initialYearToDisplay);

                    if (data.chart_data.detailed_product_history_by_quarter) {
                        renderProductHistoryForYear(initialYearToDisplay, data.chart_data.detailed_product_history_by_quarter);
                    } else {
                        if(productHistoryContentEl) productHistoryContentEl.innerHTML = '<p class="text-muted text-center">No detailed product history available.</p>';
                    }
                    if (data.chart_data.yearly_product_summary_table_data) {
                        currentYearlyProductData = data.chart_data.yearly_product_summary_table_data[initialYearToDisplay] || [];
                        renderYearlyProductSummaryTable(initialYearToDisplay, currentYearlyProductData);
                        addYearlyProductTableSorting();
                    } else {
                        showError(yearlyProductSummaryErrorEl, "Yearly product summary data not found.");
                        if(yearlyProductSummaryTableTotalsRowEl) yearlyProductSummaryTableTotalsRowEl.style.display = 'none';
                        currentYearlyProductData = [];
                    }
                } else {
                    populateSharedYearDropdowns([], null);
                    if(productHistoryContentEl) productHistoryContentEl.innerHTML = '<p class="text-muted text-center">No product history data available for any year.</p>';
                    if (yearlyProductSummaryTableBodyEl) {
                        yearlyProductSummaryTableBodyEl.innerHTML = `<tr><td colspan="4" class="text-center text-muted p-3">No product summary data available for any year.</td></tr>`;
                    }
                    if(yearlyProductSummaryTableTotalsRowEl) yearlyProductSummaryTableTotalsRowEl.style.display = 'none';
                    if(productHistoryErrorEl) showError(productHistoryErrorEl, "No years with product data found.");
                    if(yearlyProductSummaryErrorEl) showError(yearlyProductSummaryErrorEl, "No years with product data found.");
                    currentYearlyProductData = [];
                }

            } else {
                populateSharedYearDropdowns([], null);
                if(cadenceChartErrorEl) showError(cadenceChartErrorEl, "No timeline chart data available.");
                if(revenueChartErrorEl) showError(revenueChartErrorEl, "No revenue history data available.");
                if(productHistoryErrorEl) showError(productHistoryErrorEl, "Product history data missing.");
                if(yearlyProductSummaryErrorEl) showError(yearlyProductSummaryErrorEl, "Yearly product summary data missing.");
                if(yearlyProductSummaryTableTotalsRowEl) yearlyProductSummaryTableTotalsRowEl.style.display = 'none';
                currentYearlyProductData = [];
            }

            if (rollingAnalysisContentEl) {
                if (data.rolling_sku_analysis && Array.isArray(data.rolling_sku_analysis)) {
                    showLoading(rollingAnalysisLoadingEl, false);
                    hideError(rollingAnalysisErrorEl);
                    rollingAnalysisContentEl.style.display = 'block';

                    currentRollingSkuData = data.rolling_sku_analysis;
                    renderSkuAnalysis(currentRollingSkuData, data.prediction.missing_top_products);

                    addRollingSkuTableSorting();

                } else {
                    showLoading(rollingAnalysisLoadingEl, false);
                    showError(rollingAnalysisErrorEl, 'Rolling 12-month SKU analysis data is not available for this account.');
                }
            }

            if(accountDetailContent) accountDetailContent.style.display = 'block';
        } catch (error) {
            console.error("Failed to fetch or process account details:", error);
            if(pageErrorIndicator) showError(pageErrorIndicator, `Failed to load details: ${error.message}`);
            if(cadenceChartLoadingEl) showLoading(cadenceChartLoadingEl, false);
            if(cadenceChartErrorEl) showError(cadenceChartErrorEl, "Failed to load chart data.");
            if(revenueChartLoadingEl) showLoading(revenueChartLoadingEl, false);
            if(revenueChartErrorEl) showError(revenueChartErrorEl, "Failed to load chart data.");
            if(productHistoryLoadingEl) showLoading(productHistoryLoadingEl, false);
            if(productHistoryErrorEl) showError(productHistoryErrorEl, "Failed to load product history.");
            if(yearlyProductSummaryLoadingEl) showLoading(yearlyProductSummaryLoadingEl, false);
            if(yearlyProductSummaryErrorEl) showError(yearlyProductSummaryErrorEl, `Failed to load yearly summary: ${error.message}`);
            if(yearlyProductSummaryTableTotalsRowEl) yearlyProductSummaryTableTotalsRowEl.style.display = 'none';
            if(rollingAnalysisLoadingEl) showLoading(rollingAnalysisLoadingEl, false);
            if(rollingAnalysisErrorEl) showError(rollingAnalysisErrorEl, "Failed to load SKU analysis.");

            if (growthOpportunityCardEl) growthOpportunityCardEl.style.display = 'block';
            if (growthEngineMessageEl) {
                growthEngineMessageEl.textContent = "Error loading growth suggestions.";
                growthEngineMessageEl.className = 'mb-2 alert alert-danger small';
                growthEngineMessageEl.style.display = 'block';
             }
            if (growthEngineDetailsEl) growthEngineDetailsEl.style.display = 'none';
            if (growthEngineStatusBadgeEl) growthEngineStatusBadgeEl.style.display = 'none';
            populateSharedYearDropdowns([], null);
            currentYearlyProductData = [];
        } finally {
            if(pageLoadingIndicator) showLoading(pageLoadingIndicator, false);
            if(productHistoryLoadingEl) showLoading(productHistoryLoadingEl, false);
            if(yearlyProductSummaryLoadingEl) showLoading(yearlyProductSummaryLoadingEl, false);
            if(rollingAnalysisLoadingEl) showLoading(rollingAnalysisLoadingEl, false);
        }
    }

    function getattr(obj, key, defaultValue = undefined) {
        if (obj && typeof obj === 'object' && key in obj) {
            return obj[key];
        }
        return defaultValue;
    }

    function renderDataFreshness(uploadData) {
        if (!dataFreshnessIndicatorEl || !distributorStatusContainerEl) return;
    
        distributorStatusContainerEl.innerHTML = ''; // Clear previous
    
        uploadData.sort((a, b) => a.distributor.localeCompare(b.distributor));
    
        const fragments = [];
        uploadData.forEach(dist => {
            // Build the cadence string in parentheses, or an empty string if cadence is 'daily' or 'unknown'
            let cadenceText = '';
            if (dist.cadence && dist.cadence !== 'daily' && dist.cadence !== 'unknown') {
                cadenceText = ` <span class="text-muted small">(${dist.cadence})</span>`;
            }
    
            // Create the "DISTRIBUTOR: MM/DD (cadence)" string
            const textFragment = `
                <strong class="me-1">${dist.distributor}:</strong>${formatDate(dist.last_upload, 'short')}${cadenceText}
            `;
            fragments.push(textFragment);
        });
    
        // Join all fragments with a separator
        distributorStatusContainerEl.innerHTML = fragments.join('<span class="mx-2 text-muted">|</span>');
    
        dataFreshnessIndicatorEl.style.display = 'block';
    }

    function populatePredictionMetrics(pred, historicalSummary, detailedProductHistory) {
        if (!pred || typeof pred !== 'object') {
            console.error("populatePredictionMetrics: invalid prediction data provided.", pred);
            if (accountNameEl) accountNameEl.textContent = 'Account Detail (Error)';
            if (ctaTop30CoverageEl) ctaTop30CoverageEl.textContent = 'N/A';
            if (detailGrowthPacePctEl) detailGrowthPacePctEl.textContent = 'N/A';
            return;
        }

        if (accountNameEl) accountNameEl.textContent = getattr(pred, 'name', 'Account Detail');

        const currentYear = new Date().getFullYear();
        const py = currentYear - 1;
        const currentYearStr = currentYear.toString();

        if (detailBaseCardCodeEl) detailBaseCardCodeEl.textContent = getattr(pred, 'base_card_code', 'N/A');
        if (detailFullAddressEl) detailFullAddressEl.textContent = getattr(pred, 'full_address', 'N/A');
        if (detailSalesRepNameEl) {
            detailSalesRepNameEl.textContent = getattr(pred, 'sales_rep_name',
                (getattr(pred, 'sales_rep') ? `ID: ${pred.sales_rep}` : 'N/A')
            );
        }
        /*
        if (detailDistributorNameEl) {
            const distributors = getattr(pred, 'distributors', null);
            if (distributors && Array.isArray(distributors) && distributors.length > 0) {
                detailDistributorNameEl.textContent = distributors.join(', ');
            } else {
                detailDistributorNameEl.textContent = getattr(pred, 'distributor', 'N/A');
            }
        }
        */

        if (detailPyYearLabelPerfEl) detailPyYearLabelPerfEl.textContent = py.toString();

        const pyHistData = Array.isArray(historicalSummary) ?
                           historicalSummary.find(h => h && typeof h === 'object' && h.year === py)
                           : null;

        const pyRevenueValue = pyHistData?.revenue ?? getattr(pred, 'py_total_revenue', 0.0);

        if (detailPyTotalSalesLabelPerfEl) detailPyTotalSalesLabelPerfEl.textContent = `${py} Total Sales`;
        if (detailPyTotalRevenuePerfEl) detailPyTotalRevenuePerfEl.textContent = formatCurrency(pyRevenueValue);

        if (detailCyYearLabelPerfEl) detailCyYearLabelPerfEl.textContent = currentYear.toString();
        if (detailCytdRevenueValuePerfEl) detailCytdRevenueValuePerfEl.textContent = formatCurrency(getattr(pred, 'cytd_revenue', 0.0));
        if (detailYepRevenueValuePerfEl) detailYepRevenueValuePerfEl.textContent = formatCurrency(getattr(pred, 'yep_revenue', 0.0));

        let latestQuarterCoverageValue = null;
        if (detailedProductHistory && detailedProductHistory[currentYearStr]) {
            const quarters = ["Q4", "Q3", "Q2", "Q1"];
            for (const qtr of quarters) {
                const quarterData = detailedProductHistory[currentYearStr][qtr];
                if (quarterData && quarterData.metrics &&
                    (getattr(quarterData.metrics, 'total_items_in_quarter', 0) > 0 || getattr(quarterData.metrics, 'total_revenue_in_quarter', 0) > 0) &&
                    quarterData.metrics.count_top_30_skus_carried !== undefined
                   ) {
                    const carriedInQtr = parseInt(quarterData.metrics.count_top_30_skus_carried) || 0;
                    const totalTopSkusPossible = 30;
                    if (totalTopSkusPossible > 0) {
                        latestQuarterCoverageValue = (carriedInQtr / totalTopSkusPossible) * 100;
                        break;
                    }
                }
            }
        }

        if (latestQuarterCoverageValue === null) {
            const storedCoverage = getattr(pred, 'product_coverage_percentage', null);
            if (storedCoverage !== null && !isNaN(parseFloat(storedCoverage))) {
                latestQuarterCoverageValue = parseFloat(storedCoverage);
            } else {
                latestQuarterCoverageValue = 0;
            }
        }

        if (ctaTop30CoverageEl) {
            ctaTop30CoverageEl.textContent = `${formatValue(latestQuarterCoverageValue, 0)}%`;
        }

        if (btnGenerateMarketingEmailEl) {
            btnGenerateMarketingEmailEl.onclick = function() { alert('Marketing Email generation coming soon!'); };
        }
        if (btnGenerateDiscountEmailEl) {
            btnGenerateDiscountEmailEl.onclick = function() { alert('Discount Email generation coming soon!'); };
        }
        if (btnGenerateReminderEmailEl) {
            btnGenerateReminderEmailEl.onclick = function() { alert('Reminder Email generation coming soon!'); };
        }

        let pacePctText = '--%';
        const paceVsLyValue = getattr(pred, 'pace_vs_ly', null);

        if (paceVsLyValue !== null && pyRevenueValue !== null && !isNaN(parseFloat(paceVsLyValue)) ) {
            const pyRevNumeric = parseFloat(pyRevenueValue);
            if (!isNaN(pyRevNumeric)) {
                if (pyRevNumeric > 0) {
                    //const pacePercent = (parseFloat(paceVsLyValue) / pyRevNumeric) * 100;
                    const pacePercent = (parseFloat(paceVsLyValue));
                    pacePctText = `${pacePercent >= 0 ? '+' : ''}${formatValue(pacePercent, 1)}%`;
                } else if (getattr(pred, 'yep_revenue', 0) > 0) {
                    pacePctText = 'New Growth';
                } else {
                    pacePctText = '0.0%';
                }
            } else {
                pacePctText = 'N/A';
            }
        } else if (getattr(pred, 'yep_revenue', 0) > 0 && (pyRevenueValue === null || parseFloat(pyRevenueValue) === 0 || isNaN(parseFloat(pyRevenueValue)))) {
            pacePctText = 'New Growth';
        } else {
            pacePctText = 'N/A';
        }

        if (detailGrowthPacePctEl) {
            detailGrowthPacePctEl.textContent = pacePctText;
            let color = 'var(--theme-text-muted)';
            let fontSize = '2.2rem';
            const numericPaceVsLy = parseFloat(paceVsLyValue);

            if (pacePctText === 'New Growth') {
                color = 'var(--bs-info)';
                fontSize = '1.8rem';
            } else if (pacePctText === 'N/A' || pacePctText === '0.0%') {
                if (pacePctText === 'N/A') fontSize = '1.8rem';
            } else if (!isNaN(numericPaceVsLy)) {
                if (numericPaceVsLy > 0.001) { color = 'var(--bs-success)'; }
                else if (numericPaceVsLy < -0.001) { color = 'var(--bs-danger)'; }
            }
            detailGrowthPacePctEl.style.color = color;
            detailGrowthPacePctEl.style.fontSize = fontSize;
        }
    }

    function populateAnalysisMetrics(analysis) {
        if (!revenueTrendEl || !revenueTrendR2El || !revenueForecastEl) { console.warn("One or more analysis DOM elements are missing."); return; }
        let trendText = 'N/A'; let r2Text = ''; let forecastText = 'N/A'; let modelTypeText = '';

        if (analysis && analysis.revenue_trend) {
            const trend = analysis.revenue_trend;
            if (trend.model_type && trend.model_type !== "N/A") {
                modelTypeText = `<span class="text-muted small fst-italic">(${trend.model_type})</span>`;
            }

            if (trend.slope !== null && !isNaN(trend.slope) && typeof trend.slope === 'number') {
                 const trendValueAbs = Math.abs(trend.slope); const formattedSlope = formatCurrency(trendValueAbs, 0);
                 const flatThreshold = Math.max(10, 0.005 * (window.fullFetchedData?.prediction?.account_total || 2000));
                 let trendDirection = 'Flat'; let trendClass = 'trend-neutral';
                 if (trend.slope > flatThreshold) { trendDirection = 'Increasing'; trendClass = 'trend-positive'; }
                 else if (trend.slope < -flatThreshold) { trendDirection = 'Decreasing'; trendClass = 'trend-negative'; }
                 trendText = `<span class="trend-value ${trendClass}">${trend.slope < 0 ? '-' : ''}${formattedSlope}/year</span> (${trendDirection})`;
            } else {
                trendText = `Trend N/A ${modelTypeText}`;
                if (!(trend.model_type && trend.model_type.toLowerCase().includes("data"))) {
                    trendText = `Trend N/A`;
                }
            }

            if (trend.r_squared !== null && !isNaN(trend.r_squared) && typeof trend.r_squared === 'number') {
                 let fitDescription = "Very Weak Fit";
                 if (trend.r_squared >= 0.8) fitDescription = "Strong Fit"; else if (trend.r_squared >= 0.6) fitDescription = "Good Fit";
                 else if (trend.r_squared >= 0.4) fitDescription = "Moderate Fit"; else if (trend.r_squared >= 0.2) fitDescription = "Weak Fit";
                 if (trend.slope !== null && !isNaN(trend.slope)) { r2Text = `(R² = ${formatValue(trend.r_squared, 2)} - ${fitDescription})`; }
                 else { r2Text = '(Trend Fit N/A)'; }
            } else if (trend.slope !== null && !isNaN(trend.slope)) {
                r2Text = '(Trend Fit N/A)';
            } else {
                r2Text = '';
            }

            if (trend.forecast_next !== null && !isNaN(trend.forecast_next) && typeof trend.forecast_next === 'number') {
                forecastText = formatCurrency(trend.forecast_next, 0);
                if (trend.forecast_method && trend.forecast_method === "linear_regression" && modelTypeText) {
                    forecastText += ` ${modelTypeText}`;
                }
            } else {
                if (trend.forecast_method === "insufficient_data" || trend.forecast_method === "insufficient_historical_data"){
                     forecastText = 'N/A <span class="text-muted small fst-italic">(Insufficient Data)</span>';
                } else if (trend.model_type && trend.model_type !== "N/A") {
                    forecastText = `Forecast N/A ${modelTypeText}`;
                } else {
                    forecastText = 'Forecast N/A';
                }
            }
        } else {
            trendText = 'Analysis Data Unavailable';
            forecastText = 'Analysis Data Unavailable';
            r2Text = '';
        }

        revenueTrendEl.innerHTML = trendText;
        revenueTrendR2El.textContent = r2Text;
        revenueForecastEl.innerHTML = forecastText;
    }

    function populateGrowthEngine(engineData) {
        if (!engineData || !growthEngineDetailsEl || !growthEngineMessageEl || !growthEngineStatusBadgeEl ||
            !targetYepDisplayEl || !additionalRevNeededDisplayContainerEl || !additionalRevNeededDisplayEl ||
            !suggestedNextPurchaseAmountDisplayEl) {
            console.error("GROWTH ENGINE ERROR: Missing essential data or DOM elements");
            if (growthEngineMessageEl) {
                growthEngineMessageEl.textContent = "Could not load growth suggestions (UI error).";
                growthEngineMessageEl.className = 'mb-2 alert alert-warning small';
                growthEngineMessageEl.style.display = 'block';
            }
            if (growthEngineDetailsEl) growthEngineDetailsEl.style.display = 'none';
            if (growthEngineStatusBadgeEl) growthEngineStatusBadgeEl.style.display = 'none';
            return;
        }

        growthEngineMessageEl.style.display = 'none';
        growthEngineMessageEl.className = 'mb-2 text-muted small fst-italic';
        growthEngineDetailsEl.style.display = 'none';
        growthEngineStatusBadgeEl.style.display = 'none';
        if (additionalRevNeededDisplayContainerEl) {
            additionalRevNeededDisplayContainerEl.style.display = 'none';
        }

        if (engineData.already_on_track) {
            growthEngineMessageEl.textContent = engineData.message || "Account is already on track or exceeding +1% YEP target!";
            growthEngineMessageEl.className = 'mb-2 alert alert-success small text-center';
            growthEngineMessageEl.style.display = 'block';
            growthEngineStatusBadgeEl.textContent = 'On Track!';
            growthEngineStatusBadgeEl.className = 'badge bg-success ms-2';
            growthEngineStatusBadgeEl.style.display = 'inline-block';

            targetYepDisplayEl.textContent = engineData.target_yep_plus_1_pct !== null ? formatCurrency(engineData.target_yep_plus_1_pct) : 'N/A';
            suggestedNextPurchaseAmountDisplayEl.textContent = 'Maintain Pace!';

            if (growthEngineRecommendedProductsEl) {
                growthEngineRecommendedProductsEl.innerHTML = '<div class="products-empty-state"><i class="fas fa-check-circle text-success"></i><p class="mb-0 text-success">Keep up the great work!</p></div>';
            }
            growthEngineDetailsEl.style.display = 'block';
            return;
        }

        growthEngineDetailsEl.style.display = 'block';

        if (engineData.message && !engineData.message.startsWith("Aim for ~")) {
            if (engineData.message === "Data insufficient for growth suggestion.") {
                growthEngineMessageEl.textContent = engineData.message;
                growthEngineMessageEl.className = 'mb-2 alert alert-warning small';
                growthEngineMessageEl.style.display = 'block';
            } else {
                growthEngineMessageEl.style.display = 'none';
            }
        } else {
            growthEngineMessageEl.style.display = 'none';
        }

        growthEngineStatusBadgeEl.textContent = 'Opportunity';
        growthEngineStatusBadgeEl.className = 'badge bg-primary ms-2';
        growthEngineStatusBadgeEl.style.display = 'inline-block';

        targetYepDisplayEl.textContent = engineData.target_yep_plus_1_pct !== null ? formatCurrency(engineData.target_yep_plus_1_pct) : 'N/A';

        if (engineData.additional_revenue_needed_eoy !== null && engineData.additional_revenue_needed_eoy > 0) {
            additionalRevNeededDisplayEl.textContent = formatCurrency(engineData.additional_revenue_needed_eoy);
            if (additionalRevNeededDisplayContainerEl) {
                additionalRevNeededDisplayContainerEl.style.display = 'block';
            }
        } else {
            if (additionalRevNeededDisplayContainerEl) {
                additionalRevNeededDisplayContainerEl.style.display = 'none';
            }
        }

        if (engineData.suggested_next_purchase_amount !== null) {
            suggestedNextPurchaseAmountDisplayEl.textContent = formatCurrency(engineData.suggested_next_purchase_amount);
        } else {
            suggestedNextPurchaseAmountDisplayEl.textContent = 'N/A';
        }

        if (growthEngineRecommendedProductsEl) {
            growthEngineRecommendedProductsEl.innerHTML = '';

            if (engineData.recommended_products && engineData.recommended_products.length > 0) {
                engineData.recommended_products.forEach(prod => {
                    const productDiv = document.createElement('div');
                    productDiv.className = 'product-item';

                    const reason = prod.reason || 'Recommended';
                    let badgeClass = 'bg-secondary';
                    let badgeIcon = '';

                    if (reason.includes('win-back') || reason.includes('last year')) {
                        badgeClass = 'bg-warning text-dark';
                        badgeIcon = '<i class="fas fa-undo"></i>';
                    } else if (reason.includes('Never purchased') || reason.includes('new product')) {
                        badgeClass = 'bg-success';
                        badgeIcon = '<i class="fas fa-plus"></i>';
                    } else if (reason.includes('years ago') || reason.includes('reactivation')) {
                        badgeClass = 'bg-info text-dark';
                        badgeIcon = '<i class="fas fa-history"></i>';
                    }

                    productDiv.innerHTML = `
                        <div class="product-name">${prod.description || prod.sku || 'N/A'}</div>
                        <div class="product-sku">SKU: ${prod.sku || 'N/A'}</div>
                        <span class="product-reason-badge badge ${badgeClass}">${badgeIcon}${reason}</span>
                    `;

                    growthEngineRecommendedProductsEl.appendChild(productDiv);
                });
            } else {
                growthEngineRecommendedProductsEl.innerHTML = `
                    <div class="products-empty-state">
                        <i class="fas fa-chart-line"></i>
                        <p class="mb-2 text-muted small"><strong>SPINS data recommendations coming soon</strong></p>
                    </div>
                    <div class="p-2 bg-light rounded border-start border-3 border-warning">
                        <div class="small text-muted mb-2"><strong>Example recommendations:</strong></div>
                        <div class="product-item" style="margin-bottom: 0.5rem;">
                            <div class="product-name">SKU X (Immune Support)</div>
                            <div class="product-sku">Category growing +15% in store's region</div>
                            <span class="product-reason-badge badge bg-danger"><i class="fas fa-exclamation-triangle"></i>Missing</span>
                        </div>
                        <div class="product-item" style="margin-bottom: 0.5rem;">
                            <div class="product-name">SKU Y (Sleep)</div>
                            <div class="product-sku">Category growing +10% in store's region</div>
                            <span class="product-reason-badge badge bg-warning text-dark"><i class="fas fa-exclamation-triangle"></i>Missing</span>
                        </div>
                        <div class="product-item">
                            <div class="product-name">SKU Z (Gummies)</div>
                            <div class="product-sku">Gummies trending +20% locally</div>
                            <span class="product-reason-badge badge bg-info text-dark"><i class="fas fa-info-circle"></i>Missing</span>
                        </div>
                    </div>
                `;
            }
        } else {
            console.warn("growthEngineRecommendedProductsEl not found in DOM!");
        }
    }

    function populateSharedYearDropdowns(yearsArray, selectedYear) {
        const dropdowns = [productHistoryYearSelectEl, yearlyProductSummaryYearSelectEl];
        dropdowns.forEach(dropdown => {
            if (!dropdown) return;
            dropdown.innerHTML = '';
            if (yearsArray.length === 0) {
                const option = document.createElement('option');
                option.value = ""; option.textContent = "N/A";
                dropdown.appendChild(option);
                dropdown.disabled = true;
            } else {
                yearsArray.forEach(year => {
                    const option = document.createElement('option');
                    option.value = year; option.textContent = year;
                    dropdown.appendChild(option);
                });
                dropdown.disabled = false;
                if (selectedYear) {
                    dropdown.value = selectedYear;
                }
            }
        });
        if (productHistoryYearSelectEl) productHistoryYearSelectEl.removeEventListener('change', handleSharedYearChange);
        if (yearlyProductSummaryYearSelectEl) yearlyProductSummaryYearSelectEl.removeEventListener('change', handleSharedYearChange);
        if (productHistoryYearSelectEl && yearsArray.length > 0) productHistoryYearSelectEl.addEventListener('change', handleSharedYearChange);
        if (yearlyProductSummaryYearSelectEl && yearsArray.length > 0) yearlyProductSummaryYearSelectEl.addEventListener('change', handleSharedYearChange);
    }

    function handleSharedYearChange(event) {
        const selectedYear = event.target.value;
        if (event.target === productHistoryYearSelectEl && yearlyProductSummaryYearSelectEl) yearlyProductSummaryYearSelectEl.value = selectedYear;
        else if (event.target === yearlyProductSummaryYearSelectEl && productHistoryYearSelectEl) productHistoryYearSelectEl.value = selectedYear;

        if (window.fullFetchedData && window.fullFetchedData.chart_data) {
            if (window.fullFetchedData.chart_data.detailed_product_history_by_quarter) {
                renderProductHistoryForYear(selectedYear, window.fullFetchedData.chart_data.detailed_product_history_by_quarter);
            } else { if(productHistoryErrorEl) showError(productHistoryErrorEl, "Quarterly product data missing for update."); }

            if (window.fullFetchedData.chart_data.yearly_product_summary_table_data) {
                currentYearlyProductData = window.fullFetchedData.chart_data.yearly_product_summary_table_data[selectedYear] || [];
                yearlyProductSortColumn = 'total_revenue_year';
                yearlyProductSortDirection = 'desc';
                renderYearlyProductSummaryTable(selectedYear, currentYearlyProductData);
            } else {
                if(yearlyProductSummaryErrorEl) showError(yearlyProductSummaryErrorEl, "Yearly product data missing for update.");
                if(yearlyProductSummaryTableTotalsRowEl) yearlyProductSummaryTableTotalsRowEl.style.display = 'none';
                currentYearlyProductData = [];
            }
        }
    }

    function renderProductHistoryForYear(year, allYearsData) {
        if (!productHistoryContentEl) { return; }
        showLoading(productHistoryLoadingEl, true);
        hideError(productHistoryErrorEl);
        productHistoryContentEl.innerHTML = '';

        if (!year || !allYearsData || !allYearsData[year]) {
            productHistoryContentEl.innerHTML = `<p class="text-muted text-center mt-3">No detailed product data available for ${year}.</p>`;
            showLoading(productHistoryLoadingEl, false);
            return;
        }

        const yearDataByQuarter = allYearsData[year];
        const quarters = ["Q1", "Q2", "Q3", "Q4"];
        let hasAnyDataForSelectedYearOverall = false;

        // --- Build the content for each quarter first ---
        const quarterlyContentMap = {};

        quarters.forEach(qtrKey => {
            const quarterDetail = yearDataByQuarter[qtrKey] || {};
            const productList = (quarterDetail.products || []).sort((a, b) => (b.revenue || 0) - (a.revenue || 0));
            const metrics = quarterDetail.metrics || {};
            let quarterContentHtml = '';

            // Product List HTML
            const productListHtml = productList.length > 0
                ? `<ul class="list-unstyled mb-0">${productList.map(product => {
                    const description = product.description || 'N/A';
                    const sku = product.sku || 'N/A';
                    const quantity = product.quantity || 0;
                    const revenue = product.revenue || 0.0;
                    const isTop30 = product.is_top_30;
                    let itemStyle = "border-radius: 3px; margin-bottom: 0.5rem; padding: 0.3rem;";
                    if (isTop30) {
                        itemStyle += "background-color: var(--theme-green-x-lighter, #e8f5e9); border-left: 4px solid var(--theme-green, #4CAF50); padding-left: 8px;";
                    }
                    // Simplified status for brevity
                    let statusDisplay = '';
                    if(product.status_in_qtr === "Newly Added this Qtr (vs Prev Qtr)") {
                        statusDisplay = `<span class="badge bg-warning text-dark me-1" style="font-size:0.6rem;">New</span>`;
                    }
                    return `<li class="product-history-item" style="${itemStyle}"><div>${statusDisplay}<strong class="fw-bold">${description}</strong></div><div class="small text-muted" style="font-size: 0.75rem;">SKU: ${sku}</div><div class="small" style="font-size: 0.75rem;">Qty: ${formatValue(quantity,0)} | Revenue: ${formatCurrency(revenue)}</div></li>`;
                }).join('')}</ul>`
                : `<p class="text-muted small fst-italic text-center my-3">No products purchased this quarter.</p>`;
            
            if (productList.length > 0) hasAnyDataForSelectedYearOverall = true;

            // Metrics List HTML
            const generateMetricProductListHtml = (products) => {
                if (!products || products.length === 0) return `<div class="text-muted small fst-italic p-1">None.</div>`;
                return `<ul class="list-unstyled small mt-1 mb-0 ps-2" style="font-size: 0.9em;">${products.map(p =>
                    `<li class="mb-1 p-1 border-bottom border-light"><div><strong>${p.description || 'N/A'}</strong></div><div class="text-muted">SKU: ${p.sku || 'N/A'}</div></li>`
                ).join('')}</ul>`;
            };
            const metricsHtml = Object.keys(metrics).length > 0
                ? `<details class="mb-1 product-metric-details"><summary class="fw-bold" style="cursor: pointer;">Added SKUs: ${metrics.items_added_details?.length ?? 0}</summary><div class="collapsible-product-list p-1 border rounded bg-light mt-1">${generateMetricProductListHtml(metrics.items_added_details)}</div></details>
                   <details class="mb-1 product-metric-details"><summary class="fw-bold text-danger" style="cursor: pointer;">Dropped SKUs: ${metrics.items_dropped_details?.length ?? 0}</summary><div class="collapsible-product-list p-1 border rounded bg-light mt-1">${generateMetricProductListHtml(metrics.items_dropped_details)}</div></details>
                   <details class="mb-1 product-metric-details"><summary class="fw-bold text-success" style="cursor: pointer;">Top 30 SKUs Carried: ${metrics.count_top_30_skus_carried ?? 0}</summary><div class="collapsible-product-list p-1 border rounded bg-light mt-1">${generateMetricProductListHtml(metrics.top_30_skus_carried_details)}</div></details>`
                : `<p class="text-muted small fst-italic">Metrics unavailable.</p>`;

            quarterContentHtml = `
                <div class="product-list-area p-2" style="max-height: 250px; overflow-y: auto; border-bottom: 1px solid var(--bs-border-color);">
                    ${productListHtml}
                </div>
                <div class="quarterly-summary-metrics quarter-metrics-summary p-2 mt-2" style="font-size: 0.75rem; line-height: 1.5;">
                    ${metricsHtml}
                </div>
            `;
            quarterlyContentMap[qtrKey] = quarterContentHtml;
        });

        // --- Now, build the final responsive HTML ---
        if (!hasAnyDataForSelectedYearOverall) {
            productHistoryContentEl.innerHTML = `<p class="text-muted text-center mt-3">No products purchased in ${year}.</p>`;
        } else {
            // Mobile Tabs View
            const mobileTabsNav = quarters.map((qtrKey, index) =>
                `<li class="nav-item" role="presentation"><button class="nav-link ${index === 0 ? 'active' : ''}" id="tab-${qtrKey}" data-bs-toggle="tab" data-bs-target="#pane-${qtrKey}" type="button" role="tab">${qtrKey}</button></li>`
            ).join('');

            const mobileTabsContent = quarters.map((qtrKey, index) =>
                `<div class="tab-pane fade show ${index === 0 ? 'active' : ''}" id="pane-${qtrKey}" role="tabpanel"><div class="metric-card h-100">${quarterlyContentMap[qtrKey]}</div></div>`
            ).join('');

            const mobileHtml = `
                <div class="d-lg-none">
                    <ul class="nav nav-tabs nav-fill" id="quarterlyTabs" role="tablist">${mobileTabsNav}</ul>
                    <div class="tab-content" id="quarterlyTabsContent">${mobileTabsContent}</div>
                </div>
            `;

            // Desktop Grid View
            const desktopGridColumns = quarters.map(qtrKey =>
                `<div class="col-md-6 col-lg-3 mb-3 d-flex flex-column">
                    <div class="metric-card h-100">
                        <div class="metric-card-header">${qtrKey} ${year}</div>
                        ${quarterlyContentMap[qtrKey]}
                    </div>
                </div>`
            ).join('');

            const desktopHtml = `
                <div class="d-none d-lg-block">
                    <div class="row g-3">${desktopGridColumns}</div>
                </div>
            `;

            productHistoryContentEl.innerHTML = mobileHtml + desktopHtml;
        }

        showLoading(productHistoryLoadingEl, false);
    }


    function renderYearlyProductSummaryTable(selectedYear, productsForSelectedYear) {
        if (!yearlyProductSummaryTableBodyEl || !yearlyTotalQuantityCellEl || !yearlyTotalRevenueCellEl || !yearlyTotalsForYearEl || !yearlyProductSummaryTableTotalsRowEl) {
            console.error("Yearly product summary table or footer elements not found.");
            if (yearlyProductSummaryErrorEl) showError(yearlyProductSummaryErrorEl, "UI elements for yearly summary table are missing.");
            return;
        }

        showLoading(yearlyProductSummaryLoadingEl, true);
        hideError(yearlyProductSummaryErrorEl);
        yearlyProductSummaryTableBodyEl.innerHTML = '';
        yearlyProductSummaryTableTotalsRowEl.style.display = 'none';

        if (!productsForSelectedYear || productsForSelectedYear.length === 0) {
            showLoading(yearlyProductSummaryLoadingEl, false);
            const message = `No products purchased in ${selectedYear}.`;
            yearlyProductSummaryTableBodyEl.innerHTML = `<tr><td colspan="4" class="text-center text-muted p-3">${message}</td></tr>`;
            updateSortIndicators();
            return;
        }

        const sortedData = [...productsForSelectedYear].sort((a, b) => {
            let valA = a[yearlyProductSortColumn];
            let valB = b[yearlyProductSortColumn];
            if (yearlyProductSortColumn === 'description' || yearlyProductSortColumn === 'sku') {
                valA = (valA || '').toString().toLowerCase();
                valB = (valB || '').toString().toLowerCase();
                return yearlyProductSortDirection === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
            } else {
                valA = parseFloat(valA || 0);
                valB = parseFloat(valB || 0);
                return yearlyProductSortDirection === 'asc' ? valA - valB : valB - valA;
            }
        });

        let cumulativeTotalQuantity = 0;
        let cumulativeTotalRevenue = 0;

        sortedData.forEach(product => {
            const row = yearlyProductSummaryTableBodyEl.insertRow();

            const descriptionCell = row.insertCell();
            let descriptionHtml = product.description || 'N/A';
            if (product.is_top_30) {
                descriptionHtml += ` <i class="fas fa-star text-warning ms-1" title="Top 30 Product"></i>`;
            }
            descriptionCell.innerHTML = descriptionHtml;

            row.insertCell().textContent = product.sku || 'N/A';

            const qtyCell = row.insertCell();
            const currentQty = product.total_quantity_year || 0;
            qtyCell.textContent = formatValue(currentQty, 0);
            qtyCell.classList.add('text-end');
            cumulativeTotalQuantity += currentQty;

            const revCell = row.insertCell();
            const currentRev = product.total_revenue_year || 0.0;
            revCell.textContent = formatCurrency(currentRev);
            revCell.classList.add('text-end');
            cumulativeTotalRevenue += currentRev;
        });

        if (yearlyTotalsForYearEl) yearlyTotalsForYearEl.textContent = selectedYear;
        if (yearlyTotalQuantityCellEl) yearlyTotalQuantityCellEl.textContent = formatValue(cumulativeTotalQuantity, 0);
        if (yearlyTotalRevenueCellEl) yearlyTotalRevenueCellEl.textContent = formatCurrency(cumulativeTotalRevenue);
        if (yearlyProductSummaryTableTotalsRowEl) yearlyProductSummaryTableTotalsRowEl.style.display = '';

        updateSortIndicators();
        showLoading(yearlyProductSummaryLoadingEl, false);
    }

    function addYearlyProductTableSorting() {
        if (!yearlyProductSummaryTableHeadEl) return;
        const headers = yearlyProductSummaryTableHeadEl.querySelectorAll('th[data-column]');
        headers.forEach(th => {
            th.removeEventListener('click', handleYearlyProductSort);
            th.addEventListener('click', handleYearlyProductSort);
        });
    }

    function handleYearlyProductSort(event) {
        const newSortColumn = event.currentTarget.dataset.column;
        if (!newSortColumn) return;

        if (yearlyProductSortColumn === newSortColumn) {
            yearlyProductSortDirection = yearlyProductSortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            yearlyProductSortColumn = newSortColumn;
            yearlyProductSortDirection = (newSortColumn === 'description' || newSortColumn === 'sku') ? 'asc' : 'desc';
        }
        const selectedYear = yearlyProductSummaryYearSelectEl ? yearlyProductSummaryYearSelectEl.value : null;
        if (selectedYear && currentYearlyProductData) {
             renderYearlyProductSummaryTable(selectedYear, currentYearlyProductData);
        }
    }

    function updateSortIndicators() {
        if (!yearlyProductSummaryTableHeadEl) return;
        const headers = yearlyProductSummaryTableHeadEl.querySelectorAll('th[data-column]');
        headers.forEach(th => {
            const indicator = th.querySelector('.sort-indicator');
            if (indicator) {
                th.classList.remove('sort-active');
                if (th.dataset.column === yearlyProductSortColumn) {
                    indicator.innerHTML = yearlyProductSortDirection === 'asc' ? '<i class="fas fa-arrow-up"></i>' : '<i class="fas fa-arrow-down"></i>';
                    th.classList.add('sort-active');
                } else {
                    indicator.innerHTML = '<i class="fas fa-sort" style="opacity: 0.3;"></i>';
                }
            }
        });
    }

    function addRollingSkuTableSorting() {
        if (!rollingSkuAnalysisTableHeadEl) return;
        const headers = rollingSkuAnalysisTableHeadEl.querySelectorAll('th[data-column]');
        headers.forEach(th => {
            th.removeEventListener('click', handleRollingSkuSort);
            th.addEventListener('click', handleRollingSkuSort);
        });
    }

    function handleRollingSkuSort(event) {
        const newSortColumn = event.currentTarget.dataset.column;
        if (!newSortColumn) return;

        if (rollingSkuSortColumn === newSortColumn) {
            rollingSkuSortDirection = rollingSkuSortDirection === 'asc' ? 'desc' : 'asc';
        } else {
            rollingSkuSortColumn = newSortColumn;
            rollingSkuSortDirection = (newSortColumn === 'description' || newSortColumn === 'item_code') ? 'asc' : 'desc';
        }
        renderSkuAnalysis(currentRollingSkuData, window.fullFetchedData?.prediction?.missing_top_products || []);
    }

    function updateRollingSkuSortIndicators() {
        if (!rollingSkuAnalysisTableHeadEl) return;
        const headers = rollingSkuAnalysisTableHeadEl.querySelectorAll('th[data-column]');
        headers.forEach(th => {
            const indicator = th.querySelector('.sort-indicator');
            if (indicator) {
                th.classList.remove('sort-active');
                if (th.dataset.column === rollingSkuSortColumn) {
                    indicator.innerHTML = rollingSkuSortDirection === 'asc' ? '<i class="fas fa-arrow-up"></i>' : '<i class="fas fa-arrow-down"></i>';
                    th.classList.add('sort-active');
                } else {
                    indicator.innerHTML = '<i class="fas fa-sort" style="opacity: 0.3;"></i>';
                }
            }
        });
    }

    function showPurchaseDetailsModal(pointData) {
        if (!purchaseDetailModal || !modalTitleEl || !modalBodyEl) {
            console.error("Modal elements not found!");
            return;
        }
    
        // This correctly uses the originalDate property we set above
        const dateToShow = pointData.originalDate || pointData.x;
        
        const distributor = pointData.datasetLabel || 'Details';
        modalTitleEl.textContent = `Purchase Details: ${distributor} on ${formatDate(dateToShow)}`;
    
        const detailsList = pointData.details || [];
        if (detailsList.length === 0) {
            modalBodyEl.innerHTML = '<p class="text-center text-muted">No specific product details available for this purchase.</p>';
        } else {
            let tableHtml = `
                <table class="table table-sm table-striped">
                    <thead>
                        <tr>
                            <th>Product Description</th>
                            <th>SKU</th>
                            <th class="text-end">Quantity</th>
                            <th class="text-end">Revenue</th>
                        </tr>
                    </thead>
                    <tbody>
            `;
            detailsList.forEach(item => {
                tableHtml += `
                    <tr>
                        <td>${item.description || 'N/A'}</td>
                        <td>${item.sku || 'N/A'}</td>
                        <td class="text-end">${formatValue(item.quantity, 0)}</td>
                        <td class="text-end">${formatCurrency(item.revenue)}</td>
                    </tr>
                `;
            });
            tableHtml += '</tbody></table>';
            modalBodyEl.innerHTML = tableHtml;
        }
    
        purchaseDetailModal.show();
    }

    function renderDetailedPurchaseTimelineChart(cyTimelineData, pyTimelineData, nextExpectedDateStr) {
        showLoading(cadenceChartLoadingEl, false);
        if (!cadenceChartCanvas) { showError(cadenceChartErrorEl, "Canvas element not found."); return; }
    
        const processTimelineData = (data, yearSuffix) => {
            const dataByDistributor = {};
            (data || []).forEach(point => {
                const distributor = point.distributor || 'UNKNOWN';
                ensureDistributorColor(distributor);
                if (!dataByDistributor[distributor]) {
                    dataByDistributor[distributor] = {
                        label: `${distributor} (${yearSuffix})`,
                        data: [],
                        backgroundColor: DISTRIBUTOR_COLORS[distributor],
                        borderColor: DISTRIBUTOR_COLORS[distributor],
                        pointRadius: yearSuffix === 'CY' ? 6 : 5,
                        pointHoverRadius: 8,
                        pointStyle: yearSuffix === 'CY' ? 'circle' : 'rect'
                    };
                }
                
                // --- COMBINED FIX START ---
                // 1. Parse date string correctly to avoid timezone shifts
                const parts = point.x.split('T')[0].split('-');
                const originalDate = new Date(parts[0], parts[1] - 1, parts[2]);
    
                let displayDate;
                if (yearSuffix === 'PY') {
                    // For PY points, the display date is normalized (shifted) for the chart axis
                     displayDate = new Date(originalDate); // Create a copy
                     displayDate.setFullYear(new Date().getFullYear());
                } else {
                    // For CY points, the display date is just the original date
                    displayDate = originalDate;
                }
    
                // 2. Store BOTH the date for plotting (displayDate) and the TRUE original date
                dataByDistributor[distributor].data.push({
                    x: displayDate, // The date used for the X-axis (could be shifted)
                    y: point.daily_revenue,
                    details: point.details,
                    originalDate: originalDate // The TRUE, un-shifted date for the modal
                });
                // --- COMBINED FIX END ---
            });
            return Object.values(dataByDistributor);
        };
    
        const allDatasets = [
            ...processTimelineData(cyTimelineData, 'CY'),
            ...processTimelineData(pyTimelineData, 'PY')
        ];
    
        hideError(cadenceChartErrorEl);
        if (allDatasets.length === 0) {
            showError(cadenceChartErrorEl, "No purchase data available for CY or PY.");
            if (cadenceChartInstance) { cadenceChartInstance.destroy(); }
            return;
        }
    
        const currentYear = new Date().getFullYear();
        const yearStart = new Date(currentYear, 0, 1);
        const yearEnd = new Date(currentYear, 11, 31);
        const ctx = cadenceChartCanvas.getContext('2d');
        if (cadenceChartInstance) cadenceChartInstance.destroy();
    
        let annotations = {};
        if (Chart.registry.plugins.get('annotation') && nextExpectedDateStr) {
            try {
                const nextExpectedDate = new Date(nextExpectedDateStr);
                if (!isNaN(nextExpectedDate.getTime()) && nextExpectedDate.getFullYear() === currentYear) {
                    annotations = { line1: { type: 'line', scaleID: 'x', value: nextExpectedDate.valueOf(), borderColor: 'rgba(220, 53, 69, 0.7)', borderWidth: 2, borderDash: [6, 6], label: { enabled: true, content: `Expected (${formatDate(nextExpectedDate, 'short')})`, position: 'start', backgroundColor: 'rgba(220, 53, 69, 0.1)', color: '#dc3545', font: { size: 10 } } } };
                }
            } catch (e) { console.error("Error processing date for annotation:", e); }
        }
    
        cadenceChartInstance = new Chart(ctx, {
            type: 'scatter',
            data: { datasets: allDatasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                onClick: (event, elements, chart) => {
                    if (elements.length > 0) {
                        const { datasetIndex, index } = elements[0];
                        const clickedPointData = chart.data.datasets[datasetIndex].data[index];
                        clickedPointData.datasetLabel = chart.data.datasets[datasetIndex].label;
                        showPurchaseDetailsModal(clickedPointData);
                    }
                },
                onHover: (event, chartElement) => {
                    event.native.target.style.cursor = chartElement[0] ? 'pointer' : 'default';
                },
                scales: {
                    x: { type: 'time', time: { unit: 'month' }, min: yearStart.valueOf(), max: yearEnd.valueOf(), title: { display: true, text: 'Date' } },
                    y: { beginAtZero: true, title: { display: true, text: 'Purchase Amount ($)' }, ticks: { callback: value => formatCurrency(value) } }
                },
                plugins: {
                    tooltip: {
                        enabled: false
                    },
                    legend: { position: 'bottom', labels: { usePointStyle: true } },
                    annotation: { annotations: annotations }
                }
            }
        });
    }

    function renderRevenueChart(revenueData, analysisData, correctCurrentYearYep) {
        if(revenueChartLoadingEl) showLoading(revenueChartLoadingEl, false);
        if (!revenueChartCanvas) { if(revenueChartErrorEl) showError(revenueChartErrorEl, "Canvas element not found for revenue chart."); return; }
        if (!revenueData || !Array.isArray(revenueData.years) || !Array.isArray(revenueData.revenues) || revenueData.years.length === 0) {
            if(revenueChartErrorEl) showError(revenueChartErrorEl, "No revenue history data available to render chart.");
            if (revenueChartInstance) { revenueChartInstance.destroy(); revenueChartInstance = null; } return;
        }
        if(revenueChartErrorEl) hideError(revenueChartErrorEl);
        const ctx = revenueChartCanvas.getContext('2d');
        if (revenueChartInstance) { revenueChartInstance.destroy(); revenueChartInstance = null; }
        let chartLabels = [...revenueData.years]; let chartBarData = [...revenueData.revenues];
        let regressionForecastForNextYearValue = null; let regressionForecastForNextYearLabel = null;
        if (analysisData && analysisData.revenue_trend && analysisData.revenue_trend.forecast_year && analysisData.revenue_trend.forecast_next !== null && !isNaN(analysisData.revenue_trend.forecast_next)) {
            regressionForecastForNextYearLabel = analysisData.revenue_trend.forecast_year.toString();
            regressionForecastForNextYearValue = parseFloat(analysisData.revenue_trend.forecast_next);
            if (!chartLabels.includes(regressionForecastForNextYearLabel)) {
                chartLabels.push(regressionForecastForNextYearLabel);
                chartBarData.push(regressionForecastForNextYearValue);
            } else { const idx = chartLabels.indexOf(regressionForecastForNextYearLabel); chartBarData[idx] = regressionForecastForNextYearValue; }
        }
        const barColors = chartLabels.map((yearLabel) => {
            if (yearLabel === revenueData.currentYear) return 'rgba(25, 135, 84, 0.8)';
            else if (yearLabel === regressionForecastForNextYearLabel) return 'rgba(60, 179, 113, 0.7)';
            else if (yearLabel === revenueData.previousYear) return 'rgba(25, 135, 84, 0.6)';
            else return 'rgba(25, 135, 84, 0.4)';
        });
        const validBarValuesForScale = chartBarData.filter(val => val !== null && !isNaN(val) && val !== undefined);
        let maxChartYScale = Math.max(...validBarValuesForScale, 0);
        if (correctCurrentYearYep !== null && !isNaN(correctCurrentYearYep) && correctCurrentYearYep > maxChartYScale) maxChartYScale = correctCurrentYearYep;
        maxChartYScale = maxChartYScale > 0 ? maxChartYScale * 1.15 : 100;
        const datasets = [{ label: 'Revenue/YTD/Forecast', data: chartBarData, backgroundColor: barColors, borderColor: 'rgba(25, 135, 84, 1)', borderWidth: 1, order: 1 }];
        const trendLineDataPoints = [];
        for (let i = 0; i < revenueData.years.length; i++) {
            const yearStr = revenueData.years[i]; const revenueVal = revenueData.revenues[i];
            if (parseInt(yearStr) <= parseInt(revenueData.previousYear) && revenueVal !== null && !isNaN(revenueVal)) trendLineDataPoints.push({ x: yearStr, y: revenueVal });
        }
        if (regressionForecastForNextYearLabel && regressionForecastForNextYearValue !== null) {
            if (trendLineDataPoints.length > 0 && trendLineDataPoints[trendLineDataPoints.length -1].x !== revenueData.previousYear && parseInt(revenueData.previousYear) < parseInt(regressionForecastForNextYearLabel) ) {
                 const prevYearIdx = revenueData.years.indexOf(revenueData.previousYear);
                 if (prevYearIdx !== -1 && revenueData.revenues[prevYearIdx] !== null) trendLineDataPoints.push({x: revenueData.previousYear, y: revenueData.revenues[prevYearIdx]});
            }
            trendLineDataPoints.push({x: regressionForecastForNextYearLabel, y: regressionForecastForNextYearValue});
        }
        trendLineDataPoints.sort((a,b) => parseInt(a.x) - parseInt(b.x));
        if (trendLineDataPoints.length > 1) {
            datasets.push({ type: 'line', label: 'Historical Trend & Forecast', data: trendLineDataPoints,
                borderColor: 'rgba(153, 102, 255, 0.7)', borderWidth: 2,
                pointRadius: (context) => { const lastPointIndex = context.dataset.data.length - 1; if (context.dataIndex === lastPointIndex && context.dataset.data[lastPointIndex]?.x === regressionForecastForNextYearLabel) return 4; return 0; },
                pointBackgroundColor: 'rgba(153, 102, 255, 1)', fill: false, tension: 0.1, order: 0
            });
        }
        const annotations = {};
        const historicalCompletedActualsForAvg = revenueData.revenues.slice(0, revenueData.currentYearIndex > -1 ? revenueData.currentYearIndex : revenueData.revenues.length).filter(r => r !== null && !isNaN(r));
        const avgHistoricalRevenue = calculateAverage(historicalCompletedActualsForAvg);
        if (!isNaN(avgHistoricalRevenue) && avgHistoricalRevenue > 0) {
            annotations.avgLine = { type: 'line', scaleID: 'y', value: avgHistoricalRevenue, borderColor: 'rgba(102, 102, 102, 0.5)', borderWidth: 1, borderDash: [6, 6], label: { content: `Hist. Avg (${revenueData.years[0]}-${revenueData.previousYear}): ${formatCurrency(avgHistoricalRevenue)}`, enabled: true, position: 'end', backgroundColor: 'rgba(102, 102, 102, 0.1)', color: '#666', font: { size: 10 } } };
        }
        if (correctCurrentYearYep !== null && !isNaN(correctCurrentYearYep) && revenueData.currentYearIndex !== -1 && chartLabels[revenueData.currentYearIndex] === revenueData.currentYear) {
            const currentYearYTDActualBarValue = chartBarData[revenueData.currentYearIndex];
            if (currentYearYTDActualBarValue !== null && !isNaN(currentYearYTDActualBarValue) && correctCurrentYearYep > currentYearYTDActualBarValue) {
                annotations.yepLine = { type: 'line', yMin: currentYearYTDActualBarValue, yMax: correctCurrentYearYep, xMin: revenueData.currentYearIndex, xMax: revenueData.currentYearIndex, borderColor: 'rgba(255, 99, 132, 0.5)', borderWidth: 2, borderDash: [5, 5] };
            }
            annotations.yepLabel = { type: 'label', xValue: revenueData.currentYearIndex, yValue: correctCurrentYearYep, content: [`${revenueData.currentYear} YEP: ${formatCurrency(correctCurrentYearYep)}`], font: { size: 10 }, color: 'rgb(220, 53, 69)', backgroundColor: 'rgba(255, 255, 255, 0.7)', padding: 3, yAdjust: -15 };
        }
        if (regressionForecastForNextYearLabel && regressionForecastForNextYearValue !== null) {
            const forecastYearIndexOnChart = chartLabels.indexOf(regressionForecastForNextYearLabel);
            if (forecastYearIndexOnChart !== -1 && regressionForecastForNextYearLabel !== revenueData.currentYear) {
                annotations.nextYearForecastLabel = { type: 'label', xValue: forecastYearIndexOnChart, yValue: regressionForecastForNextYearValue, content: [`Forecast ${regressionForecastForNextYearLabel}: ${formatCurrency(regressionForecastForNextYearValue)}`], font: {size: 10}, color: 'rgb(60, 179, 113)', backgroundColor: 'rgba(255,255,255,0.7)', padding: 3, yAdjust: -10 };
            }
        }
        revenueChartInstance = new Chart(ctx, { type: 'bar', data: { labels: chartLabels, datasets: datasets },
            options: { responsive: true, maintainAspectRatio: false, animation: { duration: 1200 },
                scales: { y: { beginAtZero: true, suggestedMax: maxChartYScale, title: { display: true, text: 'Total Revenue ($)' }, ticks: { callback: function(value) { return formatCurrency(value); } } }, x: { title: { display: true, text: 'Year' } } },
                plugins: { legend: { display: true, labels: {boxWidth: 12} },
                    tooltip: { callbacks: {
                            title: function(tooltipItems) { return tooltipItems[0].label; },
                            label: function(context) {
                                if (context.dataset.label === 'Historical Trend & Forecast') {
                                    const pointYear = context.label; const pointValue = context.parsed.y;
                                    if (pointYear === regressionForecastForNextYearLabel) return `Trend Forecast ${pointYear}: ${formatCurrency(pointValue)}`;
                                    return `Trend ${pointYear}: ${formatCurrency(pointValue)}`;
                                }
                                const yearLabelInTooltip = context.label; const barValueInTooltip = context.parsed.y; let tooltipLines = [];
                                if (yearLabelInTooltip === revenueData.currentYear) {
                                    tooltipLines.push(`YTD Revenue: ${formatCurrency(barValueInTooltip)}`);
                                    if (correctCurrentYearYep !== null && !isNaN(correctCurrentYearYep)) {
                                        tooltipLines.push(`${revenueData.currentYear} YEP: ${formatCurrency(correctCurrentYearYep)}`);
                                        const prevYearActualRevenue = (revenueData.previousYearIndex !== -1 && revenueData.previousYearIndex < revenueData.revenues.length) ? revenueData.revenues[revenueData.previousYearIndex] : null;
                                        if (prevYearActualRevenue !== null && !isNaN(prevYearActualRevenue) && prevYearActualRevenue > 0) {
                                            const yepYoYGrowth = ((correctCurrentYearYep - prevYearActualRevenue) / prevYearActualRevenue) * 100;
                                            tooltipLines.push(`YoY (YEP vs ${revenueData.previousYear} Actual): ${formatValue(yepYoYGrowth, 1)}%`);
                                        } else if (prevYearActualRevenue === 0 && correctCurrentYearYep > 0) tooltipLines.push(`YoY (YEP vs ${revenueData.previousYear} Actual): New Growth`);
                                        else tooltipLines.push(`YoY (YEP vs ${revenueData.previousYear} Actual): N/A`);
                                    }
                                } else if (yearLabelInTooltip === regressionForecastForNextYearLabel) {
                                    tooltipLines.push(`Forecasted Revenue: ${formatCurrency(barValueInTooltip)}`);
                                    let currentYearYepForComparison = correctCurrentYearYep; let comparisonLabelForYoY = `${revenueData.currentYear} YEP`;
                                    if (currentYearYepForComparison !== null && !isNaN(currentYearYepForComparison) && currentYearYepForComparison > 0) {
                                        const forecastYoYGrowth = ((barValueInTooltip - currentYearYepForComparison) / currentYearYepForComparison) * 100;
                                        tooltipLines.push(`YoY (vs ${comparisonLabelForYoY}): ${formatValue(forecastYoYGrowth, 1)}%`);
                                    } else if (currentYearYepForComparison === 0 && barValueInTooltip > 0) tooltipLines.push(`YoY (vs ${comparisonLabelForYoY}): New Growth`);
                                    else tooltipLines.push(`YoY (vs ${comparisonLabelForYoY}): N/A`);
                                } else {
                                    tooltipLines.push(`Actual Revenue: ${formatCurrency(barValueInTooltip)}`);
                                    const historicalYearIndex = revenueData.years.indexOf(yearLabelInTooltip);
                                    if (historicalYearIndex !== -1 && revenueData.growthRates && historicalYearIndex < revenueData.growthRates.length && revenueData.growthRates[historicalYearIndex] !== null) {
                                        const growthPct = revenueData.growthRates[historicalYearIndex];
                                        tooltipLines.push(`YoY Growth: ${formatValue(growthPct,1)}%`);
                                    }
                                }
                                return tooltipLines;
                            }
                        }
                    },
                    annotation: { annotations: annotations }
                }
            }
        });
    }

    function renderSkuAnalysis(analysisData, missingProductsData) {
        const rollingSkuAnalysisTableBodyEl = rollingSkuAnalysisTableEl ? rollingSkuAnalysisTableEl.querySelector('tbody') : null;
        const top30OpportunitiesListEl = document.getElementById('top30OpportunitiesList');
        const top30ActiveListEl = document.getElementById('top30ActiveList'); // New element reference
    
        if (!rollingSkuAnalysisTableBodyEl || !top30OpportunitiesListEl || !top30ActiveListEl) {
            console.error("Missing elements for SKU analysis rendering.");
            return;
        }
    
        // --- Populates the "Purchased SKUs (APL)" table ---
        rollingSkuAnalysisTableBodyEl.innerHTML = '';
        if (analysisData && analysisData.length > 0) {
            const sortedData = [...analysisData].sort((a, b) => {
                let valA = a[rollingSkuSortColumn];
                let valB = b[rollingSkuSortColumn];
                if (rollingSkuSortColumn === 'description' || rollingSkuSortColumn === 'item_code' || rollingSkuSortColumn === 'prior_12m_rev') {
                    valA = (valA || '').toString().toLowerCase();
                    valB = (valB || '').toString().toLowerCase();
                    return rollingSkuSortDirection === 'asc' ? valA.localeCompare(valB) : valB.localeCompare(valA);
                } else {
                    valA = parseFloat(valA === null ? -Infinity : valA);
                    valB = parseFloat(valB === null ? -Infinity : valB);
                    return rollingSkuSortDirection === 'asc' ? valA - valB : valB - valA;
                }
            });
    
            sortedData.forEach(sku => {
                const row = rollingSkuAnalysisTableBodyEl.insertRow();
    
                const descriptionCell = row.insertCell();
                let descriptionHtml = sku.description || 'N/A';
                if (sku.is_top_30 === true) {
                    descriptionHtml += ` <i class="fas fa-star text-warning ms-1" title="Top 30 Product"></i>`;
                }
                descriptionCell.innerHTML = descriptionHtml;
    
                row.insertCell().textContent = sku.item_code || 'N/A';
    
                const priorRevCell = row.insertCell();
                priorRevCell.textContent = formatCurrency(sku.prior_12m_rev);
                priorRevCell.classList.add('text-end');
    
                const revCell = row.insertCell();
                revCell.textContent = formatCurrency(sku.current_12m_rev);
                revCell.classList.add('text-end');
    
                const yepCell = row.insertCell();
                yepCell.textContent = sku.sku_yep ? formatCurrency(sku.sku_yep) : 'N/A';
                yepCell.classList.add('text-end');
    
                const changeCell = row.insertCell();
                changeCell.classList.add('text-end');
                const changePct = sku.yoy_change_pct;
                const priorRev = sku.prior_12m_rev;
    
                if (changePct !== null && typeof changePct !== 'undefined') {
                    changeCell.textContent = `${changePct >= 0 ? '+' : ''}${formatValue(changePct, 1)}%`;
                    if (changePct > 0.1) {
                        changeCell.classList.add('text-success', 'fw-bold');
                    } else if (changePct < -0.1) {
                        changeCell.classList.add('text-warning', 'fw-bold');
                    } else {
                        if (sku.current_12m_rev === 0 && priorRev > 0) {
                            changeCell.textContent = "Dropped";
                            changeCell.classList.add('text-danger', 'fw-bold');
                        } else {
                            changeCell.classList.add('text-muted');
                        }
                    }
                } else if (sku.current_12m_rev > 0 && (priorRev === 0 || priorRev === null)) {
                    changeCell.innerHTML = `<span class="badge bg-success">New</span>`;
                    changeCell.classList.remove('text-end');
                    changeCell.classList.add('text-center');
                } else {
                    changeCell.textContent = 'N/A';
                    changeCell.classList.add('text-muted');
                }
            });
        } else {
            const row = rollingSkuAnalysisTableBodyEl.insertRow();
            const cell = row.insertCell();
            cell.colSpan = 6;
            cell.textContent = "No products purchased in the last 12 months.";
            cell.classList.add('text-center', 'text-muted', 'p-3');
        }
        updateRollingSkuSortIndicators();
    
        // --- NEW: Populates the "Top 30 (Active)" tab ---
        top30ActiveListEl.innerHTML = '';
        const activeTop30Skus = analysisData.filter(sku => sku.is_top_30 === true);
        if (activeTop30Skus.length > 0) {
            activeTop30Skus.sort((a,b) => (b.current_12m_rev || 0) - (a.current_12m_rev || 0));
            activeTop30Skus.forEach(product => {
                const item = document.createElement('div');
                item.className = 'list-group-item';
                const description = product.description || 'N/A';
                const sku = product.item_code || 'N/A';
                const revenue = product.current_12m_rev || 0;
                item.innerHTML = `
                    <div class="d-flex justify-content-between align-items-center">
                        <div>
                            <div class="fw-bold">${description}</div>
                            <div class="small text-muted">SKU: ${sku}</div>
                        </div>
                        <div class="fw-bold fs-5">${formatCurrency(revenue)}</div>
                    </div>
                `;
                top30ActiveListEl.appendChild(item);
            });
        } else {
            const emptyState = document.createElement('div');
            emptyState.className = 'list-group-item text-center text-muted p-4';
            emptyState.innerHTML = `<p class="mb-0"><i class="fas fa-info-circle me-2"></i>No Top 30 products have been purchased in the last 12 months.</p>`;
            top30ActiveListEl.appendChild(emptyState);
        }
    
        // --- Populates the "Top 30 (Missing)" tab ---
        top30OpportunitiesListEl.innerHTML = '';
        if (missingProductsData && missingProductsData.length > 0) {
            missingProductsData.forEach(product => {
                const item = document.createElement('div');
                item.className = 'list-group-item opportunity-item';
                const reason = product.reason || "Missing Top 30 Product";
                let badgeHtml = '';
                if (reason.toLowerCase().includes('never purchased')) {
                    badgeHtml = `<span class="badge opportunity-badge bg-success float-end"><i class="fas fa-plus"></i> New</span>`;
                } else if (reason.toLowerCase().includes('last year')) {
                    badgeHtml = `<span class="badge opportunity-badge bg-warning text-dark float-end"><i class="fas fa-undo"></i> Win-Back</span>`;
                } else if (reason.toLowerCase().includes('reactivation') || reason.toLowerCase().includes('years ago')) {
                    badgeHtml = `<span class="badge opportunity-badge bg-info text-dark float-end"><i class="fas fa-history"></i> Reactivate</span>`;
                } else {
                    badgeHtml = `<span class="badge opportunity-badge bg-primary float-end"><i class="fas fa-plus"></i> Opportunity</span>`;
                }
                const description = product.description || 'N/A';
                const sku = product.sku || 'N/A';
                item.innerHTML = `
                    <div class="opportunity-header">
                        <div class="opportunity-name">${description}</div>
                        ${badgeHtml}
                    </div>
                    <div class="opportunity-sku">SKU: ${sku}</div>
                    <div class="opportunity-insight">${reason}</div>
                `;
                top30OpportunitiesListEl.appendChild(item);
            });
        } else {
            const emptyState = document.createElement('div');
            emptyState.className = 'list-group-item text-center text-success p-4';
            emptyState.innerHTML = `
                <div>
                    <i class="fas fa-check-circle fa-2x mb-2"></i>
                    <h6 class="mb-0">Excellent Coverage!</h6>
                    <p class="small mb-0">This account carries all Top 30 SKUs.</p>
                </div>
            `;
            top30OpportunitiesListEl.appendChild(emptyState);
        }
    }


    // --- Initial Fetch ---
    fetchAccountDetails();
});