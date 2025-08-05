// static/js/strategic_dashboard_list.js
// Purpose: Manages the Strategic Dashboard list page (KPIs, Filters, Table, Portfolio Charts).
// Context Panel logic has been REMOVED. Row clicks navigate to a separate detail page.

document.addEventListener('DOMContentLoaded', function () {
    console.log("Strategic Dashboard LIST JS Initializing...");

    // --- State & Config ---
    let accountTable = null; // DataTable instance
    let allAccountData = []; // Holds raw data from the last successful API fetch
    let currentSummaryStats = null; // Holds summary stats from the last fetch
    let healthChartInstance = null;
    let segmentChartInstance = null;
    // REMOVED: selectedAccountId - No longer needed for context panel
    let activeQuickFilter = 'all'; // Track the current quick filter button state

    // Basic Config
    const API_ENDPOINT = '/api/strategic/accounts_v2'; // Using the V2 endpoint
    const ACCOUNT_DETAIL_URL_BASE = '/dashboard/account'; // *** ENSURE THIS IS THE CORRECT BASE URL FOR YOUR DETAIL PAGE ROUTE ***
    const HIGH_PRIORITY_THRESHOLD = 75; // For quick filter count summary
    const MED_PRIORITY_THRESHOLD = 50;  // For quick filter count summary
    const HEALTH_POOR_THRESHOLD = 40; // For filtering
    const PACE_DECLINE_THRESHOLD = -10; // For filtering
    const PACE_INCREASE_THRESHOLD = 10; // For filtering
    const GROWTH_HEALTH_THRESHOLD = 60; // For growth opps filter
    const GROWTH_MISSING_PRODUCTS_THRESHOLD = 3; // For growth opps filter

    // Color mapping for health badges (Used in table render)
    const healthColorMapping = [
        { threshold: 80, color: '#198754', name: 'Excellent', badge: 'bg-health-excellent' },
        { threshold: 60, color: '#8dc63f', name: 'Good', badge: 'bg-health-good' },
        { threshold: 40, color: '#e9c46a', name: 'Average', badge: 'bg-health-average' },
        { threshold: 20, color: '#fd7e14', name: 'Poor', badge: 'bg-health-poor' },
        { threshold: 0,  color: '#d16b55', name: 'Critical', badge: 'bg-health-critical' }
    ];
    // Colors for segment chart
    const segmentDistColors = [
        '#8dc63f', // Bright Green
        '#4b802c', // Dark Green
        '#5baa5f', // Light Green
        '#4c9175', // Teal-Green Info
        '#e1ead4', // Light Muted Green
        '#e9c46a', // Gold Warning
        '#ebddcd', // Light Beige
        '#d16b55', // Soft Red Danger
        // Add more distinct variations or repeat if needed
        '#a3cfbb', // Muted Green Border color
        '#8a9b8e', // Muted Green-Grey Text
        '#b3d1b4', // Another light green variation
        '#6f9a72'  // Another muted green variation
    ];
    // --- End State & Config ---


    // --- DOM Element References ---
    // Filters
    const repFilterEl = document.getElementById('strategicSalesRepFilter');
    const distributorFilterEl = document.getElementById('distributorFilter');
    const applyFiltersBtn = document.getElementById('applyMainFilters');

    // KPIs & Quick Filters
    const kpiTotalAccountsEl = document.getElementById('kpiTotalAccounts');
    const kpiPyLabelEl = document.getElementById('kpiPyLabel');
    const kpiPySalesEl = document.getElementById('kpiPySales');
    const kpiCyLabelEl = document.getElementById('kpiCyLabel');
    const kpiCytdSalesEl = document.getElementById('kpiCytdSales');
    const kpiYepValueEl = document.getElementById('kpiYepValue');
    const kpiPaceVsPyEl = document.getElementById('kpiPaceVsPy');

    const quickFilterContainer = document.querySelector('.quick-filters');
    // Snapshot counts spans
    const countPriority1El = document.getElementById('countPriority1');
    const countPriority2El = document.getElementById('countPriority2');
    const countDueThisWeekEl = document.getElementById('countDueThisWeek');
    const countOverdueEl = document.getElementById('countOverdue');
    const countLowHealthEl = document.getElementById('countLowHealth');
    const countLowPaceEl = document.getElementById('countLowPace');
    const countHighPaceEl = document.getElementById('countHighPace');
    const countGrowthOppsEl = document.getElementById('countGrowthOpps');
    const countAllEl = document.getElementById('countAll');

    // Action Hub Table
    const accountTableEl = document.getElementById('accountActionTable');
    const tableLoadingIndicator = document.getElementById('tableLoadingIndicator');
    const tableNoDataIndicator = document.getElementById('tableNoDataIndicator');

    // Distribution Charts
    const healthDistCanvas = document.getElementById('healthDistChart');
    const segmentDistCanvas = document.getElementById('segmentDistChart');
    // REMOVED: Context Panel DOM References
    // --- End DOM Element References ---


    // --- Initialization ---
    initializeDashboard();

    // --- Main Initialization Function ---
    function initializeDashboard() {
        console.log("Running initializeDashboard...");
        showTableLoading(false);
        initializeDataTable();
        initializeKpiCharts();
        setupEventListeners();

        fetchFilterOptions()
            .then(populateStaticFilters)
            .then(() => {
                console.log("Filter options populated. Checking for initial Rep ID.");
                handleInitialRepSelection();
            })
            .catch(err => {
                console.error("Initialization failed during filter fetch/population:", err);
                showNotification("Error initializing dashboard filters.", "error");
                showTableNoData(true, "Error loading filters.");
                clearDashboardData();
            });

        clearKpiSummary();
        setActiveQuickFilter('all');
    }

    // --- Event Listeners Setup ---
    function setupEventListeners() {
        console.log("Setting up event listeners...");

        // Main Filter Apply Button
        if (applyFiltersBtn) {
            applyFiltersBtn.addEventListener('click', fetchStrategicAccountData);
            console.log("Listener attached to Apply Filters Button.");
        } else {
            console.error("Apply Filters Button (#applyMainFilters) not found!");
        }

        // Quick Filter Buttons
        if (quickFilterContainer) {
            quickFilterContainer.addEventListener('click', function(event) {
                const button = event.target.closest('.quick-filter-btn');
                if (button && !button.disabled) {
                    const filterType = button.dataset.filter;
                    setActiveQuickFilter(filterType);
                    applyQuickFilter(filterType);
                }
            });
            console.log("Listener attached to Quick Filter container.");
        } else {
             console.error("Quick Filter container (.quick-filters) not found!");
        }

        // Table Row Click -> NAVIGATION
        if (accountTableEl) {
            $(accountTableEl).on('click', 'tbody tr', function (event) {
                if (!accountTable) return;

                // Prevent action if clicking on an actual link inside the row
                if (event.target.tagName === 'A' || event.target.closest('a')) {
                    console.log("Clicked on a link inside the row, not navigating.");
                    return; // Don't navigate if they clicked the explicit link
                }

                const rowData = accountTable.row(this).data();
                if (rowData) {
                    const accountCode = rowData.canonical_code || rowData.id; // Prefer canonical
                    if (accountCode) {
                        console.log(`Navigating to details for account code: ${accountCode}`);
                        // Use the base URL defined earlier
                        window.location.href = `${ACCOUNT_DETAIL_URL_BASE}/${accountCode}`;
                        // For opening in a new tab:
                        // window.open(`${ACCOUNT_DETAIL_URL_BASE}/${accountCode}`, '_blank');
                    } else {
                        console.warn("Could not get canonical_code or id from row data:", rowData);
                        showNotification("Could not determine account identifier.", "error");
                    }
                } else {
                     console.warn("Clicked row has no associated data.");
                }
            });
            console.log("Listener attached to Table Body for NAVIGATION.");
        } else {
            console.error("Account Table Element (#accountActionTable) not found for listener!");
        }
    }

    // --- Initial Load / URL Handling ---
    function handleInitialRepSelection() {
        // initialSalesRepId is defined globally via the inline script in HTML
        if (typeof initialSalesRepId !== 'undefined' && initialSalesRepId !== null && repFilterEl) {
            const optionExists = Array.from(repFilterEl.options).some(opt => opt.value == initialSalesRepId);
            if (optionExists) {
                repFilterEl.value = initialSalesRepId;
                console.log(`Pre-selecting Rep ID: ${initialSalesRepId} from initial data.`);

                const urlParams = new URLSearchParams(window.location.search);
                const distFromUrl = urlParams.get('distributor');
                if (distFromUrl && distributorFilterEl) {
                    const distOptionExists = Array.from(distributorFilterEl.options).some(opt => opt.value === distFromUrl);
                    if (distOptionExists) distributorFilterEl.value = distFromUrl;
                }
                const filterFromUrl = urlParams.get('filter');
                if (filterFromUrl) {
                    setActiveQuickFilter(filterFromUrl);
                }

                if (applyFiltersBtn) {
                    showNotification(`Loading data for Rep ID: ${initialSalesRepId}...`, 'info');
                    setTimeout(() => {
                         console.log("Triggering automatic data load via initial Rep ID.");
                         applyFiltersBtn.click(); // Trigger the fetch
                     }, 50);
                } else {
                     console.error("Apply button not found, cannot trigger initial load automatically.");
                }
            } else {
                console.warn(`Initial Rep ID '${initialSalesRepId}' provided but not found in dropdown options.`);
                showNotification(`Sales Rep ID '${initialSalesRepId}' not found. Please select a valid rep.`, 'warning');
                showTableNoData(true, "Select a valid Sales Rep.");
                clearDashboardData();
            }
        } else {
            console.log("No initial Rep ID provided. Waiting for user selection.");
            showTableNoData(true, "Select a Sales Rep to load data.");
            clearDashboardData();
        }
    }

    // --- Data Fetching ---
    async function fetchStrategicAccountData() {
        const selectedRep = repFilterEl?.value;
        const selectedDist = distributorFilterEl?.value;
        console.log(`fetchStrategicAccountData started. Rep: ${selectedRep}, Dist: ${selectedDist}`);

        if (!selectedRep) {
            showNotification('Please select a Sales Rep.', 'warning');
            clearDashboardData();
            showTableNoData(true, "Please select a Sales Rep.");
            return;
        }

        showTableLoading(true);
        disableQuickFilters(true);

        const params = new URLSearchParams({ sales_rep: selectedRep });
        if (selectedDist) params.append('distributor', selectedDist);
        const fetchUrl = `${API_ENDPOINT}?${params.toString()}`;
        console.log(`Fetching from API: ${fetchUrl}`);

        try {
            const response = await fetch(fetchUrl);
            if (!response.ok) {
                let errorText = `API Error ${response.status}`;
                try { errorText = await response.text(); } catch (e) {}
                throw new Error(errorText);
            }

            const data = await response.json();
            console.log("API Data Received:", data);

            if (!data || typeof data !== 'object' || !Array.isArray(data.accounts) || typeof data.summary_stats !== 'object') {
                throw new Error("Received invalid data format from server.");
            }

            allAccountData = data.accounts || [];
            currentSummaryStats = data.summary_stats || {};
            console.log(`Processed ${allAccountData.length} accounts.`);

            updateSnapshotAndKPIs(currentSummaryStats); // Update KPIs/Counts based on ALL data
            renderActionHubTable(allAccountData); // Load ALL data into DataTable
            applyQuickFilter(activeQuickFilter); // Re-apply active client-side filter AFTER table render

            showTableNoData(allAccountData.length === 0);

        } catch (error) {
            console.error('Error fetching/processing strategic data:', error);
            showNotification(`Failed to load account data: ${error.message || 'Unknown error'}`, 'error');
            clearDashboardData();
            showTableNoData(true, `Error loading data: ${error.message || 'Server error'}`);
        } finally {
            console.log("--- FINALLY block executing ---"); // <<< ADD THIS
            showTableLoading(false);
            disableQuickFilters(false);
            console.log("fetchStrategicAccountData finished.");
        }
    }

    // --- UI Update Functions ---
    function updateSnapshotAndKPIs(stats) {
        console.log("Updating KPIs and Snapshot counts", stats);
        if (!stats) {
            clearKpiSummary();
            return;
        }

        if (kpiTotalAccountsEl) kpiTotalAccountsEl.textContent = stats.total_accounts ?? '-';

        // --- 1. Set Dynamic Year Labels ---
        const currentYear = new Date().getFullYear();
        const prevYear = currentYear - 1;
        if (kpiPyLabelEl) kpiPyLabelEl.textContent = `${prevYear}`;
        if (kpiCyLabelEl) kpiCyLabelEl.textContent = `${currentYear}`;

        // --- 2. Get Values from API data ---
        // Assumes the API provides these keys in the summary_stats object
        const pySales = stats.total_py_revenue ?? 0;
        const cytdSales = stats.total_cytd_revenue ?? 0;
        const yepValue = stats.total_yep ?? 0;

        // --- 3. Calculate Pace Percentage ---
        let pacePercent = null;
        if (pySales > 0) {
            pacePercent = ((yepValue - pySales) / pySales) * 100;
        }

        // --- 4. Populate the KPI Cards ---
        // Use the new formatCurrencyExact function for precision
        if (kpiPySalesEl) kpiPySalesEl.textContent = formatCurrencyExact(pySales);
        if (kpiCytdSalesEl) kpiCytdSalesEl.textContent = formatCurrencyExact(cytdSales);
        if (kpiYepValueEl) kpiYepValueEl.textContent = formatCurrencyExact(yepValue);
        
        // Format and display the percentage
        if (kpiPaceVsPyEl) {
            if (pacePercent !== null) {
                const paceColor = pacePercent >= 0 ? 'text-success' : 'text-danger';
                kpiPaceVsPyEl.innerHTML = `<span class="${paceColor}">${pacePercent >= 0 ? '+' : ''}${pacePercent.toFixed(1)}%</span>`;
            } else if (yepValue > 0) {
                kpiPaceVsPyEl.innerHTML = `<span class="text-success">New Growth</span>`;
            } else {
                kpiPaceVsPyEl.textContent = '-';
            }
        }
        
        // (The logic for the quick filter counts below remains the same)
        if (countPriority1El) countPriority1El.textContent = stats.count_priority1 ?? 0;
        if (countPriority2El) countPriority2El.textContent = stats.count_priority2 ?? 0;
        if (countDueThisWeekEl) countDueThisWeekEl.textContent = stats.count_due_this_week ?? 0;
        if (countOverdueEl) countOverdueEl.textContent = stats.count_overdue ?? 0;
        if (countLowHealthEl) countLowHealthEl.textContent = stats.count_low_health ?? 0;
        if (countLowPaceEl) countLowPaceEl.textContent = stats.count_low_pace ?? 0;
        if (countHighPaceEl) countHighPaceEl.textContent = stats.count_high_pace ?? 0;
        if (countGrowthOppsEl) countGrowthOppsEl.textContent = stats.count_growth_opps ?? 0;
        if (countAllEl) countAllEl.textContent = stats.total_accounts ?? 0;

        // Update Charts based on the full dataset received for this filter set
        aggregateAndDrawCharts(allAccountData);
    }

    function aggregateAndDrawCharts(accounts) {
        // (Keep existing logic - aggregates counts for portfolio charts)
         console.log(`Aggregating chart data for ${accounts?.length} accounts.`);
         if (!accounts || accounts.length === 0) {
             if (healthChartInstance) { /* clear */ }
             if (segmentChartInstance) { /* clear */ }
             return;
         }
         const healthCounts = {}; /* aggregate */
         const segmentCounts = {}; /* aggregate */
         accounts.forEach(acc => { /* populate counts */ });
         updateHealthDistChart(healthCounts);
         updateSegmentDistChart(segmentCounts);
    }

    // --- DataTable Initialization ---
    function initializeDataTable() {
        console.log("Initializing DataTable...");
        if (accountTable) {
            console.log("Destroying existing DataTable instance.");
            accountTable.destroy(); // Destroy the instance
            if (accountTableEl) {
                const tbody = accountTableEl.querySelector('tbody');
                const thead = accountTableEl.querySelector('thead');
                if(tbody) tbody.innerHTML = '';
                if(thead) thead.innerHTML = '';
                console.log("Table body and head cleared.");
            }
            accountTable = null; // Ensure reference is cleared
        }
    
        try {
            accountTable = $(accountTableEl).DataTable({
                data: [],
                deferRender: true,
                processing: false,
                serverSide: false,
                paging: true,
                pageLength: 15,
                lengthChange: true,
                lengthMenu: [[15, 25, 50, 100, -1], [15, 25, 50, 100, "All"]],
                searching: true,
                ordering: true,
                info: true,
                destroy: true,
                language: {
                    search: "_INPUT_",
                    searchPlaceholder: "Search accounts...",
                    lengthMenu: "Show _MENU_",
                    info: "Showing _START_ to _END_ of _TOTAL_ accounts",
                    infoEmpty: "No accounts found",
                    infoFiltered: "(filtered from _MAX_ total accounts)",
                    paginate: { first: "<<", last: ">>", next: ">", previous: "<" },
                    emptyTable: "No data available for the selected filters.",
                    processing: `<div class="spinner-border spinner-border-sm text-primary" role="status"><span class="visually-hidden">Loading...</span></div>`
                },
                columns: [
                    { // 0: Account Name (Link)
                        title: "Account Name",
                        className: "account-name-cell",
                        render: function(data, type, row) {
                            const accountName = row.name || row.account_name || 'N/A';
                            const code = row.canonical_code || row.id || '';
                            if (code && type === 'display') {
                                return `<a href="${ACCOUNT_DETAIL_URL_BASE}/${code}" title="View Account Details">${accountName}</a>`;
                            }
                            return accountName; // For searching/sorting
                        }
                    },
                    { // 1: Priority Score
                        title: "Priority",
                        className: "priority-score-cell text-center",
                        render: function(data, type, row) {
                            const score = row.enhanced_priority_score;
                            return score !== null && score !== undefined ? parseFloat(score).toFixed(1) : "-";
                        }
                    },
                    { // 2: Health Score Badge
                        title: "Health",
                        className: "health-cell text-center",
                        render: function(data, type, row) {
                            const score = row.health_score;
                            if (score === null || score === undefined) return "-";
                            const scoreNum = parseFloat(score);
                            const colorInfo = getHealthColorInfo(scoreNum);
                            const display = `<span class="badge ${colorInfo.badge}" title="${colorInfo.name}">${scoreNum.toFixed(0)}</span>`;
    
                            if (type === 'display') return display;
                            if (type === 'sort' || type === 'type') return scoreNum; // Sort by number
                            return scoreNum.toFixed(0); // Filter by number text
                        }
                    },
                    { // 3: Due / Overdue
                        title: "Due / Overdue",
                        className: "due-overdue-cell", // Remove text-center if desired
                        render: function(data, type, row) {
                            const overdue = row.days_overdue;
                            let display = '-';
                            let sortVal = 9999; // Default sort value (far in future)
                            let badgeClass = 'bg-light text-dark'; // Default badge
    
                            if (overdue !== null && overdue > 0) {
                                display = `<span class="badge bg-danger">Overdue ${overdue}d</span>`;
                                sortVal = -overdue; // Negative for sorting overdue first
                            } else if (row.next_expected_purchase_date) {
                                try {
                                    const dueDate = new Date(row.next_expected_purchase_date);
                                    const today = new Date(); today.setHours(0,0,0,0);
                                    const dueDateOnly = new Date(dueDate); dueDateOnly.setHours(0,0,0,0);
                                    sortVal = Math.round((dueDateOnly - today) / (1000 * 60 * 60 * 24)); // Days diff for sorting
    
                                    if (sortVal >= 0 && sortVal <= 7) {
                                        badgeClass = 'bg-warning text-dark';
                                        display = `<span class="badge ${badgeClass}">Due in ${sortVal+1}d</span>`;
                                    } else if (sortVal > 7) {
                                        badgeClass = 'bg-info text-dark';
                                        display = `<span class="badge ${badgeClass}">Due ${formatDate(dueDate, 'short')}</span>`;
                                    } else {
                                        // Due date passed but not marked overdue yet?
                                         badgeClass = 'bg-secondary text-white';
                                         display = `<span class="badge ${badgeClass}">Expected ${formatDate(dueDate, 'short')}</span>`;
                                    }
                                } catch (e) { display = '-'; sortVal = 9999; badgeClass='bg-light text-dark';}
                            }
    
                            if (type === 'display') {
                                return display;
                            } else if (type === 'sort' || type === 'type') {
                                return sortVal; // Use calculated days diff for sorting
                            }
                            // For filtering, return the text content if needed, or just the sort value
                            return display.replace(/<[^>]*>/g, ''); // Return text without HTML for filtering
                        }
                    },
                    { // 4: Reason Icons
                        title: "Reason(s)",
                        className: "reason-cell",
                        orderable: false,
                        searchable: false,
                        render: function(data, type, row) { return generateReasonIcons(row); }
                    },
                    { // 5: Pace %
                        title: "Pace %",
                        className: "pace-percent-cell text-end",
                        render: function(data, type, row) {
                            const pace = row.pace_vs_ly;
                            const pyRev = row.py_total_revenue;
                            let display = "-";
                            let sortVal = -Infinity; // Default sort for N/A
    
                            if (pace !== null && pyRev !== null && pyRev !== undefined && pyRev > 0) {
                                //const pacePercent = (pace / pyRev) * 100;
                                const pacePercent = pace;
                                sortVal = pacePercent; // Sort by actual percentage
                                const color = pacePercent <= PACE_DECLINE_THRESHOLD ? 'text-danger' : (pacePercent >= PACE_INCREASE_THRESHOLD ? 'text-success' : 'text-dark');
                                display = `<span class="${color}">${pacePercent >= 0 ? '+' : ''}${formatNumber(pacePercent, 1)}%</span>`;
                            } else if (row.yep_revenue > 0 && (pyRev === null || pyRev === 0)) {
                                display = `<span class="text-success">🌱 New</span>`;
                                sortVal = Infinity; // Sort new growth high
                            }
                            if (type === 'display') return display;
                            if (type === 'sort' || type === 'type') return sortVal;
                             return display.replace(/<[^>]*>/g, ''); // Filter text
                        }
                    },
                    { // 6: YEP
                        title: "YEP",
                        className: "yep-cell text-end",
                        render: function(data, type, row) {
                             const yep = row.yep_revenue;
                             if (type === 'display') return formatCurrency(yep);
                             return yep !== null && yep !== undefined ? yep : -1; // Sort N/A low
                        }
                    },
                    { // 7: RFM Segment
                        title: "RFM",
                        className: "rfm-cell",
                        render: function(data, type, row) { return row.rfm_segment || "N/A"; }
                    },
                    { // 8: Coverage % (Hidden by default, XL+)
                        title: "Coverage %",
                        className: "coverage-cell d-none d-xl-table-cell text-end",
                        render: function(data, type, row) {
                            const coverage = row.product_coverage_percentage;
                            if (type === 'display') return coverage !== null ? formatNumber(coverage, 1) + '%' : 'N/A';
                            return coverage !== null ? coverage : -1; // Sort N/A low
                        }
                    },
                    { // 9: Last Order Date (Hidden by default, XXL+)
                        title: "Last Order",
                        className: "last-order-cell d-none d-xxl-table-cell text-end",
                        render: function(data, type, row) {
                            const dateStr = row.last_purchase_date;
                            if (type === 'display') return formatDate(dateStr);
                            if (type === 'sort' || type === 'type') {
                                // Sort by date object or timestamp for accuracy
                                try { return dateStr ? new Date(dateStr).getTime() : 0; } catch(e){ return 0; }
                            }
                            return formatDate(dateStr); // For filtering
                        }
                    }
                ],
                // Default order
                order: [[1, 'desc']], // Sort by Priority Score descending
                
                // --- ENHANCED drawCallback ---
                drawCallback: function (settings) {
                    console.log("DataTable drawCallback executed.");
                    const api = this.api();
                    const pageInfo = api.page.info();
               
                    // Check if the table is empty AFTER filtering
                    const isEmpty = pageInfo.recordsDisplay === 0; // True if no records displayed on current page after filter
               
                    console.log(`DRAW CALLBACK - pageInfo:`, pageInfo);
                    console.log(`DRAW CALLBACK - isEmpty (recordsDisplay === 0): ${isEmpty}`);
               
                    // Show/Hide based on isEmpty
                    showTableNoData(isEmpty, "No accounts match the current filters.");
               
                    console.log(`DRAW CALLBACK - tableNoDataIndicator display should be: ${isEmpty ? 'flex' : 'none'}`);
               
                    // --- Tooltip logic ---
                    var tooltipTriggerList = [].slice.call(document.querySelectorAll('#accountActionTable [data-bs-toggle="tooltip"]'));
                    tooltipTriggerList.forEach(function (tooltipTriggerEl) {
                        var existingTooltip = bootstrap.Tooltip.getInstance(tooltipTriggerEl);
                        if (existingTooltip) { existingTooltip.dispose(); }
                        new bootstrap.Tooltip(tooltipTriggerEl, { container: 'body', trigger: 'hover' });
                    });
                },
                // --- End ENHANCED drawCallback ---
                
                createdRow: function(row, data, dataIndex) {
                    // Keep existing row ID logic
                    try {
                        const code = data.canonical_code || data.id;
                        if (code) { 
                            $(row).attr('id', `account-row-${code}`); 
                        }
                    } catch (e) { 
                        console.error("Error in createdRow callback", e); 
                    }
                }
            });
            console.log("DataTable initialized successfully.");
        } catch (e) {
            console.error("DataTable Initialization Error:", e);
            showNotification("Error initializing accounts table.", "error");
        }
    }

    // Helper for rendering the table (adds data)
    function renderActionHubTable(accounts) {
        console.log(`Rendering DataTable with ${accounts?.length || 0} accounts.`);
        
        if (!accountTable) {
            console.error("DataTable instance not initialized!");
            return;
        }
        
        try {
            // Debug the data before adding it
            if (accounts && accounts.length > 0) {
                console.log("First account object:", accounts[0]);
            }
            
            console.log("Clearing table and adding new data...");
            accountTable.clear();
            
            // Only add accounts if there are actually accounts to add
            if (accounts && accounts.length > 0) {
                accountTable.rows.add(accounts);
            }
            
            console.log("Calling accountTable.draw(false)...");
            accountTable.draw(false);
            console.log("DataTable redraw complete.");
            
            // Only show the "No Data" message if there are truly no accounts
            const isEmpty = !accounts || accounts.length === 0;
            showTableNoData(isEmpty, isEmpty ? "No account data found for the selected filters." : "");
            
        } catch (e) {
            console.error("Error during DataTable rendering:", e);
            showNotification("Error displaying account table.", "error");
        }
    }

    // --- Helper for Reason Icons in Table ---
    function generateReasonIcons(rowData) {
        // (Keep existing logic - generates icons based on rowData)
         if (!rowData) return '<span class="text-muted small">-</span>';
         let icons = '';
         const thresholds = getThresholds();
         if (rowData.days_overdue > 7) icons += `<i class="fas fa-calendar-times text-danger reason-icon" title="Overdue ${rowData.days_overdue}d"></i>`;
         else if (rowData.days_overdue > 0) icons += `<i class="fas fa-calendar-day text-warning reason-icon" title="Due ${rowData.days_overdue}d ago"></i>`;
         if (rowData.health_score !== null && rowData.health_score < thresholds.poor) { icons += `<i class="fas fa-heart-broken text-danger reason-icon" title="Low Health (${formatNumber(rowData.health_score, 0)})"></i>`; }
         if (rowData.rfm_segment && ["Can't Lose", "At Risk"].includes(rowData.rfm_segment)) { icons += `<i class="fas fa-exclamation-triangle text-danger reason-icon" title="Segment: ${rowData.rfm_segment}"></i>`; }
         const pace = rowData.pace_vs_ly;
         const pyRev = rowData.py_total_revenue;
         if (pace !== null && pyRev !== null && pyRev > 0) {
             const pacePercent = (pace / pyRev) * 100;
             if (pacePercent <= thresholds.pace_decline) { icons += `<i class="fas fa-angle-double-down text-danger reason-icon" title="Pace Decline (${formatNumber(pacePercent, 0)}%)"></i>`; }
         } else if (pace !== null && pace < -500) { icons += `<i class="fas fa-angle-down text-warning reason-icon" title="Pace Decline (${formatCurrency(pace,0)})"></i>`; }
         const missing = rowData.missing_top_products || [];
         if (missing.length >= thresholds.missing_products) { icons += `<i class="fas fa-puzzle-piece text-info reason-icon" title="Upsell Opportunity (${missing.length})"></i>`; }
         if (icons === '' && rowData.health_score !== null && rowData.health_score < thresholds.good) { icons += `<i class="fas fa-notes-medical text-warning reason-icon" title="Avg Health (${formatNumber(rowData.health_score, 0)})"></i>`; }
         return icons || '<span class="text-muted small">-</span>';
    }

    // --- Quick Filter Logic ---
    function setActiveQuickFilter(filterType) {
        console.log(`Setting active quick filter: ${filterType}`);
        activeQuickFilter = filterType; // Store the active filter type
        
        document.querySelectorAll('.quick-filter-btn').forEach(btn => {
            const isCurrent = btn.dataset.filter === filterType;
            btn.classList.toggle('active', isCurrent);
            
            // Remove all button styles first
            btn.classList.remove('btn-danger', 'btn-warning', 'btn-info', 'btn-success', 'btn-primary', 'btn-secondary', 
                                  'btn-outline-danger', 'btn-outline-warning', 'btn-outline-info', 
                                  'btn-outline-success', 'btn-outline-primary', 'btn-outline-secondary');
            
            // Add appropriate style based on filter type and whether it's active
            if (isCurrent) {
                // Active button styling - use filled versions with Irwin colors
                switch(filterType) {
                    case 'priority1':
                    case 'priority2':
                        btn.classList.add('btn-primary'); // Use primary (bright green) for priority
                        break;
                    case 'overdue':
                    case 'low_pace':
                    case 'low_health':
                        btn.classList.add('btn-danger'); // Use danger for negative filters
                        break;
                    case 'due_this_week':
                        btn.classList.add('btn-warning'); // Use warning for due soon
                        break;
                    case 'growth_opps':
                    case 'high_pace':
                        btn.classList.add('btn-success'); // Use success (dark green) for growth
                        break;
                    case 'all':
                        btn.classList.add('btn-secondary'); // Use secondary for "all"
                        break;
                    default:
                        btn.classList.add('btn-info'); // Use info as fallback
                }
            } else {
                // Inactive button styling - use outline versions
                switch(btn.dataset.filter) {
                    case 'priority1':
                    case 'priority2':
                        btn.classList.add('btn-outline-primary');
                        break;
                    case 'overdue':
                    case 'low_pace':
                    case 'low_health':
                        btn.classList.add('btn-outline-danger');
                        break;
                    case 'due_this_week':
                        btn.classList.add('btn-outline-warning');
                        break;
                    case 'growth_opps':
                    case 'high_pace':
                        btn.classList.add('btn-outline-success');
                        break;
                    case 'all':
                        btn.classList.add('btn-outline-secondary');
                        break;
                    default:
                        btn.classList.add('btn-outline-info');
                }
            }
        });
    }

    function applyQuickFilter(filterType) {
        console.log(`Applying quick filter: ${filterType}`);
        if (!accountTable) {
             console.warn("Attempted to apply filter, but DataTable not initialized.");
             return;
        }

        // Remove any existing custom filters first
        // $.fn.dataTable.ext.search is a global array, pop removes the last added function
        $.fn.dataTable.ext.search.pop();

        if (filterType !== 'all') {
            const thresholds = getThresholds();
            console.log("Using thresholds for filtering:", thresholds);

            // Add the new custom filter function
            $.fn.dataTable.ext.search.push(
                function (settings, data, dataIndex, rowData, counter) {
                    // 'rowData' is the original data object for the row
                    // 'data' is an array of the string representations of the row's cells (less useful here)
                    if (!rowData) return false; // Should not happen if data is loaded correctly

                    // --- Your existing switch statement logic ---
                    switch (filterType) {
                        case 'priority1':
                            return (rowData.enhanced_priority_score ?? -1) >= thresholds.p1;
                        case 'priority2':
                            const score = rowData.enhanced_priority_score ?? -1;
                            return score >= thresholds.p2 && score < thresholds.p1;
                        case 'due_this_week':
                            if (!rowData.next_expected_purchase_date) return false;
                            try {
                                const today = new Date(); today.setHours(0,0,0,0);
                                const endOfWeek = new Date(today); endOfWeek.setDate(today.getDate() + 6);
                                const dueDate = new Date(rowData.next_expected_purchase_date); dueDate.setHours(0,0,0,0);
                                return dueDate >= today && dueDate <= endOfWeek;
                            } catch(e) { return false; }
                        case 'overdue':
                            return (rowData.days_overdue ?? 0) > 0;
                        case 'low_health':
                            return (rowData.health_score ?? 101) < thresholds.poor;
                        case 'low_pace':
                            const paceLow = rowData.pace_vs_ly;
                            const pyRevLow = rowData.py_total_revenue;
                            const currentThreshold = thresholds.pace_decline; // Use the threshold from getThresholds()
                            let low_pace_met_js = false; // Flag

                            // *** Add logging specifically for your chosen test_canonical_code ***
                            const test_canonical_code = '02VA9583_NATURESOUTLET'; // <<< REPLACE WITH SAME CODE AS PYTHON
                            if (rowData.canonical_code === test_canonical_code) {
                                console.log(`[JS DEBUG ${rowData.canonical_code}] Checking Low Pace Filter:`);
                                console.log(`  paceLow: ${paceLow} (Type: ${typeof paceLow})`);
                                console.log(`  pyRevLow: ${pyRevLow} (Type: ${typeof pyRevLow})`);
                                console.log(`  Condition (paceLow !== null): ${paceLow !== null}`);
                                // JS doesn't have 'is not None', null/undefined check is typical
                                console.log(`  Condition (pyRevLow !== null): ${pyRevLow !== null}`);
                                console.log(`  Condition (pyRevLow > 0): ${pyRevLow !== null ? pyRevLow > 0 : 'N/A'}`);
                            }

                            if (paceLow !== null && pyRevLow !== null && pyRevLow > 0) {
                                const pacePercent = (paceLow / pyRevLow * 100);
                                const meetsThreshold = pacePercent <= currentThreshold;
                                if (rowData.canonical_code === test_canonical_code) {
                                    console.log(`  Calculated pacePercent: ${pacePercent}`);
                                    console.log(`  Threshold: ${currentThreshold}`);
                                    console.log(`  Comparison (pacePercent <= threshold): ${meetsThreshold}`);
                                }
                                low_pace_met_js = meetsThreshold; // Store result
                            } else {
                                if (rowData.canonical_code === test_canonical_code) {
                                    console.log(`  Skipped calculation because conditions not met.`);
                                }
                            }

                            // Log final result for the test account
                            if (rowData.canonical_code === test_canonical_code) {
                                console.log(`  >>> Meets Low Pace Criteria (JS): ${low_pace_met_js}`);
                            }
                            return low_pace_met_js; // Return the result for the filter
                         case 'high_pace':
                            const paceHigh = rowData.pace_vs_ly;
                            const pyRevHigh = rowData.py_total_revenue;
                            if (paceHigh !== null && pyRevHigh !== null && pyRevHigh > 0) {
                                return (paceHigh / pyRevHigh * 100) >= thresholds.pace_increase;
                            }
                             return false;
                         case 'growth_opps':
                             // Assuming isGrowthOpportunity is defined elsewhere and works
                             return isGrowthOpportunity(rowData, thresholds);
                        default:
                            return true; // Show row if filter type is unknown or 'all' (though 'all' is handled below)
                    }
                    // --- End of switch statement ---
                }
            );
        } else {
             console.log("Clearing quick filters (showing all).");
             // No filter function needed, pop() above already cleared it.
        }

        // Redraw the table to apply the filter visually
        try {
            // *** CHANGE HERE: Use a callback for chart updates ***
            accountTable.off('draw.dt.quickfilter').on('draw.dt.quickfilter', function () {
                // This code runs AFTER the table has finished redrawing with the filter applied
                console.log("DataTable draw complete after filter:", filterType);

                // Get the data that PASSED the filter
                const filteredData = accountTable.rows({ search: 'applied' }).data().toArray();
                console.log(`Updating charts with ${filteredData.length} filtered accounts.`);

                // Update charts using only the filtered data
                aggregateAndDrawCharts(filteredData);

                // Remove this specific draw event listener to avoid multiple triggers
                accountTable.off('draw.dt.quickfilter');
            });

            // Trigger the redraw (which will then trigger the 'draw.dt.quickfilter' event)
            accountTable.draw();
            console.log("DataTable redraw triggered for filter:", filterType);

        } catch(e) {
            console.error("Error redrawing DataTable after filter:", e);
            // Ensure charts are cleared or show an error state if draw fails
            aggregateAndDrawCharts([]); // Clear charts on error
        }
    }


    function isGrowthOpportunity(rowData, thresholds) {
        // Add detailed logging for debugging specific accounts if needed
        // console.log(`Checking Growth Opp for ${rowData?.canonical_code}: Health=${rowData?.health_score}, Pace=${rowData?.pace_vs_ly}, PY=${rowData?.py_total_revenue}, Missing=${rowData?.missing_top_products?.length}, Segment=${rowData?.rfm_segment}, Due=${rowData?.next_expected_purchase_date}`);

        try {
            // Rule 1: Must have sufficient health score
            // Use ?? 0 to safely handle null/undefined health_score
            if ((rowData.health_score ?? 0) < thresholds.growth_health) {
                // console.log(`  -> Failed Health Check`);
                return false; // Does not meet minimum health
            }

            // Rule 2: Check Pace % Increase
            const pace = rowData.pace_vs_ly;
            const pyRev = rowData.py_total_revenue;
            // Check if values are valid numbers and pyRev is positive
            if (pace !== null && pace !== undefined && !isNaN(pace) &&
                pyRev !== null && pyRev !== undefined && !isNaN(pyRev) && pyRev > 0)
            {
                const pacePercent = (pace / pyRev) * 100;
                if (pacePercent >= thresholds.pace_increase) {
                    // console.log(`  -> Met via Pace: ${pacePercent.toFixed(1)}% >= ${thresholds.pace_increase}%`);
                    return true; // Meets criteria via Pace
                }
            }

            // Rule 3: Check Missing Products
            // Ensure rowData.missing_top_products is an array passed from API
            const missing = rowData.missing_top_products; // Assumes API sends an array or null/undefined
            if (Array.isArray(missing)) { // Check if it's actually an array
                 if (missing.length >= thresholds.missing_products) {
                     // console.log(`  -> Met via Missing Products: ${missing.length} >= ${thresholds.missing_products}`);
                    return true; // Meets criteria via Missing Products
                 }
            } else if (missing) {
                 // Log a warning if it exists but isn't an array
                 console.warn(`Growth Opp Check ${rowData?.canonical_code}: missing_top_products was not an array:`, missing);
            }


            // Rule 4: Check High Value Segment Due Soon
            if (rowData.rfm_segment && ["Champions", "Loyal Customers"].includes(rowData.rfm_segment)) {
                if (rowData.next_expected_purchase_date) {
                    try {
                        const today = new Date(); today.setHours(0,0,0,0);
                        const next14Days = new Date(today); next14Days.setDate(today.getDate() + 14); // 14 days from today (inclusive)
                        const dueDate = new Date(rowData.next_expected_purchase_date);

                        // Check if dueDate is a valid date object before normalizing
                        if (!isNaN(dueDate.getTime())) {
                            const dueDateOnly = new Date(dueDate); dueDateOnly.setHours(0,0,0,0); // Normalize

                            if (dueDateOnly <= next14Days) { // Check if valid date and within range (includes today up to 14 days from now)
                                // console.log(`  -> Met via Segment/Due Date: ${formatDate(dueDateOnly)}`);
                                return true; // Meets criteria via Segment/Due Date
                            }
                        } else {
                             console.warn(`Growth Opp Check ${rowData?.canonical_code}: Invalid due date string received:`, rowData.next_expected_purchase_date);
                        }
                    } catch(e) {
                         // Log error during date parsing/comparison
                         console.error(`Growth Opp Check ${rowData?.canonical_code}: Error processing due date`, e);
                    }
                }
            }
        } catch (e) {
             // Log any unexpected errors during the checks
             console.error(`Error in isGrowthOpportunity check for ${rowData?.canonical_code}:`, e, "Data:", rowData);
        }

        // If none of the criteria (Pace, Missing, Due Date) were met after passing health check
        // console.log(`  -> Did not meet any growth criteria.`);
        return false;
     }

    // Helper to get current thresholds
    function getThresholds() {
        // Returns an object containing all relevant threshold values
        return {
            p1: HIGH_PRIORITY_THRESHOLD,            // For Priority 1 filter
            p2: MED_PRIORITY_THRESHOLD,             // For Priority 2 filter
            poor: HEALTH_POOR_THRESHOLD,            // For Low Health filter
            // 'good' isn't strictly needed for filtering but might be useful elsewhere
            good: GROWTH_HEALTH_THRESHOLD,          // Reference for average/good health boundary
            growth_health: GROWTH_HEALTH_THRESHOLD, // For Growth Opps filter
            pace_decline: PACE_DECLINE_THRESHOLD,       // For Low Pace filter
            pace_increase: PACE_INCREASE_THRESHOLD,      // For High Pace filter
            missing_products: GROWTH_MISSING_PRODUCTS_THRESHOLD // For Growth Opps filter
            // Add any other thresholds used if necessary
        };
    }

    // REMOVED: Context Panel Reason/Action Generator - No longer needed here

    // --- Chart Initialization & Updates ---
    function initializeKpiCharts() {
        console.log("Initializing KPI charts...");
        
        // Destroy existing instances first
        if (healthChartInstance) {
            healthChartInstance.destroy();
            healthChartInstance = null; // Reset variable
        }
        if (segmentChartInstance) {
            segmentChartInstance.destroy();
            segmentChartInstance = null; // Reset variable
        }
    
        // Common chart options
        const commonOptions = {
            responsive: true,
            maintainAspectRatio: false,
            plugins: {
                legend: {
                    position: 'right',
                    labels: {
                        boxWidth: 12,
                        font: {
                            size: 10
                        }
                    }
                },
                tooltip: {
                    bodyFont: {
                        size: 12
                    },
                    titleFont: {
                        size: 13
                    }
                }
            }
        };
        
        // Doughnut-specific options
        const doughnutOptions = {
            ...commonOptions,
            cutout: '65%',
            plugins: {
                ...commonOptions.plugins,
                legend: {
                    ...commonOptions.plugins.legend,
                    position: 'right'
                }
            }
        };
    
        try { // Wrap individual chart creation in try/catch
            // Initialize Health Chart
            if (healthDistCanvas) {
                const ctxHealth = healthDistCanvas.getContext('2d');
                if (ctxHealth) {
                    healthChartInstance = new Chart(ctxHealth, {
                        type: 'doughnut',
                        data: { 
                            labels: ['No Data'], 
                            datasets: [{ 
                                label: 'Health Distribution',
                                data: [100], 
                                backgroundColor: ['rgba(200, 200, 200, 0.5)'],
                                borderWidth: 1,
                                hoverOffset: 8
                            }] 
                        },
                        options: doughnutOptions
                    });
                    
                    // Check if chart instance was created successfully
                    if (!healthChartInstance || typeof healthChartInstance.update !== 'function') {
                        console.error("Health Chart instance not properly initialized");
                        healthChartInstance = null;
                    } else {
                        console.log("Health Chart Instance CREATED:", healthChartInstance);
                    }
                } else { 
                    console.error("Failed to get 2D context for Health Chart canvas."); 
                }
            } else { 
                console.error("Health Chart canvas element not found during init."); 
            }
    
            // Initialize Segment Chart
            if (segmentDistCanvas) {
                const ctxSegment = segmentDistCanvas.getContext('2d');
                if (ctxSegment) {
                    segmentChartInstance = new Chart(ctxSegment, {
                        type: 'doughnut',
                        data: { 
                            labels: ['No Data'], 
                            datasets: [{ 
                                label: 'RFM Segment Distribution',
                                data: [100], 
                                backgroundColor: ['rgba(200, 200, 200, 0.5)'],
                                borderWidth: 1,
                                hoverOffset: 8
                            }] 
                        },
                        options: doughnutOptions
                    });
                    
                    // Check if chart instance was created successfully
                    if (!segmentChartInstance || typeof segmentChartInstance.update !== 'function') {
                        console.error("Segment Chart instance not properly initialized");
                        segmentChartInstance = null;
                    } else {
                        console.log("Segment Chart Instance CREATED:", segmentChartInstance);
                    }
                } else { 
                    console.error("Failed to get 2D context for Segment Chart canvas."); 
                }
            } else { 
                console.error("Segment Chart canvas element not found during init."); 
            }
    
        } catch (error) {
            console.error("Error during KPI chart initialization:", error);
            // Ensure instances are null if creation failed
            if (healthChartInstance) { 
                try {
                    healthChartInstance.destroy(); 
                } catch (e) {
                    console.error("Error destroying health chart:", e);
                }
                healthChartInstance = null; 
            }
            if (segmentChartInstance) { 
                try {
                    segmentChartInstance.destroy(); 
                } catch (e) {
                    console.error("Error destroying segment chart:", e);
                }
                segmentChartInstance = null; 
            }
        }
        
        console.log("KPI charts initialization finished. Health chart:", 
                    !!healthChartInstance ? "created" : "failed", 
                    "Segment chart:", 
                    !!segmentChartInstance ? "created" : "failed");
    }
    
    function aggregateAndDrawCharts(accounts) {
        console.log(`Aggregating chart data for ${accounts?.length || 0} accounts.`);
        
        // Check if chart instances are ready
        if (!healthChartInstance || !segmentChartInstance) {
            console.warn("Chart instances not ready for aggregation. Cannot update charts.");
            // Do not proceed if charts aren't initialized
            return;
        }
        
        // Clear charts if no data
        if (!accounts || accounts.length === 0) {
            console.log("No accounts to aggregate, charts cleared.");
            
            // Pass empty counts to clear
            updateHealthDistChart({});
            updateSegmentDistChart({});
            return;
        }
        
        // Initialize count objects
        const healthCounts = {};
        healthColorMapping.forEach(cat => { 
            healthCounts[cat.name] = 0; 
        });
        healthCounts['Unknown'] = 0;
        
        const segmentCounts = {};
        segmentCounts['Unknown'] = 0;
        
        // Aggregate counts from account data
        try {
            accounts.forEach(acc => {
                // Health categorization
                if (acc.health_score != null && acc.health_score !== undefined) {
                    const category = getHealthCategoryName(acc.health_score);
                    healthCounts[category] = (healthCounts[category] || 0) + 1;
                } else {
                    healthCounts['Unknown']++;
                }
                
                // Segment categorization
                if (acc.rfm_segment && acc.rfm_segment.trim() !== '') {
                    const segment = acc.rfm_segment.trim();
                    segmentCounts[segment] = (segmentCounts[segment] || 0) + 1;
                } else {
                    segmentCounts['Unknown']++;
                }
            });
        } catch (error) {
            console.error("Error during account data aggregation:", error);
            // Continue with whatever data we collected, or return if critical
        }
        
        // Log the final counts before updating charts
        console.log("Final Aggregated Health Counts:", JSON.stringify(healthCounts));
        console.log("Final Aggregated Segment Counts:", JSON.stringify(segmentCounts));
        
        // Check for empty/invalid counts (all zeros or missing categories)
        const hasValidHealthData = Object.values(healthCounts).some(count => count > 0);
        const hasValidSegmentData = Object.values(segmentCounts).some(count => count > 0);
        
        if (!hasValidHealthData) {
            console.warn("No valid health data found after aggregation.");
        }
        
        if (!hasValidSegmentData) {
            console.warn("No valid segment data found after aggregation.");
        }
        
        // Call update functions AFTER logging
        updateHealthDistChart(healthCounts);
        updateSegmentDistChart(segmentCounts);
        
        console.log("Chart updates initiated.");
    }
    
    function updateHealthDistChart(healthCounts) {
        // Check if instance exists BEFORE proceeding
        if (!healthChartInstance) {
            console.warn("Cannot update health chart - instance is null.");
            return;
        }
        
        console.log("Updating Health Chart with counts:", healthCounts); // Log input counts
        
        // Handle empty input case
        if (!healthCounts || Object.keys(healthCounts).length === 0) {
            console.log("Empty health counts provided, displaying 'No Data' placeholder.");
            
            const labels = ['No Data'];
            const data = [100];
            const colors = ['rgba(200, 200, 200, 0.5)'];
            
            console.log("Health Chart - Setting placeholder data:", { labels, data, colors });
            
            try {
                healthChartInstance.data.labels = labels;
                healthChartInstance.data.datasets[0].data = data;
                healthChartInstance.data.datasets[0].backgroundColor = colors;
                healthChartInstance.update();
                console.log("Health Chart Updated with placeholder data.");
            } catch(e) {
                console.error("Error updating health chart with placeholder data:", e);
            }
            
            return;
        }
        
        // Filter out zero counts and prepare data for chart
        const data = [];
        const labels = [];
        const colors = [];
        
        try {
            // Sort health categories from highest threshold to lowest
            const sortedCategories = [...healthColorMapping].sort((a, b) => b.threshold - a.threshold);
            
            sortedCategories.forEach(category => {
                const count = healthCounts[category.name] || 0;
                console.log(` -> Category: ${category.name}, Count: ${count}, Extracted Color: ${category.color} (Type: ${typeof category.color})`);
                if (count > 0) {
                    labels.push(`${category.name} (${count})`);
                    data.push(count);
                    colors.push(category.color);
                }
            });
            
            // Add unknown if it exists and is not zero
            if (healthCounts['Unknown'] > 0) {
                labels.push(`Unknown (${healthCounts['Unknown']})`);
                data.push(healthCounts['Unknown']);
                colors.push('rgba(180, 180, 180, 0.7)'); // Gray for unknown
            }
            
            // If no data after filtering, show a placeholder
            if (data.length === 0) {
                console.log("No non-zero health counts found, displaying 'No Data' placeholder.");
                labels.push('No Data');
                data.push(100);
                colors.push('rgba(200, 200, 200, 0.5)');
            }
        } catch (error) {
            console.error("Error processing health counts:", error);
            // Fallback to placeholder data
            labels.push('Error');
            data.push(100);
            colors.push('rgba(255, 0, 0, 0.3)'); // Light red for error
        }
        
        console.log("Health Chart - Setting Data:", { 
            labels, 
            data, 
            colors,
            totalAccounts: data.reduce((sum, count) => sum + (count === 100 ? 0 : count), 0)
        });
        
        try {
            healthChartInstance.data.labels = labels;
            healthChartInstance.data.datasets[0].data = data;
            healthChartInstance.data.datasets[0].backgroundColor = colors;
            healthChartInstance.update(); // Update the chart
            console.log("Health Chart Updated successfully.");
        } catch(e) {
            console.error("Error updating health chart:", e);
        }
    }
    
    function updateSegmentDistChart(segmentCounts) {
        // Check if instance exists BEFORE proceeding
        if (!segmentChartInstance) {
            console.warn("Cannot update segment chart - instance is null.");
            return;
        }
        
        if (!segmentCounts) {
            console.warn("Invalid segment counts provided to updateSegmentDistChart.");
            return;
        }
        
        console.log("Updating Segment Chart with counts:", segmentCounts); // Log input counts
        
        // Filter out zero counts and prepare data for chart
        const data = [];
        const labels = [];
        const colors = [];
        let colorIndex = 0;
        
        try {
            // Sort segment names alphabetically for consistency
            const segments = Object.keys(segmentCounts)
                .filter(segment => segmentCounts[segment] > 0)
                .sort();
            
            // Handle 'Unknown' segment specially - move to end
            const unknownIndex = segments.indexOf('Unknown');
            if (unknownIndex !== -1) {
                segments.splice(unknownIndex, 1);
                if (segmentCounts['Unknown'] > 0) {
                    segments.push('Unknown');
                }
            }
            
            // Add segments to chart data arrays
            segments.forEach(segment => {
                const count = segmentCounts[segment];
                labels.push(`${segment} (${count})`);
                data.push(count);
                
                // Use special color for Unknown, otherwise rotate through segment colors
                if (segment === 'Unknown') {
                    colors.push('rgba(180, 180, 180, 0.7)');
                } else {
                    colors.push(segmentDistColors[colorIndex % segmentDistColors.length]);
                    colorIndex++;
                }
            });
            
            // If no data after filtering, show a placeholder
            if (data.length === 0) {
                console.log("No non-zero segment counts found, displaying 'No Data' placeholder.");
                labels.push('No Data');
                data.push(100);
                colors.push('rgba(200, 200, 200, 0.5)');
            }
        } catch (error) {
            console.error("Error processing segment counts:", error);
            // Fallback to placeholder data
            labels.push('Error');
            data.push(100);
            colors.push('rgba(255, 0, 0, 0.3)'); // Light red for error
        }
        
        console.log("Segment Chart - Setting Data:", { 
            labels, 
            data, 
            colors,
            totalAccounts: data.reduce((sum, count) => sum + (count === 100 ? 0 : count), 0)
        });
        
        try {
            // Update chart data
            segmentChartInstance.data.labels = labels;
            segmentChartInstance.data.datasets[0].data = data;
            segmentChartInstance.data.datasets[0].backgroundColor = colors;
            
            // Redraw the chart
            segmentChartInstance.update();
            console.log("Segment Chart Updated successfully.");
        } catch(e) {
            console.error("Error updating segment chart:", e);
        }
    }


    // --- Utilities ---
    // (Keep formatCurrency, formatNumber, formatDate, getHealthColorInfo, getHealthCategoryName)
    // (Keep showNotification, showTableLoading, showTableNoData)
    // (Keep clearDashboardData, clearKpiSummary, disableQuickFilters)
    // (Keep fetchFilterOptions, populateStaticFilters, populateFilterDropdown)
    function formatCurrency(value, digits = 0) {
        if (value === null || value === undefined || isNaN(value)) return '-';
        const absVal = Math.abs(value); const sign = value < 0 ? '-' : '';
        if (absVal >= 1e6) return `${sign}${(absVal / 1e6).toFixed(digits)}M`;
        if (absVal >= 1e3) return `${sign}${(absVal / 1e3).toFixed(digits)}K`;
        return value.toLocaleString('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0 });
    }
    function formatNumber(value, digits = 0) {
        if (value === null || value === undefined || isNaN(value)) return null;
        return parseFloat(value.toFixed(digits));
    }
    function formatDate(dateString, format = 'long') {
        if (!dateString) return 'N/A';
        try {
            const date = new Date(dateString);
            if (isNaN(date.getTime())) return 'Invalid Date';
            if (format === 'short') return `${date.getMonth() + 1}/${date.getDate()}`;
            return `${date.getMonth() + 1}/${date.getDate()}/${date.getFullYear()}`;
        } catch (e) { return 'Invalid Date'; }
    }
    function getHealthColorInfo(healthScore) {
        if (healthScore === null || healthScore === undefined || isNaN(healthScore)) { return { color: 'rgba(128, 128, 128, 0.7)', name: 'Unknown', badge: 'bg-secondary' }; }
        const sortedMapping = [...healthColorMapping].sort((a,b) => b.threshold - a.threshold);
        const mapping = sortedMapping.find(m => healthScore >= m.threshold);
        return mapping || { color: 'rgba(231, 76, 60, 1)', name: 'Critical', badge: 'bg-danger' };
    }
    function getHealthCategoryName(healthScore) { return getHealthColorInfo(healthScore).name; }
    function showNotification(message, type = 'info') {
        console.log(`[${type.toUpperCase()}] Notification: ${message}`);
        // Basic alert fallback if needed, or implement your notification UI later
        // alert(`[${type}] ${message}`);
    }
    function showTableLoading(isLoading) {
        console.log(`Setting table loading active state: ${isLoading}`);
        const tableLoadingIndicator = document.getElementById('tableLoadingIndicator');
        if (!tableLoadingIndicator) {
            console.error("!!! tableLoadingIndicator element not found in showTableLoading !!!");
            return;
        }
    
        if (isLoading) {
            tableLoadingIndicator.classList.add('active');
        } else {
            tableLoadingIndicator.classList.remove('active');
        }
        // Log the class list to confirm
        console.log(` -> tableLoadingIndicator classList: ${tableLoadingIndicator.className}`);
    }
    // Improved showTableNoData function

    
    function showTableNoData(isNoData, message = "No data available.") {
        const tableNoDataIndicator = document.getElementById('tableNoDataIndicator'); // Still need reference to set text
    
        if (isNoData) {
            // Set the message inside the indicator
            if (tableNoDataIndicator) {
                const pElement = tableNoDataIndicator.querySelector('p');
                if (pElement) pElement.textContent = message;
            } else {
                console.error("!!! tableNoDataIndicator element not found in showTableNoData !!!");
            }
            // Add class to BODY to make indicator visible via CSS
            document.body.classList.add('show-no-data');
            console.log(`Showing No Data Indicator by adding 'show-no-data' to body. Message: "${message}"`);
        } else {
            // Remove class from BODY to hide indicator via CSS
            document.body.classList.remove('show-no-data');
            console.log("Hiding No Data Indicator by removing 'show-no-data' from body.");
        }
    
        // Hide loading indicator whenever controlling no-data indicator
        const tableLoadingIndicator = document.getElementById('tableLoadingIndicator');
        if(tableLoadingIndicator) {
             tableLoadingIndicator.classList.remove('active'); // Assuming loading uses classList too
        }
    }
   
  
    function clearDashboardData() {
        console.log("Clearing dashboard data...");
        allAccountData = [];
        currentSummaryStats = null;
        if (accountTable) {
            // Clear and redraw immediately. The drawCallback will handle
            // showing the appropriate 'no data' message after the draw.
            accountTable.clear().draw();
            console.log("DataTable cleared by clearDashboardData. Draw triggered.");
        } else {
            // If table isn't even initialized, ensure the indicator is shown initially
            // with the correct prompt.
            console.log("DataTable not initialized, showing initial 'Select Rep' message.");
            showTableNoData(true, "Select a Sales Rep to load data.");
        }
        // Clear other parts of the UI
        clearKpiSummary();
        setActiveQuickFilter('all'); // Reset quick filter button visually

        // --- REMOVED THE REDUNDANT/PROBLEMATIC CALL TO showTableNoData HERE ---
    }
    function clearKpiSummary() {
        console.log("Clearing KPI Summary");
        

        // Reset KPI elements
        if (kpiTotalAccountsEl) kpiTotalAccountsEl.textContent = '-';
        if (kpiPyLabelEl) kpiPyLabelEl.textContent = 'Prev Year';
        if (kpiCyLabelEl) kpiCyLabelEl.textContent = 'Curr Year';
        if (kpiPySalesEl) kpiPySalesEl.textContent = '-';
        if (kpiCytdSalesEl) kpiCytdSalesEl.textContent = '-';
        if (kpiYepValueEl) kpiYepValueEl.textContent = '-';
        if (kpiPaceVsPyEl) kpiPaceVsPyEl.textContent = '-';
        
        // Reset counter elements
        if (countPriority1El) countPriority1El.textContent = '0';
        if (countPriority2El) countPriority2El.textContent = '0';
        if (countDueThisWeekEl) countDueThisWeekEl.textContent = '0';
        if (countOverdueEl) countOverdueEl.textContent = '0';
        if (countLowHealthEl) countLowHealthEl.textContent = '0';
        if (countLowPaceEl) countLowPaceEl.textContent = '0';
        if (countHighPaceEl) countHighPaceEl.textContent = '0';
        if (countGrowthOppsEl) countGrowthOppsEl.textContent = '0';
        if (countAllEl) countAllEl.textContent = '0';
        
        // Clear charts with placeholder data
        if (healthChartInstance && healthChartInstance.data && 
            healthChartInstance.data.datasets && healthChartInstance.data.datasets.length > 0) {
            
            healthChartInstance.data.labels = ['No Data'];
            healthChartInstance.data.datasets[0].data = [100];
            healthChartInstance.data.datasets[0].backgroundColor = ['rgba(200, 200, 200, 0.5)'];
            healthChartInstance.update();
        } else {
            console.warn("Health chart instance or dataset not ready for clearing.");
        }
        
        if (segmentChartInstance && segmentChartInstance.data && 
            segmentChartInstance.data.datasets && segmentChartInstance.data.datasets.length > 0) {
            
            segmentChartInstance.data.labels = ['No Data'];
            segmentChartInstance.data.datasets[0].data = [100];
            segmentChartInstance.data.datasets[0].backgroundColor = ['rgba(200, 200, 200, 0.5)'];
            segmentChartInstance.update();
        } else {
            console.warn("Segment chart instance or dataset not ready for clearing.");
        }
    }
   
    function disableQuickFilters(isDisabled) {
         console.log(`Setting quick filters disabled state: ${isDisabled}`);
         document.querySelectorAll('.quick-filter-btn').forEach(btn => {
             btn.disabled = isDisabled;
             btn.classList.toggle('disabled', isDisabled);
         });
    }
    async function fetchFilterOptions() {
        console.log("Fetching filter options..."); // Keep log
        try {
            // Use the correct API endpoint that returns rep ID and Name
            const response = await fetch(`/api/sales-manager/sales_rep_performance`); // Or another endpoint if this one isn't ideal
            if (!response.ok) {
                 // Try to get more specific error message
                 let errorText = `HTTP ${response.status}`;
                 try { errorText += `: ${await response.text()}`; } catch(e) {}
                 throw new Error(errorText);
            }
            const data = await response.json();
            console.log("Filter options RAW RESPONSE:", data); // Keep log
            // Basic validation of received data structure
            if (!data || typeof data !== 'object' || !data.performance || !Array.isArray(data.performance)) {
                 console.error("Invalid data structure received for filters:", data);
                 throw new Error("Invalid filter data structure from server.");
            }
            console.log("Filter options fetched successfully.");
            return data; // Return the full data object containing 'performance'
        } catch (error) {
            console.error("!!! Failed to fetch filter options:", error);
            showNotification("Could not load filter options.", "error");
            return null; // Return null on error so .then chain can handle it
        }
    }
    function populateStaticFilters(data) {
        console.log("populateStaticFilters received data:", data); // Keep this log

        // Add robust checks
        if (!data || typeof data !== 'object') {
            console.error("populateStaticFilters received invalid data type:", data);
            // Populate with empty options to prevent later errors if elements exist
            if (distributorFilterEl) populateFilterDropdown(distributorFilterEl, [], "All Distributors");
            if (repFilterEl) populateFilterDropdown(repFilterEl, [], "-- Select a Rep --", true);
            return; // Stop processing
        }
        if (!data.performance || !Array.isArray(data.performance)) {
            console.warn("No valid 'performance' array found in data for filters.");
            // Populate with empty options
            if (distributorFilterEl) populateFilterDropdown(distributorFilterEl, [], "All Distributors");
            if (repFilterEl) populateFilterDropdown(repFilterEl, [], "-- Select a Rep --", true);
            return; // Stop processing
        }

        // *** THIS IS THE CRUCIAL PROCESSING BLOCK ***
        // Extract unique reps with ID and Name
        const uniqueReps = data.performance
           .map(r => ({
                id: r.sales_rep, // Get the ID
                name: r.sales_rep_name || `Rep #${r.sales_rep}` // Get the name, fallback if missing
            }))
           .filter(r => r.id != null && r.name) // Ensure both ID and Name exist and are not null
           .filter((rep, index, self) => index === self.findIndex((r) => r.id === rep.id)) // Filter to get unique reps based on ID
           .sort((a,b)=> String(a.name).localeCompare(String(b.name))); // Sort alphabetically by name

        console.log("Processed uniqueReps:", uniqueReps); // Check this output in the console

        // Extract unique distributors (assuming it might come from performance data too, adjust if needed)
        // If distributor isn't in performance, you might need another API call or source
        const uniqueDistributors = Array.from(new Set(
                data.performance
                    .map(r => r.distributor) // Assuming distributor info is in the performance object
                    .filter(Boolean) // Filter out null/empty distributor names
             ))
            .sort();

        console.log("Processed uniqueDistributors:", uniqueDistributors);

        // *** END OF CRUCIAL PROCESSING BLOCK ***

        // Now populate the dropdowns using the processed arrays
        populateFilterDropdown(distributorFilterEl, uniqueDistributors, "All Distributors");
        populateFilterDropdown(repFilterEl, uniqueReps, "-- Select a Rep --", true); // Pass uniqueReps here
    }
    function populateFilterDropdown(selectElement, optionsArray, defaultOptionText, useIdAsValue = false) {
        // Ensure the target element exists
        if (!selectElement) {
            console.error("Target select element is NULL or undefined for:", defaultOptionText);
            return;
        }
        console.log(`Populating dropdown for '${defaultOptionText}' with ${optionsArray?.length ?? 0} options.`);

        // Clear existing options FIRST
        selectElement.innerHTML = ''; // Clear everything

        // Add the default/placeholder option
        const defaultOption = document.createElement('option');
        defaultOption.value = ""; // Empty value for the placeholder
        defaultOption.textContent = defaultOptionText;
        selectElement.appendChild(defaultOption);

        // Check if optionsArray is valid
        if (!Array.isArray(optionsArray)) {
            console.warn("Invalid or empty optionsArray provided for:", defaultOptionText); // Warn instead of error
            return; // Stop if the array is invalid or empty
        }

        // Loop through the options and append them
        optionsArray.forEach((optionData, index) => {
            const option = document.createElement('option');

            if (useIdAsValue && typeof optionData === 'object' && optionData !== null) {
                // Handling objects like { id: '10.0', name: 'Rep Name' }
                const optionValue = optionData.id ?? ''; // Use ID as value
                const optionText = optionData.name ?? `Invalid (ID: ${optionData.id})`; // Use Name as text
                option.value = optionValue;
                option.textContent = optionText;

                // Only append if there's a valid ID (value)
                if (optionValue !== '') { // Check specifically for non-empty string ID
                    selectElement.appendChild(option);
                } else {
                    console.warn("Skipping Option due to missing or empty ID:", optionData);
                }
            } else if (!useIdAsValue && typeof optionData === 'string' && optionData) {
                // Handling simple string arrays like distributors ['Dist A', 'Dist B']
                option.value = optionData;
                option.textContent = optionData;
                selectElement.appendChild(option);
            } else if (optionData) {
                 // Handle other simple values (numbers, etc.) if !useIdAsValue
                 const value = String(optionData);
                 option.value = value;
                 option.textContent = value;
                 selectElement.appendChild(option);
            } else {
                // Log if the optionData is invalid or unexpected format
                console.warn(`Skipping invalid option data at index ${index}:`, optionData);
            }
        });
        console.log(`Finished populating dropdown '${selectElement.id}'. Option count: ${selectElement.options.length}`);
    }

}); // <--- END of DOMContentLoaded Listener

