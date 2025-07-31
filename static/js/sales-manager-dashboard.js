// Sales Manager Dashboard JavaScript
document.addEventListener('DOMContentLoaded', function() {
    // --- Global Variables ---
    let dashboardData = {
        salesReps: [],         // Full list of reps {id, name} from performanceData
        accounts: [],          // Detailed account list, usually from /yoy_growth, includes health/churn/growth
        distributors: [],      // Unique distributor names
        years: [],             // Available years for filter
        performanceData: [],   // Performance summary per rep {sales_rep, name, revenue, count, yoy, avgHealth, avgChurn, prev_...}
        topAccountsByRep: {}   // Top revenue accounts per rep {repId: [account_list]}
    };

    let filters = {
        year: null,            // Set during initialization
        distributor: '',
        salesRep: ''
    };

    // Chart objects
    let salesRepPerformanceChart, revenueDistributionChart, yoyGrowthChart, healthScoreDistributionChart;
    let accountYearlyRevenueChart, accountYearlyTransactionsChart; // Modal charts

    // --- DOM Element References ---
    const yearFilter = document.getElementById('yearFilter');
    const distributorFilter = document.getElementById('distributorFilter');
    const salesRepFilter = document.getElementById('salesRepFilter');
    const applyFiltersBtn = document.getElementById('applyFilters');
    const refreshDataBtn = document.getElementById('refreshData');
    const currentDateElem = document.getElementById('currentDate');
    const sidebarToggle = document.getElementById('sidebarToggle');
    const sidebar = document.getElementById('sidebar');
    const topAccountsContainer = document.getElementById('topAccountsContainer');
    const accountsTable = document.getElementById('accountsTable');
    const exportCsvBtn = document.getElementById('exportCsv');
    const kpiSection = document.querySelector('.kpi-summary'); // KPI cards container
    const mainContentTitle = document.querySelector('.nav-title h1'); // Main H1 title
    const accountDetailsModalElement = document.getElementById('accountDetailsModal');
    const accountDetailsModal = accountDetailsModalElement ? new bootstrap.Modal(accountDetailsModalElement) : null;

    // --- Initialization ---
    initializeDashboard();

    // --- Event Listeners ---
    if (applyFiltersBtn) {
        applyFiltersBtn.addEventListener('click', () => {
            filters.year = parseInt(yearFilter.value);
            filters.distributor = distributorFilter.value;
            filters.salesRep = salesRepFilter.value;
            console.log("Applying filters:", filters);
            loadDashboardData();
        });
    }

    if (refreshDataBtn) {
        refreshDataBtn.addEventListener('click', () => {
            showNotification('Refreshing dashboard data...', 'info');
            loadDashboardData(true); // Force refresh
        });
    }

    if (exportCsvBtn) {
        exportCsvBtn.addEventListener('click', () => exportTableToCsv('accounts-data.csv'));
    }

    if (sidebarToggle && sidebar) {
        sidebarToggle.addEventListener('click', () => sidebar.classList.toggle('active'));
    }

    if (accountsTable) {
        accountsTable.addEventListener('click', (e) => {
            const button = e.target.closest('.btn-view-account');
            if (button) {
                // MODIFIED: Changed variable name for clarity but keeping the attribute name
                const canonicalCode = button.dataset.cardCode;
                if (canonicalCode) openAccountDetails(canonicalCode);
                else showNotification('Could not get account ID.', 'error');
            }
        });
    }

    // --- Core Functions ---

    function initializeDashboard() {
        console.log("Initializing dashboard...");
        if (currentDateElem) currentDateElem.textContent = moment().format('MMMM D, YYYY');
        initializeCharts();
        loadAvailableYears().then(() => {
            if (dashboardData.years.length > 0) {
                filters.year = dashboardData.years[0]; // Default to most recent
                if (yearFilter) yearFilter.value = filters.year;
            } else {
                filters.year = new Date().getFullYear(); // Fallback
                if (yearFilter) yearFilter.value = filters.year;
            }
            console.log("Default year set to:", filters.year);
            loadDashboardData(); // Initial data load
        }).catch(err => {
            console.error("Initialization failed during year loading:", err);
            filters.year = new Date().getFullYear(); // Fallback year
            if(yearFilter) { // Populate filter with defaults on error
                 const currentYear = filters.year;
                 yearFilter.innerHTML = '';
                 [currentYear, currentYear - 1, currentYear - 2].forEach(y => {
                     const option = document.createElement('option');
                     option.value = y; option.textContent = y; yearFilter.appendChild(option);
                 });
                 yearFilter.value = filters.year;
            }
            loadDashboardData(); // Attempt to load data even if years failed
        });
    }

    async function loadAvailableYears() {
        console.log("Loading available years...");
        try {
            const response = await fetch('/api/sales-manager/years');
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();

            if (data.years && data.years.length > 0) {
                dashboardData.years = data.years.sort((a, b) => b - a); // Sort desc
                if (yearFilter) {
                    yearFilter.innerHTML = '';
                    dashboardData.years.forEach(year => {
                        const option = document.createElement('option');
                        option.value = year; option.textContent = year; yearFilter.appendChild(option);
                    });
                }
            } else { throw new Error("No years available from API"); }
        } catch (error) {
            console.error('Error loading available years:', error);
            showNotification('Failed to load year options. Using defaults.', 'warning');
            dashboardData.years = []; // Clear years on error
            throw error; // Re-throw for initializeDashboard's catch block
        }
    }

    async function loadDashboardData(forceRefresh = false) {
        if (!filters.year) {
            showNotification('Please select a year.', 'warning');
            return;
        }
        console.log("Loading dashboard data for filters:", filters);
        showLoading(true);
        try {
            await loadSalesRepPerformance(); // Fetch first to populate filters
            // Use Promise.all for parallel fetching where possible
            await Promise.all([
                 loadTopAccountsByRep(),    // Depends on filters populated by performance
                 loadAccountsData()         // Depends on filters populated by performance
            ]);
            // Update UI after all data is fetched
            updateDashboardTitle();
            updateKPISummary();
            updateAllCharts();
            updateTopAccountsSection();
            updateDataTable();
            if (forceRefresh) showNotification('Dashboard data refreshed', 'success');
        } catch (error) {
            console.error('Error loading dashboard data:', error);
            showNotification('Failed to load dashboard data.', 'error');
        } finally {
            showLoading(false);
        }
    }

    async function loadSalesRepPerformance() {
        const url = `/api/sales-manager/sales_rep_performance?year=${filters.year}${filters.distributor ? `&distributor=${encodeURIComponent(filters.distributor)}` : ''}`;
        // console.log("Fetching sales rep performance:", url); // Keep log removed or minimal
        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            dashboardData.performanceData = data.performance || []; // Store the raw performance data

            // --- Extract unique distributors (no change here) ---
            const distinctDistributors = new Set(dashboardData.performanceData.map(rep => rep.distributor).filter(Boolean));
            dashboardData.distributors = Array.from(distinctDistributors).sort();

            // --- Create Sales Rep List - Handling Unassigned ---
            dashboardData.salesReps = dashboardData.performanceData
                .map(rep => ({
                    // Assign special ID '__UNASSIGNED__' if rep ID is missing/null/empty
                    id: (rep.sales_rep !== null && rep.sales_rep !== undefined && String(rep.sales_rep).trim() !== '') ? String(rep.sales_rep) : '__UNASSIGNED__',
                    // Assign clear name 'Unassigned Accounts' or use rep name/ID
                    name: rep.sales_rep_name || ( (rep.sales_rep !== null && rep.sales_rep !== undefined && String(rep.sales_rep).trim() !== '') ? `Rep #${rep.sales_rep}` : 'Unassigned Accounts')
                }))
                // Group all unassigned under one entry using reduce
                .reduce((accumulator, currentRep) => {
                    // If current is unassigned...
                    if (currentRep.id === '__UNASSIGNED__') {
                        // ...and we haven't added the main 'Unassigned Accounts' entry yet...
                        if (!accumulator.find(item => item.id === '__UNASSIGNED__')) {
                            accumulator.push({ id: '__UNASSIGNED__', name: 'Unassigned Accounts' }); // Add the single entry
                        }
                    }
                    // If current is an assigned rep...
                    else {
                        // ...and we haven't added this specific rep ID yet...
                        if (!accumulator.find(item => item.id === currentRep.id)) {
                            accumulator.push(currentRep); // Add the assigned rep
                        }
                    }
                    return accumulator; // Return the updated accumulator array
                }, []) // Start with an empty array for the accumulator
                // Custom sort: Put "Unassigned Accounts" first, then sort others by name
                .sort((a, b) => {
                    if (a.id === '__UNASSIGNED__') return -1; // Unassigned always comes first
                    if (b.id === '__UNASSIGNED__') return 1;  // Unassigned always comes first
                    return a.name.localeCompare(b.name);    // Sort others alphabetically by name
                });
            // --- End Create Sales Rep List ---

            // Update filter dropdowns AFTER data processing
            updateDistributorFilter(); // Update distributors first
            updateSalesRepFilter();    // Update reps based on possibly filtered data

        } catch (error) {
            console.error('Error loading sales rep performance:', error);
            // Reset data on error
            dashboardData.performanceData = [];
            dashboardData.distributors = [];
            dashboardData.salesReps = [];
            // Update filters to reflect empty state
            updateDistributorFilter();
            updateSalesRepFilter();
            throw error; // Re-throw error to be caught by loadDashboardData
        }
    }

    async function loadTopAccountsByRep() {
        const url = `/api/sales-manager/top_accounts_by_rep?year=${filters.year}${filters.distributor ? `&distributor=${encodeURIComponent(filters.distributor)}` : ''}${filters.salesRep ? `&sales_rep=${encodeURIComponent(filters.salesRep)}` : ''}&limit=20`; // Fetch up to 20
        console.log("Fetching top accounts by rep:", url);
        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
            dashboardData.topAccountsByRep = {}; // Reset
            const processAccount = acc => ({ ...acc, yearly_revenue: acc.yearly_revenue || 0, yoy_growth: acc.yoy_growth });
            if (data.reps) { data.reps.forEach(rep => { dashboardData.topAccountsByRep[rep.sales_rep] = (rep.accounts || []).map(processAccount); }); }
            else if (data.accounts && data.sales_rep) { dashboardData.topAccountsByRep[data.sales_rep] = (data.accounts || []).map(processAccount); }
        } catch (error) {
            console.error('Error loading top accounts by rep:', error);
            dashboardData.topAccountsByRep = {}; throw error;
        }
    }

    async function loadAccountsData() {
        // Fetches detailed list for table, risk, growth - Needs health/churn from API
        const url = `/api/sales-manager/yoy_growth?year=${filters.year}${filters.distributor ? `&distributor=${encodeURIComponent(filters.distributor)}` : ''}${filters.salesRep ? `&sales_rep=${encodeURIComponent(filters.salesRep)}` : ''}&limit=100&sort=revenue&direction=desc`;
        console.log("Fetching accounts data for table:", url);
        try {
            const response = await fetch(url);
            if (!response.ok) throw new Error(`HTTP error! status: ${response.status}`);
            const data = await response.json();
             // Ensure required fields exist with defaults
            dashboardData.accounts = (data.accounts || []).map(acc => ({
                ...acc,
                current_revenue: acc.current_revenue || 0,
                yoy_growth: acc.yoy_growth, // Allow null
                health_score: acc.health_score, // Expect these from API now
                health_category: acc.health_category,
                churn_risk_score: acc.churn_risk_score
            }));
        } catch (error) {
            console.error('Error loading accounts data:', error);
            dashboardData.accounts = []; throw error;
        }
    }

    // --- UI Update Functions ---

    function updateDashboardTitle() {
        if (!mainContentTitle) return;
        let title = "Sales Manager Dashboard";
        if (filters.salesRep && dashboardData.salesReps) {
            const rep = dashboardData.salesReps.find(r => r.id === filters.salesRep);
            if (rep) title = `${rep.name}'s Performance Overview`;
        } else if (filters.distributor) {
             title = `${filters.distributor} - Performance Overview`; // Title for distributor view
        }
        mainContentTitle.textContent = title;
    }

    function updateKPISummary() {
        // ... (Implementation from previous response - calculates overall or rep-specific KPIs) ...
        // ... (Ensure safeUpdate and updateTrendIndicator are defined below) ...
         if (!kpiSection) return;
        console.log("Updating KPI summary...");
        let revenue = 0, accounts = 0, avgHealth = 0;
        let revenueGrowth = 0, accountGrowth = 0;
        let isRepView = false;

        if (filters.salesRep && dashboardData.performanceData) {
            const repData = dashboardData.performanceData.find(rep => rep.sales_rep === filters.salesRep);
            if (repData) {
                isRepView = true;
                revenue = repData.total_revenue || 0; accounts = repData.account_count || 0;
                avgHealth = repData.avg_health_score ?? 0; 
                revenueGrowth = repData.yoy_revenue_growth ?? 0; accountGrowth = repData.yoy_account_growth ?? 0;
            }
        }
        if (!isRepView && dashboardData.performanceData) { // Overall calculation
            const perfData = dashboardData.performanceData;
            revenue = perfData.reduce((sum, rep) => sum + (rep.total_revenue || 0), 0);
            accounts = perfData.reduce((sum, rep) => sum + (rep.account_count || 0), 0);
            let th=0, hc=0, trpy=0, tapy=0; // Abbreviated accumulators
            perfData.forEach(rep => {
                if (rep.avg_health_score !== null && !isNaN(rep.avg_health_score)) { th += rep.avg_health_score; hc++; }
                trpy += (rep.prev_year_revenue || 0); tapy += (rep.prev_year_accounts || 0);
            });
            avgHealth = hc > 0 ? (th / hc) : 0; 
            revenueGrowth = (trpy > 0) ? ((revenue - trpy) / trpy) * 100 : (revenue > 0 ? 100 : 0);
            accountGrowth = (tapy > 0) ? ((accounts - tapy) / tapy) * 100 : (accounts > 0 ? 100 : 0);
        }

        const titleSuffix = isRepView ? " (Selected Rep)" : (filters.distributor ? ` (${filters.distributor})` : " (Overall)");
        document.querySelector('.total-revenue h3').textContent = `Total Revenue${titleSuffix}`;
        document.querySelector('.total-accounts h3').textContent = `Total Accounts${titleSuffix}`;
        document.querySelector('.avg-health h3').textContent = `Avg Health Score${titleSuffix}`;

        safeUpdate('totalRevenue', formatCurrency(revenue)); safeUpdate('totalAccounts', accounts.toLocaleString());
        safeUpdate('avgHealth', avgHealth.toFixed(1)); 
        updateTrendIndicator('revenueTrend', revenueGrowth); updateTrendIndicator('accountsTrend', accountGrowth);
        updateTrendIndicator('healthTrend', 0); 
        console.log("KPI summary updated.");
    }

    function updateTrendIndicator(elementId, value, inverse = false) {
        // ... (Implementation from previous response - formats % and applies +/- icon/class) ...
        const element = document.getElementById(elementId); if (!element) return;
        const trendValue = element.querySelector('.trend-value'); const icon = element.querySelector('i');
        if (!trendValue || !icon) return;
        const numericValue = Number(value);
        if (isNaN(numericValue)){ trendValue.textContent = 'N/A'; element.className='kpi-trend'; icon.className = 'fas'; return; }
        trendValue.textContent = `${Math.abs(numericValue).toFixed(1)}%`;
        element.classList.remove('positive', 'negative'); icon.className = 'fas'; // Reset
        const isPositive = inverse ? numericValue < -0.01 : numericValue > 0.01;
        const isNegative = inverse ? numericValue > 0.01 : numericValue < -0.01;
        if (isPositive) { element.classList.add('positive'); icon.classList.add('fa-arrow-up'); }
        else if (isNegative) { element.classList.add('negative'); icon.classList.add('fa-arrow-down'); }
        else { icon.classList.add('fa-minus'); } // Neutral
    }

    function initializeCharts() {
        // ... (Implementation from previous response - initializes all 4 main chart objects) ...
        console.log("Initializing charts...");
        Chart.defaults.font.family = "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif"; Chart.defaults.font.size = 12; Chart.defaults.color = '#666';
        const initChart = (id, config) => { const ctx = document.getElementById(id); if(ctx) return new Chart(ctx.getContext('2d'), config); else { console.warn(`Canvas not found: ${id}`); return null; }};
        salesRepPerformanceChart = initChart('salesRepPerformanceChart', { type: 'bar', data: { labels: [], datasets: [{ label: 'Revenue', data: [], backgroundColor: 'rgba(52, 152, 219, 0.7)' }, { label: 'YoY Growth', data: [], type: 'line', yAxisID: 'percentage', borderColor: 'rgba(46, 204, 113, 1)', backgroundColor: 'rgba(46, 204, 113, 0.1)', pointRadius: 3 }] }, options: { responsive: true, maintainAspectRatio: false, scales: { y: { ticks: { callback: formatCurrencyShort }}, percentage: { position: 'right', title: {display: true, text: 'Growth %'}, ticks: { callback: v => v + '%'}} }, plugins: { legend: { position: 'top'}, tooltip: { callbacks: { label: ctx => ctx.datasetIndex === 0 ? `Revenue: ${formatCurrency(ctx.raw)}` : `YoY Growth: ${ctx.raw.toFixed(1)}%` }}}} });
        revenueDistributionChart = initChart('revenueDistributionChart', { type: 'doughnut', data: { labels: [], datasets: [{ data: [], borderWidth: 1 }] }, options: { responsive: true, maintainAspectRatio: false, plugins:{ legend: { position: 'bottom' }, tooltip: { callbacks: { label: ctx => `${formatCurrency(ctx.raw)} (${((ctx.raw / ctx.dataset.data.reduce((a, b) => a + b, 1e-6)) * 100).toFixed(1)}%)` }}}} });
        yoyGrowthChart = initChart('yoyGrowthChart', { type: 'bar', data: { labels: [], datasets: [{ label: 'YoY Growth %', data: [] }] }, options: { responsive: true, maintainAspectRatio: false, indexAxis: 'y', scales: { x: { title: { display: true, text: 'Growth %'}, ticks: { callback: v => v + '%' } } }, plugins: { legend: {display: false }, tooltip: { callbacks: {label: ctx => `Growth: ${ctx.raw.toFixed(1)}%` }}}} });
        healthScoreDistributionChart = initChart('healthScoreDistributionChart', { type: 'doughnut', data: { labels: ['Excellent', 'Good', 'Average', 'Poor', 'Critical'], datasets: [{ data: [0, 0, 0, 0, 0], backgroundColor: ['rgba(46, 204, 113, 0.7)', 'rgba(52, 152, 219, 0.7)', 'rgba(241, 196, 15, 0.7)', 'rgba(230, 126, 34, 0.7)', 'rgba(231, 76, 60, 0.7)'] }] }, options: { responsive: true, maintainAspectRatio: false, plugins: { legend: { position: 'bottom' }} } });
        console.log("Charts initialized.");
    }

    function updateAllCharts() {
        // ... (Implementation from previous response - calls individual chart update functions) ...
        console.log("Updating all charts...");
        updateSalesRepPerformanceChart(); updateRevenueDistributionChart();
        updateYoYGrowthChart(); updateHealthScoreDistributionChart();
        console.log("Charts updated.");
    }

    function updateSalesRepPerformanceChart() {
        // ... (Implementation from previous response - updates with top 15 reps) ...
        if (!salesRepPerformanceChart || !dashboardData.performanceData) return;
        const topReps = [...dashboardData.performanceData].sort((a, b) => (b.total_revenue || 0) - (a.total_revenue || 0)).slice(0, 15);
        salesRepPerformanceChart.data.labels = topReps.map(rep => rep.sales_rep_name || `Rep #${rep.sales_rep || '?'}`);
        salesRepPerformanceChart.data.datasets[0].data = topReps.map(rep => rep.total_revenue || 0);
        salesRepPerformanceChart.data.datasets[1].data = topReps.map(rep => rep.yoy_revenue_growth ?? 0);
        salesRepPerformanceChart.update();
    }

    function updateRevenueDistributionChart() {
        // ... (Implementation from previous response - shows distribution by rep or by account) ...
         if (!revenueDistributionChart) return;
         let labels = []; let data = [];
         const colorPalette = ['#3498db', '#2ecc71', '#9b59b6', '#f39c12', '#e74c3c', '#34495e', '#1abc9c', '#f1c40f', '#e67e22', '#95a5a6']; // Example palette
         const topN = 7; // Show top N + Others

         if (filters.salesRep && dashboardData.topAccountsByRep[filters.salesRep]) {
            const accounts = [...dashboardData.topAccountsByRep[filters.salesRep]].sort((a,b) => (b.yearly_revenue || 0) - (a.yearly_revenue || 0));
            const topAccounts = accounts.slice(0, topN);
            const otherRevenue = accounts.slice(topN).reduce((sum, acc) => sum + (acc.yearly_revenue || 0), 0);
            // MODIFIED: Changed to use both card_code and canonical_code with fallback
            labels = topAccounts.map(acc => acc.name || `Acc #${acc.canonical_code || acc.card_code || '?'}`);
            data = topAccounts.map(acc => acc.yearly_revenue || 0);
            if (otherRevenue > 0) { labels.push('Others'); data.push(otherRevenue); }
         } else if (dashboardData.performanceData.length > 0) {
            const reps = [...dashboardData.performanceData].sort((a, b) => (b.total_revenue || 0) - (a.total_revenue || 0));
             const topReps = reps.slice(0, topN);
             const otherRevenue = reps.slice(topN).reduce((sum, rep) => sum + (rep.total_revenue || 0), 0);
             labels = topReps.map(rep => rep.sales_rep_name || `Rep #${rep.sales_rep || '?'}`);
             data = topReps.map(rep => rep.total_revenue || 0);
             if (otherRevenue > 0) { labels.push('Others'); data.push(otherRevenue); }
         }
         revenueDistributionChart.data.labels = labels; revenueDistributionChart.data.datasets[0].data = data;
         revenueDistributionChart.data.datasets[0].backgroundColor = labels.map((_, i) => colorPalette[i % colorPalette.length]);
         revenueDistributionChart.update();
    }

    function updateYoYGrowthChart() {
        if (!yoyGrowthChart || !dashboardData.accounts) return;
        
        console.log("Starting YoY Growth chart update");
        console.log("Accounts data length:", dashboardData.accounts.length);
        
        const sortedByGrowth = [...dashboardData.accounts].filter(a => a.yoy_growth !== null && !isNaN(a.yoy_growth)).sort((a, b) => Math.abs(b.yoy_growth) - Math.abs(a.yoy_growth)).slice(0, 15);
        
        console.log("Filtered accounts with growth data:", sortedByGrowth.length);
        console.log("First few accounts with growth:", sortedByGrowth.slice(0, 3));
        
        yoyGrowthChart.data.labels = sortedByGrowth.map(acc => acc.name || `Acc #${acc.canonical_code || acc.card_code || '?'}`);
        const growthValues = sortedByGrowth.map(acc => acc.yoy_growth);
        
        console.log("Raw growth values:", growthValues);
        
        // Check if the values are very small (likely decimals rather than percentages)
        const isSmallDecimals = growthValues.every(v => Math.abs(v) < 1 && Math.abs(v) > 0);
        console.log("Values appear to be small decimals:", isSmallDecimals);
        
        // Convert decimal values to percentages if they're all small
        const displayValues = isSmallDecimals 
            ? growthValues.map(v => v * 100)  // Convert 0.05 to 5
            : growthValues;
        
        console.log("Display values after potential conversion:", displayValues);
        
        // Use the potentially converted values
        yoyGrowthChart.data.datasets[0].data = displayValues;
        yoyGrowthChart.data.datasets[0].backgroundColor = displayValues.map(v => v >= 0 ? 'rgba(46, 204, 113, 0.7)' : 'rgba(231, 76, 60, 0.7)');
        yoyGrowthChart.data.datasets[0].borderColor = displayValues.map(v => v >= 0 ? 'rgba(46, 204, 113, 1)' : 'rgba(231, 76, 60, 1)');
        
        yoyGrowthChart.update();
    }

    function updateHealthScoreDistributionChart() {
        // ... (Implementation from previous response - uses topAccountsByRep data, needs health_category/score) ...
         if (!healthScoreDistributionChart) return;
        const healthCategories = { 'Excellent': 0, 'Good': 0, 'Average': 0, 'Poor': 0, 'Critical': 0 };
        let accountsToAnalyze = [];
        // Use detailed accounts list if available and filtered, otherwise use top accounts
        if(filters.salesRep || filters.distributor){
            accountsToAnalyze = dashboardData.accounts || []; // Use list potentially containing health scores
        } else {
            // Fallback to using top accounts if no filter (less accurate overall view)
            accountsToAnalyze = Object.values(dashboardData.topAccountsByRep).flat();
        }

        let validCount = 0;
        accountsToAnalyze.forEach(account => {
             if (account && account.health_category && healthCategories.hasOwnProperty(account.health_category)) { healthCategories[account.health_category]++; validCount++; }
             else if (account && account.health_score !== null && !isNaN(account.health_score)){ // Fallback
                 const score = account.health_score; let cat = 'Critical';
                 if (score >= 80) cat = 'Excellent'; else if (score >= 60) cat = 'Good';
                 else if (score >= 40) cat = 'Average'; else if (score >= 20) cat = 'Poor';
                 healthCategories[cat]++; validCount++;
             }
        });
         if (validCount > 0){
            healthScoreDistributionChart.data.datasets[0].data = [ healthCategories['Excellent'], healthCategories['Good'], healthCategories['Average'], healthCategories['Poor'], healthCategories['Critical'] ];
         } else { healthScoreDistributionChart.data.datasets[0].data = [0,0,0,0,0]; }
         healthScoreDistributionChart.update();
    }

    /**
     * Update Top Accounts Section - Shows multiple rep panels OR
     * focused view for a single selected rep (At Risk, Top Growth ONLY)
     */
    function updateTopAccountsSection() {
        if (!topAccountsContainer) {
            console.warn("Top accounts container element not found.");
            return;
        }
        console.log("Updating Top Accounts Section...");
        topAccountsContainer.innerHTML = ''; // Clear existing content
        topAccountsContainer.className = 'top-accounts-container'; // Reset class


        if (filters.salesRep) {
            // --- Single Rep Focused View ---
            console.log(`Rendering focused view for rep: ${filters.salesRep}`);
            const repData = dashboardData.performanceData.find(r => r.sales_rep === filters.salesRep);
            const repName = repData?.sales_rep_name || `Rep #${filters.salesRep || '?'}`;

            // Use dashboardData.accounts (filtered by /yoy_growth API) for Risk/Growth lists
            // Ensure this data source includes health/churn scores from the API
            const repAccountsDetails = dashboardData.accounts || [];

            topAccountsContainer.classList.add('single-rep-view-active'); // Add specific class

            const panelsRow = document.createElement('div');
            // Use Bootstrap row with gutters, justify content center
            panelsRow.className = 'row g-3 justify-content-center';

            // --- Panel 1: At Risk Accounts --- (KEEP)
            const atRiskAccounts = repAccountsDetails
            .filter(acc => (acc.churn_risk_score !== null && acc.churn_risk_score >= 70) || (acc.health_score !== null && acc.health_score < 40)) // Risk thresholds
            .sort((a, b) => (b.current_revenue || 0) - (a.current_revenue || 0)) // Sort by current revenue desc
            .slice(0, 15); // Show up to 15 risky accounts

            const atRiskPanel = createAccountPanel(
                "High Risk Accounts",
                atRiskAccounts,
                account => `Risk: ${account.churn_risk_score?.toFixed(1) ?? 'N/A'}% | Health: ${account.health_score?.toFixed(1) ?? 'N/A'}`, // Primary: Risk/Health score
                account => renderValueBadge(account.current_revenue || 0) // Secondary: Revenue value
            );
            atRiskPanel.classList.add('col-md-6'); // Assign grid column class (half width)
            panelsRow.appendChild(atRiskPanel);


            // --- Panel 2: Top Growth Accounts --- (KEEP)
            const topGrowthAccounts = repAccountsDetails
            .filter(acc => acc.yoy_growth !== null && acc.yoy_growth > 5) // Positive growth > 5%
            .sort((a, b) => (b.yoy_growth || 0) - (a.yoy_growth || 0)) // Sort by growth desc
            .slice(0, 15); // Show up to 15 growth accounts

            const topGrowthPanel = createAccountPanel(
                "Top Growth Opportunities",
                topGrowthAccounts,
                account => renderGrowthBadge(account.yoy_growth), // Primary: Growth badge
                account => renderValueBadge(account.current_revenue || 0) // Secondary: Revenue value
            );
            topGrowthPanel.classList.add('col-md-6'); // Assign grid column class (half width)
            panelsRow.appendChild(topGrowthPanel);


            // --- Panel 3: Top Revenue Accounts --- (REMOVED)
            // The call to createAccountPanel for Top Revenue is omitted


            topAccountsContainer.appendChild(panelsRow); // Add the row containing the 2 panels

        } else {
            // --- Multi-Rep View (No specific rep selected) ---
            console.log("Rendering multi-rep view.");
            // Ensure correct class for multi-view layout (might need display:flex etc.)
            topAccountsContainer.classList.add('d-flex', 'flex-wrap', 'gap-3'); // Use flexbox for multi-rep panels

            const salesRepsWithAccounts = Object.entries(dashboardData.topAccountsByRep)
                .filter(([_, accounts]) => accounts && accounts.length > 0); // Filter out reps with no top accounts

            if (salesRepsWithAccounts.length === 0){
                topAccountsContainer.innerHTML = '<p class="text-muted text-center w-100">No top accounts data available for any rep.</p>'; // Added w-100 for centering
                return;
            }

            salesRepsWithAccounts.forEach(([repId, accounts]) => {
                const repData = dashboardData.performanceData.find(r => r.sales_rep === repId);
                const repName = repData?.sales_rep_name || `Rep #${repId || '?'}`;
                const repGrowth = (repData?.yoy_revenue_growth !== null && !isNaN(repData?.yoy_revenue_growth))
                    ? repData.yoy_revenue_growth.toFixed(1) + '%' : 'N/A';

                // Create the panel using the specific class for multi-view
                const panel = document.createElement('div');
                panel.className = 'sales-rep-panel'; // Style for multi-rep panels

                panel.innerHTML = `
                    <div class="rep-header">
                        <h3>${escapeHtml(repName)}</h3>
                        <div class="rep-performance">
                            <div class="rep-metric">${escapeHtml(repGrowth)} YoY Rev</div>
                        </div>
                    </div>
                    <ul class="rep-accounts-list"></ul>
                `;

                const accountsList = panel.querySelector('.rep-accounts-list');
                // Show top N accounts as fetched by loadTopAccountsByRep (e.g., limit=20 was used)
                // You can adjust the slice here if needed (e.g., accounts.slice(0, 5) to show fewer)
                accounts.slice(0, 10).forEach(account => {
                    const li = document.createElement('li');
                    li.className = 'account-item';
                    
                    // MODIFIED: Use canonical_code with fallback to card_code
                    li.dataset.cardCode = account.canonical_code || account.card_code || '';

                    // --- Partner Icon ---
                    const partnerIcon = account.is_partner
                        ? '<i class="fas fa-heart text-danger ms-1 small" title="Partner Account"></i>' // Smaller icon here
                        : '';
                    // --- End Partner Icon ---

                    // MODIFIED: Add icon to account name details
                    li.innerHTML = `
                        <div class="account-details me-2">
                            <div class="account-name text-truncate" title="${escapeHtml(account.name || '')}">${escapeHtml(account.name||'?')}${partnerIcon}</div>
                            <div class="account-revenue">${formatCurrency(account.yearly_revenue||0)}</div>
                        </div>
                        <div class="ms-auto">
                            ${renderGrowthBadge(account.yoy_growth)}
                        </div>
                    `;

                    li.addEventListener('click', function() {
                        const code = this.dataset.cardCode;
                        if (code){ openAccountDetails(code); }
                        else { showNotification("Missing account ID.", "error"); }
                    });
                    accountsList.appendChild(li);
                });
                topAccountsContainer.appendChild(panel);
            });
        }
        console.log("Top Accounts Section updated.");
    }

    function createAccountPanel(title, accounts, primaryMetricFn, secondaryMetricFn) {
        // ... (Implementation from previous response - creates a col-md-4 with styled panel and list) ...
         const col = document.createElement('div'); col.className = 'col-md-4 mb-3'; // Use Bootstrap column & margin
         const panel = document.createElement('div'); panel.className = 'sales-rep-panel h-100 d-flex flex-column'; // Flex column
         panel.innerHTML = `<div class="rep-header flex-shrink-0"><h3>${escapeHtml(title)}</h3></div><ul class="rep-accounts-list flex-grow-1"></ul>`; // Header shrinks 0, list grows
         const accountsList = panel.querySelector('.rep-accounts-list');
         if (!accounts || accounts.length === 0) { accountsList.innerHTML = '<li class="account-item text-muted p-3">None found</li>'; }
         else { accounts.forEach(account => {
                 const li = document.createElement('li'); li.className = 'account-item'; 
                 
                 // MODIFIED: Use canonical_code with fallback to card_code
                 li.dataset.cardCode = account.canonical_code || account.card_code || '';
                 
                 li.innerHTML = `<div class="account-details me-2"><div class="account-name text-truncate">${escapeHtml(account.name||'?')}</div><div class="account-revenue">${primaryMetricFn(account)}</div></div><div class="ms-auto">${secondaryMetricFn(account)}</div>`; // Use ms-auto for secondary metric alignment
                 li.addEventListener('click', function() { if(this.dataset.cardCode) openAccountDetails(this.dataset.cardCode); else showNotification("Missing account ID.", "error");});
                 accountsList.appendChild(li); });
         }
         col.appendChild(panel); return col;
    }

    function renderGrowthBadge(growthValue) {
        // ... (Implementation from previous response - returns HTML string for growth badge) ...
        if (growthValue === null || typeof growthValue === 'undefined' || isNaN(growthValue)) return '<span class="account-growth text-muted fs-sm">--</span>';
        const growthClass = growthValue >= 0 ? 'growth-positive' : 'growth-negative';
        const growthIcon = growthValue >= 0 ? 'fa-arrow-up' : 'fa-arrow-down';
        return `<div class="account-growth ${growthClass} fs-sm"><i class="growth-icon fas ${growthIcon} me-1"></i>${Math.abs(growthValue).toFixed(1)}%</div>`; // Added fs-sm for smaller text
    }

    function renderValueBadge(value){
        // ... (Implementation from previous response - returns HTML string for value badge) ...
         if (value === null || typeof value === 'undefined' || isNaN(value)) return '<span class="badge bg-secondary-light text-secondary fw-normal ms-2 fs-sm">N/A</span>';
         return `<span class="badge bg-light text-dark border fw-normal ms-2 fs-sm">${formatCurrencyShort(value)}</span>`; // Added fs-sm
     }

    function updateDataTable() {
        // ... (Implementation from previous response - populates main table body) ...
        // ... (Ensure HTML thead has 7 columns: Account, Revenue, YoY Growth, Distributor, Health, Churn, Actions) ...
        if (!accountsTable) return; const tbody = accountsTable.querySelector('tbody'); if (!tbody) return;
        tbody.innerHTML = ''; // Clear
        if (!dashboardData.accounts || dashboardData.accounts.length === 0) { tbody.innerHTML = '<tr><td colspan="6" class="text-center text-muted">No accounts found matching filters.</td></tr>'; }

        dashboardData.accounts.forEach(account => {
            const row = tbody.insertRow();
            // --- Partner Icon ---
            const partnerIcon = account.is_partner
            ? '<i class="fas fa-heart text-danger ms-1" title="Partner Account"></i>' // Solid heart, red color, margin start
            : '';
            // --- End Partner Icon ---

            // MODIFIED: Add icon to Account Name cell
            row.insertCell().innerHTML = `${escapeHtml(account.name || 'N/A')}${partnerIcon}`;

            row.insertCell().textContent = formatCurrency(account.current_revenue || 0);
            row.insertCell().innerHTML = renderGrowthBadge(account.yoy_growth); // Use helper
            row.insertCell().textContent = account.distributor || 'N/A';
            

             // Health Score Badge/Cell
            const healthCell = row.insertCell(); const hs = account.health_score; let hc = 'secondary'; let ht = 'N/A';
            if (hs !== null && !isNaN(hs)) { ht = hs.toFixed(1); if (hs >= 80) hc = 'success'; else if (hs >= 60) hc = 'primary'; else if (hs >= 40) hc = 'warning'; else hc = 'danger'; }
            healthCell.innerHTML = `<span class="badge bg-${hc}-light text-${hc}">${ht}</span>`;


             // Action Button Cell
             const actionCell = row.insertCell();
             
             // MODIFIED: Use canonical_code with fallback to card_code
             const accountCode = account.canonical_code || account.card_code;
             
             if (accountCode) actionCell.innerHTML = `<button class="btn btn-sm btn-outline-primary btn-view-account" data-card-code="${escapeHtml(accountCode)}"><i class="fas fa-eye me-1"></i> View</button>`;
             else actionCell.innerHTML = `<span class="text-muted">No ID</span>`;
        });
        console.log("Data table updated.");
    }

    // --- Account Details Modal Functions ---

    function openAccountDetails(canonicalCode) {
        // MODIFIED: Renamed parameter from cardCode to canonicalCode for clarity
        if (!accountDetailsModal) return;
        
        // MODIFIED: Added console logging for debugging
        console.log(`Opening account details for canonical code: ${canonicalCode}`);
        
        let account = findAccountData(canonicalCode); 
        if (!account) { 
            console.error(`Account not found for canonical code: ${canonicalCode}`);
            showNotification('Account details not available.', 'error'); 
            return; 
        }
        
        setText('accountDetailsTitle', 'Account Details'); 
        setText('accountName', account.name || 'N/A');
        
        // MODIFIED: Use canonical_code instead of card_code
        setText('accountCode', `ID: ${escapeHtml(account.canonical_code || account.card_code || 'N/A')}`);
        
        setText('accountAddress', account.full_address || 'Address not available');
        setText('accountRevenue', formatCurrency(account.yearly_revenue ?? account.current_revenue ?? 0));
        setText('accountSalesRep', account.sales_rep_name || (account.sales_rep ? `Rep #${account.sales_rep}` : 'N/A'));
        
        // Health/Growth Badges (copied logic from updateDataTable essentially)
        const healthElem = document.getElementById('accountHealth'); 
        const hs = account.health_score;
        if(healthElem){ 
            let hc = 'secondary'; 
            let ht = 'N/A'; 
            if(hs!==null && !isNaN(hs)){
                ht=hs.toFixed(1); 
                if(hs>=80)hc='success'; 
                else if(hs>=60)hc='primary'; 
                else if(hs>=40)hc='warning'; 
                else hc='danger';
            } 
            healthElem.innerHTML = `<span class="badge bg-${hc}">${ht}</span>`;
        }
        
        const growthElem = document.getElementById('accountGrowth'); 
        const gv = account.yoy_growth;
        if(growthElem){ 
            if(gv!==null && !isNaN(gv)){ 
                const gc=gv>=0?'success':'danger'; 
                const gi=gv>=0?'fa-arrow-up':'fa-arrow-down'; 
                growthElem.innerHTML=`<span class="badge bg-${gc}-light text-${gc}"><i class="fas ${gi} me-1"></i>${Math.abs(gv).toFixed(1)}% YoY</span>`;
            } else {
                growthElem.innerHTML='<span class="badge bg-secondary">N/A</span>';
            }
        }
        
        // Fetch history (charts & products)
        destroyModalCharts();
        fetchAccountHistory(canonicalCode);
        accountDetailsModal.show();
    }

    function findAccountData(identifier) {
        // MODIFIED: Renamed parameter from cardCode to identifier, and search by canonical_code first, then card_code
        console.log(`Finding account data for identifier: ${identifier}`);
        
        // Try searching by canonical_code first
        let account = dashboardData.accounts?.find(a => a.canonical_code === identifier);
        if (account) {
            console.log("Account found by canonical_code in dashboardData.accounts");
            return { ...account };
        }
        
        // If not found by canonical_code, try searching by card_code as fallback
        account = dashboardData.accounts?.find(a => a.card_code === identifier);
        if (account) {
            console.log("Account found by card_code in dashboardData.accounts");
            return { ...account };
        }
        
        // Search in topAccountsByRep by canonical_code first
        for (const repAccounts of Object.values(dashboardData.topAccountsByRep)) {
            account = repAccounts.find(a => a.canonical_code === identifier);
            if (account) {
                console.log("Account found by canonical_code in dashboardData.topAccountsByRep");
                return { ...account };
            }
        }
        
        // Search in topAccountsByRep by card_code as fallback
        for (const repAccounts of Object.values(dashboardData.topAccountsByRep)) {
            account = repAccounts.find(a => a.card_code === identifier);
            if (account) {
                console.log("Account found by card_code in dashboardData.topAccountsByRep");
                return { ...account };
            }
        }
        
        console.error(`Account not found for identifier: ${identifier}`);
        return null;
    }

    async function fetchAccountHistory(canonicalCode) {
        // MODIFIED: Renamed parameter from cardCode to canonicalCode for clarity
        console.log(`Fetching yearly history for ${canonicalCode}`);
        const yearlyProductsContainer = document.getElementById('yearlyProductsContainer');
        destroyModalCharts(); 
        showSampleAccountHistoryCharts(); // Show placeholders
        if (yearlyProductsContainer) yearlyProductsContainer.innerHTML = '<p class="text-center text-muted">Loading products...</p>';
        
        // MODIFIED: Use canonicalCode in the API endpoint
        const url = `/api/sales-manager/accounts/${encodeURIComponent(canonicalCode)}/history`;
        
        try {
            const response = await fetch(url);
            if (!response.ok) { 
                console.error(`Error fetching history: ${response.status}`);
                if (response.status === 404) { 
                    updateYearlyProducts(null); 
                } else { 
                    throw new Error(`HTTP error! status: ${response.status}`);
                } 
                return; 
            }
            
            const data = await response.json(); 
            console.log(`History response for ${canonicalCode}:`, data);
            
            if (data.yearly_history?.years?.length > 0) updateAccountHistoryCharts(data.yearly_history); 
            else console.warn(`No chart data for ${canonicalCode}.`);
            
            if (data.products_by_year) updateYearlyProducts(data.products_by_year); 
            else { 
                console.warn(`No product data for ${canonicalCode}.`); 
                updateYearlyProducts(null); 
            }
        } catch (error) { 
            console.error(`Error fetching history for ${canonicalCode}:`, error); 
            showNotification(`Error loading history for ${canonicalCode}.`, 'error'); 
            if (yearlyProductsContainer) yearlyProductsContainer.innerHTML = '<p class="text-center text-danger">Error loading products.</p>'; 
        }
    }

    function updateYearlyProducts(productsByYear) {
        // ... (Implementation from previous response - renders product badges by year) ...
        const container = document.getElementById('yearlyProductsContainer'); if (!container) return; container.innerHTML = '';
        if (!productsByYear || Object.keys(productsByYear).length === 0) { container.innerHTML = '<p class="text-center text-muted">No product history found.</p>'; return; }
        const sortedYears = Object.keys(productsByYear).sort((a, b) => b - a);
        sortedYears.forEach(year => {
            const yearData = productsByYear[year]; const newProds = yearData.new || []; const reordProds = yearData.reordered || [];
            if (newProds.length > 0 || reordProds.length > 0) {
                const yearSection = document.createElement('div'); yearSection.className = 'mb-3';
                yearSection.innerHTML = `<h6 class="fw-bold mb-2">${year}</h6>`; // Header added directly
                if(newProds.length > 0){ yearSection.innerHTML += '<small class="text-success fw-bold d-block mb-1">New:</small>'; const nl = document.createElement('div'); nl.className='d-flex flex-wrap gap-1 mb-2'; newProds.forEach(p=>{nl.innerHTML += `<span class="badge bg-success-light text-success border border-success fw-normal">${escapeHtml(p)}</span>`;}); yearSection.appendChild(nl); }
                if(reordProds.length > 0){ yearSection.innerHTML += `<small class="text-muted fw-bold d-block ${newProds.length > 0 ? 'mt-2' : ''} mb-1">Reordered:</small>`; const rl = document.createElement('div'); rl.className='d-flex flex-wrap gap-1'; reordProds.forEach(p=>{rl.innerHTML += `<span class="badge bg-light text-dark border fw-normal">${escapeHtml(p)}</span>`;}); yearSection.appendChild(rl); }
                container.appendChild(yearSection);
            }
        });
    }

    function destroyModalCharts() {
        // ... (Implementation from previous response - destroys chart objects) ...
        if (accountYearlyRevenueChart) { accountYearlyRevenueChart.destroy(); accountYearlyRevenueChart = null; }
        if (accountYearlyTransactionsChart) { accountYearlyTransactionsChart.destroy(); accountYearlyTransactionsChart = null; }
    }

    function updateAccountHistoryCharts(yearly_history) {
        // ... (Implementation from previous response - creates yearly revenue/transaction charts) ...
        console.log("Updating modal charts:", yearly_history); destroyModalCharts();
        const years = yearly_history.years || []; const revenues = yearly_history.revenue || []; const transactions = yearly_history.transactions || [];
        const revCanvas = document.getElementById('accountRevenueHistory'); if(revCanvas){ const ctx = revCanvas.getContext('2d'); accountYearlyRevenueChart = new Chart(ctx, {type:'bar',data:{labels:years, datasets:[{label:'Revenue', data:revenues, backgroundColor:'rgba(52,152,219,0.7)'}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}, title:{display:true, text:'Revenue per Year'}}, scales:{y:{ticks:{callback:formatCurrencyShort}}}}});}
        const transCanvas = document.getElementById('accountTransactionsHistory'); if(transCanvas){ const ctx = transCanvas.getContext('2d'); accountYearlyTransactionsChart = new Chart(ctx, {type:'line', data:{labels:years, datasets:[{label:'Transactions', data:transactions, borderColor:'rgba(46,204,113,1)', fill:false, tension: 0.1}]}, options:{responsive:true, maintainAspectRatio:false, plugins:{legend:{display:false}, title:{display:true, text:'Transactions per Year'}}, scales:{y:{beginAtZero:true, title:{display:true, text:'Count'}}}}});}
    }

    function showSampleAccountHistoryCharts() {
        // ... (Implementation from previous response - shows sample yearly charts) ...
        console.log("Showing sample yearly charts."); destroyModalCharts();
        const currentYr = new Date().getFullYear(); const years = [currentYr - 2, currentYr - 1, currentYr]; const revenues = [52000, 65000, 61000]; const transactions = [45, 55, 52];
        updateAccountHistoryCharts({years, revenue: revenues, transactions}); // Reuse update function with sample data
    }

    // --- Filter Update Functions ---

    function updateDistributorFilter() {
        // ... (Implementation from previous response - populates distributor dropdown) ...
         if (!distributorFilter) return; const currentVal = distributorFilter.value; distributorFilter.innerHTML = '<option value="">All Distributors</option>';
         dashboardData.distributors.forEach(d => { const opt = document.createElement('option'); opt.value = d; opt.textContent = d; if(d === currentVal) opt.selected = true; distributorFilter.appendChild(opt); });
    }

    function updateSalesRepFilter() {
        if (!salesRepFilter) {
            console.error("Sales Rep Filter element not found!");
            return;
        }
        const currentSelectedValue = filters.salesRep; // Get the currently applied filter value
        salesRepFilter.innerHTML = '<option value="">All Sales Reps</option>'; // Start with the "All" option

        // Determine which reps to show based on the distributor filter
        let allowedRepIds = null;
        if (filters.distributor && dashboardData.performanceData) {
            // Get the set of VALID rep IDs associated with the selected distributor
             allowedRepIds = new Set(dashboardData.performanceData
                .filter(p => p.distributor === filters.distributor && p.sales_rep !== null && p.sales_rep !== undefined && String(p.sales_rep).trim() !== '')
                .map(p => String(p.sales_rep)) // Get IDs as strings
             );
             // Also allow the __UNASSIGNED__ option if the distributor filter is active
             // (Optional: you might decide unassigned shouldn't show when filtering by distributor)
             // allowedRepIds.add('__UNASSIGNED__');
        }

        // Filter the global, sorted dashboardData.salesReps list
        const repsToShow = dashboardData.salesReps.filter(rep => {
            if (allowedRepIds) {
                // If filtering by distributor, only show reps in that distributor's set OR the unassigned option
                return allowedRepIds.has(rep.id) || rep.id === '__UNASSIGNED__';
            }
            return true; // Otherwise, show all reps (including Unassigned)
        });


        // Add options to the dropdown
        repsToShow.forEach(rep => {
            const opt = document.createElement('option');
            opt.value = rep.id; // This will be the rep ID string or "__UNASSIGNED__"
            opt.textContent = rep.name; // Rep Name or "Unassigned Accounts"
            // Reselect the previously selected value if it's still in the list
            if(rep.id === currentSelectedValue) {
                opt.selected = true;
            }
            salesRepFilter.appendChild(opt);
        });

        // Final check: If the previously selected rep is no longer in the options
        // (e.g., distributor filter removed them), reset selection to "All"
        if (currentSelectedValue && !repsToShow.some(rep => rep.id === currentSelectedValue)) {
            salesRepFilter.value = ""; // Select "All Sales Reps" in dropdown
            filters.salesRep = ""; // Update the stored filter state to match
            console.log("Previously selected rep not in current distributor's list, defaulting filter to All Reps.");
        } else {
             // Otherwise, ensure the dropdown visually matches the stored filter state
             salesRepFilter.value = currentSelectedValue;
        }
    }

    // --- Utility Functions ---

    function exportTableToCsv(filename) {
        // ... (Implementation from previous response - uses PapaParse) ...
         if (typeof Papa === 'undefined') { showNotification('CSV Export library not loaded.', 'error'); return; } if (!dashboardData.accounts || dashboardData.accounts.length === 0) { showNotification('No data to export.', 'warning'); return; }
         const headers = ['Account', 'Current Revenue', 'YoY Growth %', 'Distributor', 'Health Score']; // Added Health/Churn
         const csvData = [headers];
         dashboardData.accounts.forEach(acc => { csvData.push([ acc.name || '', acc.current_revenue || 0, (acc.yoy_growth !== null && !isNaN(acc.yoy_growth)) ? acc.yoy_growth.toFixed(2) : '', acc.distributor || '', (acc.health_score !== null && !isNaN(acc.health_score)) ? acc.health_score.toFixed(1) : '', ]); });
         const csv = Papa.unparse(csvData); const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' }); const url = URL.createObjectURL(blob);
         const link = document.createElement('a'); link.href = url; link.setAttribute('download', filename); document.body.appendChild(link); link.click(); document.body.removeChild(link); URL.revokeObjectURL(url);
    }

    /**
     * Helper function to safely set text content of an element by ID
     * @param {string} elementId The ID of the element
     * @param {string|number} text The text or value to set
     */
     function safeUpdate(elementId, text) { // Same as setText, kept for compatibility if used elsewhere
        const elem = document.getElementById(elementId);
        if (elem) {
            elem.textContent = text ?? 'N/A'; // Use 'N/A' if text is null/undefined
        } else {
            console.warn(`Element not found in safeUpdate: ${elementId}`);
        }
     }

    /**
      * Helper function to safely set text content (duplicate of safeUpdate, choose one or keep both)
      */
      function setText(elementId, text) {
          const elem = document.getElementById(elementId);
          if (elem) {
              elem.textContent = text ?? 'N/A'; // Use 'N/A' if text is null/undefined
          } else {
              console.warn(`Element not found in setText: ${elementId}`);
          }
      }
    

    function formatCurrency(value) { /* ... */ const n = Number(value); return isNaN(n)?'$0.00':new Intl.NumberFormat('en-US',{style:'currency',currency:'USD'}).format(n); }
    function formatCurrencyShort(value) { /* ... */ const n = Number(value); if(isNaN(n))return'$0'; if(Math.abs(n)>=1e9)return'$'+(n/1e9).toFixed(1)+'B'; if(Math.abs(n)>=1e6)return'$'+(n/1e6).toFixed(1)+'M'; if(Math.abs(n)>=1e3)return'$'+(n/1e3).toFixed(1)+'K'; return'$'+n.toFixed(0);}
    function formatDate(dateString) { /* ... */ if(!dateString||!moment)return'N/A'; const d=moment(dateString); return d.isValid()?d.format('MMM D, YYYY'):'Invalid';}
    function showLoading(isLoading) { /* ... */ document.body.classList.toggle('is-loading', isLoading); console.log(isLoading?"Loading...":"Loaded."); }
    function showNotification(message, type = 'info') { /* ... */ if(typeof Toastify==='undefined'){alert(message);return;} const opts={text:message,duration:3000,close:true,gravity:'top',position:'right',stopOnFocus:true,style:{background:'#333'}}; if(type==='success')opts.style.background="linear-gradient(to right, #00b09b, #96c93d)"; else if(type==='error')opts.style.background="linear-gradient(to right, #ff5f6d, #ffc371)"; else if(type==='warning')opts.style.background="linear-gradient(to right, #f39c12, #f1c40f)"; else opts.style.background="linear-gradient(to right, #3498db, #2ecc71)"; Toastify(opts).showToast(); }
    function escapeHtml(unsafe) { /* ... */ if(unsafe===null||typeof unsafe==='undefined')return''; return String(unsafe).replace(/&/g,"&").replace(/</g,"<").replace(/>/g,">").replace(/"/g,'"').replace(/'/g,"'");}
    function setText(elementId, text) { /* ... */ const e=document.getElementById(elementId); if(e)e.textContent=text??'N/A'; }

}); // End DOMContentLoaded