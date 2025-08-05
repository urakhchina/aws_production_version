// static/js/dashboard_utils.js

// --- START Helper Function Implementations ---

/**
 * Safely formats a numeric value with specified precision, prefix, suffix, and fallback.
 * @param {*} value - The value to format (should be convertible to number).
 * @param {number} [digits=0] - Number of decimal places.
 * @param {string} [prefix=''] - String to prepend.
 * @param {string} [suffix=''] - String to append.
 * @param {string} [fallback='--'] - String to return if value is invalid.
 * @returns {string} The formatted string or the fallback.
 */
function formatValue(value, digits = 0, prefix = '', suffix = '', fallback = '--') {
    // Check for null, undefined explicitly first
    if (value === null || value === undefined) {
        return fallback;
    }
    // Try converting to float
    const num = parseFloat(value);
    // Check if conversion resulted in NaN
    if (isNaN(num)) {
        return fallback;
    }
    try {
         // Use toFixed for specified decimal places
         const formattedNumber = num.toFixed(digits);
         return `${prefix}${formattedNumber}${suffix}`;
    } catch (e) {
         // Catch potential errors from toFixed (though unlikely with NaN check)
         console.error("Error formatting value:", value, e);
         return fallback;
    }
}

/**
 * Formats a number as currency, using M/K suffixes for large numbers.
 * @param {*} value - The numeric value to format.
 * @param {number} [digits=0] - Decimal places for values < 1000 (ignored for M/K).
 * @returns {string} Formatted currency string (e.g., $1.2M, $50K, $1,234) or '$--'.
 */
function formatCurrency(value, digits = 0) {
    // Check for null, undefined, or non-numeric input early
    if (value === null || value === undefined) return '$--';
    const num = parseFloat(value);
    if (isNaN(num)) return '$--';

    const absVal = Math.abs(num);
    const sign = num < 0 ? '-' : '';
    let formattedVal;

    if (absVal >= 1e6) { // Millions
        formattedVal = `${sign}${(absVal / 1e6).toFixed(1)}M`; // Always 1 decimal for M
    } else if (absVal >= 1e3) { // Thousands
        formattedVal = `${sign}${(absVal / 1e3).toFixed(1)}K`; // Always 1 decimal for K
    } else { // Below 1000
        // Use Intl.NumberFormat for locale-aware comma separation and currency symbol
        try {
            formattedVal = num.toLocaleString('en-US', {
                style: 'currency',
                currency: 'USD', // Or your desired currency
                minimumFractionDigits: digits,
                maximumFractionDigits: digits
            });
        } catch(e) {
            // Fallback basic formatting if Intl fails
             console.error("Intl.NumberFormat failed:", e);
             formattedVal = `${sign}$${absVal.toFixed(digits)}`;
        }
    }
    return formattedVal;
}

/**
* Formats a number as currency with exact precision to two decimal places.
* @param {*} value - The numeric value to format.
* @returns {string} Formatted currency string (e.g., $1,234.56) or '$--'.
*/
function formatCurrencyExact(value) {
   if (value === null || value === undefined) return '$--';
   const num = parseFloat(value);
   if (isNaN(num)) return '$--';

   try {
       return num.toLocaleString('en-US', {
           style: 'currency',
           currency: 'USD',
           minimumFractionDigits: 2,
           maximumFractionDigits: 2
       });
   } catch(e) {
       console.error("formatCurrencyExact failed:", e);
       return `$${num.toFixed(2)}`; // Fallback
   }
}

/**
 * Formats an ISO date string or Date object into MM/DD or MM/DD/YYYY.
 * @param {string|Date|null|undefined} dateString - The date input.
 * @param {'long'|'short'} [format='long'] - 'long' for MM/DD/YYYY, 'short' for MM/DD.
 * @returns {string} Formatted date string or 'N/A' / 'Invalid Date'.
 */
 function formatDate(dateString, format = 'long') {
    if (!dateString) return 'N/A'; // Handle null, undefined, empty string

    try {
        // Attempt to create a Date object
        const date = new Date(dateString);

        // Check if the created date object is valid
        if (isNaN(date.getTime())) {
            // getTime() returns NaN for invalid dates
            return 'Invalid Date';
        }

        // Extract parts (add 1 to month because it's 0-indexed)
        const month = date.getMonth() + 1;
        const day = date.getDate();
        const year = date.getFullYear();

        // Format based on request
        if (format === 'short') {
            return `${month}/${day}`; // MM/DD
        } else {
            // Default to MM/DD/YYYY
            return `${month}/${day}/${year}`;
        }
    } catch (e) {
        // Catch any other errors during date processing
        console.error("Error formatting date:", dateString, e);
        return 'Invalid Date';
    }
}

// You'll need the healthColorMapping constant here too, or pass it in
const healthColorMapping = [
    { threshold: 80, color: '#198754', name: 'Excellent', badge: 'bg-health-excellent' },
    { threshold: 60, color: '#8dc63f', name: 'Good', badge: 'bg-health-good' },
    { threshold: 40, color: '#e9c46a', name: 'Average', badge: 'bg-health-average' },
    { threshold: 20, color: '#fd7e14', name: 'Poor', badge: 'bg-health-poor' },
    { threshold: 0,  color: '#d16b55', name: 'Critical', badge: 'bg-health-critical' }
];

// Assumed structure in dashboard_utils.js
function getHealthColorInfo(healthScore) {
    if (healthScore === null || healthScore === undefined || isNaN(healthScore)) {
        // Returns a fallback object for invalid scores
        return { color: '#6c757d', name: 'Unknown', badge: 'bg-secondary' };
    }
    const score = parseFloat(healthScore);
    // Mapping needs to be defined HERE or accessible globally
    const sortedMapping = [...healthColorMapping].sort((a,b) => b.threshold - a.threshold);
    for (const mapping of sortedMapping) {
         if (score >= mapping.threshold) {
             // Returns a mapping object if score matches threshold
             return mapping;
         }
    }
    // Fallback if score is below the lowest threshold (e.g., < 0)
    // It SHOULD return the object matching threshold 0, but let's add an explicit fallback return
    // This fallback might have been missing or incorrect
    console.warn(`Health score ${score} fell through all thresholds. Returning default Critical.`); // Add a warning
    return { color: '#dc3545', name: 'Critical', badge: 'bg-health-critical' };
}

function getHealthCategoryName(healthScore) {
    return getHealthColorInfo(healthScore).name;
}

// Add any other shared utility functions here...