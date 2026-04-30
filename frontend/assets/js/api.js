const BASE_URL = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
  ? 'http://localhost:8000'
  : 'https://tunisia-meteo-api.onrender.com';

const API = {

  getHeaders() {
    return { 'Content-Type': 'application/json' };
  },

  async request(endpoint, options = {}) {
    console.log("Mocking API Endpoint:", endpoint);
    
    // --- MOCK DATA INJECTION ---
    if (endpoint.includes('/api/dashboard/summary')) {
       return { total_cities: 24, avg_temperature: 24.5, active_alerts: 2, last_update: new Date().toISOString() };
    }
    if (endpoint.includes('/api/dashboard/current')) {
       return [
         { city: "Tunis", temperature: 26.2, humidity: 60, wind_speed: 15, condition: "Sunny", lat: 36.8, lon: 10.18 },
         { city: "Sfax", temperature: 28.5, humidity: 65, wind_speed: 12, condition: "Clear", lat: 34.74, lon: 10.76 },
         { city: "Sousse", temperature: 27.1, humidity: 68, wind_speed: 18, condition: "Partly Cloudy", lat: 35.82, lon: 10.63 },
         { city: "Gabes", temperature: 29.8, humidity: 55, wind_speed: 10, condition: "Sunny", lat: 33.88, lon: 10.09 },
         { city: "Tozeur", temperature: 35.5, humidity: 20, wind_speed: 25, condition: "Hot", lat: 33.91, lon: 8.13 },
         { city: "Bizerte", temperature: 24.0, humidity: 70, wind_speed: 20, condition: "Cloudy", lat: 37.27, lon: 9.87 }
       ];
    }
    if (endpoint.includes('/api/forecast')) {
       return {
         city: "Tunis",
         daily: [
           { date: "2026-05-01", max_temp: 28, min_temp: 18, condition: "Sunny", precip_prob: 0 },
           { date: "2026-05-02", max_temp: 29, min_temp: 19, condition: "Clear", precip_prob: 10 },
           { date: "2026-05-03", max_temp: 26, min_temp: 17, condition: "Cloudy", precip_prob: 40 },
           { date: "2026-05-04", max_temp: 24, min_temp: 16, condition: "Rain", precip_prob: 80 },
           { date: "2026-05-05", max_temp: 25, min_temp: 16, condition: "Partly Cloudy", precip_prob: 20 }
         ]
       };
    }
    if (endpoint.includes('/api/analysis/peaks')) {
       return [
         { city: "Tozeur", date: "2024-07-15", temperature: 48.5, severity: "extreme" },
         { city: "Kebili", date: "2024-07-16", temperature: 47.2, severity: "high" },
         { city: "Tataouine", date: "2024-07-17", temperature: 46.8, severity: "high" },
         { city: "Medenine", date: "2024-07-18", temperature: 45.1, severity: "medium" },
         { city: "Gafsa", date: "2024-07-19", temperature: 44.5, severity: "medium" }
       ];
    }
    if (endpoint.includes('/api/analysis/correlation')) {
       return [
         { variable_a: "temperature", variable_b: "humidity", pearson_r: -0.75 },
         { variable_a: "temperature", variable_b: "precipitation", pearson_r: -0.45 },
         { variable_a: "wind_speed", variable_b: "humidity", pearson_r: -0.20 },
         { variable_a: "temperature", variable_b: "wind_speed", pearson_r: 0.15 }
       ];
    }
    if (endpoint.includes('/api/analysis/annual')) {
       return [
         { year: 2020, avg_temp: 23.1, max_temp: 42.0, min_temp: 4.1, total_precip: 300, hot_days: 30, cold_days: 15 },
         { year: 2021, avg_temp: 23.5, max_temp: 42.5, min_temp: 3.8, total_precip: 280, hot_days: 35, cold_days: 12 },
         { year: 2022, avg_temp: 23.8, max_temp: 43.1, min_temp: 3.5, total_precip: 250, hot_days: 40, cold_days: 10 },
         { year: 2023, avg_temp: 24.2, max_temp: 44.0, min_temp: 3.2, total_precip: 210, hot_days: 45, cold_days: 8 },
         { year: 2024, avg_temp: 24.8, max_temp: 45.2, min_temp: 3.0, total_precip: 180, hot_days: 52, cold_days: 5 }
       ];
    }
    if (endpoint.includes('/api/analysis/monthly')) {
       return [
         { year: 2024, month: 1, avg_temp: 12, total_precip: 40 },
         { year: 2024, month: 2, avg_temp: 14, total_precip: 30 },
         { year: 2024, month: 3, avg_temp: 17, total_precip: 25 },
         { year: 2024, month: 4, avg_temp: 20, total_precip: 20 },
         { year: 2024, month: 5, avg_temp: 24, total_precip: 10 },
         { year: 2024, month: 6, avg_temp: 28, total_precip: 5 },
         { year: 2024, month: 7, avg_temp: 32, total_precip: 0 },
         { year: 2024, month: 8, avg_temp: 31, total_precip: 2 },
         { year: 2024, month: 9, avg_temp: 27, total_precip: 15 },
         { year: 2024, month: 10, avg_temp: 23, total_precip: 35 },
         { year: 2024, month: 11, avg_temp: 18, total_precip: 45 },
         { year: 2024, month: 12, avg_temp: 14, total_precip: 50 }
       ];
    }
    if (endpoint.includes('/api/alerts')) {
       return [
         { id: 1, city: "Tozeur", type: "HEATWAVE", severity: "RED", message: "Vague de chaleur extrême (> 48°C)", active: true, created_at: new Date().toISOString() },
         { id: 2, city: "Bizerte", type: "WIND", severity: "YELLOW", message: "Vents violents prévus (80 km/h)", active: true, created_at: new Date().toISOString() }
       ];
    }

    return [];
  },



  // ── Dashboard ─────────────────────────────────────────────
  async getDashboardSummary() {
    return this.request('/api/dashboard/summary');
  },
  async getCurrentAll(params = {}) {
    const q = new URLSearchParams(params).toString();
    return this.request(`/api/dashboard/current${q ? '?' + q : ''}`);
  },
  async getCurrentCity(city) {
    return this.request(`/api/dashboard/current/${encodeURIComponent(city)}`);
  },

  // ── Forecast ──────────────────────────────────────────────
  async getForecastCity(city) {
    return this.request(`/api/forecast/${encodeURIComponent(city)}`);
  },
  async getForecastToday() {
    return this.request('/api/forecast/today/national');
  },

  // ── Analysis ──────────────────────────────────────────────
  async getMonthly(params = {}) {
    const q = new URLSearchParams(params).toString();
    return this.request(`/api/analysis/monthly${q ? '?' + q : ''}`);
  },
  async getPeaks(params = {}) {
    const q = new URLSearchParams(params).toString();
    return this.request(`/api/analysis/peaks${q ? '?' + q : ''}`);
  },
  async getCorrelation(season = 'annual') {
    return this.request(`/api/analysis/correlation?season=${season}`);
  },
  async getAnnual(params = {}) {
    const q = new URLSearchParams(params).toString();
    return this.request(`/api/analysis/annual${q ? '?' + q : ''}`);
  },

  // ── Alerts ────────────────────────────────────────────────
  async getActiveAlerts() {
    return this.request('/api/alerts/active'); // public
  },
  async getAlertHistory(params = {}) {
    const q = new URLSearchParams(params).toString();
    return this.request(`/api/alerts/history${q ? '?' + q : ''}`);
  },
};