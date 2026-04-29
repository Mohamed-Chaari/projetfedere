const BASE_URL = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
  ? 'http://localhost:8000'
  : 'https://tunisia-meteo-api.onrender.com';

const API = {

  getHeaders() {
    return { 'Content-Type': 'application/json' };
  },

  async request(endpoint, options = {}) {
    const url = `${BASE_URL}${endpoint}`;
    const config = {
      headers: this.getHeaders(),
      ...options,
    };
    try {
      const res = await fetch(url, config);
      if (res.status === 401 || res.status === 403) {
        localStorage.removeItem('meteo_token');
        localStorage.removeItem('meteo_user');
        console.warn('API returned 401/403. Authentication failed.');
        return null;
      }
      if (!res.ok) {
        const err = await res.json().catch(() => ({}));
        throw new Error(err.detail || `HTTP ${res.status}`);
      }
      return await res.json();
    } catch (err) {
      console.error(`API Error [${endpoint}]:`, err.message);
      throw err;
    }
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