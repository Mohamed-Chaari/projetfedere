const BASE_URL = (window.location.hostname === 'localhost' || window.location.hostname === '127.0.0.1')
  ? 'http://localhost:8000'
  : 'https://tunisia-meteo-api.onrender.com';

const API = {

  getToken() {
    return localStorage.getItem('meteo_token');
  },

  getHeaders(auth = true) {
    const headers = { 'Content-Type': 'application/json' };
    if (auth) {
      const token = this.getToken();
      if (token) {
        headers['Authorization'] = `Bearer ${token}`;
      }
      // Don't redirect here — let requireAuth() handle auth gating
    }
    return headers;
  },

  async request(endpoint, options = {}, auth = true) {
    const url = `${BASE_URL}${endpoint}`;
    const config = {
      headers: this.getHeaders(auth),
      ...options,
    };
    try {
      const res = await fetch(url, config);
      if (res.status === 401 || res.status === 403) {
        // Only clear token and redirect if we're not already on the login page
        // This prevents redirect loops when the backend rejects an expired token
        const onLoginPage = window.location.pathname.endsWith('login.html');
        if (!onLoginPage) {
          localStorage.removeItem('meteo_token');
          localStorage.removeItem('meteo_user');
          window.location.href = './login.html';
        }
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

  // ── Auth ──────────────────────────────────────────────────
  async login(username, password) {
    const form = new FormData();
    form.append('username', username);
    form.append('password', password);
    const res = await fetch(`${BASE_URL}/api/auth/login`, {
      method: 'POST', body: form,
    });
    if (!res.ok) throw new Error('Invalid credentials');
    return await res.json();
  },

  async getMe() {
    return this.request('/api/auth/me');
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
    return this.request('/api/alerts/active', {}, false); // public
  },
  async getAlertHistory(params = {}) {
    const q = new URLSearchParams(params).toString();
    return this.request(`/api/alerts/history${q ? '?' + q : ''}`);
  },
};