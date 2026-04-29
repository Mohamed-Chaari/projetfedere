const Auth = {
  getToken() { return localStorage.getItem('meteo_token'); },
  setToken(token) { localStorage.setItem('meteo_token', token); },
  removeToken() { localStorage.removeItem('meteo_token'); },
  isLoggedIn() { return !!this.getToken(); },
  requireAuth() {
    if (!this.isLoggedIn()) {
      // Prevent redirect loop if already on login page
      if (!window.location.pathname.endsWith('login.html')) {
        window.location.href = './login.html';
      }
      // Throw to stop further page initialization
      throw new Error('AUTH_REQUIRED');
    }
  },
  logout() {
    this.removeToken();
    localStorage.removeItem('meteo_user');
    window.location.href = './login.html';
  },
};