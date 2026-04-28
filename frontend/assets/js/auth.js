const Auth = {
  getToken() { return localStorage.getItem('meteo_token'); },
  setToken(token) { localStorage.setItem('meteo_token', token); },
  removeToken() { localStorage.removeItem('meteo_token'); },
  isLoggedIn() { return !!this.getToken(); },
  requireAuth() {
    if (!this.isLoggedIn()) {
      window.location.href = './login.html';
    }
  },
  logout() {
    this.removeToken();
    localStorage.removeItem('meteo_user');
    window.location.href = './login.html';
  },
};