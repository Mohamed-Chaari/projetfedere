const Auth = {
  getToken() { return localStorage.getItem('meteo_token'); },
  setToken(token) { localStorage.setItem('meteo_token', token); },
  removeToken() { localStorage.removeItem('meteo_token'); },
  isLoggedIn() { return !!this.getToken(); },
  requireAuth() {
    if (!this.isLoggedIn()) {
      if (!window.location.pathname.includes('login')) {
        window.location.href = './login.html';
      }
      return false;
    }
    return true;
  },
  logout() {
    this.removeToken();
    localStorage.removeItem('meteo_user');
    window.location.href = './login.html';
  },
};