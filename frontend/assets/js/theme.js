const Theme = {
  init() {
    const saved = localStorage.getItem('meteo_theme') || 'dark';
    this.apply(saved);
    document.querySelector('.theme-toggle')
      ?.addEventListener('click', () => this.toggle());
  },
  apply(theme) {
    document.documentElement.setAttribute('data-theme', theme);
    localStorage.setItem('meteo_theme', theme);
    // Update Plotly charts if any
    if (window.Plotly) {
      document.querySelectorAll('.plotly-chart').forEach(el => {
        Plotly.relayout(el, {
          paper_bgcolor: 'transparent',
          plot_bgcolor: 'transparent',
          font: { color: theme === 'dark' ? '#E8F4FD' : '#0D1B2A' }
        });
      });
    }
  },
  toggle() {
    const current = localStorage.getItem('meteo_theme') || 'dark';
    this.apply(current === 'dark' ? 'light' : 'dark');
  },
};