const VideoBg = {
  // Maps WMO weather codes to video files
  codeToVideo(weatherCode) {
    if (weatherCode === 0 || weatherCode === 1) return 'sunny';
    if (weatherCode <= 3)  return 'cloudy';
    if (weatherCode >= 51) return 'rainy';
    return 'cloudy';
  },

  // Set video based on dominant national weather code
  setFromWeatherCode(code) {
    const video = document.getElementById('bg-video');
    if (!video) return;

    // Add error handler as requested
    video.addEventListener('error', () => {
      video.style.display = 'none';  // hide broken video
      document.querySelector('.video-overlay').style.opacity = '0.95';
    });

    const type = this.codeToVideo(code);
    const src = `/frontend/assets/videos/${type}.mp4`;
    if (!video.src.endsWith(src)) {
      video.style.opacity = 0;
      setTimeout(() => {
        video.src = src;
        video.load();
        video.play().catch(() => {}); // autoplay may be blocked
        video.style.opacity = null;
      }, 500);
    }
  },

  // Set based on time of day if no weather data
  setFromTime() {
    const hour = new Date().getHours();
    const type = (hour >= 20 || hour < 6) ? 'night' : 'cloudy';
    this.setFromWeatherCode(type === 'night' ? -1 : 2);
  },
};