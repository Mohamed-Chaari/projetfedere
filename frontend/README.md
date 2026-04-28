# Tunisia Meteo - Frontend

The frontend for the Tunisia Meteo application is built purely with Vanilla JS, HTML5, and CSS3.
It integrates Leaflet for interactive maps and Plotly for responsive charts.

## Deployment to GitHub Pages

This project is configured to automatically deploy the `frontend/` directory to GitHub Pages via GitHub Actions whenever changes are pushed to the `main` branch.

### Setup Instructions

1. Go to your repository's **Settings**.
2. Navigate to the **Pages** menu on the left sidebar.
3. Under **Build and deployment**, set the **Source** dropdown to **GitHub Actions**.
4. Make sure your changes to the `frontend/` directory are pushed to the `main` branch.
5. The GitHub Action will automatically build and deploy the site.
6. The frontend will be available at: `https://<YOUR_GITHUB_USERNAME>.github.io/<YOUR_REPO_NAME>/`

*Note: Since the backend API handles authentication, the frontend dynamically points to the deployed Render.com API URL when running on GitHub pages.*