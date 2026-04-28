# weather.tn

## API Endpoints

| Method | Endpoint | Auth | Description |
|--------|----------|------|-------------|
| POST | /api/auth/login | No | Get JWT token |
| GET | /api/dashboard/summary | Yes | National KPIs |
| GET | /api/dashboard/current | Yes | Live 221 cities |
| GET | /api/dashboard/current/{city} | Yes | One city live |
| GET | /api/forecast/{city} | Yes | 7-day forecast |
| GET | /api/forecast/today/national | Yes | Today all cities |
| GET | /api/analysis/monthly | Yes | Monthly averages |
| GET | /api/analysis/peaks | Yes | Temperature peaks |
| GET | /api/analysis/correlation | Yes | Pearson matrix |
| GET | /api/analysis/annual | Yes | Annual stats |
| GET | /api/alerts/active | No | Active alerts |
| GET | /api/alerts/history | Yes | Alert history |
| GET | /health | No | API health |

## DEPLOYMENT STEPS

1. Push to GitHub
2. Go to render.com → New Web Service
3. Connect GitHub repo: Mohamed-Chaari/projetfedere
4. Settings:
   - Build Command: `pip install -r requirements-api.txt`
   - Start Command: `uvicorn api.main:app --host 0.0.0.0 --port $PORT`
   - Root Directory: (leave empty)
5. Add Environment Variables:
   `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD`, `JWT_SECRET`
6. Deploy → Get URL → add to ALLOWED_ORIGINS
