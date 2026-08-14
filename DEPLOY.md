# PubQuiz Arena — Deploy to Render

## Quick Deploy

1. Push this repo to GitHub
2. Go to [render.com](https://render.com) → New + → Web Service
3. Connect your GitHub repo
4. Render auto-detects `render.yaml` — click Deploy

## Local Development

```bash
cd PubQuizArena
pip install -r requirements.txt
python3 run.py
```

## Platform Notes

- **Free tier**: spins down after 15 min inactivity — click the service to wake it up
- **WebSockets**: fully supported on Render
- **Port**: Render sets `$PORT` automatically
