# GitHub Actions Setup Guide for BMW Scraping Pipeline

This workflow automatically runs the BMW car scraping pipeline on a schedule using GitHub Actions.

## Schedule

The workflow runs at:

- **9:00 UTC** = 10:00 CET / 11:00 CEST (Europe/Brussels)
- **14:00 UTC** = 15:00 CET / 16:00 CEST (Europe/Brussels)

You can also trigger it manually from the GitHub UI.

## Setup Steps

### 1. Add GitHub Secrets

Go to your repository on GitHub:

1. Click **Settings** → **Secrets and variables** → **Actions**
2. Create the following secrets:

| Secret Name    | Value                                                         |
| -------------- | ------------------------------------------------------------- |
| `SUPABASE_URL` | Your Supabase project URL (e.g., `https://xxxxx.supabase.co`) |
| `SUPABASE_KEY` | Your Supabase API key (service role or anon key)              |
| `BMW_URL`      | The BMW inventory URL with your filters                       |

**How to get these values:**

- **SUPABASE_URL**: Supabase Dashboard → Settings → API → Project URL
- **SUPABASE_KEY**: Supabase Dashboard → Settings → API → Service Role Key (or anon public)
- **BMW_URL**: From your Azure Function App environment variables, or construct it manually

### 2. Commit and Push

The workflow file is already in `.github/workflows/bmw-scraping.yml`. Just commit and push:

```bash
git add .github/workflows/bmw-scraping.yml
git commit -m "Add GitHub Actions workflow for BMW scraping"
git push
```

### 3. Verify Workflow

1. Go to your GitHub repository
2. Click **Actions** tab
3. You should see "BMW Car Scraping Pipeline" workflow

### 4. Test Manually

Click the workflow → **Run workflow** → **Run workflow** button to test it immediately.

### 5. Monitor Runs

- Go to **Actions** tab to see all runs
- Click on a run to see detailed logs
- Check **Artifacts** tab after a run completes to download results

## Advantages Over Azure Functions

✅ **Free** - GitHub Actions provides 2,000 minutes/month free (way more than you need)
✅ **Simple** - No Docker, no container issues
✅ **Reliable** - GitHub infrastructure, no cold-start issues
✅ **Transparent** - See logs directly in GitHub UI
✅ **Easy debugging** - Can re-run failed workflows with one click
✅ **Built-in artifacts** - Download results directly from GitHub

## Cost

- **GitHub Actions**: 2,000 free minutes/month (your 2 runs/day = ~2 hours/month, well within limit)
- **Supabase**: Same as before (you're already paying)

## Troubleshooting

### Workflow doesn't run on schedule

- GitHub Actions requires your repo to have activity in the last 60 days
- Push a commit to keep it active
- Or manually trigger it from the UI

### Playwright timeout

- The first run might timeout due to browser installation
- Re-run it, subsequent runs should be faster

### Secrets not working

- Double-check secret names are exactly as specified
- Make sure you're using the right Supabase keys (service role vs anon)

## Manual Trigger Example

You can also trigger runs from GitHub CLI:

```bash
gh workflow run bmw-scraping.yml
```

Or from Python/cron on your local machine:

```bash
curl -X POST \
  -H "Authorization: token YOUR_GITHUB_TOKEN" \
  -H "Accept: application/vnd.github.v3+raw" \
  https://api.github.com/repos/YOUR_USERNAME/car_scraping/actions/workflows/bmw-scraping.yml/dispatches \
  -d '{"ref":"main"}'
```
