# Render.com Deployment Guide

## Step-by-Step Instructions

### 1. ✅ Code is Ready
All critical fixes have been committed and pushed to GitHub.

### 2. Deploy on Render.com

**You have Render.com open in your browser. Follow these steps:**

#### A. Sign In / Sign Up
- If you have an account: Click **"Sign In"**
- If new: Click **"Get Started"** and create an account (use GitHub login for faster setup)

#### B. Create New Blueprint
1. Once logged in, click **"New +"** button (top right)
2. Select **"Blueprint"** from the dropdown
3. Click **"Connect GitHub"** if not already connected
4. Select your repository: `kasparro-backend-Ishaan-Verma`
5. Click **"Connect"**

#### C. Render Will Auto-Configure
Render will automatically detect the `render.yaml` file and show:
- ✅ PostgreSQL Database: `kasparro-postgres`
- ✅ Web Service: `kasparro-api`
- ✅ Background Worker: `kasparro-etl`

#### D. Review and Deploy
1. Review the services (all should be on **Free** plan)
2. Click **"Apply"** or **"Create Resources"**
3. Wait for deployment (5-10 minutes)

### 3. Run Database Migrations

After deployment completes:

1. Go to the **`kasparro-api`** web service
2. Click **"Shell"** tab (or **"Console"**)
3. Run these commands:

```bash
# Run initial schema
psql $DATABASE_URL < migrations/init.sql

# Run entity resolution migration
psql $DATABASE_URL < migrations/add_master_coins.sql
```

### 4. Get Your Public URL

1. In the `kasparro-api` service, find the **URL** at the top
2. It will look like: `https://kasparro-api-XXXX.onrender.com`
3. Copy this URL

### 5. Verify Deployment

Test these endpoints:

```bash
# Replace with your actual URL
curl https://kasparro-api-XXXX.onrender.com/health
curl https://kasparro-api-XXXX.onrender.com/docs
curl https://kasparro-api-XXXX.onrender.com/data?symbol=BTC
```

### 6. Update README

Once you have the URL, update the README.md:
- Replace `https://kasparro-api.onrender.com` with your actual URL
- Commit and push the change

### 7. Submit to Kasparro

Fill out the form with your live deployment URL!

---

## Troubleshooting

**If deployment fails:**
- Check the **Logs** tab in Render dashboard
- Ensure environment variables are set (database credentials are auto-configured)
- Verify the build completed successfully

**If migrations fail:**
- Make sure you're in the web service shell, not the database shell
- Try running migrations one at a time
- Check that `$DATABASE_URL` is set: `echo $DATABASE_URL`

**If health check fails:**
- Wait 30-60 seconds for the service to fully start
- Check logs for errors
- Verify the service is running (not sleeping)
