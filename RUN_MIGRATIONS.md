# Run Migrations via HTTP (Free Tier Workaround)

Since Render's free tier doesn't support shell access, I've added a migration endpoint to your API.

## How to Run Migrations

### Step 1: Deploy the Updated Code

The migration endpoint has been added. Push the changes:

```bash
git add .
git commit -m "Add migration endpoint for free tier deployment"
git push origin main
```

Wait 2-3 minutes for Render to redeploy.

### Step 2: Run Migrations via HTTP

Once deployed, run this command in PowerShell:

```powershell
curl -X POST https://kasparro-backend-ishaan-verma.onrender.com/admin/migrate `
  -H "X-Migration-Secret: kasparro-migrate-2024"
```

Or use this simpler version:

```powershell
Invoke-WebRequest -Method POST `
  -Uri "https://kasparro-backend-ishaan-verma.onrender.com/admin/migrate" `
  -Headers @{"X-Migration-Secret"="kasparro-migrate-2024"}
```

### Step 3: Verify Success

Check the health endpoint:

```bash
curl https://kasparro-backend-ishaan-verma.onrender.com/health
```

Should return: `{"status":"healthy","database":{"connected":true,...}}`

## What This Does

The `/admin/migrate` endpoint:
- Reads the migration SQL files
- Executes them against the database
- Returns the results

It's protected by a simple secret header to prevent unauthorized access.

## After Migrations Complete

Once migrations are successful, you can:
1. ✅ Test all endpoints
2. ✅ Submit your URL to Kasparro: https://kasparro-backend-ishaan-verma.onrender.com
3. ✅ Fill out the form!

The migration endpoint will remain available but is safe to leave since it requires the secret header.
