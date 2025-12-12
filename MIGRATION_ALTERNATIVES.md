# Alternative Migration Methods (Free Tier)

Since Render's free tier doesn't support shell access, here are alternative methods to run your database migrations:

## Method 1: Use PostgreSQL Client Locally (Recommended)

### Step 1: Get Database Connection String

1. Go to Render Dashboard: https://dashboard.render.com
2. Click on your **PostgreSQL database** (not the web service)
3. Scroll down to **"Connections"** section
4. Copy the **"External Database URL"**
   - It looks like: `postgres://username:password@hostname/database`

### Step 2: Run Migrations from Your Computer

Open PowerShell and run:

```powershell
# Set the connection string (replace with your actual URL)
$env:DATABASE_URL = "your-postgres-connection-url-here"

# Run migrations
psql $env:DATABASE_URL -f migrations/init.sql
psql $env:DATABASE_URL -f migrations/add_master_coins.sql
```

**Don't have psql installed?** Install PostgreSQL client:
- Download from: https://www.postgresql.org/download/windows/
- Or use: `winget install PostgreSQL.PostgreSQL`

---

## Method 2: Add Migration Endpoint to API (Quick Fix)

I can add a special endpoint to your API that runs migrations when you call it. This is a temporary solution just to get your deployment working.

Would you like me to add this endpoint? It would be:
- `POST /admin/migrate` - Runs all migrations
- Protected by a simple secret key

---

## Method 3: Use Render's PostgreSQL Dashboard

1. Go to your PostgreSQL database in Render
2. Click **"Connect"** 
3. Use the web-based SQL editor to paste and run the migration SQL directly

---

## Recommended Approach

**I recommend Method 2** - let me add a migration endpoint to your API. This is the fastest way to get your deployment working right now.

Should I add the migration endpoint?
