# Database Migration Instructions for Render.com

## Current Status
✅ **API Deployed**: https://kasparro-backend-ishaan-verma.onrender.com  
⚠️ **Database**: Not yet initialized (migrations need to be run)

## How to Run Migrations

### Option 1: Via Render Dashboard (Recommended)

1. **Go to Render Dashboard**
   - Open: https://dashboard.render.com
   - Find your `kasparro-api` web service

2. **Open Shell**
   - Click on the `kasparro-api` service
   - Click the **"Shell"** tab in the left sidebar
   - Wait for the shell to connect

3. **Run Migrations**
   Copy and paste these commands one at a time:

   ```bash
   # First, check database connection
   echo $DATABASE_URL
   
   # Run initial schema migration
   psql $DATABASE_URL < migrations/init.sql
   
   # Run entity resolution migration
   psql $DATABASE_URL < migrations/add_master_coins.sql
   ```

4. **Verify Success**
   You should see output like:
   ```
   CREATE TABLE
   CREATE TABLE
   CREATE INDEX
   INSERT 0 15
   ```

### Option 2: Via PostgreSQL Dashboard

1. Go to your PostgreSQL database in Render
2. Click **"Connect"** → **"External Connection"**
3. Use the provided connection string with `psql` locally:
   ```bash
   psql <your-connection-string> < migrations/init.sql
   psql <your-connection-string> < migrations/add_master_coins.sql
   ```

## Verify Migrations Worked

After running migrations, test these endpoints:

```bash
# Health check should show database connected
curl https://kasparro-backend-ishaan-verma.onrender.com/health

# Should return: {"status":"healthy","database":{"connected":true,...}}

# Check stats
curl https://kasparro-backend-ishaan-verma.onrender.com/stats

# View API docs
open https://kasparro-backend-ishaan-verma.onrender.com/docs
```

## Troubleshooting

**If migrations fail:**
- Make sure you're in the web service shell, not a local terminal
- Verify `$DATABASE_URL` is set: `echo $DATABASE_URL`
- Check that migration files exist: `ls migrations/`
- Try running each SQL command separately

**If database still shows unhealthy:**
- Wait 30-60 seconds for the service to restart
- Check the logs in Render dashboard
- Verify the PostgreSQL service is running

## Next Steps

Once migrations are complete:
1. ✅ Test all endpoints
2. ✅ Submit your deployment URL to Kasparro
3. ✅ Fill out the form with: https://kasparro-backend-ishaan-verma.onrender.com
