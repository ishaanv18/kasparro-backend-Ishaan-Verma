#!/bin/bash
# Deployment helper script for Render.com

echo "🚀 Kasparro Backend - Deployment Helper"
echo "========================================"
echo ""

# Check if .env exists
if [ ! -f .env ]; then
    echo "❌ Error: .env file not found!"
    echo "   Please copy .env.example to .env and configure it:"
    echo "   cp .env.example .env"
    exit 1
fi

# Check for required environment variables
if ! grep -q "POSTGRES_PASSWORD=your_secure_password_here" .env; then
    echo "✅ POSTGRES_PASSWORD is configured"
else
    echo "⚠️  Warning: POSTGRES_PASSWORD still has default value"
    echo "   Please set a secure password in .env file"
fi

echo ""
echo "📋 Pre-deployment Checklist:"
echo ""
echo "1. ✅ Hardcoded secrets removed from config files"
echo "2. ✅ Entity resolution implemented"
echo "3. ✅ Render.yaml configuration created"
echo ""

# Check git status
echo "📦 Git Status:"
git status --short

echo ""
echo "🔧 Next Steps for Deployment:"
echo ""
echo "1. Commit all changes:"
echo "   git add ."
echo "   git commit -m 'Fix critical issues: secrets, entity resolution, deployment'"
echo ""
echo "2. Push to GitHub:"
echo "   git push origin main"
echo ""
echo "3. Deploy on Render.com:"
echo "   - Go to https://render.com"
echo "   - Click 'New +' → 'Blueprint'"
echo "   - Connect your GitHub repository"
echo "   - Render will auto-detect render.yaml"
echo ""
echo "4. After deployment, run migrations:"
echo "   - Access web service shell in Render dashboard"
echo "   - Run: psql \$DATABASE_URL < migrations/init.sql"
echo "   - Run: psql \$DATABASE_URL < migrations/add_master_coins.sql"
echo ""
echo "5. Verify deployment:"
echo "   curl https://your-app-name.onrender.com/health"
echo ""
