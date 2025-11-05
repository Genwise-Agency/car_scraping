# Azure Functions CLI Deployment Guide

## Prerequisites

- Azure CLI installed and logged in
- Azure Functions Core Tools installed

## Quick Deployment Steps

### 1. Login to Azure (if not already logged in)

```bash
az login
```

### 2. Set your Function App details

```bash
# Replace with your actual values
FUNCTION_APP_NAME="car-scraping-function"
RESOURCE_GROUP="personal-rg"
```

### 3. Deploy the Function Code

```bash
cd AzureFunctionApp
func azure functionapp publish car-scraping-function --python
cd ..
```

### 4. Set Environment Variables

```bash
az functionapp config appsettings set \
  --name car-scraping-function \
  --resource-group personal-rg \
  --settings \
    SUPABASE_URL="your-supabase-url" \
    SUPABASE_KEY="your-supabase-key" \
    SYNC_DB="true" \
    TEST_LIMIT="0" \
    BMW_URL="your-bmw-url"
```

**Note:** `SCM_DO_BUILD_DURING_DEPLOYMENT` and `ENABLE_ORYX_BUILD` are only available on higher-tier Function App plans (Premium/App Service plans). They are not needed for Playwright installation.

### 5. Configure Playwright Browser Installation (Startup Command)

Set the startup command to automatically install Playwright browsers when the function app starts:

```bash
az functionapp config set \
  --name car-scraping-function \
  --resource-group personal-rg \
  --startup-file "bash startup.sh"
```

Alternatively, you can set it directly as a startup command:

```bash
az functionapp config appsettings set \
  --name car-scraping-function \
  --resource-group personal-rg \
  --settings \
    SCM_COMMAND_IDLE_TIMEOUT="1800" \
    WEBSITE_USE_PLACEHOLDER="0"
```

Then set the startup command:

```bash
az functionapp config set \
  --name car-scraping-function \
  --resource-group personal-rg \
  --startup-file "python -m playwright install chromium --with-deps || true"
```

**Note:** The startup script (`startup.sh`) is included in the deployment and will automatically install Playwright browsers on each function app start/restart.

### 6. Test the Function

```bash
# Get function URL
FUNCTION_URL=$(az functionapp show --name car-scraping-function --resource-group personal-rg --query defaultHostName -o tsv)

# Test with a small limit
curl "https://$FUNCTION_URL/api/BmwScrapingFunction?test_limit=5&sync_db=true"
```

## Alternative: Use the Deployment Script

Run the automated deployment script:

```bash
./deploy.sh
```

This script will:

- Check Azure login
- Verify/create resource group
- Verify/create Function App
- Deploy the code
- Provide next steps for environment variables
