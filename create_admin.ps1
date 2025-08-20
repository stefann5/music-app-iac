$USER_POOL_ID = "eu-central-1_HjtzoewwV"
$TABLE_NAME = "MusicApp-Users"
$ADMIN_EMAIL = "admin@yourdomain.com"
$ADMIN_PASSWORD = "AdminPassword123!"
$ADMIN_USER_ID = "admin-$([DateTimeOffset]::UtcNow.ToUnixTimeSeconds())"
$CURRENT_DATE = (Get-Date).ToUniversalTime().ToString("yyyy-MM-ddTHH:mm:ss.fffZ")

Write-Host "Creating admin user..." -ForegroundColor Green

# Step 1: Create in Cognito
Write-Host "Step 1: Creating user in Cognito..." -ForegroundColor Yellow
aws cognito-idp admin-create-user `
    --user-pool-id $USER_POOL_ID `
    --username admin `
    --user-attributes "Name=email,Value=$ADMIN_EMAIL" "Name=given_name,Value=Admin" "Name=family_name,Value=User" "Name=birthdate,Value=1990-01-01" "Name=preferred_username,Value=admin" "Name=custom:role,Value=admin" "Name=custom:subscription_type,Value=premium" `
    --temporary-password TempAdmin123! `
    --message-action SUPPRESS

# Step 2: Set permanent password
Write-Host "Step 2: Setting permanent password..." -ForegroundColor Yellow
aws cognito-idp admin-set-user-password `
    --user-pool-id $USER_POOL_ID `
    --username admin `
    --password $ADMIN_PASSWORD `
    --permanent

# Step 3: Add to group
Write-Host "Step 3: Adding to administrators group..." -ForegroundColor Yellow
aws cognito-idp admin-add-user-to-group `
    --user-pool-id $USER_POOL_ID `
    --username admin `
    --group-name administrators

# Step 4: Add to DynamoDB using temporary JSON file
# Step 4: Add to DynamoDB - FIXED (No BOM)
Write-Host "Step 4: Adding to DynamoDB..." -ForegroundColor Yellow

$jsonContent = @"
{
    "userId": {"S": "$ADMIN_USER_ID"},
    "cognitoUserId": {"S": "admin"},
    "username": {"S": "admin"},
    "email": {"S": "$ADMIN_EMAIL"},
    "firstName": {"S": "Admin"},
    "lastName": {"S": "User"},
    "dateOfBirth": {"S": "1990-01-01"},
    "role": {"S": "admin"},
    "subscriptionType": {"S": "premium"},
    "status": {"S": "active"},
    "createdAt": {"S": "$CURRENT_DATE"},
    "lastLogin": {"NULL": true}
}
"@

$tempJsonFile = "admin_user_temp.json"

# FIX: Use System.IO.File instead of Out-File to avoid BOM
[System.IO.File]::WriteAllText($tempJsonFile, $jsonContent)

# Use the JSON file
aws dynamodb put-item --table-name $TABLE_NAME --item "file://$tempJsonFile"

# Clean up
Remove-Item $tempJsonFile

Write-Host ""
Write-Host "🎉 Admin user created successfully!" -ForegroundColor Green
Write-Host "Username: admin" -ForegroundColor Cyan
Write-Host "Password: $ADMIN_PASSWORD" -ForegroundColor Cyan
Write-Host "Email: $ADMIN_EMAIL" -ForegroundColor Cyan