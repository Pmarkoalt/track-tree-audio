#!/bin/bash

# Setup script for Track Tree Audio Service (Development)
echo "🎵 Setting up Track Tree Audio Service for development..."

# Create .env file if it doesn't exist
if [ ! -f .env ]; then
    echo "Creating .env file..."
    cat > .env << EOF
# Development Environment for Track Tree Audio Service

# Server Configuration
PORT=8080

# Security (use mock token for development)
DEMUCSSVC_TOKEN=mock-token-for-development

# Webhook Configuration
API_WEBHOOK_URL_ALLOWLIST=http://localhost:4000/api/webhooks/demucs,https://api.track-tree.com/webhooks/demucs

# S3/R2 Configuration (optional for development)
S3_ENDPOINT=http://localhost:9000
S3_REGION=us-east-1
S3_BUCKET=track-tree-audio
S3_ACCESS_KEY_ID=tracktree
S3_SECRET_ACCESS_KEY=tracktree123

# Redis Configuration (optional for development)
REDIS_URL=redis://localhost:6379/0

# GPU Configuration (optional for development)
CUDA_VISIBLE_DEVICES=0
EOF
    echo "✅ Created .env file"
    echo "⚠️  Using mock token for development - no real GPU processing"
else
    echo "✅ .env file already exists"
fi

# Install Python dependencies
echo "Installing Python dependencies..."
if command -v pip &> /dev/null; then
    pip install -r requirements.txt
    echo "✅ Dependencies installed"
else
    echo "❌ pip not found. Please install Python dependencies manually:"
    echo "   pip install -r requirements.txt"
fi

echo "✅ Audio service setup complete!"
echo ""
echo "To start the audio service:"
echo "  python -m uvicorn src.main:app --reload --port 8080"
echo ""
echo "The service will be available at:"
echo "  - Health check: http://localhost:8080/healthz"
echo "  - API docs: http://localhost:8080/docs"
echo "  - Split endpoint: http://localhost:8080/split"
echo ""
echo "Note: This is using mock processing for development."
echo "Real Demucs GPU processing requires proper CUDA setup."
