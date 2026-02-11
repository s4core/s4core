# Console Setup

The S4 Console is a Next.js application located at `frontend/s4-console/` in the repository.

## Quick Start with Docker Compose

The easiest way to run the console alongside S4:

```bash
S4_ROOT_PASSWORD=password12345 docker compose up --build
```

- **S4 API**: http://localhost:9000
- **Web Console**: http://localhost:3000

Login with `root` / `password12345`.

## Development Setup

### Prerequisites

- Node.js 18+
- npm or yarn

### Install Dependencies

```bash
cd frontend/s4-console
npm install
```

### Configure Backend URL

The console connects to the S4 backend API. By default, it proxies API requests to `http://localhost:9000` via the Next.js config.

For custom backend URLs:

```bash
export S4_BACKEND_URL=http://your-s4-server:9000
```

### Start Development Server

```bash
npm run dev
```

Open http://localhost:3000 in your browser.

### Build for Production

```bash
npm run build
npm start
```

## Docker

Build the console image separately:

```bash
cd frontend/s4-console
docker build -t s4-console .
```

Run with a backend URL:

```bash
docker run -d \
  --name s4-console \
  -p 3000:3000 \
  -e S4_BACKEND_URL=http://s4-server:9000 \
  s4-console
```

## Requirements

The console requires IAM to be enabled on the S4 server (`S4_ROOT_PASSWORD` must be set). Without IAM, the login endpoint is unavailable and the console cannot authenticate.
