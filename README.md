# TabularExtract

Universal PDF table extractor. Extracts clean, Excel-ready tables from any PDF.

## Local Setup

```bash
cp .env.example .env
# Fill in ANTHROPIC_API_KEY and STRIPE_SECRET_KEY in .env

docker build -t tabularextract .
docker run -p 8000:8000 --env-file .env tabularextract
