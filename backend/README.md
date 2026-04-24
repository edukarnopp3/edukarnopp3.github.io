# Backend ISEQ

Backend FastAPI para gerar jobs de exportação ISEQ, juntar arquivos `.xlsx` por parâmetro e devolver linhas normalizadas para o painel.

## Rodar localmente

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:ISEQ_EXPORT_DIR="C:\Users\eduardo\Downloads"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Com `ISEQ_EXPORT_DIR`, o backend usa arquivos já exportados do ISEQ para testar a API. A automação autenticada real do site deve ser implementada em `app/collector.py`, substituindo o coletor local.

## Endpoints

- `POST /api/iseq/jobs`
- `GET /api/iseq/jobs/{id}`
- `GET /api/iseq/jobs/{id}/data`
- `GET /api/health`

Payload para criar job:

```json
{
  "equipment_id": "1C:69:20:C7:31:D8",
  "start": "2026-03-01T00:00:00",
  "end": "2026-03-31T23:59:59"
}
```
