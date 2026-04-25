# Backend ISEQ

Backend FastAPI para gerar jobs de exportação ISEQ, juntar arquivos `.xlsx` por parâmetro e devolver linhas normalizadas para o painel.

## Rodar localmente

Modo assistido, sem copiar token manualmente:

```powershell
cd backend
python login_and_run.py
```

Na primeira vez, se aparecer aviso de Playwright ausente, instale com:

```powershell
pip install playwright
python -m playwright install chromium
```

Esse modo abre uma janela de login do ISEQ, espera voce entrar, captura o token localmente e sobe o backend em `http://127.0.0.1:8000`. O token nao e impresso nem salvo no repositorio.

Por padrao, o backend baixa 1 relatorio por vez e divide o periodo em blocos de 1 dia. Essa configuracao e mais lenta, mas evita os erros `504 Gateway Time-out` que aparecem em relatorios grandes, principalmente CO2.

Se o site estiver estavel, voce pode ajustar antes de iniciar:

```powershell
$env:ISEQ_JOB_WORKERS="4"
$env:ISEQ_CHUNK_DAYS="14"
python login_and_run.py
```

Use `ISEQ_JOB_WORKERS` entre 1 e 6. Quanto maior, mais rapido tende a ficar, mas tambem aumenta a chance de o site limitar ou demorar respostas. Se a API do ISEQ comecar a dar timeout, use `1`. Use `ISEQ_CHUNK_DAYS` maior, por exemplo `3` ou `7`, se os relatórios diarios estiverem estaveis.

Modo simples, sem instalar FastAPI, usando a API do ISEQ:

```powershell
cd backend
$env:ISEQ_BEARER_TOKEN="COLE_SEU_TOKEN_AQUI"
python dev_server.py
```

O token deve vir do login atual do ISEQ e nao deve ser salvo no GitHub. Se ele expirar, faca login de novo no ISEQ e atualize essa variavel.

Modo simples, sem instalar FastAPI, usando arquivos ja baixados:

```powershell
cd backend
python dev_server.py
```

Sem `ISEQ_BEARER_TOKEN`, esse modo usa automaticamente a pasta `Downloads` como fonte dos `.xlsx` ja exportados do ISEQ. Com o servidor aberto, use `http://127.0.0.1:8000` no painel.

Modo FastAPI, igual ao deploy:

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:ISEQ_EXPORT_DIR="C:\Users\eduardo\Downloads"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

Use `ISEQ_BEARER_TOKEN` para baixar pela API do ISEQ. Use `ISEQ_EXPORT_DIR` apenas para testar com arquivos ja exportados.

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
