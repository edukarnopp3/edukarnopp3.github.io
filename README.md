# Monitor de Qualidade do Ar

Painel estático para análise de dados dos sensores ISEQ, com upload manual de Excel e integração preparada para um backend FastAPI.

## Publicação no GitHub Pages

Use este diretório como repositório GitHub e publique a branch principal pelo GitHub Pages. O arquivo `index.html` redireciona para `index_completo_corrigido.html`.

## Uso local do painel

Abra `index_completo_corrigido.html` no navegador. O upload aceita:

- planilhas antigas com aba `Dados brutos` e coluna `data_local`;
- exportações ISEQ com aba `Dados` no formato longo (`Timestamp (Local)`, `Parâmetro solicitado`, `Valor`);
- vários arquivos ISEQ ao mesmo tempo, um por parâmetro.

## Backend ISEQ

O backend fica em `backend/` e expõe jobs para buscar dados por intervalo. Para iniciar com login assistido, sem copiar token manualmente:

```powershell
cd backend
python login_and_run.py
```

Na primeira vez, se aparecer aviso de Playwright ausente, instale com:

```powershell
pip install playwright
python -m playwright install chromium
```

Esse modo abre o ISEQ, espera voce fazer login e inicia o backend em `http://127.0.0.1:8000` sem imprimir o token.

Para testar localmente sem instalar FastAPI e usando token manual:

```powershell
cd backend
$env:ISEQ_BEARER_TOKEN="COLE_SEU_TOKEN_AQUI"
python dev_server.py
```

Nao salve esse token no GitHub. Se ele expirar, faca login novamente no ISEQ e atualize a variavel.

Para testar com arquivos `.xlsx` ja exportados:

```powershell
cd backend
python dev_server.py
```

Abra o painel e mantenha a URL do backend como `http://127.0.0.1:8000`.

Para rodar com FastAPI, igual ao deploy:

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:ISEQ_EXPORT_DIR="C:\Users\eduardo\Downloads"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

No painel, configure a URL do backend como `http://127.0.0.1:8000`.
