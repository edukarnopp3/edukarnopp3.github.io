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

O backend fica em `backend/` e expõe jobs para buscar dados por intervalo. Enquanto a automação autenticada do site não estiver plugada, ele pode ser testado com arquivos `.xlsx` já exportados:

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
$env:ISEQ_EXPORT_DIR="C:\Users\eduardo\Downloads"
uvicorn app.main:app --reload --host 127.0.0.1 --port 8000
```

No painel, configure a URL do backend como `http://127.0.0.1:8000`.
