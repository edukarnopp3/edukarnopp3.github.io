# Monitor de Qualidade do Ar

Dashboard do TCC para importar e analisar os nove parâmetros dos sensores ISEQ, comparar dois equipamentos instalados no mesmo ambiente e avaliar sua concordância estatística.

## Site

O frontend é publicado pelo GitHub Pages e usa o backend hospedado no Render. A página inicial redireciona para `index_completo_corrigido.html`.

Cada pessoa entra com a própria conta ISEQ. A senha não é armazenada; o backend guarda somente o token criptografado e separa sensores, importações e leituras por usuário.

## Fontes de dados

- importação automática dos ambientes vinculados à conta ISEQ;
- comparação simultânea entre dois sensores;
- planilhas antigas com aba `Dados brutos`;
- exportações ISEQ com aba `Dados` no formato longo.

## Desenvolvimento local

Inicie o backend:

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python login_and_run.py
```

Em outro terminal, sirva o frontend:

```powershell
python -m http.server 8765 --bind 127.0.0.1
```

Abra `http://127.0.0.1:8765/`.

As instruções do banco e do deploy estão em `backend/README.md`.
