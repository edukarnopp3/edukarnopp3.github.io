# Backend ISEQ

Backend FastAPI do dashboard de qualidade do ar. Cada usuário entra com a própria conta ISEQ; a senha é usada somente na requisição de login e descartada. O token recebido é criptografado antes de ser salvo.

## Dados persistidos

- usuários e conexões ISEQ;
- sessões do dashboard;
- ambientes/sensores de cada conta;
- histórico e progresso das importações;
- leituras normalizadas dos nove parâmetros.

As leituras usam uma linha por sensor e timestamp. Uma nova importação do mesmo período atualiza os registros existentes em vez de duplicá-los.
Todos os sensores usam o mesmo banco, mas os dados são isolados por usuário e `sensor_id`. Isso evita manter vários bancos gratuitos e permite comparar os sensores diretamente.

O backend também registra a cobertura de cada parâmetro por período. Ao pedir novamente um intervalo já importado, o dashboard lê o Supabase e consulta a ISEQ somente para as partes que ainda estiverem ausentes.
Cada parâmetro e intervalo concluído é salvo imediatamente. Se o Render reiniciar durante uma importação longa, uma nova tentativa reaproveita as etapas já persistidas. Os arquivos XLSX temporários são removidos depois da gravação para reduzir o uso de disco.

## Rodar localmente

```powershell
cd backend
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt
python login_and_run.py
```

Abra o dashboard e faça o login ISEQ dentro da própria página. No modo local, os dados são gravados em `backend/storage/airquality.db`.

## Banco Supabase no Render

1. Crie um projeto no Supabase.
2. No projeto, abra `Connect` e copie a URI do `Session pooler`.
3. Troque `[YOUR-PASSWORD]` na URI pela senha do banco.
4. No Render, abra o serviço `iseq-export-backend` e adicione `DATABASE_URL` com essa URI.
5. Adicione `APP_SECRET` com uma sequência aleatória de pelo menos 32 caracteres.
6. Remova `ISEQ_BEARER_TOKEN`; ele não é mais usado.
7. Salve e faça um novo deploy.

O backend cria as tabelas automaticamente na primeira inicialização. Nunca coloque `DATABASE_URL`, `APP_SECRET`, senhas ou tokens no GitHub.
As tabelas recebem Row Level Security sem políticas públicas; somente o backend conectado ao PostgreSQL acessa os registros.

## Variáveis

```text
DATABASE_URL=postgresql://postgres.PROJETO:SENHA@HOST.pooler.supabase.com:5432/postgres
APP_SECRET=VALOR_ALEATORIO_COM_32_OU_MAIS_CARACTERES
APP_SESSION_HOURS=12
CORS_ORIGINS=https://edukarnopp3.github.io
ISEQ_STORAGE_DIR=storage
ISEQ_JOB_WORKERS=3
ISEQ_DAILY_WORKERS=3
ISEQ_CHUNK_DAYS=1
CRON_SECRET=OUTRA_CHAVE_ALEATORIA_COM_32_OU_MAIS_CARACTERES
```

No Render, `DATABASE_URL` é obrigatória. Sem ela, o endpoint de saúde continua respondendo, mas o login mostra que o banco persistente ainda não foi configurado.

## Endpoints

- `POST /api/auth/iseq/login`
- `GET /api/auth/session`
- `POST /api/auth/logout`
- `GET /api/iseq/equipment`
- `POST /api/iseq/jobs`
- `GET /api/iseq/jobs/{id}`
- `GET /api/iseq/jobs/{id}/data`
- `GET /api/health`

Todos os endpoints ISEQ, exceto o login, exigem a sessão do dashboard no cabeçalho `Authorization`.

## Sincronizacao diaria

O workflow `.github/workflows/daily-iseq-sync.yml` executa todos os dias as
03:17 no fuso `America/Sao_Paulo`. Ele acorda o Render, importa o dia anterior
para todos os sensores de contas ISEQ conectadas e acompanha os trabalhos ate
o fim. Cada sensor usa tres requisicoes simultaneas, totalizando seis para os
dois sensores atuais.

Crie o mesmo segredo aleatorio, com pelo menos 32 caracteres, em dois lugares:

1. Render: variavel de ambiente `CRON_SECRET`.
2. GitHub: `Settings > Secrets and variables > Actions > New repository secret`,
   tambem com o nome `CRON_SECRET`.

O segredo nunca deve ser salvo diretamente no repositorio. Os endpoints
`/api/cron/*` exigem o cabecalho `X-Cron-Secret`.

## Testes

```powershell
cd backend
.\.venv\Scripts\python.exe -m unittest discover -s tests -v
```
