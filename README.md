# Discord Token Host - Teste

Painel Python simples para testar hospedagem de bots Discord via token.

## Funções atuais

- Login básico do painel
- Adicionar bot por token
- Salvar token criptografado no MongoDB
- Ligar bot
- Desligar bot
- Ver status e último erro
- Excluir bot quando estiver offline

## Variáveis de ambiente

Copie `.env.example` para `.env` se for testar localmente.
No Railway, coloque essas variáveis no painel do projeto.

```env
MONGO_URI=mongodb+srv://...
DATABASE_NAME=discord_host_test
ADMIN_USER=seijin
ADMIN_PASSWORD=senha_forte
FERNET_KEY=chave_gerada
AUTO_RESTART_ON_BOOT=false
PORT=8000
```

Gere a chave:

```bash
python generate_key.py
```

## Rodar localmente

```bash
pip install -r requirements.txt
uvicorn main:app --host 0.0.0.0 --port 8000
```

Abra:

```txt
http://127.0.0.1:8000
```

## Railway

Start command:

```bash
uvicorn main:app --host 0.0.0.0 --port $PORT
```

## Aviso

Não use tokens principais ainda. Essa base é para teste estrutural.
Para produção real, o ideal é isolar cada bot em processo/container próprio.
