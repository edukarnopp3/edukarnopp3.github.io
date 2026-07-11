from fastapi import FastAPI, Header, HTTPException
from pydantic import BaseModel


app = FastAPI()


class LoginPayload(BaseModel):
    usernameOrEmail: str
    password: str


def require_mock_token(authorization: str | None) -> None:
    if authorization != "Bearer mock-iseq-token":
        raise HTTPException(status_code=403, detail="Token inválido.")


@app.post("/login")
def login(payload: LoginPayload) -> dict[str, str]:
    if payload.usernameOrEmail != "demo@iseq.local" or payload.password != "demo-secret":
        raise HTTPException(status_code=401, detail="Credenciais inválidas.")
    return {"token": "mock-iseq-token"}


@app.get("/me")
def me(authorization: str | None = Header(default=None)) -> dict[str, object]:
    require_mock_token(authorization)
    return {
        "id": 9001,
        "username": "pesquisadora-demo",
        "email": "demo@iseq.local",
        "nome": "Pesquisadora Demo",
    }


@app.get("/equipment")
def equipment(authorization: str | None = Header(default=None)) -> list[dict[str, str]]:
    require_mock_token(authorization)
    return [
        {"mac": "AA:BB:CC:DD:EE:01", "ambiente": "Sala 01"},
        {"mac": "AA:BB:CC:DD:EE:02", "ambiente": "Sala 02"},
    ]
