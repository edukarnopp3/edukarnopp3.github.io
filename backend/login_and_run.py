from __future__ import annotations

import argparse
import os
from pathlib import Path

import uvicorn


def main() -> int:
    parser = argparse.ArgumentParser(description="Inicia o backend local do dashboard ISEQ.")
    parser.add_argument(
        "--print-token",
        action="store_true",
        help="Opção antiga mantida por compatibilidade; o token não precisa mais ser copiado.",
    )
    args = parser.parse_args()

    os.environ.setdefault(
        "ISEQ_STORAGE_DIR",
        str(Path(__file__).resolve().parent / "storage"),
    )
    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))

    if args.print_token:
        print("O token não precisa mais ser copiado. O login agora acontece dentro do dashboard.")
    print(f"Backend local em http://{host}:{port}")
    print("Abra o dashboard e entre com sua conta ISEQ.")
    uvicorn.run("app.main:app", host=host, port=port)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
