from __future__ import annotations

import os
from pathlib import Path
import sys
import time


LOGIN_URL = "https://sensores.iseq.com.br/login"


def main() -> int:
    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        print("Playwright nao esta instalado.")
        print("Instale uma vez com:")
        print("  pip install playwright")
        print("  python -m playwright install chromium")
        return 1

    print("Abrindo janela de login do ISEQ...")
    print("Faca login normalmente. O token sera usado so nesta execucao e nao sera impresso.")

    with sync_playwright() as playwright:
        user_data_dir = Path(__file__).resolve().parent / ".browser-profile"
        browser_type = playwright.chromium
        context = browser_type.launch_persistent_context(
            str(user_data_dir),
            headless=False,
            channel="chrome",
            viewport={"width": 1366, "height": 900},
        )
        page = context.pages[0] if context.pages else context.new_page()
        page.goto(LOGIN_URL)

        token = wait_for_token(page)
        context.close()

    if not token:
        print("Nao encontrei token. Tente fazer login novamente e aguardar o dashboard abrir.")
        return 1

    os.environ["ISEQ_BEARER_TOKEN"] = token
    os.environ.setdefault("ISEQ_STORAGE_DIR", str(Path(__file__).resolve().parent / "storage"))

    from dev_server import Handler
    from http.server import ThreadingHTTPServer

    host = os.getenv("HOST", "127.0.0.1")
    port = int(os.getenv("PORT", "8000"))
    print(f"Login detectado. Backend local em http://{host}:{port}")
    print("Deixe esta janela aberta enquanto usa o painel.")
    ThreadingHTTPServer((host, port), Handler).serve_forever()
    return 0


def wait_for_token(page) -> str | None:
    deadline = time.time() + 300
    last_url = ""
    while time.time() < deadline:
        try:
            current_url = page.url
            if current_url != last_url:
                last_url = current_url
                print(f"Pagina atual: {current_url}")
            token = page.evaluate("() => localStorage.getItem('token')")
            if token:
                return token
        except Exception:
            pass
        time.sleep(1)
    return None


if __name__ == "__main__":
    raise SystemExit(main())
