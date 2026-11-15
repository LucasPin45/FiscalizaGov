# segredos.py - leitura tolerante dos secrets (Streamlit Cloud) e variaveis de ambiente
import os


def _tentar(secrets, *caminho):
    try:
        v = secrets
        for c in caminho:
            v = v[c]
        return str(v).strip()
    except Exception:
        return ""


def chave_transparencia(secrets=None, env=None) -> str:
    """TRANSPARENCIA_API_KEY na raiz dos Secrets, dentro de [auth] ou na variavel de ambiente.

    No TOML, tudo que vem depois de uma tabela como [auth] pertence a ela; por isso a chave
    colada logo abaixo de `senha = ...` fica em auth.TRANSPARENCIA_API_KEY.
    """
    env = os.environ if env is None else env
    return (_tentar(secrets, "TRANSPARENCIA_API_KEY")
            or _tentar(secrets, "auth", "TRANSPARENCIA_API_KEY")
            or str(env.get("TRANSPARENCIA_API_KEY", "")).strip())
